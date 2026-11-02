"""SIGTERM en procesos en reposo y durante una entrega pendiente de confirmación."""

from dataclasses import replace
import multiprocessing
import os
from pathlib import Path
import signal
from queue import Empty
from time import monotonic

from src.middleware.topologia import TopologiaGrupos
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import ConteoPalabra, Esquema, Lote
from tests.soporte import EscenarioRabbitMQ
from .soporte import ejecutar_worker, entrada_prueba
from .http_q2 import IMAGEN, servir_imagen

PLAZO_APAGADO_SEGUNDOS = 5


def apagar_conjunto(caso, procesos, plazo=PLAZO_APAGADO_SEGUNDOS):
    """Envía a todos primero y exige limpieza y salida dentro de un plazo compartido."""
    limite = monotonic() + plazo
    for proceso, _, continuar in procesos:
        os.kill(proceso.pid, signal.SIGTERM)
        if continuar is not None:
            os.kill(proceso.pid, signal.SIGTERM)
    for _, _, continuar in procesos:
        if continuar is not None:
            continuar.set()
    for proceso, _, _ in procesos:
        proceso.join(max(0, limite - monotonic()))
    pendientes = [proceso for proceso, _, _ in procesos if proceso.is_alive()]
    for proceso in pendientes:
        proceso.kill()
    for proceso in pendientes:
        proceso.join(PLAZO_APAGADO_SEGUNDOS)
    caso.assertFalse(pendientes, f'Se excedieron {plazo}s; se envió SIGKILL')
    for proceso, eventos, _ in procesos:
        caso.assertEqual(proceso.exitcode, 0, 'El worker no terminó mediante su limpieza normal')
        # El hijo une el hilo de la cola antes de salir: el aviso ya debe estar disponible.
        try:
            aviso = eventos.get_nowait()
        except Empty:
            caso.fail('El worker terminó sin verificar la liberación de recursos')
        caso.assertEqual(aviso, 'limpio')


class EscenariosWorker:
    """Casos comunes ejecutados en un virtual host nuevo para cada worker y estado."""

    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'gateway'
    plazo_apagado = PLAZO_APAGADO_SEGUNDOS

    def apagar_en_plazo(self, proceso, eventos, continuar=None):
        apagar_conjunto(self, [(proceso, eventos, continuar)], self.plazo_apagado)

    def comprobar_apagado(self, modo):
        continuar = multiprocessing.get_context('spawn').Event()
        self.addCleanup(continuar.set)
        fuente, datos = entrada_prueba(self.grupo)
        if modo != 'inactivo':
            datos = replace(datos, contenido=replace(datos.contenido, registros=datos.contenido.registros * 4))
            with self.abrir(fuente, informar_error=self.fail) as productor:
                productor.publicar(self.grupo, datos)
        proceso, eventos = self.lanzar(ejecutar_worker, self.grupo, modo, continuar)
        esperado = {'inactivo': 'esperando', 'activo': 'procesado_sin_ack'}.get(modo, 'computando')
        self.assertEqual(self.esperar(eventos), esperado)
        self.apagar_en_plazo(proceso, eventos, continuar if modo != 'inactivo' else None)
        # Sin EOF de entrada, ningún worker puede fabricar un cierre exitoso.
        for grupo in self.configuracion.grupos:
            topologia = TopologiaGrupos(self.configurar(grupo.nombre))
            colas = topologia.colas_recepcion()
            if colas is not None:
                self.assertEqual(self.estado(colas.control).message_count, 0)
        for cola in self.topologia.colas_datos():
            self.assertEqual(self.estado(cola).consumer_count, 0)
            self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).consumer_count, 0)
        if modo != 'inactivo':
            destino = TopologiaGrupos(self.configurar(fuente)).destino_datos(self.grupo, datos.entrada.nombre)
            self.assertEqual(self.estado(destino.cola).message_count, 1, 'La entrega pendiente fue confirmada')
        else:
            self.comprobar_capacidad_recuperada()

    def test_sigterm_inactivo(self):
        """Despierta la recepción sin datos y cierra recursos sin terminar por la señal por defecto."""
        self.comprobar_apagado('inactivo')

    def test_sigterm_con_entrega_pendiente(self):
        """Interrumpe después de procesar, conserva la entrega sin ACK y limpia estado propio."""
        self.comprobar_apagado('activo')

    def test_sigterm_durante_el_lote(self):
        """Abandona el lote dentro del procesamiento, sin llegar al final del callback."""
        modo = 'serializacion' if self.grupo == 'q1_formatter' else 'computo'
        self.comprobar_apagado(modo)


class Router(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'date_router'

    def test_sigterm_durante_decodificacion(self):
        self.comprobar_apagado('decodificacion')


class FiltroIdioma(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q1_filter_lang'


class Formateador(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q1_formatter'


class Ocurrencias(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'word_occurrences'

    def test_sigterm_dentro_de_un_abstract(self):
        self.comprobar_apagado('tokenizacion')


class Frecuencia(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'frecuencias'

    def test_sigterm_finalizador_sin_capacidad(self):
        """Interrumpe el vaciado del parcial: no publica datos extra ni EOF hacia Top N."""
        with self.abrir('frecuencias', informar_error=self.fail) as productor:
            productor.publicar('top_n', Datos(Entrada('previa', 'conteos'), Lote(
                Esquema.CONTEO_PALABRA, (ConteoPalabra('ocupada', 1),),
            )), clave_afinidad='cliente')
        # La afinidad incluye la ejecución; con una sola réplica la cola es la misma.
        fuente, datos = entrada_prueba(self.grupo)
        with self.abrir(fuente, informar_error=self.fail) as productor:
            productor.publicar(self.grupo, datos)
            productor.finalizar_entrada(self.grupo, datos.entrada)
        continuar = multiprocessing.get_context('spawn').Event()
        proceso, eventos = self.lanzar(ejecutar_worker, self.grupo, 'capacidad', continuar)
        self.assertEqual(self.esperar(eventos), 'sin_capacidad')
        self.apagar_en_plazo(proceso, eventos)
        destino = TopologiaGrupos(self.configurar('frecuencias')).destino_datos('top_n', 'conteos', 0)
        self.assertEqual(self.estado(destino.cola).message_count, 1)
        colas = TopologiaGrupos(self.configurar('top_n')).colas_recepcion()
        self.assertEqual(self.estado(colas.control).message_count, 0)
        self.assertEqual(self.estado(self.topologia.cola_capacidad(destino.cola)).consumer_count, 0)


class TopN(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'top_n'

    def test_sigterm_durante_ordenacion(self):
        self.comprobar_apagado('ordenacion')


class Promedio(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q5_average'


class Comparador(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q5_comparator'

    def test_sigterm_durante_escritura(self):
        self.comprobar_apagado('almacenamiento')


class ConjuntoReplicas(EscenarioRabbitMQ):
    """Apagado de réplicas activas e inactivas con ejecuciones independientes."""

    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'gateway'

    def configurar(self, grupo=None, instancia=0):
        base = super().configurar(grupo, instancia)
        cantidades = {'frecuencias': 3, 'top_n': 2}
        return replace(
            base, grupos=tuple(replace(g, instancias=cantidades.get(g.nombre, g.instancias))
                               for g in base.grupos),
            limites=replace(base.limites, max_mensajes_por_cola=3),
        )

    def test_apagado_conjunto_de_replicas_y_ejecuciones(self):
        """Tres frecuencias, dos Top N y un comparador deben salir dentro de los mismos 5s."""
        with self.abrir('word_occurrences', informar_error=self.fail) as productor:
            for numero in range(3):
                _, datos = entrada_prueba('frecuencias')
                datos = replace(datos, entrada=Entrada(f'cliente-{numero}', 'palabras'),
                                contenido=replace(datos.contenido, registros=datos.contenido.registros * 4))
                productor.publicar('frecuencias', datos)
            _, datos = entrada_prueba('q5_comparator')
            productor.publicar('q5_comparator', datos)
        procesos = []
        participantes = [('frecuencias', n, 'computo') for n in range(3)]
        participantes += [('top_n', n, 'inactivo') for n in range(2)]
        participantes.append(('q5_comparator', 0, 'activo'))
        for grupo, instancia, modo in participantes:
            continuar = multiprocessing.get_context('spawn').Event()
            self.addCleanup(continuar.set)
            proceso, eventos = self.lanzar(ejecutar_worker, grupo, modo, continuar, instancia=instancia)
            esperado = {'computo': 'computando', 'inactivo': 'esperando', 'activo': 'procesado_sin_ack'}[modo]
            self.assertEqual(self.esperar(eventos), esperado)
            procesos.append((proceso, eventos, continuar if modo != 'inactivo' else None))
        apagar_conjunto(self, procesos)
        for grupo in self.configuracion.grupos:
            for instancia in range(grupo.instancias):
                colas = TopologiaGrupos(self.configurar(grupo.nombre, instancia)).colas_recepcion()
                if colas is not None:
                    self.assertEqual(self.estado(colas.control).message_count, 0)
        for cola in self.topologia.colas_datos():
            self.assertEqual(self.estado(cola).consumer_count, 0)
            self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).consumer_count, 0)


class FiltroBiografico(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q2_filter_bio'

    def test_sigterm_dentro_de_las_secciones(self):
        self.comprobar_apagado('secciones')


class Descargador(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q2_image_downloader'
    plazo_apagado = 30

    def comprobar_descarga(self, modo):
        liberar = multiprocessing.get_context('spawn').Event()
        servidor, avisos = self.lanzar(servir_imagen, 'q2_image_downloader', modo, liberar)
        self.addCleanup(liberar.set)
        puerto = self.esperar(avisos)
        fuente, datos = entrada_prueba(self.grupo)
        solicitud = replace(datos.contenido.registros[0], url_imagen=f'http://127.0.0.1:{puerto}/imagen')
        datos = replace(datos, contenido=replace(datos.contenido, registros=(solicitud,)))
        with self.abrir(fuente, informar_error=self.fail) as productor:
            productor.publicar(self.grupo, datos)
        continuar = multiprocessing.get_context('spawn').Event()
        self.addCleanup(continuar.set)
        observacion = modo if modo in ('descarga', 'fragmentos') else f'http_{modo}'
        proceso, eventos = self.lanzar(ejecutar_worker, self.grupo, observacion, continuar)
        self.assertEqual(self.esperar(avisos), 'peticion')
        esperados = {'cuerpo': 'leyendo', 'reintento': 'reintentando',
                     'completo': 'procesado_sin_ack', 'descarga': 'computando', 'fragmentos': 'computando'}
        if modo in esperados:
            self.assertEqual(self.esperar(eventos), esperados[modo])
        barrera = continuar if modo in ('completo', 'descarga', 'fragmentos') else None
        self.apagar_en_plazo(proceso, eventos, barrera)
        liberar.set()
        self.finalizar(servidor)
        salida = TopologiaGrupos(self.configurar(self.grupo)).destino_datos(
            'gateway_resultados', 'resultados_finales_q2',
        )
        self.assertEqual(self.estado(salida.cola).message_count, int(modo == 'completo'))
        if modo == 'completo':
            with self.abrir('gateway_resultados', informar_error=self.fail) as receptor:
                recibidos = self.recibir(receptor).contenido.registros
            self.assertEqual(b''.join(r.fragmento for r in recibidos), IMAGEN)
            self.assertTrue(all(r.total_bytes == len(IMAGEN) for r in recibidos))
        colas = TopologiaGrupos(self.configurar('gateway_resultados')).colas_recepcion()
        self.assertEqual(self.estado(colas.control).message_count, 0)
        entrada = TopologiaGrupos(self.configurar(fuente)).destino_datos(self.grupo, datos.entrada.nombre)
        self.assertEqual(self.estado(entrada.cola).message_count, 1)

    def test_sigterm_esperando_cabeceras_http(self):
        self.comprobar_descarga('cabeceras')

    def test_sigterm_esperando_cuerpo_http(self):
        self.comprobar_descarga('cuerpo')

    def test_sigterm_durante_descarga_http(self):
        self.comprobar_descarga('descarga')

    def test_sigterm_durante_fragmentacion(self):
        self.comprobar_descarga('fragmentos')

    def test_sigterm_despierta_reintento_http(self):
        self.comprobar_descarga('reintento')

    def test_descarga_completa_y_sigterm_antes_del_ack(self):
        self.comprobar_descarga('completo')
