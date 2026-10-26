"""Escenarios de cierre distribuido, dependencias y acumulados con RabbitMQ real."""

from collections import Counter
import multiprocessing
from pathlib import Path
from queue import Empty

import pika

from src.common.protocol.dto import (
    CierreProductor, Datos, Entrada, EnvioDatos, IdentidadProceso, SobreMensaje, TipoMensaje,
)
from src.common.protocol.registros import ConteoPalabra, Esquema, Lote
from src.middleware.capacidad import CABECERA_CAPACIDAD, VERSION_CAPACIDAD, CapacidadDatos
from src.middleware.codificacion import CodificadorMensajes
from src.middleware.interfaz import EntradaFinalizada, ErrorSeguimientoEntrada
from src.middleware.topologia import TopologiaGrupos
from tests.soporte import EscenarioRabbitMQ, PLAZO_BLOQUEO
from .soporte import consumir, palabras, producir


class EscenariosEOF(EscenarioRabbitMQ):
    """Comprueba el cierre por ejecución y entrada sin simular conexiones ni ACK."""

    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'productores'

    def abrir(self, grupo, instancia=0, **opciones):
        opciones.setdefault('informar_error', self.fail)
        return super().abrir(grupo, instancia, **opciones)

    def iniciar_consumidores(self, grupo, entradas, **opciones):
        consumidores = []
        for instancia in range(self.configuracion.obtener_grupo(grupo).instancias):
            proceso, eventos = self.lanzar(
                consumir, grupo, entradas, opciones.get('inicio'), opciones.get('trabajo'),
                opciones.get('observar', False), opciones.get('acumular', False),
                opciones.get('usar_evento_fin', False), instancia=instancia,
            )
            self.assertEqual(self.esperar(eventos), ('listo',))
            consumidores.append((proceso, eventos))
        return consumidores

    def esperar_tipo(self, eventos, tipo):
        while True:
            evento = self.esperar(eventos)
            if evento == tipo or evento[0] == tipo:
                return evento
            self.assertIn(evento[0], ('avance', 'fin', 'limpieza_pendiente'))

    def resultados(self, consumidores, entradas):
        datos = []
        for proceso, eventos in consumidores:
            _, recibidos, fines = self.esperar_tipo(eventos, 'resultado')
            self.assertEqual(fines, set(entradas))
            datos.extend(recibidos)
            self.finalizar(proceso)
        return datos

    def cerrar_productores(self, productores, grupo, entradas):
        for entrada in entradas:
            for productor in productores:
                productor.finalizar_entrada(grupo, entrada)

    def inyectar_control(self, destino, sobre):
        codificador = CodificadorMensajes(self.configuracion.limites.max_bytes_por_mensaje)
        self.inspector.canal.confirm_delivery()
        self.inspector.canal.basic_publish(
            exchange=destino.exchange, routing_key=destino.clave,
            body=codificador.codificar(sobre), mandatory=True,
        )

    def test_productores_concurrentes_clientes_intercalados_y_entrada_vacia(self):
        """Dos productores aportan a tres réplicas; cada cliente conserva todos sus lotes."""
        entradas = tuple(Entrada(cliente, 'palabras') for cliente in ('A', 'B', 'vacio'))
        consumidores = self.iniciar_consumidores('consumidores', entradas, usar_evento_fin=True)
        fuentes, esperados = [], []
        for instancia in range(2):
            envios = tuple(
                EnvioDatos('consumidores', palabras(cliente, f'{instancia}-{numero}', 'été'))
                for numero in range(3) for cliente in ('A', 'B')
            )
            esperados.extend(envio.datos for envio in envios)
            fuentes.append(self.lanzar(
                producir, 'productores', envios,
                tuple(('consumidores', entrada) for entrada in entradas), instancia=instancia,
            ))
        for proceso, eventos in fuentes:
            self.assertEqual(self.esperar(eventos), ('producido',))
            self.finalizar(proceso)
        self.assertCountEqual(self.resultados(consumidores, entradas), esperados)
        self.comprobar_capacidad_recuperada()

    def test_eof_espera_datos_encolados_y_trabajo_en_curso(self):
        """La barrera espera tanto una entrega aún en cola como un callback retenido."""
        for retener_trabajo in (False, True):
            with self.subTest(trabajo_en_curso=retener_trabajo):
                self.comprobar_espera_de_replica(retener_trabajo)
        self.comprobar_capacidad_recuperada()

    def comprobar_espera_de_replica(self, retener_trabajo):
        entrada = Entrada(f'pendiente_{retener_trabajo}', 'palabras')
        fuentes = [self.abrir('productores', indice) for indice in range(2)]
        datos = palabras(entrada.id_ejecucion, 'aporte pendiente')
        coordinadora = self.topologia.instancia_coordinadora('consumidores', entrada)
        # Una réplica distinta conserva datos mientras la coordinadora tramita el EOF.
        for numero in range(100):
            envio = EnvioDatos('consumidores', datos, f'destino_{numero}')
            elegida = fuentes[0]._publicador._instancia_destinataria(envio)
            if elegida != coordinadora:
                break
        else:
            self.fail('No se encontró una clave dirigida a otra réplica')
        permiso = multiprocessing.get_context('spawn').Event()
        consumidores = []
        for indice in range(3):
            proceso, eventos = self.lanzar(
                consumir, 'consumidores', (entrada,),
                permiso if indice == elegida and not retener_trabajo else None,
                permiso if indice == elegida and retener_trabajo else None,
                True, instancia=indice,
            )
            self.assertEqual(self.esperar(eventos), ('listo',))
            consumidores.append((proceso, eventos))
        fuentes[0].publicar_en_ramas((envio,))
        if retener_trabajo:
            self.assertEqual(self.esperar(consumidores[elegida][1]), ('procesando', entrada))
        self.cerrar_productores(fuentes, 'consumidores', (entrada,))
        for otra in range(3):
            if otra != elegida:
                self.assertEqual(self.esperar(consumidores[otra][1]), ('avance', entrada, 0))
        # El checkpoint prueba trabajo retenido; la espera acotada detecta cierres prematuros.
        for _, eventos in consumidores:
            with self.assertRaises(Empty):
                eventos.get(timeout=PLAZO_BLOQUEO)
        permiso.set()
        self.assertEqual(self.resultados(consumidores, (entrada,)), [datos])

    def test_ramas_independientes_y_dos_entradas_incluida_una_vacia(self):
        """Cerrar una rama no cierra otra ni habilita limpiar dependencias aún vacías."""
        palabras_a = Entrada('A', 'palabras')
        referencias_a = Entrada('A', 'referencias')
        palabras_b = Entrada('B', 'palabras')
        referencias_b = Entrada('B', 'referencias')
        fuentes = [self.abrir('productores', indice) for indice in range(2)]
        rama = self.iniciar_consumidores('consumidores', (palabras_a,))
        entradas = (palabras_a, referencias_a, palabras_b, referencias_b)
        combinadores = self.iniciar_consumidores('combinadores', entradas)
        inicial = palabras('A', 'compartido')
        fuentes[0].publicar_en_ramas(tuple(
            EnvioDatos(grupo, inicial) for grupo in ('consumidores', 'combinadores')
        ))
        self.cerrar_productores(fuentes, 'consumidores', (palabras_a,))
        self.assertEqual(self.resultados(rama, (palabras_a,)), [inicial])
        adicional = palabras('A', 'solo otra rama')
        fuentes[1].publicar('combinadores', adicional)
        self.cerrar_productores(fuentes, 'combinadores', (palabras_a,))
        for _, eventos in combinadores:
            self.assertEqual(self.esperar_tipo(eventos, 'fin'), ('fin', palabras_a, frozenset({'referencias'})))
            self.assertEqual(self.esperar_tipo(eventos, 'limpieza_pendiente')[1], 'A')
        referencia = palabras('B', 'referencia', entrada='referencias')
        fuentes[0].publicar('combinadores', referencia)
        self.cerrar_productores(fuentes, 'combinadores', (referencias_b, palabras_b, referencias_a))
        self.assertCountEqual(self.resultados(combinadores, entradas), [inicial, adicional, referencia])
        self.comprobar_capacidad_recuperada()

    def test_acumulados_antes_del_eof_con_salida_saturada_y_rama_sin_resultados(self):
        """Finalizadores esperan capacidad; el sumidero recibe todos los acumulados antes del fin."""
        clientes = ('A', 'sin_resultados', 'vacio')
        entradas = tuple(Entrada(cliente, 'palabras') for cliente in clientes)
        salidas = tuple(Entrada(cliente, 'conteos') for cliente in clientes)
        fuentes = [self.abrir('productores', indice) for indice in range(2)]
        consumidores = self.iniciar_consumidores('consumidores', entradas, acumular=True)
        lote = palabras('A', 'uno', 'dos', 'uno')
        envio = EnvioDatos('consumidores', lote, 'acumular_juntos')
        elegida = fuentes[0]._publicador._instancia_destinataria(envio)
        fuentes[0].publicar_en_ramas((envio,))
        descartados = palabras('sin_resultados', 'omitir este registro')
        fuentes[1].publicar('consumidores', descartados)
        self.cerrar_productores(fuentes, 'consumidores', entradas)
        self.esperar_tipo(consumidores[elegida][1], 'esperando')
        with self.assertRaises(Empty):
            consumidores[elegida][1].get(timeout=PLAZO_BLOQUEO)
        sumidero = self.iniciar_consumidores('sumidero', salidas)
        self.assertCountEqual(self.resultados(consumidores, entradas), [lote, descartados])
        recibidos = self.resultados(sumidero, salidas)
        conteos = Counter()
        for datos in recibidos:
            self.assertEqual(datos.entrada, salidas[0])
            for registro in datos.contenido.registros:
                conteos[registro.palabra] += registro.cantidad
        self.assertEqual(conteos, Counter(registro.palabra for registro in lote.contenido.registros))
        self.comprobar_capacidad_recuperada()

    def test_cierres_y_controles_repetidos_no_duplican_finalizaciones(self):
        """Repite EOF y FIN por AMQP; otra ejecución mantiene a las réplicas recibiendo."""
        entrada, testigo = Entrada('A', 'palabras'), Entrada('testigo', 'palabras')
        fuentes = [self.abrir('productores', indice) for indice in range(2)]
        consumidores = self.iniciar_consumidores('consumidores', (entrada, testigo))
        indice = self.topologia.instancia_coordinadora('consumidores', entrada)
        destino = self.topologia.destino_control('consumidores', 'palabras', indice)
        datos = palabras('A', 'aporte único')
        fuentes[0].publicar('consumidores', datos)
        fuentes[0].finalizar_entrada('consumidores', entrada)
        with self.assertRaises(EntradaFinalizada):
            fuentes[0].publicar('consumidores', datos)
        self.inyectar_control(destino, SobreMensaje(
            entrada, TipoMensaje.EOF, IdentidadProceso('productores', 0), CierreProductor(1),
        ))
        fuentes[1].finalizar_entrada('consumidores', entrada)
        for _, eventos in consumidores:
            self.assertEqual(self.esperar_tipo(eventos, 'fin')[1], entrada)
        topologia = TopologiaGrupos(self.configurar('consumidores', indice))
        destino = topologia.destino_coordinacion('palabras')
        for _ in range(2):
            self.inyectar_control(destino, SobreMensaje(
                entrada, TipoMensaje.FIN_ENTRADA, IdentidadProceso('consumidores', indice), None,
            ))
        # El testigo se publica después de los duplicados sobre el mismo canal de prueba.
        indice_testigo = self.topologia.instancia_coordinadora('consumidores', testigo)
        destino = self.topologia.destino_control('consumidores', 'palabras', indice_testigo)
        for indice in range(2):
            self.inyectar_control(destino, SobreMensaje(
                testigo, TipoMensaje.EOF, IdentidadProceso('productores', indice), CierreProductor(0),
            ))
        self.assertEqual(self.resultados(consumidores, (entrada, testigo)), [datos])
        self.comprobar_capacidad_recuperada()

    def test_rechazos_resueltos_y_error_del_finalizador_no_confirma_el_cierre(self):
        """Los registros rechazados cuentan como resueltos; un finalizador fallido conserva su EOF."""
        entrada = Entrada('rechazos', 'conteos')
        errores, recibidos, finales = [], [], []
        receptor = self.abrir('sumidero', informar_error=errores.append)
        topologia = TopologiaGrupos(self.configurar('consumidores'))
        destino = topologia.destino_datos('sumidero', 'conteos')
        codificador = CodificadorMensajes(self.configuracion.limites.max_bytes_por_mensaje)
        capacidad = CapacidadDatos(self.inspector, topologia)
        self.inspector.canal.confirm_delivery()
        for palabras_lote in (('válido', 'corromper'), ('corromper',)):
            lote = Lote(Esquema.CONTEO_PALABRA, tuple(ConteoPalabra(p, 1) for p in palabras_lote))
            cuerpo = bytearray(codificador.codificar(SobreMensaje(
                entrada, TipoMensaje.DATOS, IdentidadProceso('consumidores', 0), lote,
            )))
            cuerpo[cuerpo.index(b'corromper')] = 255
            capacidad.adquirir(destino.cola)
            self.inspector.canal.basic_publish(
                exchange=destino.exchange, routing_key=destino.clave, body=bytes(cuerpo),
                mandatory=True, properties=pika.BasicProperties(
                    headers={CABECERA_CAPACIDAD: VERSION_CAPACIDAD},
                ),
            )
            receptor.recibir(recibidos.append, al_finalizar=finales.append)
        self.assertEqual(len(errores), 2)
        self.assertEqual(recibidos, [Datos(entrada, Lote(
            Esquema.CONTEO_PALABRA, (ConteoPalabra('válido', 1),),
        ))])
        destino = topologia.destino_control('sumidero', 'conteos', 0)
        for indice in range(3):
            self.inyectar_control(destino, SobreMensaje(
                entrada, TipoMensaje.EOF, IdentidadProceso('consumidores', indice),
                CierreProductor(2 if indice == 0 else 0),
            ))
        error = ValueError('Error deliberado al emitir acumulados')

        def fallar(recibida):
            self.assertEqual(recibida, entrada)
            raise error

        with self.assertRaises(ValueError) as resultado:
            receptor.recibir(recibidos.append, al_finalizar=fallar)
        self.assertIs(resultado.exception, error)
        with self.assertRaises(ErrorSeguimientoEntrada):
            receptor.liberar_ejecucion(entrada.id_ejecucion)
        receptor.recibir(recibidos.append, al_finalizar=finales.append)
        self.assertEqual(finales, [entrada])
        receptor.liberar_ejecucion(entrada.id_ejecucion)
        self.comprobar_capacidad_recuperada()
