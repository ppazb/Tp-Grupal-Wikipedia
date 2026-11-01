"""SIGTERM en procesos en reposo y durante una entrega pendiente de confirmación."""

import multiprocessing
import os
from pathlib import Path
import signal

from src.middleware.topologia import TopologiaGrupos
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import ConteoPalabra, Esquema, Lote
from tests.soporte import EscenarioRabbitMQ
from .soporte import ejecutar_worker, entrada_prueba


class EscenariosWorker:
    """Casos comunes ejecutados en un virtual host nuevo para cada worker y estado."""

    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'gateway'

    def comprobar_apagado(self, modo):
        continuar = multiprocessing.get_context('spawn').Event()
        self.addCleanup(continuar.set)
        fuente, datos = entrada_prueba(self.grupo)
        if modo == 'activo':
            with self.abrir(fuente, informar_error=self.fail) as productor:
                productor.publicar(self.grupo, datos)
        proceso, eventos = self.lanzar(ejecutar_worker, self.grupo, modo, continuar)
        self.assertEqual(self.esperar(eventos), 'esperando' if modo == 'inactivo' else 'procesado_sin_ack')
        os.kill(proceso.pid, signal.SIGTERM)
        if modo == 'activo':
            os.kill(proceso.pid, signal.SIGTERM)
        continuar.set()
        self.assertEqual(self.esperar(eventos), 'limpio')
        self.finalizar(proceso)
        # Sin EOF de entrada, ningún worker puede fabricar un cierre exitoso.
        for grupo in self.configuracion.grupos:
            topologia = TopologiaGrupos(self.configurar(grupo.nombre))
            colas = topologia.colas_recepcion()
            if colas is not None:
                self.assertEqual(self.estado(colas.control).message_count, 0)
        for cola in self.topologia.colas_datos():
            self.assertEqual(self.estado(cola).consumer_count, 0)
            self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).consumer_count, 0)
        if modo == 'activo':
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


class Router(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'date_router'


class FiltroIdioma(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q1_filter_lang'


class Formateador(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q1_formatter'


class Ocurrencias(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'word_occurrences'


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
        os.kill(proceso.pid, signal.SIGTERM)
        self.assertEqual(self.esperar(eventos), 'limpio')
        self.finalizar(proceso)
        destino = TopologiaGrupos(self.configurar('frecuencias')).destino_datos('top_n', 'conteos', 0)
        self.assertEqual(self.estado(destino.cola).message_count, 1)
        colas = TopologiaGrupos(self.configurar('top_n')).colas_recepcion()
        self.assertEqual(self.estado(colas.control).message_count, 0)
        self.assertEqual(self.estado(self.topologia.cola_capacidad(destino.cola)).consumer_count, 0)


class TopN(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'top_n'


class Promedio(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q5_average'


class Comparador(EscenariosWorker, EscenarioRabbitMQ):
    grupo = 'q5_comparator'
