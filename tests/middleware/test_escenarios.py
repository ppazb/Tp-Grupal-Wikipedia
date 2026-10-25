"""Escenarios de reparto, presión de carga, rechazos y recursos con RabbitMQ real."""

from contextlib import ExitStack
import multiprocessing
from queue import Empty
import signal
import unittest
from unittest.mock import patch
from uuid import uuid4

from src.common.protocol.dto import CierreProductor, EnvioDatos, SobreMensaje, TipoMensaje
from src.common.protocol.registros import CodigoError
from src.middleware.broker_connection import ConexionBroker
from src.middleware.capacidad import CABECERA_CAPACIDAD, VERSION_CAPACIDAD, CapacidadDatos
from src.middleware.codificacion import CodificadorMensajes
from src.middleware.configuracion import ErrorConfiguracion
from src.middleware.inicializar import inicializar_capacidad
from src.middleware.interfaz import ErrorCapacidad, ErrorMensaje, MiddlewareCerrado, OperacionInterrumpida
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.middleware.topologia import TopologiaGrupos
import pika

from .soporte import (
    PLAZO_BLOQUEO, PLAZO_ESCENARIO, PLAZO_PROCESO, administrar_virtual_host,
    aviso, configurar, consumir_en_proceso, conteo, palabras, publicar_en_proceso,
)


def vencer_plazo(numero, marco):
    raise AssertionError('El escenario excedió su plazo; posible bloqueo de comunicación')


class EscenariosMiddleware(unittest.TestCase):
    """Cada caso usa un virtual host propio y libera procesos, conexiones y colas."""

    def setUp(self):
        anterior = signal.signal(signal.SIGALRM, vencer_plazo)
        self.addCleanup(signal.signal, signal.SIGALRM, anterior)
        self.addCleanup(signal.alarm, 0)
        signal.alarm(PLAZO_ESCENARIO)
        self.virtual_host = f'escenario_{uuid4().hex}'
        self.configuracion = configurar(self.virtual_host)
        administrar_virtual_host(self.configuracion, 'PUT')
        self.addCleanup(administrar_virtual_host, self.configuracion, 'DELETE')
        self.recursos = ExitStack()
        self.addCleanup(self.recursos.close)
        inicializar_capacidad(self.configuracion)
        self.inspector = self.recursos.enter_context(ConexionBroker(self.configuracion.broker))
        self.topologia = TopologiaGrupos(self.configuracion)

    def abrir(self, grupo, instancia=0):
        opciones = {}
        if grupo == 'gateway_resultados':
            opciones['informar_error'] = self.fail
        return self.recursos.enter_context(MiddlewareRabbitMQ(
            configurar(self.virtual_host, grupo, instancia), **opciones,
        ))

    def lanzar(self, funcion, grupo, *argumentos, instancia=0):
        contexto = multiprocessing.get_context('spawn')
        eventos = contexto.Queue(maxsize=16)
        proceso = contexto.Process(
            target=funcion,
            args=(configurar(self.virtual_host, grupo, instancia), eventos, *argumentos),
        )
        proceso.start()
        self.addCleanup(self.limpiar_proceso, proceso, eventos)
        return proceso, eventos

    def limpiar_proceso(self, proceso, eventos):
        if proceso.is_alive():
            # Solo se fuerza al auxiliar de prueba cuando falló o terminó el escenario.
            proceso.terminate()
        proceso.join(PLAZO_PROCESO)
        if proceso.is_alive():
            proceso.kill()
            proceso.join(PLAZO_PROCESO)
        eventos.close()
        eventos.join_thread()
        self.assertFalse(proceso.is_alive(), 'No se pudo cerrar el proceso auxiliar')
        proceso.close()

    def esperar(self, eventos):
        try:
            return eventos.get(timeout=PLAZO_PROCESO)
        except Empty:
            self.fail('El proceso auxiliar no informó progreso dentro del plazo')

    def finalizar(self, proceso):
        proceso.join(PLAZO_PROCESO)
        self.assertEqual(proceso.exitcode, 0, 'El proceso auxiliar falló o quedó bloqueado')

    def comprobar_bloqueado(self, eventos):
        # La notificación anterior proviene del bucle de E/S de la reserva real.
        self.assertEqual(self.esperar(eventos), 'esperando')
        with self.assertRaises(Empty):
            eventos.get(timeout=PLAZO_BLOQUEO)

    def recibir(self, middleware):
        recibidos = []
        middleware.recibir(recibidos.append)
        self.assertEqual(len(recibidos), 1)
        return recibidos[0]

    def estado(self, cola):
        return self.inspector.canal.queue_declare(queue=cola, passive=True).method

    def comprobar_capacidad_recuperada(self):
        for cola in self.topologia.colas_datos():
            with self.subTest(cola=cola):
                datos = self.estado(cola)
                permisos = self.estado(self.topologia.cola_capacidad(cola))
                self.assertEqual((datos.message_count, datos.consumer_count), (0, 0))
                self.assertEqual((permisos.message_count, permisos.consumer_count), (1, 0))

    def inyectar(self, cuerpo, *, reservar=True):
        """Introduce bytes inválidos por AMQP sin agregar accesos al código productivo."""
        destino = self.topologia.destino_datos('frecuencias', 'palabras')
        if reservar:
            CapacidadDatos(self.inspector, self.topologia).adquirir(destino.cola)
        self.inspector.canal.confirm_delivery()
        self.inspector.canal.basic_publish(
            exchange=destino.exchange, routing_key=destino.clave, body=cuerpo,
            mandatory=True, properties=pika.BasicProperties(
                headers={CABECERA_CAPACIDAD: VERSION_CAPACIDAD} if reservar else None,
                content_type='application/octet-stream',
            ),
        )

    def test_reparto_entre_instancias_y_afinidad_sin_mezclar_ejecuciones(self):
        """Dos consumidores comparten reparto; una clave mantiene su destinatario."""
        productor = self.abrir('ocurrencias')
        consumidores = [
            self.lanzar(consumir_en_proceso, 'frecuencias', instancia=instancia)
            for instancia in range(2)
        ]
        for _, eventos in consumidores:
            self.assertEqual(self.esperar(eventos), 'listo')
        esperados = [palabras('cliente_A', 'uno'), palabras('cliente_B', 'dos')]
        for datos in esperados:
            productor.publicar('frecuencias', datos)
        recibidos = []
        for proceso, eventos in consumidores:
            recibidos.append(self.esperar(eventos))
            self.assertEqual(self.esperar(eventos), 'terminado')
            self.finalizar(proceso)
        self.assertCountEqual(recibidos, esperados)

        elegido = None
        for palabra in ('primero', 'segundo'):
            datos = palabras('cliente_A', palabra)
            productor.publicar('frecuencias', datos, clave_afinidad='misma_palabra')
            pendientes = []
            for instancia in range(2):
                topologia = TopologiaGrupos(configurar(self.virtual_host, 'frecuencias', instancia))
                pendientes.append(self.estado(topologia.colas_recepcion().afinidad).message_count)
            self.assertEqual(sum(pendientes), 1)
            instancia = pendientes.index(1)
            if elegido is not None:
                self.assertEqual(instancia, elegido)
            elegido = instancia
            self.assertEqual(self.recibir(self.abrir('frecuencias', instancia)), datos)
        self.comprobar_capacidad_recuperada()

    def test_saturacion_propaga_la_espera_y_reanuda_el_worker(self):
        """Una salida llena bloquea al worker sin ACK anticipado ni reprocesamiento."""
        productor = self.abrir('ocurrencias')
        frecuencia = self.abrir('frecuencias')
        top = self.abrir('top_n')
        frecuencia.publicar('top_n', conteo('primero'))
        entrada = palabras('cliente_A', 'trabajo')
        productor.publicar('frecuencias', entrada)
        proceso, eventos = self.lanzar(consumir_en_proceso, 'frecuencias', True)
        self.assertEqual(self.esperar(eventos), 'listo')
        self.assertEqual(self.esperar(eventos), entrada)
        self.comprobar_bloqueado(eventos)
        cola = self.topologia.destino_datos('frecuencias', 'palabras').cola
        self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).message_count, 0)
        siguiente = palabras('cliente_B', 'siguiente')
        fuente, publicaciones = self.lanzar(
            publicar_en_proceso, 'ocurrencias', (EnvioDatos('frecuencias', siguiente),), 1,
        )
        self.comprobar_bloqueado(publicaciones)
        self.assertEqual(self.recibir(top), conteo('primero'))
        self.assertEqual(self.esperar(eventos), 'terminado')
        self.finalizar(proceso)
        self.assertEqual(self.esperar(publicaciones), 'publicado')
        self.finalizar(fuente)
        self.assertEqual(self.recibir(top), conteo('segundo'))
        self.assertEqual(self.recibir(frecuencia), siguiente)
        self.comprobar_capacidad_recuperada()

    def test_copia_a_ramas_no_repite_la_salida_confirmada(self):
        """La segunda rama espera y la primera conserva una única publicación."""
        productor = self.abrir('ocurrencias')
        frecuencia = self.abrir('frecuencias')
        gateway = self.abrir('gateway_resultados')
        productor.publicar('gateway_resultados', aviso('anterior'))
        datos = palabras('cliente_A', 'rama')
        nuevo = aviso('nuevo')
        proceso, eventos = self.lanzar(
            publicar_en_proceso, 'ocurrencias',
            (EnvioDatos('frecuencias', datos), EnvioDatos('gateway_resultados', nuevo)), 2,
        )
        self.comprobar_bloqueado(eventos)
        cola = self.topologia.destino_datos('frecuencias', 'palabras').cola
        self.assertEqual(self.estado(cola).message_count, 1)
        self.assertEqual(self.recibir(gateway), aviso('anterior'))
        self.assertEqual(self.esperar(eventos), 'publicado')
        self.finalizar(proceso)
        self.assertEqual(self.estado(cola).message_count, 1)
        self.assertEqual(self.recibir(frecuencia), datos)
        self.assertEqual(self.recibir(gateway), nuevo)
        self.comprobar_capacidad_recuperada()

    def test_registro_invalido_notifica_y_conserva_los_validos(self):
        """Un aviso espera capacidad y luego entrega solo los registros válidos."""
        productor = self.abrir('ocurrencias')
        gateway = self.abrir('gateway_resultados')
        productor.publicar('gateway_resultados', aviso('ocupar'))
        datos = palabras('cliente_A', 'uno', 'corromper', 'tres')
        codificador = CodificadorMensajes(self.configuracion.limites.max_bytes_por_mensaje)
        sobre = SobreMensaje(datos.entrada, TipoMensaje.DATOS, self.configuracion.proceso, datos.contenido)
        cuerpo = bytearray(codificador.codificar(sobre))
        # Se altera solo un texto delimitado, conservando las longitudes del lote.
        cuerpo[cuerpo.index(b'corromper')] = 255
        self.inyectar(bytes(cuerpo))
        proceso, eventos = self.lanzar(consumir_en_proceso, 'frecuencias', False, True)
        self.assertEqual(self.esperar(eventos), 'listo')
        self.comprobar_bloqueado(eventos)
        self.assertEqual(self.recibir(gateway), aviso('ocupar'))
        self.assertEqual(self.esperar(eventos), palabras('cliente_A', 'uno', 'tres'))
        self.assertEqual(self.esperar(eventos), 'terminado')
        self.finalizar(proceso)
        notificacion = self.recibir(gateway)
        self.assertEqual(notificacion.entrada.id_ejecucion, 'cliente_A')
        self.assertEqual(len(notificacion.contenido.registros), 1)
        error = notificacion.contenido.registros[0]
        self.assertEqual(error.codigo, CodigoError.REGISTRO_INVALIDO)
        self.assertEqual(error.indice_registro, 1)
        self.assertEqual(error.entrada_afectada, 'palabras')
        self.comprobar_capacidad_recuperada()

    def test_sobres_invalidos_se_rechazan_sin_detener_el_consumidor(self):
        """Rechaza bytes truncados, controles en datos y mensajes sin reserva."""
        frecuencia = self.abrir('frecuencias')
        gateway = self.abrir('gateway_resultados')
        productor = self.abrir('ocurrencias')
        self.inyectar(bytes([TipoMensaje.DATOS.value]))
        frecuencia.recibir(lambda datos: self.fail('Procesó un sobre truncado'))
        self.inyectar(bytes([TipoMensaje.DATOS.value]), reservar=False)
        frecuencia.recibir(lambda datos: self.fail('Procesó un mensaje sin reserva'))
        entrada = palabras('cliente_A', 'control').entrada
        codificador = CodificadorMensajes(self.configuracion.limites.max_bytes_por_mensaje)
        self.inyectar(codificador.codificar(SobreMensaje(
            entrada, TipoMensaje.EOF, self.configuracion.proceso, CierreProductor(0),
        )))
        frecuencia.recibir(lambda datos: self.fail('Procesó EOF como datos'))
        error = self.recibir(gateway).contenido.registros[0]
        self.assertEqual(error.codigo, CodigoError.MENSAJE_INVALIDO)
        with self.assertRaises(ErrorConfiguracion):
            productor.publicar('destino_inexistente', palabras('cliente_A', 'mal'))
        with self.assertRaises(ErrorMensaje):
            productor.publicar('frecuencias', palabras('', 'sin ejecución'))
        datos = palabras('cliente_B', 'válido')
        productor.publicar('frecuencias', datos)
        self.assertEqual(self.recibir(frecuencia), datos)
        self.comprobar_capacidad_recuperada()

    def test_excepcion_del_procesador_no_confirma_la_entrega(self):
        """La excepción original se propaga y otro receptor puede procesar la entrega."""
        productor = self.abrir('ocurrencias')
        frecuencia = self.abrir('frecuencias')
        datos = palabras('cliente_A', 'pendiente')
        productor.publicar('frecuencias', datos)
        error_negocio = ValueError('Error deliberado del procesador de prueba')

        def fallar(recibido):
            self.assertEqual(recibido, datos)
            raise error_negocio

        with self.assertRaises(ValueError) as resultado:
            frecuencia.recibir(fallar)
        self.assertIs(resultado.exception, error_negocio)
        cola = self.topologia.destino_datos('frecuencias', 'palabras').cola
        self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).message_count, 0)
        frecuencia.cerrar()
        self.assertEqual(self.recibir(self.abrir('frecuencias', 1)), datos)
        self.comprobar_capacidad_recuperada()

    def test_interrupcion_limpieza_por_ejecucion_y_cierre(self):
        """Interrumpir no publica; liberar un cliente no elimina recursos de otro."""
        productor = self.abrir('ocurrencias')
        frecuencia = self.abrir('frecuencias')
        datos = palabras('cliente_A', 'ocupado')
        productor.publicar('frecuencias', datos)
        interrumpido = self.abrir('ocurrencias')
        interrumpido._conexion.programar(interrumpido.interrumpir)
        with self.assertRaises(OperacionInterrumpida):
            interrumpido.publicar('frecuencias', palabras('cliente_B', 'cancelado'))
        self.assertEqual(self.recibir(frecuencia), datos)
        frecuencia._conexion.programar(frecuencia.detener_recepcion)
        frecuencia.recibir(lambda datos: self.fail('Recibió después de detenerse'))
        liberados = []
        productor.registrar_recurso('cliente_A', 'archivo', lambda: liberados.append('A'))
        productor.registrar_recurso('cliente_B', 'archivo', lambda: liberados.append('B'))
        productor.liberar_ejecucion('cliente_A')
        productor.liberar_ejecucion('cliente_A')
        self.assertEqual(liberados, ['A'])
        self.comprobar_capacidad_recuperada()
        conexion = productor._conexion._conexion
        canales = [productor._conexion.canal, productor._capacidad._canal]
        productor.cerrar()
        productor.cerrar()
        self.assertEqual(liberados, ['A', 'B'])
        self.assertTrue(conexion.is_closed)
        self.assertTrue(all(canal.is_closed for canal in canales))
        with self.assertRaises(MiddlewareCerrado):
            productor.publicar('frecuencias', datos)

    def test_inicializacion_repetida_y_apertura_parcial_no_dejan_recursos(self):
        """No duplica permisos y cierra la conexión si falta la marca de capacidad."""
        with self.assertRaises(ErrorCapacidad):
            inicializar_capacidad(self.configuracion)
        self.comprobar_capacidad_recuperada()
        self.inspector.canal.queue_delete(queue=self.topologia.marca_capacidad())
        conexion = ConexionBroker(self.configuracion.broker)
        # Se conserva la conexión real para inspeccionar su cierre tras el error.
        with patch('src.middleware.rabbitmq.ConexionBroker', return_value=conexion):
            with self.assertRaises(ErrorConfiguracion):
                MiddlewareRabbitMQ(self.configuracion)
        self.assertIsNone(conexion._conexion)
        self.assertEqual(conexion._canales_adicionales, [])
        with self.assertRaises(MiddlewareCerrado):
            conexion.abrir()
