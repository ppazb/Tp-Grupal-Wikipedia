"""Observación del gateway real, sus hilos propietarios y sus conexiones."""

from contextlib import ExitStack
import importlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import socket
from tempfile import TemporaryDirectory
import threading
from unittest.mock import patch

from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.middleware.capacidad import CapacidadDatos


def ejecutar_gateway(configuracion, eventos, modo):
    with TemporaryDirectory(prefix='sigterm-gateway-') as temporal, ExitStack() as recursos:
        config = json.loads(Path(__file__).with_name('config.json').read_text())
        config['broker']['virtual_host'] = configuracion.broker.virtual_host
        archivo = Path(temporal) / 'config.json'
        archivo.write_text(json.dumps(config))
        entorno = dict(MIDDLEWARE_CONFIG=str(archivo), GATEWAY_GROUP='gateway', GATEWAY_INSTANCE='0',
                       GATEWAY_ERROR_ACK_SECONDS='10', GATEWAY_RESULTS_GROUP='gateway_resultados',
                       GATEWAY_RESULTS_INSTANCE='0', SERVER_HOST='127.0.0.1', SERVER_PORT='0')
        recursos.enter_context(patch.dict(os.environ, entorno))
        gateway = importlib.import_module('src.gateway.src.gateway')
        main = importlib.import_module('src.gateway.main')
        anteriores = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
        conexiones = []
        servidores = []
        lock = threading.Lock()
        iniciar = MiddlewareRabbitMQ.__init__
        escuchar = socket.socket.listen
        registrar = gateway.RegistroSesiones.registrar
        aceptar = gateway.ServidorGateway._aceptar
        adquirir = CapacidadDatos.adquirir
        enviar = gateway.SesionCliente.enviar

        def abrir(middleware, *args, **kwargs):
            iniciar(middleware, *args, **kwargs)
            with lock:
                conexiones.append((middleware, threading.get_ident()))
            if middleware._identidad.grupo == 'gateway_resultados':
                middleware._conexion.programar(lambda: eventos.put('receptor'))

        def propietario(nombre):
            original = getattr(MiddlewareRabbitMQ, nombre)
            def comprobar(middleware, *args, **kwargs):
                with lock:
                    for conexion, hilo in conexiones:
                        if conexion is middleware:
                            assert hilo == threading.get_ident(), f'{nombre} desde otro hilo'
                return original(middleware, *args, **kwargs)
            return comprobar

        def escuchar_observado(servidor, *args):
            escuchar(servidor, *args)
            if any(instancia._socket is servidor for instancia in servidores):
                eventos.put(('puerto', servidor.getsockname()[1]))

        def registrar_observado(registro, ejecucion, sesion):
            registrar(registro, ejecucion, sesion)
            eventos.put(('cliente', ejecucion))
            if modo == 'envio':
                sesion._socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)

        def aceptar_observado(servidor):
            servidores.append(servidor)
            return aceptar(servidor)

        def adquirir_observado(capacidad, cola):
            capacidad._conexion.programar(lambda: eventos.put('sin_capacidad'))
            return adquirir(capacidad, cola)

        def enviar_observado(sesion, tipo, cuerpo):
            if modo == 'envio' and tipo.name == 'RESULTADO':
                eventos.put('enviando')
            return enviar(sesion, tipo, cuerpo)

        recursos.enter_context(patch.object(MiddlewareRabbitMQ, '__init__', abrir))
        for nombre in ('publicar_en_ramas', 'finalizar_entrada', 'recibir', 'cerrar'):
            recursos.enter_context(patch.object(MiddlewareRabbitMQ, nombre, propietario(nombre)))
        recursos.enter_context(patch.object(socket.socket, 'listen', escuchar_observado))
        recursos.enter_context(patch.object(gateway.RegistroSesiones, 'registrar', registrar_observado))
        recursos.enter_context(patch.object(gateway.ServidorGateway, '_aceptar', aceptar_observado))
        recursos.enter_context(patch.object(gateway.SesionCliente, 'enviar', enviar_observado))
        if modo == 'capacidad':
            recursos.enter_context(patch.object(CapacidadDatos, 'adquirir', adquirir_observado))
        assert main.main() == 0
        assert all(signal.getsignal(n) == anterior for n, anterior in anteriores.items())
        for middleware, _ in conexiones:
            assert middleware._cerrada
            assert middleware._conexion._conexion is None
            assert middleware._conexion._canal is None
            aviso = middleware._conexion._apagado
            assert aviso._lectura.fileno() == aviso._escritura.fileno() == -1
        for servidor in servidores:
            assert not servidor._clientes
            assert not servidor._sesiones._sesiones
            assert not any(h.is_alive() for h in servidor._hilos)
            assert not servidor._vigilante.is_alive()
    eventos.put('limpio')
    eventos.close()
    eventos.join_thread()
    assert threading.enumerate() == [threading.main_thread()]
    assert not multiprocessing.active_children()
