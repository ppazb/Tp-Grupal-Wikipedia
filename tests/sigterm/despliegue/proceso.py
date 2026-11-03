"""Ejecuta un main real observando actividad y limpieza, sin alterar el negocio."""

import importlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import socket
import sys
import threading
from unittest.mock import patch
from contextlib import ExitStack

from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.workers.base_worker import Worker


def avisar(tipo, **datos):
    mensaje = dict(nodo=os.environ['NODO_PRUEBA'], tipo=tipo, **datos)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conexion:
        conexion.connect('/prueba/avisos.sock')
        conexion.sendall(json.dumps(mensaje).encode() + b'\n')


def main():
    modulo, *argumentos = sys.argv[1:]
    sys.argv = [modulo, *argumentos]
    conexiones = []
    vistos = set()
    lock = threading.Lock()
    anteriores = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
    abrir = MiddlewareRabbitMQ.__init__
    procesar = Worker._procesar

    def actividad():
        with lock:
            if 'activo' not in vistos:
                avisar('activo')
                vistos.add('activo')
            if 'disco' not in vistos and any(p.is_file() for p in Path('/temporales').rglob('*')):
                avisar('disco')
                vistos.add('disco')

    def abrir_observado(middleware, *args, **kwargs):
        abrir(middleware, *args, **kwargs)
        with lock:
            conexiones.append(middleware)

    def procesar_observado(worker, datos):
        procesar(worker, datos)
        actividad()

    with ExitStack() as recursos:
        recursos.enter_context(patch.object(MiddlewareRabbitMQ, '__init__', abrir_observado))
        recursos.enter_context(patch.object(Worker, '_procesar', procesar_observado))
        if modulo == 'src.client.main':
            from src.client.src import client
            enviar = client._enviar_filas
            def enviar_observado(*args, **kwargs):
                resultado = enviar(*args, **kwargs)
                actividad()
                return resultado
            recursos.enter_context(patch.object(client, '_enviar_filas', enviar_observado))
        if modulo == 'src.gateway.main':
            from src.gateway.src.gateway import PublicadorMiddleware
            publicar = PublicadorMiddleware.publicar
            def publicar_observado(publicador, ejecucion, lote):
                publicar(publicador, ejecucion, lote)
                actividad()
            recursos.enter_context(patch.object(PublicadorMiddleware, 'publicar', publicar_observado))
        entrada = importlib.import_module(modulo)
        operacion = getattr(entrada, 'ejecutar', None) or entrada.main
        resultado = operacion()
        resultado = 0 if resultado is None else resultado
        assert resultado in (0, 143), f'El proceso devolvió {resultado}'
        assert all(signal.getsignal(n) == anterior for n, anterior in anteriores.items())
        for middleware in conexiones:
            assert middleware._cerrada and middleware._conexion._conexion is None
            assert middleware._conexion._canal is None
        assert threading.enumerate() == [threading.main_thread()], 'Quedaron hilos Python'
        assert not multiprocessing.active_children(), 'Quedaron hijos'
        assert not list(Path('/temporales').iterdir()), 'Quedaron temporales'
        avisar('limpio', codigo=resultado)
        return resultado


if __name__ == '__main__':
    sys.exit(main())
