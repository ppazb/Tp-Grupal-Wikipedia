"""Recibe avisos de los procesos por un socket local sin sondeo ni pausas fijas."""

import json
from selectors import DefaultSelector, EVENT_READ
import socket
from time import monotonic


class Observador:
    def __init__(self, ruta):
        self._selector = DefaultSelector()
        self._servidor = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._servidor.bind(str(ruta))
        self._servidor.listen(64)
        self._servidor.setblocking(False)
        self._selector.register(self._servidor, EVENT_READ, None)
        self.avisos = []

    def esperar(self, condicion, plazo):
        limite = monotonic() + plazo
        while not condicion(self.avisos):
            restante = limite - monotonic()
            if restante <= 0:
                raise AssertionError(f'No llegó la evidencia esperada en {plazo}s. Avisos: {self.avisos}')
            for clave, _ in self._selector.select(restante):
                if clave.fileobj is self._servidor:
                    conexion, _ = self._servidor.accept()
                    conexion.setblocking(False)
                    self._selector.register(conexion, EVENT_READ, bytearray())
                    continue
                bloque = clave.fileobj.recv(4096)
                if bloque:
                    clave.data.extend(bloque)
                    if len(clave.data) > 4096:
                        raise ValueError('Aviso de observación demasiado grande')
                else:
                    self._selector.unregister(clave.fileobj)
                    clave.fileobj.close()
                    self.avisos.append(json.loads(clave.data))
                    print('Observado:', self.avisos[-1], flush=True)

    def cerrar(self):
        for clave in list(self._selector.get_map().values()):
            clave.fileobj.close()
        self._selector.close()


def nodos(avisos, tipo):
    return {aviso['nodo'] for aviso in avisos if aviso['tipo'] == tipo}
