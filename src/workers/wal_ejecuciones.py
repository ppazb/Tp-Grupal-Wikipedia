"""
Un WAL por ejecución en curso, más un registro de las ejecuciones ya cerradas.

Disco:  

<WAL_DIR>/<id_ejecucion en hexadecimal>.log   registros de la ejecución (se borra al cerrarla)
<WAL_DIR>/cerradas.log         un id_ejecucion por registro (se conserva)

Cada réplica debe tener su propio directorio (volumen propio, no compartido).
"""

import os

from src.common.io_adapters.wal import WAL

EXTENSION = ".log"
ARCHIVO_CERRADAS = "cerradas" + EXTENSION


class WalsPorEjecucion:
    def __init__(self, directorio=None):
        self._directorio = directorio or os.environ.get("WAL_DIR", "data/wal")
        os.makedirs(self._directorio, exist_ok=True)
        self._cerradas_wal = WAL(os.path.join(self._directorio, ARCHIVO_CERRADAS))
        self._cerradas = {datos.decode("utf-8") for datos in self._cerradas_wal.iterar()}
        self._wals = {}
        self._en_curso = self._buscar_en_curso()

    def _ruta(self, id_ejecucion):
        return os.path.join(self._directorio, id_ejecucion.encode("utf-8").hex() + EXTENSION)

    def _buscar_en_curso(self):
        """Ejecuciones con WAL en disco; borra las ya cerradas (caída antes del borrado)."""
        en_curso = []
        for nombre in sorted(os.listdir(self._directorio)):
            if not nombre.endswith(EXTENSION) or nombre == ARCHIVO_CERRADAS:
                continue
            id_ejecucion = bytes.fromhex(nombre[:-len(EXTENSION)]).decode("utf-8")
            if id_ejecucion in self._cerradas:
                os.remove(os.path.join(self._directorio, nombre))
            else:
                en_curso.append(id_ejecucion)
        return en_curso

    def recuperar(self):
        """Entrega (id_ejecucion, registros) de cada ejecución en curso, para reconstruir el estado."""
        for id_ejecucion in self._en_curso:
            yield id_ejecucion, self._wal_de(id_ejecucion).iterar()

    def en_curso(self):
        """Ids de las ejecuciones que tenían WAL en disco al abrir (sin abrir sus archivos)."""
        return tuple(self._en_curso)

    def esta_cerrada(self, id_ejecucion):
        return id_ejecucion in self._cerradas

    def agregar(self, id_ejecucion, datos):
        """Persiste un registro y devuelve su posición en el WAL de la ejecución."""
        return self._wal_de(id_ejecucion).agregar(datos)

    def iterar_con_posiciones(self, id_ejecucion):
        """Como iterar(), pero entrega (registro, posicion) para releer después con leer_en()."""
        if id_ejecucion not in self._wals and not os.path.exists(self._ruta(id_ejecucion)):
            return iter(())
        return self._wal_de(id_ejecucion).iterar_con_posiciones()

    def leer_en(self, id_ejecucion, posicion):
        return self._wal_de(id_ejecucion).leer_en(posicion)

    def iterar(self, id_ejecucion):
        """Registros guardados de una ejecución, en orden."""
        if id_ejecucion not in self._wals and not os.path.exists(self._ruta(id_ejecucion)):
            return iter(())
        return self._wal_de(id_ejecucion).iterar()

    def cerrar_ejecucion(self, id_ejecucion):
        """Marca la ejecución como cerrada y borra su WAL (en ese orden)."""
        self._cerradas_wal.agregar(id_ejecucion.encode("utf-8"))
        self._cerradas.add(id_ejecucion)
        wal = self._wals.pop(id_ejecucion, None)
        if wal is not None:
            wal.cerrar()
        if os.path.exists(self._ruta(id_ejecucion)):
            os.remove(self._ruta(id_ejecucion))

    def cerrar(self):
        for wal in self._wals.values():
            wal.cerrar()
        self._wals.clear()
        self._cerradas_wal.cerrar()

    def _wal_de(self, id_ejecucion):
        if id_ejecucion not in self._wals:
            self._wals[id_ejecucion] = WAL(self._ruta(id_ejecucion))
        return self._wals[id_ejecucion]
