"""Ventana acotada de los últimos lotes vistos de un productor.

Con afinidad, toda reentrega o copia de un lote vuelve a la misma réplica, y solo
pueden reaparecer los últimos lotes que el productor tenía sin confirmar. Recordar
los últimos K ids alcanza para deduplicar, con memoria fija.
"""

import hashlib
import struct
from collections import deque

BYTES_CLAVE = 16
_FORMATO_CANTIDAD = ">I"
_FORMATO_INDICE = ">Q"


def clave_de(id_entrada):
    """Resume el id_entrada en 16 bytes fijos para acotar la memoria."""
    return hashlib.blake2b(id_entrada.encode("utf-8"), digest_size=BYTES_CLAVE).digest()


class VentanaLotes:
    """Últimos K pares (clave del id_entrada, máximo índice visto)."""

    def __init__(self, k):
        if type(k) is not int or k <= 0:
            raise ValueError("k debe ser un entero positivo")
        self._k = k
        self._recientes = deque(maxlen=k)

    @property
    def k(self):
        return self._k

    def __len__(self):
        return len(self._recientes)

    def contiene(self, id_entrada, indice):
        """True si el id está en la ventana y indice <= máximo registrado."""
        clave = clave_de(id_entrada)
        for guardada, maximo in self._recientes:
            if guardada == clave:
                return indice <= maximo
        return False

    def registrar(self, id_entrada, indice):
        """Actualiza el máximo si la clave existe; si no, la agrega y descarta la más vieja."""
        clave = clave_de(id_entrada)
        for posicion, (guardada, maximo) in enumerate(self._recientes):
            if guardada == clave:
                if indice > maximo:
                    self._recientes[posicion] = (clave, indice)
                return
        self._recientes.append((clave, indice))

    def serializar(self):
        """Foto para el WAL: cantidad y, por entrada, clave de 16 bytes + índice u64."""
        partes = [struct.pack(_FORMATO_CANTIDAD, len(self._recientes))]
        for clave, maximo in self._recientes:
            partes.append(clave)
            partes.append(struct.pack(_FORMATO_INDICE, maximo))
        return b"".join(partes)

    @classmethod
    def deserializar(cls, datos, k):
        """Reconstruye una ventana serializada; si hay más de k entradas conserva las últimas k."""
        ventana = cls(k)
        ventana._cargar(datos)
        return ventana

    def _cargar(self, datos):
        tamano_cantidad = struct.calcsize(_FORMATO_CANTIDAD)
        tamano_entrada = BYTES_CLAVE + struct.calcsize(_FORMATO_INDICE)
        if len(datos) < tamano_cantidad:
            raise ValueError("Ventana serializada truncada")
        (cantidad,) = struct.unpack_from(_FORMATO_CANTIDAD, datos, 0)
        if len(datos) != tamano_cantidad + cantidad * tamano_entrada:
            raise ValueError("Ventana serializada con largo inconsistente")
        posicion = tamano_cantidad
        for _ in range(cantidad):
            clave = bytes(datos[posicion:posicion + BYTES_CLAVE])
            (maximo,) = struct.unpack_from(_FORMATO_INDICE, datos, posicion + BYTES_CLAVE)
            self._recientes.append((clave, maximo))
            posicion += tamano_entrada
