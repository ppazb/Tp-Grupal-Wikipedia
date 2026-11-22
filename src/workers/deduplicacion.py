"""Detección de mensajes duplicados por (id_entrada, indice).

Un mensaje es duplicado si su id_entrada ya fue registrado y su indice es menor
o igual al máximo registrado para ese id_entrada. 

El mecanismo depende de comorecibe el grupo PRODUCTOR 
-cola compartida: DedupDesdeCompartida
-afinidad: DedupDesdeAfinidad

Ambas se usan igual: 
es_duplicado(entrega) consulta sin modificar 
registrar(entrega) anota el mensaje.
"""

import hashlib
from collections import deque

# Cantidad de lotes recordados por productor: igual al prefetch (mensajes sin confirmar)
LOTES_RECORDADOS = 4


def _clave(id_entrada):
    """Resume el id_entrada en 16 bytes fijos para acotar la memoria."""
    return hashlib.blake2b(id_entrada.encode("utf-8"), digest_size=16).digest()


class DedupDesdeCompartida:
    """
    {id_ejecucion: {id_entrada: max_indice}}, sin distinguir el origen.

    Con cola compartida el reproceso puede hacerlo otra replica (otro origen),
    por eso el origen no forma parte de la clave.
    """

    def __init__(self):
        self._vistos = {}

    def es_duplicado(self, entrega):
        vistos = self._vistos.get(entrega.entrada.id_ejecucion)
        if vistos is None:
            return False
        maximo = vistos.get(_clave(entrega.id_entrada))
        return maximo is not None and entrega.indice <= maximo

    def registrar(self, entrega):
        vistos = self._vistos.setdefault(entrega.entrada.id_ejecucion, {})
        clave = _clave(entrega.id_entrada)
        vistos[clave] = max(vistos.get(clave, entrega.indice), entrega.indice)

    def cerrar_ejecucion(self, id_ejecucion):
        """Olvida los IDs de una ejecución terminada."""
        self._vistos.pop(id_ejecucion, None)


class DedupDesdeAfinidad:
    """Por cada (ejecución, origen), los últimos LOTES_RECORDADOS (id_entrada, max_indice).

    Con afinidad el reproceso lo hace siempre la misma réplica, y un productor
    que se reinicia solo puede reprocesar los lotes que tenía sin confirmar.
    La memoria es fija por ejecución: al llenarse, se olvida el lote más viejo, y al cerrar
    la ejecución se libera su ventana.
    """

    def __init__(self):
        self._recientes = {}

    def es_duplicado(self, entrega):
        maximo = self._maximo(entrega)
        return maximo is not None and entrega.indice <= maximo

    def registrar(self, entrega):
        recientes = self._recientes.setdefault(self._ventana(entrega), deque(maxlen=LOTES_RECORDADOS))
        clave = _clave(entrega.id_entrada)
        for posicion, (clave_guardada, maximo) in enumerate(recientes):
            if clave_guardada == clave:
                recientes[posicion] = (clave, max(maximo, entrega.indice))
                return
        recientes.append((clave, entrega.indice))

    def cerrar_ejecucion(self, id_ejecucion):
        """Libera las ventanas de una ejecución terminada."""
        for ventana in [v for v in self._recientes if v[0] == id_ejecucion]:
            del self._recientes[ventana]

    def _maximo(self, entrega):
        clave = _clave(entrega.id_entrada)
        for clave_guardada, maximo in self._recientes.get(self._ventana(entrega), ()):
            if clave_guardada == clave:
                return maximo
        return None

    @staticmethod
    def _ventana(entrega):
        return (entrega.entrada.id_ejecucion, entrega.origen)
