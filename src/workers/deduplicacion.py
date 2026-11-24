"""Detección de lotes duplicados por (id_entrada, indice) dentro de cada origen.

Todas las rutas entran por afinidad: un lote reentregado o republicado por su
productor vuelve siempre a la misma réplica, así que alcanza con recordar los
últimos K lotes de cada (ejecución, origen). K es limites.ventana_lotes, el mismo
valor que usa el middleware, y la memoria es fija por productor.

es_duplicado(entrega) consulta sin modificar; registrar(entrega) anota el lote.
"""

from src.middleware.eof.ventana import VentanaLotes


class DedupPorOrigen:
    """Una VentanaLotes de K ids por cada (id_ejecucion, origen)."""

    def __init__(self, k):
        self._k = k
        self._ventanas = {}

    def es_duplicado(self, entrega):
        ventana = self._ventanas.get(self._clave(entrega))
        return ventana is not None and ventana.contiene(entrega.id_entrada, entrega.indice)

    def registrar(self, entrega):
        ventana = self._ventanas.get(self._clave(entrega))
        if ventana is None:
            ventana = self._ventanas[self._clave(entrega)] = VentanaLotes(self._k)
        ventana.registrar(entrega.id_entrada, entrega.indice)

    def cerrar_ejecucion(self, id_ejecucion):
        """Libera las ventanas de una ejecución terminada."""
        for clave in [c for c in self._ventanas if c[0] == id_ejecucion]:
            del self._ventanas[clave]

    @staticmethod
    def _clave(entrega):
        return (entrega.entrada.id_ejecucion, entrega.origen)
