"""Emisión de conteos parciales acotados por ejecución, independiente del transporte."""

from collections.abc import Callable

from src.common.protocol.registros import Esquema, Lote, Palabra
from .acumulador import AcumuladorFrecuencias


class EmisorParciales:
    """Emite al alcanzar el límite de palabras distintas o finalizar una entrada.

    emitir recibe el identificador de ejecución y un Lote de ConteoPalabra. Debe
    retornar cuando la salida fue aceptada o elevar el error correspondiente.
    El límite cuenta registros de salida; no sustituye el límite de bytes del
    transporte. La instancia se utiliza secuencialmente dentro de un proceso.
    """

    def __init__(self, max_conteos_por_parcial: int, emitir: Callable[[str, Lote], None]):
        if type(max_conteos_por_parcial) is not int or max_conteos_por_parcial <= 0:
            raise ValueError("max_conteos_por_parcial debe ser un entero positivo")
        if not callable(emitir):
            raise TypeError("La operación de emisión debe ser invocable")
        self._limite = max_conteos_por_parcial
        self._emitir = emitir
        self._acumulador = AcumuladorFrecuencias()
        self._emitiendo = False

    def acumular(self, id_ejecucion: str, aporte: Palabra) -> None:
        """Suma un aporte y emite los contadores al alcanzar el límite configurado."""
        self._validar_disponible()
        # Una emisión anterior fallida conserva el parcial completo. No se admite
        # otro aporte hasta resolver su salida, para no superar el límite local.
        if self._acumulador.cantidad_palabras(id_ejecucion) >= self._limite:
            self._emitir_pendientes(id_ejecucion)
        self._acumulador.acumular(id_ejecucion, aporte)
        if self._acumulador.cantidad_palabras(id_ejecucion) >= self._limite:
            self._emitir_pendientes(id_ejecucion)

    def finalizar_entrada(self, id_ejecucion: str) -> None:
        """Emite el parcial restante; una entrada vacía no produce lotes.

        No publica EOF. El llamador debe cerrar la salida después de que esta
        operación retorne correctamente. Repetirla sin nuevos aportes no emite.
        """
        self._validar_disponible()
        self._emitir_pendientes(id_ejecucion)

    def _emitir_pendientes(self, id_ejecucion: str) -> None:
        registros = tuple(self._acumulador.iterar_conteos(id_ejecucion))
        if not registros:
            return
        self._emitiendo = True
        try:
            self._emitir(id_ejecucion, Lote(Esquema.CONTEO_PALABRA, registros))
            self._acumulador.liberar_ejecucion(id_ejecucion)
        finally:
            self._emitiendo = False

    def _validar_disponible(self) -> None:
        if self._emitiendo:
            raise RuntimeError("No se pueden modificar parciales durante su emisión")
