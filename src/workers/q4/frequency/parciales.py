"""Emisión de conteos parciales acotados por los bytes del mensaje de salida."""

from collections.abc import Callable

from src.common.protocol.registros import ConteoPalabra, Esquema, Lote, Palabra
from .acumulador import AcumuladorFrecuencias


class EmisorParciales:
    """Acumula presencias y emite antes de incorporar una palabra que no cabe.

    crear_presupuesto recibe la ejecución y devuelve el medidor del transporte.
    emitir recibe la ejecución y el lote; retorna cuando la salida fue aceptada
    o eleva el error correspondiente. Los conteos usan cantidades de ancho fijo:
    actualizar una palabra existente no modifica el tamaño del registro de salida.
    La instancia se utiliza secuencialmente dentro de un proceso.
    """

    def __init__(self, emitir: Callable[[str, Lote], None], crear_presupuesto: Callable):
        if not callable(emitir) or not callable(crear_presupuesto):
            raise TypeError('La emisión y la creación del presupuesto deben ser invocables')
        self._emitir = emitir
        self._crear_presupuesto = crear_presupuesto
        self._presupuestos = {}
        self._acumulador = AcumuladorFrecuencias()
        self._emitiendo = False
        self._cerrado = False

    def acumular(self, id_ejecucion: str, aporte: Palabra) -> None:
        """Suma el aporte, emitiendo los conteos previos si su registro nuevo no cabe."""
        self._validar_disponible()
        existente = self._acumulador.contiene_palabra(id_ejecucion, aporte)
        presupuesto = self._presupuestos.get(id_ejecucion)
        # Conserva un parcial completo cuya emisión falló hasta resolver su salida.
        if presupuesto is not None and presupuesto.completo:
            self._emitir_pendientes(id_ejecucion)
            existente = False
        if not existente:
            self._admitir_palabra(id_ejecucion, aporte)
        self._acumulador.acumular(id_ejecucion, aporte)
        if self._presupuestos[id_ejecucion].completo:
            self._emitir_pendientes(id_ejecucion)

    def _admitir_palabra(self, id_ejecucion, aporte):
        presupuesto = self._presupuestos.get(id_ejecucion)
        if presupuesto is None:
            presupuesto = self._crear_presupuesto(id_ejecucion)
        registro = ConteoPalabra(aporte.palabra, 1)
        if not presupuesto.admitir(registro):
            self._emitir_pendientes(id_ejecucion)
            presupuesto = self._crear_presupuesto(id_ejecucion)
            if not presupuesto.admitir(registro):
                raise ValueError('El registro no cabe en un parcial vacío')
        self._presupuestos[id_ejecucion] = presupuesto

    def finalizar_entrada(self, id_ejecucion: str) -> None:
        """Emite el resto antes del EOF; una entrada vacía no produce lotes.

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
            del self._presupuestos[id_ejecucion]
        finally:
            self._emitiendo = False

    def cerrar(self):
        """Libera parciales y presupuestos sin vaciarlos hacia la salida."""
        if self._cerrado:
            return
        if self._emitiendo:
            raise RuntimeError('No se puede cerrar durante una emisión')
        self._cerrado = True
        self._acumulador.cerrar()
        self._presupuestos.clear()

    def _validar_disponible(self) -> None:
        if self._cerrado:
            raise RuntimeError('El emisor de parciales está cerrado')
        if self._emitiendo:
            raise RuntimeError('No se pueden modificar parciales durante su emisión')
