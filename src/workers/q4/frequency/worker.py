"""Worker de Frecuencia: aportes por abstract, parciales y cierre de cada ejecución."""

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema
from src.workers.base_worker import Worker
from .parciales import EmisorParciales


class WorkerFrecuencia(Worker):
    """Acumula palabras y entrega conteos al grupo configurado de consolidación."""

    def __init__(self, middleware, *, entrada,
                 grupo_salida, entrada_salida):
        super().__init__(middleware)
        for nombre in (entrada, grupo_salida, entrada_salida):
            if type(nombre) is not str or not nombre.strip():
                raise ValueError('Los nombres de entrada y salida deben ser textos no vacíos')
        self._entrada = entrada
        self._grupo_salida = grupo_salida
        self._entrada_salida = entrada_salida
        self._parciales = EmisorParciales(self._emitir, self._crear_presupuesto)
        self.registrar_limpieza(self._parciales.cerrar)

    def procesar(self, datos):
        """Suma cada presencia recibida, sin deduplicar palabras entre abstracts."""
        self._validar_entrada(datos.entrada)
        if datos.contenido.esquema is not Esquema.PALABRA:
            raise ValueError('Frecuencia requiere lotes con esquema PALABRA')
        for aporte in datos.contenido.registros:
            self.comprobar_detencion()
            self._parciales.acumular(datos.entrada.id_ejecucion, aporte)

    def finalizar_entrada(self, entrada):
        """Vacía el último parcial antes de publicar el EOF, incluso sin aportes."""
        self._validar_entrada(entrada)
        self._parciales.finalizar_entrada(entrada.id_ejecucion)
        self._middleware.finalizar_entrada(
            self._grupo_salida, Entrada(entrada.id_ejecucion, self._entrada_salida),
        )

    def _crear_presupuesto(self, id_ejecucion):
        return self._middleware.crear_presupuesto_lote(
            Entrada(id_ejecucion, self._entrada_salida), Esquema.CONTEO_PALABRA,
        )

    def _emitir(self, id_ejecucion, lote):
        datos = Datos(Entrada(id_ejecucion, self._entrada_salida), lote)
        self._middleware.publicar(
            self._grupo_salida, datos, clave_afinidad=id_ejecucion,
        )

    def _validar_entrada(self, entrada):
        if entrada.nombre != self._entrada:
            raise ValueError('La entrada no corresponde al worker de Frecuencia')
