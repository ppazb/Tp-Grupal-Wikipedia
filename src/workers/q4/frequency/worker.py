"""Worker de Frecuencia: aportes por abstract, parciales y cierre de cada ejecución."""

import logging

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema
from src.workers.base_worker import Worker
from .parciales import EmisorParciales


REGISTRO = logging.getLogger(__name__)


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
        self._parciales = EmisorParciales(
            self._emitir, self._crear_presupuesto, comprobar_detencion=self.comprobar_detencion,
        )
        self.registrar_limpieza(self._parciales.cerrar)

    def procesar(self, datos):
        """Suma cada presencia recibida, sin deduplicar palabras entre abstracts."""
        self._validar_entrada(datos.entrada)
        self.anunciar_cliente("q4_frequency", datos.entrada.id_ejecucion)
        if datos.contenido.esquema is not Esquema.PALABRA:
            raise ValueError('Frecuencia requiere lotes con esquema PALABRA')
        REGISTRO.debug('Frecuencia: ejecución %s, lote recibido con %d aportes',
                       datos.entrada.id_ejecucion, len(datos.contenido.registros))
        for aporte in datos.contenido.registros:
            self.comprobar_detencion()
            self._parciales.acumular(datos.entrada.id_ejecucion, aporte)

    def finalizar_entrada(self, entrada):
        """Vacía el último parcial antes de publicar el EOF, incluso sin aportes."""
        self._validar_entrada(entrada)
        REGISTRO.info('Frecuencia: ejecución %s, entrada completa; emitiendo pendientes',
                      entrada.id_ejecucion)
        self._parciales.finalizar_entrada(entrada.id_ejecucion)
        self._middleware.finalizar_entrada(
            self._grupo_salida, Entrada(entrada.id_ejecucion, self._entrada_salida),
        )
        REGISTRO.info('Frecuencia: ejecución %s, parciales enviados y cierre publicado hacia %s/%s',
                      entrada.id_ejecucion, self._grupo_salida, self._entrada_salida)

    def _crear_presupuesto(self, id_ejecucion):
        return self._middleware.crear_presupuesto_lote(
            Entrada(id_ejecucion, self._entrada_salida), Esquema.CONTEO_PALABRA,
        )

    def _emitir(self, id_ejecucion, lote):
        datos = Datos(Entrada(id_ejecucion, self._entrada_salida), lote)
        self._middleware.publicar(
            self._grupo_salida, datos, clave_afinidad=id_ejecucion,
        )
        REGISTRO.debug('Frecuencia: ejecución %s, parcial publicado con %d conteos',
                       id_ejecucion, len(lote.registros))

    def _validar_entrada(self, entrada):
        if entrada.nombre != self._entrada:
            raise ValueError('La entrada no corresponde al worker de Frecuencia')
