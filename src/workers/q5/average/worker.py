import logging
from typing import Dict

from src.middleware.interfaz import OperacionInterrumpida
from src.workers.base_worker import WorkerBase
from src.workers.deduplicacion import DedupDesdeCompartida
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.primitivas import BYTES_U64
from src.common.protocol.registros import Esquema, Lote, CantidadPalabras, ResumenHistorico

logger = logging.getLogger(__name__)

ENTRADA = "cantidades_historicas"
TIPO_LOTE = 1  # registro del WAL: identidad del lote + suma + cantidad de artículos


class AverageQ5Worker(WorkerBase):
    """Calcula la suma y cantidad de artículos históricos (<= 2020) para determinar el promedio.

    Cada lote procesado se agrega al WAL de su ejecución; al arrancar, el estado y la
    deduplicación se reconstruyen reaplicando esos registros.
    """

    def __init__(self, ruta_configuracion: str, directorio_wal: str = None):
        super().__init__(ruta_configuracion)
        # Diccionario aislado por ejecución para soporte multi-client concurrente
        self._acumuladores: Dict[str, Dict[str, int]] = {}
        self.registrar_limpieza(self._acumuladores.clear)
        # Recibe de word_occurrences, que usa cola compartida
        self._dedup = DedupDesdeCompartida()
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        self._recuperar()

    def _recuperar(self) -> None:
        for id_ejecucion, registros in self._wals.recuperar():
            for datos in registros:
                _, identidad, lector = decodificar(datos)
                suma, articulos = lector.entero(BYTES_U64), lector.entero(BYTES_U64)
                lector.finalizar()
                self._sumar(id_ejecucion, suma, articulos)
                self._dedup.registrar(entrega_de(Entrada(id_ejecucion, ENTRADA), identidad))

    def _sumar(self, id_ejecucion: str, suma: int, articulos: int) -> None:
        totales = self._acumuladores.setdefault(id_ejecucion, {"suma": 0, "articulos": 0})
        totales["suma"] += suma
        totales["articulos"] += articulos

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q5_average", id_ejecucion)
        entrega = self.middleware.entrega_actual
        if self._wals.esta_cerrada(id_ejecucion) or self._dedup.es_duplicado(entrega):
            return

        subtotal_palabras = 0
        articulos_lote = 0
        for registro in datos.contenido.registros:
            self.comprobar_detencion()
            if isinstance(registro, CantidadPalabras):
                subtotal_palabras += registro.cantidad
                articulos_lote += 1

        # Este worker no publica por lote: solo persiste y después actualiza la memoria
        self._wals.agregar(id_ejecucion, codificar(TIPO_LOTE, entrega, lambda escritor: (
            escritor.entero(subtotal_palabras, BYTES_U64), escritor.entero(articulos_lote, BYTES_U64),
        )))
        self._sumar(id_ejecucion, subtotal_palabras, articulos_lote)
        self._dedup.registrar(entrega)

        logger.debug(
            "q5_average: lote de %d artículos para '%s'. Acumulado: %d palabras en %d artículos",
            articulos_lote, id_ejecucion,
            self._acumuladores[id_ejecucion]["suma"],
            self._acumuladores[id_ejecucion]["articulos"]
        )

    def al_finalizar(self, entrada: Entrada) -> None:
        """Al recibir EOF consolidado de históricos, emite el resumen a q5_comparator y cierra la ejecución."""
        id_ejecucion = entrada.id_ejecucion
        if self._wals.esta_cerrada(id_ejecucion):
            return

        totales = self._acumuladores.get(id_ejecucion, {"suma": 0, "articulos": 0})
        resumen = ResumenHistorico(suma=totales["suma"], articulos=totales["articulos"])
        entrada_resumen = Entrada(id_ejecucion, "resumen_historico")
        lote_resumen = Lote(Esquema.RESUMEN_HISTORICO, (resumen,))

        try:
            self.middleware.publicar(self.envio(
                "q5_comparator", Datos(entrada_resumen, lote_resumen), clave_afinidad=id_ejecucion,
            ))
            self.middleware.finalizar_entrada("q5_comparator", entrada_resumen)
        except OperacionInterrumpida:
            raise
        except Exception as error:
            logger.error("q5_average: error al publicar resumen a q5_comparator: %s", error)
            raise

        self._wals.cerrar_ejecucion(id_ejecucion)
        self._acumuladores.pop(id_ejecucion, None)
        self._dedup.cerrar_ejecucion(id_ejecucion)
