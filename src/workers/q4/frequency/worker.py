"""Worker de Frecuencia: aportes por abstract, parciales y cierre de cada ejecución."""

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.primitivas import BYTES_U32
from src.common.protocol.registros import Esquema, Palabra
from src.workers.base_worker import Worker
from src.workers.deduplicacion import DedupPorOrigen
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from .parciales import EmisorParciales


TIPO_LOTE = 1  # registro del WAL: identidad del lote + las palabras que aportó, en orden


class WorkerFrecuencia(Worker):
    """Acumula palabras y entrega conteos al grupo configurado de consolidación.

    Cada lote procesado se agrega al WAL de su ejecución con sus palabras. Al arrancar se
    reaplican en orden: el acumulador y sus emisiones son determinísticos, así que el
    estado queda igual y los parciales que se vuelvan a emitir repiten sus identificadores.
    """

    def __init__(self, middleware, *, entrada, grupo_salida, entrada_salida,
                 ventana_lotes, directorio_wal=None):
        super().__init__(middleware)
        for nombre in (entrada, grupo_salida, entrada_salida):
            if type(nombre) is not str or not nombre.strip():
                raise ValueError('Los nombres de entrada y salida deben ser textos no vacíos')
        self._entrada = entrada
        self._grupo_salida = grupo_salida
        self._entrada_salida = entrada_salida
        self._recuperando = False
        self._parciales = EmisorParciales(
            self._emitir, self._crear_presupuesto, comprobar_detencion=self.comprobar_detencion,
        )
        self.registrar_limpieza(self._parciales.cerrar)
        self._dedup = DedupPorOrigen(ventana_lotes)
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        self._recuperar()

    def _recuperar(self):
        """Reaplica los lotes del WAL sin publicar: las emisiones ya se confirmaron o se repiten igual."""
        self._recuperando = True
        try:
            for id_ejecucion, registros in self._wals.recuperar():
                for datos in registros:
                    _, identidad, lector = decodificar(datos)
                    for _ in range(lector.entero(BYTES_U32)):
                        self._parciales.acumular(id_ejecucion, Palabra(lector.texto()))
                    lector.finalizar()
                    self._dedup.registrar(entrega_de(Entrada(id_ejecucion, self._entrada), identidad))
        finally:
            self._recuperando = False

    def procesar(self, datos):
        """Suma cada presencia recibida, sin deduplicar palabras entre abstracts."""
        self._validar_entrada(datos.entrada)
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q4_frequency", id_ejecucion)
        if datos.contenido.esquema is not Esquema.PALABRA:
            raise ValueError('Frecuencia requiere lotes con esquema PALABRA')
        entrega = self._middleware.entrega_actual
        if self._wals.esta_cerrada(id_ejecucion) or self._dedup.es_duplicado(entrega):
            return
        palabras = []
        for aporte in datos.contenido.registros:
            self.comprobar_detencion()
            self._parciales.acumular(id_ejecucion, aporte)
            palabras.append(aporte.palabra)
        # Primero se emiten los parciales y después se persiste: si hay una caída en el medio,
        # el lote se reprocesa y los parciales salen con los mismos ids.
        self._wals.agregar(id_ejecucion, codificar(TIPO_LOTE, entrega, lambda escritor: (
            escritor.entero(len(palabras), BYTES_U32), [escritor.texto(palabra) for palabra in palabras],
        )))
        self._dedup.registrar(entrega)

    def finalizar_entrada(self, entrada):
        """Vacía el último parcial antes de publicar el EOF, incluso sin aportes."""
        self._validar_entrada(entrada)
        if self._wals.esta_cerrada(entrada.id_ejecucion):
            return
        self._parciales.finalizar_entrada(entrada.id_ejecucion)
        self._middleware.finalizar_entrada(
            self._grupo_salida, Entrada(entrada.id_ejecucion, self._entrada_salida),
        )
        self._wals.cerrar_ejecucion(entrada.id_ejecucion)
        self._dedup.cerrar_ejecucion(entrada.id_ejecucion)

    def _crear_presupuesto(self, id_ejecucion):
        return self._middleware.crear_presupuesto_lote(
            Entrada(id_ejecucion, self._entrada_salida), Esquema.CONTEO_PALABRA,
        )

    def _emitir(self, id_ejecucion, lote):
        if self._recuperando:
            return
        datos = Datos(Entrada(id_ejecucion, self._entrada_salida), lote)
        self._middleware.publicar(self.envio(
            self._grupo_salida, datos, clave_afinidad=id_ejecucion,
        ))

    def _validar_entrada(self, entrada):
        if entrada.nombre != self._entrada:
            raise ValueError('La entrada no corresponde al worker de Frecuencia')
