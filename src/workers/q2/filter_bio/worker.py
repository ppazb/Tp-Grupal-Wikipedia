from src.workers.base_worker import WorkerBase
from src.workers.deduplicacion import DedupPorOrigen
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, SolicitudImagen


SECCIONES_BIOGRAFICAS = frozenset({"biography", "biographie"})
ENTRADA = "historicos_q2"
TIPO_LOTE = 1  # registro del WAL: solo la identidad del lote


def _es_biografico(secciones, comprobar_detencion) -> bool:
    """Indica si alguna sección es 'biography' o 'biographie'."""
    for seccion in secciones:
        comprobar_detencion()
        if seccion.strip().lower() in SECCIONES_BIOGRAFICAS:
            return True
    return False


class FilterBioQ2(WorkerBase):
    """
    Filtra los artículos que tienen una sección biography/biographie
    y emite la solicitud (nombre, url, url_imagen) de descargar la imagen.

    Descarta los lotes repetidos para que un duplicado no dispare otra consulta a la API
    de imágenes. Recuerda en su WAL solo la identidad de cada lote procesado.
    """

    def __init__(self, ruta_configuracion: str, directorio_wal: str = None):
        super().__init__(ruta_configuracion)
        self._dedup = DedupPorOrigen(self.configuracion.limites.ventana_lotes)
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        for id_ejecucion, registros in self._wals.recuperar():
            for datos in registros:
                _, identidad, lector = decodificar(datos)
                lector.finalizar()
                self._dedup.registrar(entrega_de(Entrada(id_ejecucion, ENTRADA), identidad))

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q2_filter_bio", id_ejecucion)
        entrega = self.middleware.entrega_actual
        if self._wals.esta_cerrada(id_ejecucion) or self._dedup.es_duplicado(entrega):
            return

        solicitudes = []
        for ref in datos.contenido.registros:
            self.comprobar_detencion()
            if _es_biografico(ref.secciones, self.comprobar_detencion):
                solicitudes.append(SolicitudImagen(ref.nombre, ref.url, ref.url_imagen))

        if solicitudes:
            entrada = Entrada(id_ejecucion, "biografias_q2")
            lote = Lote(Esquema.SOLICITUD_IMAGEN, tuple(solicitudes))
            self.middleware.publicar(self.envio("q2_image_downloader", Datos(entrada, lote)))

        self._wals.agregar(id_ejecucion, codificar(TIPO_LOTE, entrega))
        self._dedup.registrar(entrega)

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga (EOF) hacia el descargador de imágenes y cierra la ejecución."""
        id_ejecucion = entrada.id_ejecucion
        if self._wals.esta_cerrada(id_ejecucion):
            return
        self.middleware.finalizar_entrada("q2_image_downloader", Entrada(id_ejecucion, "biografias_q2"))
        self._wals.cerrar_ejecucion(id_ejecucion)
        self._dedup.cerrar_ejecucion(id_ejecucion)
