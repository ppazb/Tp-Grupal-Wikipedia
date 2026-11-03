import logging

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, SolicitudImagen

logger = logging.getLogger(__name__)

SECCIONES_BIOGRAFICAS = frozenset({"biography", "biographie"})


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
    y emite la solicitud (nombre, url, url_imagen) de descargar la imagen
    """

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q2_filter_bio", datos.entrada.id_ejecucion) #aviso que llego cliente

        solicitudes = []
        for ref in datos.contenido.registros:
            self.comprobar_detencion()
            if _es_biografico(ref.secciones, self.comprobar_detencion):
                solicitudes.append(SolicitudImagen(ref.nombre, ref.url, ref.url_imagen))

        id_ejecucion = datos.entrada.id_ejecucion

        logger.info("q2_filter_bio: %d/%d artículos bibliografia para ejecución '%s'",len(solicitudes), len(datos.contenido.registros), id_ejecucion)

        if solicitudes:
            entrada = Entrada(id_ejecucion, "biografias_q2")
            lote = Lote(Esquema.SOLICITUD_IMAGEN, tuple(solicitudes))
            self.middleware.publicar("q2_image_downloader", Datos(entrada, lote))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga (EOF) hacia el descargador de imágenes."""
        logger.info("q2_filter_bio: cierre (EOF) recibido para '%s'. Propagando a q2_image_downloader.",entrada.id_ejecucion)
        self.middleware.finalizar_entrada("q2_image_downloader", Entrada(entrada.id_ejecucion, "biografias_q2"))
