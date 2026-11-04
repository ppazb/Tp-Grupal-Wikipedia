import hashlib
import logging
import os

import requests

from src.workers.base_worker import WorkerBase
from src.common.io_adapters.descargas_http import DescargadorHTTP, DescargaAgotada, ESTADOS_AUSENTES
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, FragmentoImagen, Lote

logger = logging.getLogger(__name__)

GRUPO_RESULTADOS = "gateway_resultados"
ENTRADA_RESULTADOS = "resultados_finales_q2"

# La política de Wikimedia pide un User-Agent con datos de contacto (por ejemplo la URL del proyecto);
# sin ellos responde 429 a las descargas. Se puede sobrescribir con IMAGE_USER_AGENT.
USER_AGENT = os.environ.get("IMAGE_USER_AGENT","TpGrupalWikipedia/1.0 (https://github.com/ppazb/Tp-Grupal-Wikipedia) python-requests")
TIMEOUT_SEGUNDOS = 15


class _LoteFragmentos:
    """
    Junta fragmentos hasta llenar el mensaje (según el presupuesto de bytes del middleware).
    Así se publica un mensaje por lote y no uno por fragmento.
    """

    def __init__(self, middleware, entrada):
        self._middleware = middleware
        self._entrada = entrada
        self._registros = []
        self._presupuesto = None

    def agregar(self, fragmento):
        if self._presupuesto is None:
            self._presupuesto = self._middleware.crear_presupuesto_lote(self._entrada, Esquema.FRAGMENTO_IMAGEN)

        if not self._presupuesto.admitir(fragmento):
            self.publicar()
            self._presupuesto = self._middleware.crear_presupuesto_lote(self._entrada, Esquema.FRAGMENTO_IMAGEN)
            if not self._presupuesto.admitir(fragmento):
                raise ValueError("Un fragmento de imagen no cabe en un lote vacío")

        self._registros.append(fragmento)

    def publicar(self):
        if self._registros:
            self._middleware.publicar(GRUPO_RESULTADOS, Datos(self._entrada, Lote(Esquema.FRAGMENTO_IMAGEN, tuple(self._registros))))

        self._registros = []
        self._presupuesto = None


class ImageDownloaderQ2(WorkerBase):
    """
    Descarga la imagen de cada artículo biográfico y la envía al gateway en fragmentos,
    para respetar limites.max_bytes_por_mensaje. Un artículo sin imagen se informa igual,
    con un único fragmento vacío (total_bytes = 0). Reintenta errores temporales
    hasta IMAGE_MAX_RETRIES veces (sin tope si no se define) o hasta recibir una solicitud de apagado;
    si se agotan, el artículo se publica sin imagen. Omite HTTP 404/410.
    """

    def __init__(self, ruta_configuracion):
        super().__init__(ruta_configuracion)
        self._sesion = requests.Session()
        self.registrar_limpieza(self._sesion.close)
        self._sesion.headers["User-Agent"] = USER_AGENT
        replicas = self.configuracion.obtener_grupo('q2_image_downloader').instancias
        self._descargador = DescargadorHTTP(
            esperar=lambda segundos: self.middleware.esperar(segundos),
            comprobar=self.comprobar_detencion, replicas=replicas,
        )
        # Deja margen para el sobre y los campos de texto del fragmento
        self._bytes_fragmento = self.configuracion.limites.max_bytes_por_mensaje // 4

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q2_image_downloader", datos.entrada.id_ejecucion)
        id_ejecucion = datos.entrada.id_ejecucion
        lote = _LoteFragmentos(self.middleware, Entrada(id_ejecucion, ENTRADA_RESULTADOS))
        publicadas = 0

        for solicitud in datos.contenido.registros:
            self.comprobar_detencion()
            # Sin imagen el artículo igual forma parte de la respuesta
            imagen = self._descargar(solicitud.url_imagen) if solicitud.url_imagen else b""
            if imagen is None:
                logger.info("q2_image_downloader: imagen inexistente para '%s', se descarta",
                            solicitud.url)
                continue
            self._publicar_imagen(lote, solicitud, imagen)
            publicadas += 1
        lote.publicar()

        logger.info("q2_image_downloader: %d/%d artículos publicados para ejecución '%s'",
                    publicadas, len(datos.contenido.registros), id_ejecucion)

    def _descargar(self, url_imagen: str):
        """Descarga con ritmo acotado; solo omite imágenes inexistentes o eliminadas."""
        try:
            return self._descargador.descargar(self._sesion, url_imagen, TIMEOUT_SEGUNDOS, self._leer_imagen)
        except DescargaAgotada as error:
            logger.warning("Se agotaron los reintentos, el artículo se publica sin imagen: %s", error)
            return b""
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code in ESTADOS_AUSENTES:
                logger.info("Imagen inexistente: %s (%s)", url_imagen, error.response.status_code)
                return None
            raise

    def _leer_imagen(self, respuesta):
        """Lee por bloques y cancela la descarga antes de publicar una imagen incompleta."""
        imagen = bytearray()
        for bloque in self._descargador.bloques(respuesta, self._bytes_fragmento):
            self.comprobar_detencion()
            imagen.extend(bloque)
        self.comprobar_detencion()
        return bytes(imagen)

    def _publicar_imagen(self, lote, solicitud, imagen: bytes) -> None:
        """Agrega los fragmentos de la imagen al lote en armado, publicándolo cuando se llena."""

        # Determinístico: si el lote se reentrega, el cliente reconoce la misma imagen
        id_transferencia = hashlib.sha1(solicitud.url.encode("utf-8")).hexdigest()
        total = len(imagen)

        # Una imagen vacía igual emite un fragmento, para que el artículo figure en la respuesta
        for desplazamiento in range(0, max(total, 1), self._bytes_fragmento):
            self.comprobar_detencion()

            lote.agregar(FragmentoImagen(
                id_transferencia, solicitud.nombre, solicitud.url, total, desplazamiento,
                imagen[desplazamiento:desplazamiento + self._bytes_fragmento],
            ))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Indica al gateway (EOF) de la Query 2."""

        logger.info("q2_image_downloader: cierre (EOF) recibido para '%s'. Notificando a gateway_resultados.", entrada.id_ejecucion)

        self.middleware.finalizar_entrada(GRUPO_RESULTADOS, Entrada(entrada.id_ejecucion, ENTRADA_RESULTADOS))
