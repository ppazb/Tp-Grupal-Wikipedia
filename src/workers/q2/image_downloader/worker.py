import hashlib
import logging
import os

import requests

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, FragmentoImagen, Lote

logger = logging.getLogger(__name__)

GRUPO_RESULTADOS = "gateway_resultados"
ENTRADA_RESULTADOS = "resultados_finales_q2"

# La política de Wikimedia pide un User-Agent con datos de contacto (por ejemplo la URL del proyecto);
# sin ellos responde 429 a las descargas. Se puede sobrescribir con IMAGE_USER_AGENT.
USER_AGENT = os.environ.get("IMAGE_USER_AGENT","TpGrupalWikipedia/1.0 (https://github.com/ppazb/Tp-Grupal-Wikipedia) python-requests")
TIMEOUT_SEGUNDOS = 15
MAX_INTENTOS = 2
ESPERA_ENTRE_INTENTOS_SEGUNDOS = 1.0


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
    con un único fragmento vacío (total_bytes = 0). Si la descarga falla tras MAX_INTENTOS,
    se descarta y se sigue con el siguiente.
    """

    def __init__(self, ruta_configuracion):
        super().__init__(ruta_configuracion)
        self._sesion = requests.Session()
        self.registrar_limpieza(self._sesion.close)
        self._sesion.headers["User-Agent"] = USER_AGENT
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
                logger.warning("q2_image_downloader: no se pudo descargar la imagen de '%s', se descarta",
                               solicitud.url)
                continue
            self._publicar_imagen(lote, solicitud, imagen)
            publicadas += 1
        lote.publicar()

        logger.info("q2_image_downloader: %d/%d artículos publicados para ejecución '%s'",
                    publicadas, len(datos.contenido.registros), id_ejecucion)

    def _descargar(self, url_imagen: str):
        """Intenta hasta MAX_INTENTOS veces; devuelve None si todos fallan."""
        for intento in range(1, MAX_INTENTOS + 1):
            self.comprobar_detencion()
            try:
                with self._sesion.get(url_imagen, timeout=TIMEOUT_SEGUNDOS, stream=True) as respuesta:
                    self.comprobar_detencion()
                    respuesta.raise_for_status()
                    return self._leer_imagen(respuesta)
            except requests.RequestException as error:
                self.comprobar_detencion()
                logger.warning("q2_image_downloader: intento %d/%d falló para %s: %s",
                               intento, MAX_INTENTOS, url_imagen, error)
                if intento < MAX_INTENTOS and not self._detenido:
                    self._apagado.esperar(ESPERA_ENTRE_INTENTOS_SEGUNDOS)
                    self.comprobar_detencion()
        return None

    def _leer_imagen(self, respuesta):
        """Lee por bloques y cancela la descarga antes de publicar una imagen incompleta."""
        imagen = bytearray()
        for bloque in respuesta.iter_content(chunk_size=self._bytes_fragmento):
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
