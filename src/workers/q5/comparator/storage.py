import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from src.common.protocol.registros import Candidato
from src.common.io_adapters.archivos import leer_completo, escribir_completo
from src.common.protocol.primitivas import BYTES_U32, BYTES_U64

logger = logging.getLogger(__name__)


class AlmacenamientoCandidatos:
    """Guarda candidatos en disco por ejecución para no sobrecargar la memoria RAM."""

    def __init__(self, directorio_base: str = "data/temp", *, comprobar_detencion=None):
        self._comprobar_detencion = comprobar_detencion
        base = Path(directorio_base)
        base.mkdir(parents=True, exist_ok=True)
        self._temporal = TemporaryDirectory(prefix="q5-candidatos-", dir=base)
        self._directorio = Path(self._temporal.name)
        self._archivos: dict[str, Path] = {}

    def _ruta_archivo(self, id_ejecucion: str) -> Path:
        return self._directorio / f"q5_candidatos_{id_ejecucion}.bin"

    def guardar_candidatos(self, id_ejecucion: str, candidatos: list[Candidato]) -> None:
        """Agrega candidatos en modo binario append agrupados en un solo bloque."""
        if not candidatos:
            return
        
        ruta = self._ruta_archivo(id_ejecucion)
        bloque = bytearray()
        for c in candidatos:
            if self._comprobar_detencion is not None:
                self._comprobar_detencion()
            url_bytes = c.url.encode("utf-8")
            bloque.extend(len(url_bytes).to_bytes(BYTES_U32, "big"))
            bloque.extend(url_bytes)
            bloque.extend(c.cantidad.to_bytes(BYTES_U64, "big"))

        with open(ruta, "ab") as archivo:
            self._archivos[id_ejecucion] = ruta
            escribir_completo(archivo, bloque, self._comprobar_detencion)

    def iterar_candidatos(self, id_ejecucion: str) -> Iterator[Candidato]:
        """Lee candidatos secuencialmente sin cargarlos todos a la vez en RAM."""
        ruta = self._archivos.get(id_ejecucion)
        if ruta is None:
            return

        with ruta.open("rb") as archivo:
            while True:
                len_bytes = leer_completo(
                    archivo, BYTES_U32, admitir_fin=True,
                    comprobar_detencion=self._comprobar_detencion,
                )
                if len_bytes is None:
                    return
                url_len = int.from_bytes(len_bytes, "big")
                if url_len == 0:
                    raise ValueError("URL vacía en el archivo de candidatos")
                url_bytes = leer_completo(archivo, url_len, comprobar_detencion=self._comprobar_detencion)
                cant_bytes = leer_completo(archivo, BYTES_U64, comprobar_detencion=self._comprobar_detencion)
                url = url_bytes.decode("utf-8")
                if not url.strip():
                    raise ValueError("URL vacía en el archivo de candidatos")
                cantidad = int.from_bytes(cant_bytes, "big")
                yield Candidato(url=url, cantidad=cantidad)

    def limpiar_ejecucion(self, id_ejecucion: str) -> None:
        """Elimina el archivo de una ejecución; propaga errores y admite repetir la limpieza."""
        ruta = self._archivos.get(id_ejecucion)
        if ruta is not None:
            ruta.unlink(missing_ok=True)
            del self._archivos[id_ejecucion]
            logger.debug("AlmacenamientoCandidatos: eliminado temporal %s", ruta)

    def cerrar(self):
        """Elimina únicamente los temporales de esta instancia, incluso sin EOF."""
        self._temporal.cleanup()
        self._archivos.clear()
