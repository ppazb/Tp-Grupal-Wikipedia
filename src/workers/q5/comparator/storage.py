import logging
from pathlib import Path
from typing import Iterator

from src.common.protocol.registros import Candidato

logger = logging.getLogger(__name__)


class AlmacenamientoCandidatos:
    """Guarda candidatos en disco por ejecución para no sobrecargar la memoria RAM."""

    def __init__(self, directorio_base: str = "data/temp"):
        self._directorio = Path(directorio_base)
        self._directorio.mkdir(parents=True, exist_ok=True)

    def _ruta_archivo(self, id_ejecucion: str) -> Path:
        return self._directorio / f"q5_candidatos_{id_ejecucion}.bin"

    def guardar_candidatos(self, id_ejecucion: str, candidatos: list[Candidato]) -> None:
        """Agrega candidatos en modo binario append."""
        if not candidatos:
            return
        
        ruta = self._ruta_archivo(id_ejecucion)
        with open(ruta, "ab") as archivo:
            for c in candidatos:
                url_bytes = c.url.encode("utf-8")
                # 4 bytes longitud url, N bytes url, 8 bytes cantidad
                archivo.write(len(url_bytes).to_bytes(4, "big"))
                archivo.write(url_bytes)
                archivo.write(c.cantidad.to_bytes(8, "big"))

    def iterar_candidatos(self, id_ejecucion: str) -> Iterator[Candidato]:
        """Lee candidatos secuencialmente sin cargarlos todos a la vez en RAM."""
        ruta = self._ruta_archivo(id_ejecucion)
        if not ruta.is_file():
            return

        with open(ruta, "rb") as archivo:
            while True:
                len_bytes = archivo.read(4)
                if not len_bytes or len(len_bytes) < 4:
                    break
                url_len = int.from_bytes(len_bytes, "big")
                url_bytes = archivo.read(url_len)
                cant_bytes = archivo.read(8)
                if len(cant_bytes) < 8:
                    break
                url = url_bytes.decode("utf-8", errors="replace")
                cantidad = int.from_bytes(cant_bytes, "big")
                yield Candidato(url=url, cantidad=cantidad)

    def limpiar_ejecucion(self, id_ejecucion: str) -> None:
        """Elimina el archivo temporal asociado a la ejecución"""
        ruta = self._ruta_archivo(id_ejecucion)
        if ruta.is_file():
            try:
                ruta.unlink()
                logger.debug("AlmacenamientoCandidatos: eliminado temporal %s", ruta)
            except OSError as error:
                logger.warning("AlmacenamientoCandidatos: error al eliminar %s: %s", ruta, error)

