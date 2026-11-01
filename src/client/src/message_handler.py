import json

from ...common.external_protocol import codificar_filas
from ...common.protocol.primitivas import ErrorSerializacion

BYTES_8 = 8


class LoteDemasiadoGrande(ValueError):
    """El lote serializado no entra en un frame."""


def _texto(valor):
    # Un valor nulo viaja como texto vacío; el gateway decide si la fila es válida.
    if valor is None:
        return ""
    if hasattr(valor, "isoformat"):  
        return valor.isoformat()
    return valor if isinstance(valor, str) else str(valor)


def _nombres_secciones(secciones):
    """Nombres de las secciones del artículo; una sección sin nombre se omite.
    En el Parquet la columna viene como un string con JSON, no como lista. Se usa JSON 
    para poder acceder al dato nada mas
    """
    if isinstance(secciones, str):
        try:
            secciones = json.loads(secciones)
        except ValueError:
            return ()
    return tuple(
        seccion["name"] for seccion in secciones or ()
        if isinstance(seccion, dict) and isinstance(seccion.get("name"), str)
    )


def fila_plana(fila):
    """Aplana los structs del Parquet a los campos de texto que viajan al gateway."""
    imagen = fila.get("image")
    idioma = fila.get("in_language")
    return {
        "abstract": _texto(fila.get("abstract")),
        "date_modified": _texto(fila.get("date_modified")),
        "image_content_url": _texto(imagen.get("content_url") if isinstance(imagen, dict) else None),
        "in_language": _texto(idioma.get("identifier") if isinstance(idioma, dict) else None),
        "name": _texto(fila.get("name")),
        "sections": _nombres_secciones(fila.get("sections")),
        "url": _texto(fila.get("url")),
    }


# Serializa las filas crudas del Parquet para el gateway (que es quien arma los artículos)
# y lleva la cuenta de lotes enviados y confirmados.
class ManejadorMensajes:
    def __init__(self, max_bytes):
        self._max_bytes = max_bytes
        self.lotes_confirmados = 0

    def serializar_lote(self, filas):
        try:
            return codificar_filas([fila_plana(fila) for fila in filas], self._max_bytes)
        except ErrorSerializacion as error:
            raise LoteDemasiadoGrande(str(error)) from error

    def serializar_eof(self):
        # El gateway verifica que la cantidad de lotes anunciada coincida con la recibida
        return self.lotes_confirmados.to_bytes(BYTES_8, "big")

    def validar_ack(self, contenido, esperado):
        # True si el ACK es valido y con la cantidad de lotes esperados
        # 8 bytes, entero sin signo big-endian
        return len(contenido) == BYTES_8 and int.from_bytes(contenido, "big") == esperado

    def lote_confirmado(self):
        self.lotes_confirmados += 1
