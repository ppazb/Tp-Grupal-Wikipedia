"""DTO y códigos de los registros transportados en lotes binarios."""

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Tuple, Union


class Esquema(IntEnum):
    """Identifica el formato de todos los registros de un lote."""

    ARTICULO = 0x0001
    TEXTO_ARTICULO = 0x0002
    REFERENCIA_HISTORICA = 0x0003
    REFERENCIA_BIOGRAFICA = 0x0004
    SOLICITUD_IMAGEN = 0x0005
    PALABRA_IDIOMA = 0x0006
    PALABRA = 0x0007
    CONTEO_PALABRA = 0x0008
    CANTIDAD_PALABRAS = 0x0009
    CANDIDATO = 0x000A
    RESUMEN_HISTORICO = 0x000B
    RESULTADO_Q1 = 0x000C
    FRAGMENTO_IMAGEN = 0x000D
    ERROR_REGISTRO = 0x000E


class Idioma(IntEnum):
    """Idiomas admitidos por los registros de artículos y palabras."""

    INGLES = 1
    FRANCES = 2


class CodigoError(IntEnum):
    """Clasifica rechazos de datos informados al gateway."""

    REGISTRO_INVALIDO = 1
    VALIDACION_NEGOCIO = 2
    MENSAJE_INVALIDO = 3


@dataclass(frozen=True)
class Articulo:
    """Campos del artículo utilizados por las ramas de consultas."""

    url: str
    nombre: str
    fecha: str
    idioma: Idioma
    abstract: str
    secciones: Tuple[str, ...]
    url_imagen: str


@dataclass(frozen=True)
class TextoArticulo:
    """Texto e identidad del artículo para extraer palabras."""

    url: str
    idioma: Idioma
    abstract: str


@dataclass(frozen=True)
class ReferenciaHistorica:
    """Referencia y fecha original para el filtro de idioma."""

    url: str
    fecha: str
    idioma: Idioma


@dataclass(frozen=True)
class ReferenciaBiografica:
    """Campos utilizados por el filtro biográfico."""

    nombre: str
    url: str
    secciones: Tuple[str, ...]
    url_imagen: str


@dataclass(frozen=True)
class SolicitudImagen:
    """Identifica el artículo y la imagen a descargar."""

    nombre: str
    url: str
    url_imagen: str


@dataclass(frozen=True)
class PalabraIdioma:
    """Presencia de una palabra en un idioma."""

    palabra: str
    idioma: Idioma


@dataclass(frozen=True)
class Palabra:
    """Palabra aportada o emitida por una consulta."""

    palabra: str


@dataclass(frozen=True)
class ConteoPalabra:
    """Cantidad positiva de aportes de una palabra."""

    palabra: str
    cantidad: int


@dataclass(frozen=True)
class CantidadPalabras:
    """Cantidad de palabras de un artículo histórico."""

    cantidad: int


@dataclass(frozen=True)
class Candidato:
    """Artículo y cantidad de palabras para comparar o publicar en Q5."""

    url: str
    cantidad: int


@dataclass(frozen=True)
class ResumenHistorico:
    """Suma de palabras y cantidad de artículos; cero artículos indica ausencia."""

    suma: int
    articulos: int


@dataclass(frozen=True)
class ResultadoQ1:
    """URL del artículo y fecha conservada sin normalización."""

    url: str
    fecha: str


@dataclass(frozen=True)
class FragmentoImagen:
    """Fragmento de una transferencia identificada dentro de su ejecución."""

    id_transferencia: str
    nombre: str
    url: str
    total_bytes: int
    desplazamiento: int
    fragmento: bytes


@dataclass(frozen=True)
class ErrorRegistro:
    """Rechazo de datos; el sobre identifica al proceso que lo informa."""

    entrada_afectada: str
    esquema_afectado: Optional[int]
    codigo: CodigoError
    indice_registro: Optional[int]
    referencia: Optional[str]
    detalle: str


Registro = Union[
    Articulo, TextoArticulo, ReferenciaHistorica, ReferenciaBiografica,
    SolicitudImagen, PalabraIdioma, Palabra, ConteoPalabra, CantidadPalabras,
    Candidato, ResumenHistorico, ResultadoQ1, FragmentoImagen, ErrorRegistro,
]


@dataclass(frozen=True)
class Lote:
    """Secuencia inmutable de registros con un único esquema explícito."""

    esquema: Esquema
    registros: Tuple[Registro, ...]


@dataclass(frozen=True)
class ErrorDecodificacionRegistro:
    """Rechazo de un registro delimitado, con su índice original desde cero."""

    indice: int
    codigo: CodigoError
    detalle: str
