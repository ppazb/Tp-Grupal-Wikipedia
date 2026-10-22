"""Serialización JSON UTF-8 con representación explícita de contenido binario."""

import base64
import binascii
import json
import math


MARCA_BYTES = "__bytes_base64__"


class ErrorSerializacion(ValueError):
    """El valor no se puede representar o el cuerpo recibido es inválido."""


def serializar_json(valor: object) -> bytes:
    """Codifica valores JSON, tuplas y bytes; las tuplas se reciben como listas.

    No convierte objetos arbitrarios ni DTO de negocio implícitamente. Los
    diccionarios deben tener claves de texto y no usar la marca binaria reservada.
    """
    try:
        _validar_valor(valor)
        return json.dumps(
            valor, default=_codificar_bytes, ensure_ascii=False,
            allow_nan=False, separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise ErrorSerializacion("El contenido no se puede serializar como JSON UTF-8") from error


def deserializar_json(cuerpo: bytes) -> object:
    """Decodifica JSON estricto y recupera los bytes marcados como base64."""
    if not isinstance(cuerpo, bytes):
        raise ErrorSerializacion("El cuerpo debe ser bytes")
    try:
        valor = json.loads(
            cuerpo.decode("utf-8"), object_pairs_hook=_decodificar_objeto,
            parse_constant=_rechazar_constante,
        )
        _validar_valor(valor)
        return valor
    except (TypeError, ValueError, RecursionError) as error:
        raise ErrorSerializacion("El cuerpo no contiene JSON UTF-8 válido") from error


def _validar_valor(valor):
    """Valida tipos admitidos, claves de texto, UTF-8 y números finitos."""
    if valor is None or type(valor) in (bool, int, bytes):
        return
    if type(valor) is str:
        valor.encode("utf-8")
        return
    if type(valor) is float:
        if not math.isfinite(valor):
            raise ValueError("No se admiten números no finitos")
        return
    if type(valor) in (list, tuple):
        for elemento in valor:
            _validar_valor(elemento)
        return
    if type(valor) is dict:
        if MARCA_BYTES in valor:
            raise ValueError("El contenido usa una clave reservada para bytes")
        for clave, elemento in valor.items():
            if type(clave) is not str:
                raise ValueError("Las claves de los objetos deben ser texto")
            clave.encode("utf-8")
            _validar_valor(elemento)
        return
    raise TypeError("Tipo de contenido no admitido por el serializador")


def _codificar_bytes(valor):
    if type(valor) is not bytes:
        raise TypeError("Tipo de contenido no admitido por el serializador")
    return {MARCA_BYTES: base64.b64encode(valor).decode("ascii")}


def _decodificar_objeto(pares):
    objeto = {}
    for clave, valor in pares:
        if clave in objeto:
            raise ValueError("El mensaje contiene campos repetidos")
        objeto[clave] = valor
    if MARCA_BYTES not in objeto:
        return objeto
    # La marca ocupa un objeto exclusivo, para no confundirlo con un registro.
    if len(objeto) != 1 or type(objeto[MARCA_BYTES]) is not str:
        raise ValueError("Representación binaria inválida")
    try:
        return base64.b64decode(objeto[MARCA_BYTES], validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError("Contenido base64 inválido") from error


def _rechazar_constante(valor):
    raise ValueError("No se admiten constantes numéricas fuera de JSON")
