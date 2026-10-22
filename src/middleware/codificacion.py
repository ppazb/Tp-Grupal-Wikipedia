"""Codificación del sobre interno de datos y control, sin dependencias de Pika."""

from ..common.protocol.dto import Entrada, IdentidadProceso, SobreMensaje, TipoMensaje
from ..common.protocol.serializer import (
    ErrorSerializacion,
    deserializar_json,
    serializar_json,
)
from .configuracion import ErrorConfiguracion
from .interfaz import ErrorMensaje


CAMPOS_SOBRE = {"id_ejecucion", "entrada", "tipo", "origen", "contenido"}
CAMPOS_ORIGEN = {"grupo", "instancia"}


class CodificadorMensajes:
    """Codifica y decodifica sobres JSON UTF-8, validando campos y tamaño."""

    def __init__(self, max_bytes_por_mensaje: int):
        if type(max_bytes_por_mensaje) is not int or max_bytes_por_mensaje <= 0:
            raise ErrorConfiguracion("max_bytes_por_mensaje debe ser un entero positivo")
        self._max_bytes_por_mensaje = max_bytes_por_mensaje

    def codificar(self, sobre: SobreMensaje) -> bytes:
        """Valida el sobre y devuelve su cuerpo UTF-8 listo para publicar."""
        _validar_sobre(sobre)
        campos = {
            "id_ejecucion": sobre.entrada.id_ejecucion,
            "entrada": sobre.entrada.nombre,
            "tipo": sobre.tipo.value,
            "origen": {
                "grupo": sobre.origen.grupo,
                "instancia": sobre.origen.instancia,
            },
            "contenido": sobre.contenido,
        }
        try:
            cuerpo = serializar_json(campos)
        except ErrorSerializacion as error:
            raise ErrorMensaje("No se pudo codificar el contenido del mensaje") from error
        self._validar_tamano(cuerpo)
        return cuerpo

    def decodificar(self, cuerpo: bytes) -> SobreMensaje:
        """Convierte bytes JSON UTF-8 en un SobreMensaje con campos y tamaño válidos."""
        if not isinstance(cuerpo, bytes):
            raise ErrorMensaje("El cuerpo del mensaje debe ser bytes")
        self._validar_tamano(cuerpo)
        try:
            campos = deserializar_json(cuerpo)
        except ErrorSerializacion as error:
            raise ErrorMensaje("No se pudo decodificar el cuerpo del mensaje") from error
        _validar_campos(campos, CAMPOS_SOBRE, "sobre")
        _validar_campos(campos["origen"], CAMPOS_ORIGEN, "origen")
        try:
            tipo = TipoMensaje(campos["tipo"])
        except (TypeError, ValueError) as error:
            raise ErrorMensaje("Tipo de mensaje desconocido") from error
        sobre = SobreMensaje(
            entrada=Entrada(campos["id_ejecucion"], campos["entrada"]),
            tipo=tipo,
            origen=IdentidadProceso(
                campos["origen"]["grupo"], campos["origen"]["instancia"],
            ),
            contenido=campos["contenido"],
        )
        _validar_sobre(sobre)
        return sobre

    def _validar_tamano(self, cuerpo):
        if len(cuerpo) > self._max_bytes_por_mensaje:
            raise ErrorMensaje("El mensaje supera max_bytes_por_mensaje")


def _validar_campos(objeto, esperados, nombre):
    if type(objeto) is not dict or set(objeto) != esperados:
        raise ErrorMensaje(f"Campos incompletos o desconocidos en {nombre}")


def _validar_texto(valor, nombre):
    if type(valor) is not str or not valor.strip():
        raise ErrorMensaje(f"{nombre} debe ser texto no vacío")


def _validar_sobre(sobre):
    if not isinstance(sobre, SobreMensaje):
        raise ErrorMensaje("Se esperaba un SobreMensaje")
    if not isinstance(sobre.entrada, Entrada):
        raise ErrorMensaje("El sobre debe identificar una Entrada")
    _validar_texto(sobre.entrada.id_ejecucion, "id_ejecucion")
    _validar_texto(sobre.entrada.nombre, "entrada")
    if not isinstance(sobre.origen, IdentidadProceso):
        raise ErrorMensaje("El origen debe ser una IdentidadProceso")
    _validar_texto(sobre.origen.grupo, "origen.grupo")
    if type(sobre.origen.instancia) is not int or sobre.origen.instancia < 0:
        raise ErrorMensaje("origen.instancia debe ser un entero no negativo")
    if not isinstance(sobre.tipo, TipoMensaje):
        raise ErrorMensaje("El tipo debe ser un TipoMensaje")
    if sobre.tipo is TipoMensaje.EOF and sobre.contenido is not None:
        raise ErrorMensaje("Un EOF debe tener contenido nulo")
    if sobre.tipo is TipoMensaje.DATOS and sobre.contenido is None:
        raise ErrorMensaje("Un mensaje de datos debe tener contenido no nulo")
