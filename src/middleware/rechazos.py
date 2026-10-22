"""Construcción de notificaciones acotadas de rechazo por ejecución."""

from ..common.protocol.dto import Datos, Entrada
from ..common.protocol.registros import CodigoError, ErrorRegistro, Esquema, Lote


ENTRADA_ERRORES = "errores"


def notificacion_rechazo(entrada, esquema, detalle, *, indice=None):
    """Proyecta un rechazo de decodificación a un lote dirigido a la entrada errores."""
    codigo = CodigoError.MENSAJE_INVALIDO if indice is None else CodigoError.REGISTRO_INVALIDO
    registro = ErrorRegistro(
        entrada_afectada=entrada.nombre,
        esquema_afectado=esquema,
        codigo=codigo,
        indice_registro=indice,
        referencia=None,
        detalle=detalle,
    )
    return Datos(
        Entrada(entrada.id_ejecucion, ENTRADA_ERRORES),
        Lote(Esquema.ERROR_REGISTRO, (registro,)),
    )
