"""Codificación de los esquemas explícitos de registros y lotes binarios."""

from . import registros as dto
from .primitivas import (
    BYTES_U8, BYTES_U16, BYTES_U32, BYTES_U64,
    ErrorSerializacion, LectorBinario,
)


MAX_BYTES_DETALLE_ERROR = 512
MAX_BYTES_REFERENCIA_ERROR = 1024

# Cada tupla fija el DTO y el orden y tipo de sus campos en el protocolo.
ESQUEMAS = {
    dto.Esquema.ARTICULO: (dto.Articulo, (
        ("url", "texto_requerido"), ("nombre", "texto_requerido"),
        ("fecha", "texto_requerido"), ("idioma", "idioma"),
        ("abstract", "texto"), ("secciones", "textos"), ("url_imagen", "texto"),
    )),
    dto.Esquema.TEXTO_ARTICULO: (dto.TextoArticulo, (
        ("url", "texto_requerido"), ("idioma", "idioma"), ("abstract", "texto"),
    )),
    dto.Esquema.REFERENCIA_HISTORICA: (dto.ReferenciaHistorica, (
        ("url", "texto_requerido"), ("fecha", "texto_requerido"), ("idioma", "idioma"),
    )),
    dto.Esquema.REFERENCIA_BIOGRAFICA: (dto.ReferenciaBiografica, (
        ("nombre", "texto_requerido"), ("url", "texto_requerido"),
        ("secciones", "textos"), ("url_imagen", "texto"),
    )),
    dto.Esquema.SOLICITUD_IMAGEN: (dto.SolicitudImagen, (
        ("nombre", "texto_requerido"), ("url", "texto_requerido"), ("url_imagen", "texto"),
    )),
    dto.Esquema.PALABRA_IDIOMA: (dto.PalabraIdioma, (
        ("palabra", "texto_requerido"), ("idioma", "idioma"),
    )),
    dto.Esquema.PALABRA: (dto.Palabra, (("palabra", "texto_requerido"),)),
    dto.Esquema.CONTEO_PALABRA: (dto.ConteoPalabra, (
        ("palabra", "texto_requerido"), ("cantidad", "u64"),
    )),
    dto.Esquema.CANTIDAD_PALABRAS: (dto.CantidadPalabras, (("cantidad", "u64"),)),
    dto.Esquema.CANDIDATO: (dto.Candidato, (
        ("url", "texto_requerido"), ("cantidad", "u64"),
    )),
    dto.Esquema.RESUMEN_HISTORICO: (dto.ResumenHistorico, (
        ("suma", "u64"), ("articulos", "u64"),
    )),
    dto.Esquema.RESULTADO_Q1: (dto.ResultadoQ1, (
        ("url", "texto_requerido"), ("fecha", "texto_requerido"),
    )),
    dto.Esquema.FRAGMENTO_IMAGEN: (dto.FragmentoImagen, (
        ("id_transferencia", "texto_requerido"), ("nombre", "texto_requerido"),
        ("url", "texto_requerido"), ("total_bytes", "u64"),
        ("desplazamiento", "u64"), ("fragmento", "binario"),
    )),
    dto.Esquema.ERROR_REGISTRO: (dto.ErrorRegistro, (
        ("entrada_afectada", "texto_requerido"), ("esquema_afectado", "opcional_u16"),
        ("codigo", "codigo_error"), ("indice_registro", "opcional_u32"),
        ("referencia", "opcional_texto"), ("detalle", "texto"),
    )),
}
ANCHOS = {"u16": BYTES_U16, "u32": BYTES_U32, "u64": BYTES_U64}
ENUMERACIONES = {
    "idioma": (dto.Idioma, BYTES_U8),
    "codigo_error": (dto.CodigoError, BYTES_U16),
}


def escribir_lote(escritor, lote):
    """Codifica registros homogéneos y valida sus tipos, campos y tamaños."""
    if type(lote) is not dto.Lote or type(lote.esquema) is not dto.Esquema:
        raise ErrorSerializacion("Se esperaba un Lote con esquema explícito")
    if type(lote.registros) is not tuple or not lote.registros:
        raise ErrorSerializacion("El lote debe contener una tupla no vacía de registros")
    escribir_cabecera_lote(escritor, lote.esquema, len(lote.registros))
    for registro in lote.registros:
        escribir_registro(escritor, lote.esquema, registro)


def escribir_cabecera_lote(escritor, esquema, cantidad):
    """Escribe esquema y cantidad; admite cero para medir un lote sin registros."""
    if type(esquema) is not dto.Esquema:
        raise ErrorSerializacion("Se esperaba un esquema explícito")
    escritor.entero(esquema.value, BYTES_U16)
    escritor.entero(cantidad, BYTES_U32)


def escribir_registro(escritor, esquema, registro):
    """Valida y escribe un registro delimitado según el esquema compartido."""
    clase, campos = ESQUEMAS[esquema]
    if type(registro) is not clase:
        raise ErrorSerializacion("El registro no corresponde al esquema del lote")
    inicio = escritor.iniciar_bloque()
    for nombre, tipo in campos:
        _escribir_campo(escritor, tipo, getattr(registro, nombre))
    _validar_registro(registro)
    escritor.terminar_bloque(inicio)


def leer_lote(lector, esquema):
    """Valida todas las fronteras y separa registros válidos de errores individuales."""
    clase, campos = ESQUEMAS[esquema]
    cantidad = lector.entero(BYTES_U32)
    if cantidad == 0 or cantidad > lector.restantes // BYTES_U32:
        raise ErrorSerializacion("Cantidad de registros inválida")
    # El primer recorrido no crea DTO: una frontera rota invalida todo el lote.
    fronteras = LectorBinario(lector.vista_restante(), comprobar_detencion=lector.comprobar_detencion)
    for _ in range(cantidad):
        fronteras.bloque()
    fronteras.finalizar()
    registros, errores = [], []
    for indice in range(cantidad):
        registro = LectorBinario(lector.bloque(), comprobar_detencion=lector.comprobar_detencion)
        try:
            valores = {nombre: _leer_campo(registro, tipo) for nombre, tipo in campos}
            registro.finalizar()
            valor = clase(**valores)
            _validar_registro(valor)
            registros.append(valor)
        except ErrorSerializacion as error:
            errores.append(dto.ErrorDecodificacionRegistro(
                indice, dto.CodigoError.REGISTRO_INVALIDO, str(error),
            ))
    lector.finalizar()
    return dto.Lote(esquema, tuple(registros)), tuple(errores)


def _escribir_campo(escritor, tipo, valor):
    if tipo.startswith("opcional_"):
        escritor.entero(int(valor is not None), BYTES_U8)
        if valor is not None:
            _escribir_campo(escritor, tipo.removeprefix("opcional_"), valor)
    elif tipo in ANCHOS:
        escritor.entero(valor, ANCHOS[tipo])
    elif tipo in ENUMERACIONES:
        clase, ancho = ENUMERACIONES[tipo]
        if type(valor) is not clase:
            raise ErrorSerializacion("Código enumerado inválido")
        escritor.entero(valor.value, ancho)
    elif tipo in ("texto", "texto_requerido"):
        escritor.texto(valor)
        if tipo == "texto_requerido" and not valor.strip():
            raise ErrorSerializacion("Texto obligatorio vacío")
    elif tipo == "binario":
        escritor.binario(valor)
    elif tipo == "textos":
        if type(valor) is not tuple:
            raise ErrorSerializacion("La lista de textos debe ser una tupla")
        escritor.entero(len(valor), BYTES_U32)
        for texto in valor:
            escritor.texto(texto)
    else:
        raise ErrorSerializacion("Tipo de campo no reconocido")


def _leer_campo(lector, tipo):
    if tipo.startswith("opcional_"):
        presencia = lector.entero(BYTES_U8)
        if presencia not in (0, 1):
            raise ErrorSerializacion("Marca de presencia inválida")
        return _leer_campo(lector, tipo.removeprefix("opcional_")) if presencia else None
    if tipo in ANCHOS:
        return lector.entero(ANCHOS[tipo])
    if tipo in ENUMERACIONES:
        clase, ancho = ENUMERACIONES[tipo]
        try:
            return clase(lector.entero(ancho))
        except ValueError as error:
            raise ErrorSerializacion("Código enumerado inválido") from error
    if tipo in ("texto", "texto_requerido"):
        valor = lector.texto()
        if tipo == "texto_requerido" and not valor.strip():
            raise ErrorSerializacion("Texto obligatorio vacío")
        return valor
    if tipo == "binario":
        return bytes(lector.bloque())
    if tipo == "textos":
        cantidad = lector.entero(BYTES_U32)
        if cantidad > lector.restantes // BYTES_U32:
            raise ErrorSerializacion("Cantidad de textos inválida")
        return tuple(lector.texto() for _ in range(cantidad))
    raise ErrorSerializacion("Tipo de campo no reconocido")


def _validar_registro(registro):
    if type(registro) is dto.ConteoPalabra and registro.cantidad == 0:
        raise ErrorSerializacion("El conteo de una palabra debe ser positivo")
    if type(registro) is dto.ResumenHistorico and registro.articulos == 0 and registro.suma != 0:
        raise ErrorSerializacion("Una suma sin artículos debe ser cero")
    if type(registro) is dto.FragmentoImagen:
        fin = registro.desplazamiento + len(registro.fragmento)
        if fin > registro.total_bytes or (registro.total_bytes > 0 and not registro.fragmento):
            raise ErrorSerializacion("Fragmento fuera de los límites de la imagen")
    if type(registro) is dto.ErrorRegistro:
        if (len(registro.detalle) > MAX_BYTES_DETALLE_ERROR
                or len(registro.detalle.encode("utf-8")) > MAX_BYTES_DETALLE_ERROR):
            raise ErrorSerializacion("El detalle del error supera su límite")
        if registro.referencia is not None:
            if (len(registro.referencia) > MAX_BYTES_REFERENCIA_ERROR
                    or len(registro.referencia.encode("utf-8")) > MAX_BYTES_REFERENCIA_ERROR):
                raise ErrorSerializacion("La referencia del error supera su límite")
