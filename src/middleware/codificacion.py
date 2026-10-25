"""Codificación del sobre binario y separación de rechazos por registro."""

from ..common.protocol.dto import (
    AvanceReplica, CierreProductor, Entrada, IdentidadProceso, PrepararCierre,
    ResultadoDecodificacion, SobreMensaje, TipoMensaje,
)
from ..common.protocol.primitivas import (
    BYTES_U8, BYTES_U16, BYTES_U32, BYTES_U64, ErrorSerializacion, EscritorBinario, LectorBinario,
)
from ..common.protocol.registros import Esquema, Lote
from ..common.protocol.serializer import escribir_lote, leer_lote
from .configuracion import ErrorConfiguracion
from .interfaz import ErrorMensaje, ErrorMensajeInvalido


class CodificadorMensajes:
    """Codifica sobres acotados y decodifica sus registros sin conversiones implícitas."""

    def __init__(self, max_bytes_por_mensaje: int):
        if type(max_bytes_por_mensaje) is not int or max_bytes_por_mensaje <= 0:
            raise ErrorConfiguracion("max_bytes_por_mensaje debe ser un entero positivo")
        self._max_bytes_por_mensaje = max_bytes_por_mensaje

    def codificar(self, sobre: SobreMensaje) -> bytes:
        """Valida y mide el sobre antes de construir el cuerpo binario completo."""
        _validar_sobre(sobre)
        try:
            self._escribir(EscritorBinario(self._max_bytes_por_mensaje, medir=True), sobre)
            escritor = EscritorBinario(self._max_bytes_por_mensaje)
            self._escribir(escritor, sobre)
            return escritor.resultado()
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error

    def _escribir(self, escritor, sobre):
        escritor.entero(sobre.tipo.value, BYTES_U8)
        escritor.texto(sobre.entrada.id_ejecucion)
        escritor.texto(sobre.entrada.nombre)
        escritor.texto(sobre.origen.grupo)
        escritor.entero(sobre.origen.instancia, BYTES_U32)
        inicio = escritor.iniciar_bloque()
        if sobre.tipo is TipoMensaje.DATOS:
            escribir_lote(escritor, sobre.contenido)
        elif sobre.tipo in (TipoMensaje.EOF, TipoMensaje.PREPARAR_CIERRE):
            escritor.entero(sobre.contenido.total_lotes, BYTES_U64)
        elif sobre.tipo is TipoMensaje.AVANCE_REPLICA:
            escritor.entero(sobre.contenido.lotes_resueltos, BYTES_U64)
        escritor.terminar_bloque(inicio)

    def decodificar(self, cuerpo: bytes) -> ResultadoDecodificacion:
        """Valida el sobre completo y devuelve los registros válidos y sus rechazos."""
        if type(cuerpo) is not bytes or len(cuerpo) > self._max_bytes_por_mensaje:
            raise ErrorMensajeInvalido("El cuerpo debe ser bytes dentro del límite configurado")
        entrada = origen = esquema = None
        try:
            lector = LectorBinario(cuerpo)
            tipo = TipoMensaje(lector.entero(BYTES_U8))
            id_ejecucion = lector.texto()
            nombre = lector.texto()
            grupo = lector.texto()
            instancia = lector.entero(BYTES_U32)
            for texto in (id_ejecucion, nombre, grupo):
                if not texto or texto.isspace():
                    raise ErrorSerializacion("Identificador vacío en el sobre")
            entrada = Entrada(id_ejecucion, nombre)
            origen = IdentidadProceso(grupo, instancia)
            contenido = LectorBinario(lector.bloque())
            lector.finalizar()
            if tipo is TipoMensaje.DATOS:
                esquema = contenido.entero(BYTES_U16)
                lote, errores = leer_lote(contenido, Esquema(esquema))
            else:
                constructores = {
                    TipoMensaje.EOF: CierreProductor,
                    TipoMensaje.PREPARAR_CIERRE: PrepararCierre,
                    TipoMensaje.AVANCE_REPLICA: AvanceReplica,
                }
                lote = None if tipo is TipoMensaje.FIN_ENTRADA else constructores[tipo](
                    contenido.entero(BYTES_U64),
                )
                contenido.finalizar()
                errores = ()
            sobre = SobreMensaje(entrada, tipo, origen, lote)
            return ResultadoDecodificacion(sobre, errores)
        except (ErrorSerializacion, ValueError) as error:
            detalle = str(error) if isinstance(error, ErrorSerializacion) else "Tipo o esquema desconocido"
            raise ErrorMensajeInvalido(
                detalle, entrada=entrada, origen=origen, esquema=esquema,
            ) from error

    def codificar_afinidad(self, id_ejecucion: str, clave: str) -> bytes:
        """Codifica dos textos con prefijos u32 para calcular una afinidad inequívoca."""
        _validar_texto(id_ejecucion)
        _validar_texto(clave)
        try:
            escritor = EscritorBinario(self._max_bytes_por_mensaje)
            escritor.texto(id_ejecucion)
            escritor.texto(clave)
            return escritor.resultado()
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error


def _validar_texto(valor):
    if type(valor) is not str or not valor or valor.isspace():
        raise ErrorMensaje("Se esperaba un identificador de texto no vacío")


def _validar_sobre(sobre):
    if type(sobre) is not SobreMensaje or type(sobre.entrada) is not Entrada:
        raise ErrorMensaje("Se esperaba un SobreMensaje con Entrada")
    if type(sobre.origen) is not IdentidadProceso or type(sobre.tipo) is not TipoMensaje:
        raise ErrorMensaje("El sobre debe identificar origen y tipo de mensaje")
    for texto in (sobre.entrada.id_ejecucion, sobre.entrada.nombre, sobre.origen.grupo):
        _validar_texto(texto)
    contenidos = {
        TipoMensaje.DATOS: Lote,
        TipoMensaje.EOF: CierreProductor,
        TipoMensaje.PREPARAR_CIERRE: PrepararCierre,
        TipoMensaje.AVANCE_REPLICA: AvanceReplica,
        TipoMensaje.FIN_ENTRADA: type(None),
    }
    if type(sobre.contenido) is not contenidos[sobre.tipo]:
        raise ErrorMensaje("El contenido no corresponde al tipo de mensaje")
