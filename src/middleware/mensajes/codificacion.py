"""Codificación del sobre binario y separación de rechazos por registro."""

from ...common.protocol.dto import (
    CierreProductor, Entrada, IdentidadProceso, ResultadoDecodificacion, SobreMensaje, TipoMensaje,
)
from ...common.protocol.primitivas import (
    BYTES_U8, BYTES_U16, BYTES_U32, BYTES_U64, ErrorSerializacion, EscritorBinario, LectorBinario,
)
from ...common.protocol.registros import Esquema, Lote
from ...common.protocol.serializer import escribir_cabecera_lote, escribir_lote, leer_lote
from ..configuracion.modelos import ErrorConfiguracion
from ..interfaz import ErrorMensaje, ErrorMensajeInvalido
from .presupuesto_lote import PresupuestoLote


# Tope del id_entrada: permite reservar su espacio al medir el presupuesto de un lote.
MAX_BYTES_ID_ENTRADA = 64


class CodificadorMensajes:
    """Codifica sobres acotados y decodifica sus registros sin conversiones implícitas."""

    def __init__(self, max_bytes_por_mensaje: int, *, comprobar_detencion=None):
        self._comprobar_detencion = comprobar_detencion
        if type(max_bytes_por_mensaje) is not int or max_bytes_por_mensaje <= 0:
            raise ErrorConfiguracion("max_bytes_por_mensaje debe ser un entero positivo")
        self._max_bytes_por_mensaje = max_bytes_por_mensaje

    def codificar(self, sobre: SobreMensaje) -> bytes:
        """Valida y mide el sobre antes de construir el cuerpo binario completo."""
        _validar_sobre(sobre)
        try:
            medidor = EscritorBinario(
                self._max_bytes_por_mensaje, medir=True,
                comprobar_detencion=self._comprobar_detencion,
            )
            self._escribir_sobre(medidor, sobre)
            escritor = EscritorBinario(self._max_bytes_por_mensaje,
                                       comprobar_detencion=self._comprobar_detencion)
            self._escribir_sobre(escritor, sobre)
            return escritor.resultado()
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error

    def _escribir_sobre(self, escritor, sobre):
        """Escribe la cabecera y el contenido y completa la longitud del bloque."""
        inicio = self._escribir_cabecera(escritor, sobre)
        if sobre.tipo is TipoMensaje.DATOS:
            escribir_lote(escritor, sobre.contenido)
        else:
            escritor.entero(sobre.contenido.total_lotes, BYTES_U64)
        escritor.terminar_bloque(inicio)

    def _escribir_cabecera(self, escritor, sobre):
        """Escribe las identidades y el tipo e inicia el bloque de contenido."""
        escritor.entero(sobre.tipo.value, BYTES_U8)
        escritor.texto(sobre.id_entrada)
        escritor.entero(sobre.indice, BYTES_U32)
        escritor.texto(sobre.entrada.id_ejecucion)
        escritor.texto(sobre.entrada.nombre)
        escritor.texto(sobre.origen.grupo)
        escritor.entero(sobre.origen.instancia, BYTES_U32)
        return escritor.iniciar_bloque()

    def crear_presupuesto_lote(self, entrada, origen, esquema):
        """Reserva el tamaño del sobre y la cabecera para construir un lote por bytes."""
        # Se reserva el id_entrada más largo permitido; este sobre no se publica.
        sobre = SobreMensaje(entrada, TipoMensaje.DATOS, origen, Lote(esquema, ()),
                             "x" * MAX_BYTES_ID_ENTRADA, 0)
        _validar_sobre(sobre)
        try:
            escritor = EscritorBinario(
                self._max_bytes_por_mensaje, medir=True,
                comprobar_detencion=self._comprobar_detencion,
            )
            inicio = self._escribir_cabecera(escritor, sobre)
            escribir_cabecera_lote(escritor, esquema, 0)
            escritor.terminar_bloque(inicio)
            return PresupuestoLote(self._max_bytes_por_mensaje - escritor.posicion, esquema,
                                   comprobar_detencion=self._comprobar_detencion)
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error

    def decodificar(self, cuerpo: bytes) -> ResultadoDecodificacion:
        """Valida el sobre completo y devuelve los registros válidos y sus rechazos."""
        if type(cuerpo) is not bytes or len(cuerpo) > self._max_bytes_por_mensaje:
            raise ErrorMensajeInvalido("El cuerpo debe ser bytes dentro del límite configurado")
        entrada = origen = esquema = None
        try:
            lector = LectorBinario(cuerpo, comprobar_detencion=self._comprobar_detencion)
            tipo = TipoMensaje(lector.entero(BYTES_U8))
            id_entrada = lector.texto()
            indice = lector.entero(BYTES_U32)
            if not _es_id_entrada_valido(id_entrada):
                raise ErrorSerializacion("El id_entrada del mensaje es inválido")
            id_ejecucion = lector.texto()
            nombre = lector.texto()
            grupo = lector.texto()
            instancia = lector.entero(BYTES_U32)
            for texto in (id_ejecucion, nombre, grupo):
                if not texto or texto.isspace():
                    raise ErrorSerializacion("Identificador vacío en el sobre")
            entrada = Entrada(id_ejecucion, nombre)
            origen = IdentidadProceso(grupo, instancia)
            contenido = LectorBinario(lector.bloque(), comprobar_detencion=self._comprobar_detencion)
            lector.finalizar()
            if tipo is TipoMensaje.DATOS:
                esquema = contenido.entero(BYTES_U16)
                lote, errores = leer_lote(contenido, Esquema(esquema))
            else:
                lote = CierreProductor(contenido.entero(BYTES_U64))
                contenido.finalizar()
                errores = ()
            sobre = SobreMensaje(entrada, tipo, origen, lote, id_entrada, indice)
            return ResultadoDecodificacion(sobre, errores)
        except (ErrorSerializacion, ValueError) as error:
            detalle = str(error) if isinstance(error, ErrorSerializacion) else "Tipo o esquema desconocido"
            raise ErrorMensajeInvalido(
                detalle, entrada=entrada, origen=origen, esquema=esquema,
            ) from error

    def codificar_afinidad(self, id_ejecucion: str, clave: str) -> bytes:
        """Codifica dos textos con prefijos u32 para calcular una afinidad inequívoca."""
        _validar_identificador_no_vacio(id_ejecucion)
        _validar_identificador_no_vacio(clave)
        try:
            escritor = EscritorBinario(self._max_bytes_por_mensaje,
                                       comprobar_detencion=self._comprobar_detencion)
            escritor.texto(id_ejecucion)
            escritor.texto(clave)
            return escritor.resultado()
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error


def _validar_identificador_no_vacio(valor):
    """Exige un identificador de texto no vacío."""
    if type(valor) is not str or not valor or valor.isspace():
        raise ErrorMensaje("Se esperaba un identificador de texto no vacío")


def _es_id_entrada_valido(valor):
    """Comprueba que el identificador no esté vacío y quepa en el límite UTF-8."""
    try:
        return (type(valor) is str and bool(valor) and not valor.isspace()
                and len(valor.encode("utf-8")) <= MAX_BYTES_ID_ENTRADA)
    except UnicodeError:
        return False


def _validar_sobre(sobre):
    """Valida identidades, tipo, índice y correspondencia del contenido del sobre."""
    if type(sobre) is not SobreMensaje or type(sobre.entrada) is not Entrada:
        raise ErrorMensaje("Se esperaba un SobreMensaje con Entrada")
    if type(sobre.origen) is not IdentidadProceso or type(sobre.tipo) is not TipoMensaje:
        raise ErrorMensaje("El sobre debe identificar origen y tipo de mensaje")
    if not _es_id_entrada_valido(sobre.id_entrada):
        raise ErrorMensaje("El mensaje debe tener un id_entrada de texto no vacío y acotado")
    if type(sobre.indice) is not int or not 0 <= sobre.indice < 2 ** (8 * BYTES_U32):
        raise ErrorMensaje("El índice del mensaje debe ser un entero u32")
    for texto in (sobre.entrada.id_ejecucion, sobre.entrada.nombre, sobre.origen.grupo):
        _validar_identificador_no_vacio(texto)
    contenidos = {
        TipoMensaje.DATOS: Lote,
        TipoMensaje.EOF: CierreProductor,
    }
    if type(sobre.contenido) is not contenidos[sobre.tipo]:
        raise ErrorMensaje("El contenido no corresponde al tipo de mensaje")
