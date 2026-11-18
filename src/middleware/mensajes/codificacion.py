"""Codificación del sobre binario y separación de rechazos por registro."""

from uuid import UUID

from src.common.protocol.dto import (
    AvanceReplica, CierreProductor, Entrada, IdentidadProceso, PrepararCierre,
    ResultadoDecodificacion, SobreMensaje, TipoMensaje,
)
from src.common.protocol.primitivas import (
    BYTES_U8, BYTES_U16, BYTES_U32, BYTES_U64, ErrorSerializacion, EscritorBinario, LectorBinario,
)
from src.common.protocol.registros import Esquema, Lote
from src.common.protocol.serializer import escribir_cabecera_lote, escribir_lote, leer_lote
from src.middleware.configuracion.modelos import ErrorConfiguracion
from src.middleware.interfaz import ErrorMensaje, ErrorMensajeInvalido
from src.middleware.mensajes.presupuesto import PresupuestoLote


BYTES_ID_MENSAJE = 16


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
        """Escribe la cabecera y el contenido correspondiente al tipo de mensaje."""
        inicio = self._escribir_cabecera_sobre(escritor, sobre)
        if sobre.tipo is TipoMensaje.DATOS:
            escribir_lote(escritor, sobre.contenido)
        elif sobre.tipo in (TipoMensaje.EOF, TipoMensaje.PREPARAR_CIERRE):
            escritor.entero(sobre.contenido.total_lotes, BYTES_U64)
        elif sobre.tipo is TipoMensaje.AVANCE_REPLICA:
            escritor.entero(sobre.contenido.lotes_resueltos, BYTES_U64)
        escritor.terminar_bloque(inicio)

    def _escribir_cabecera_sobre(self, escritor, sobre):
        """Escribe identidad, entrada y origen e inicia el bloque de contenido."""
        escritor.entero(sobre.tipo.value, BYTES_U8)
        escritor.entero(sobre.id_mensaje.int, BYTES_ID_MENSAJE)
        escritor.texto(sobre.entrada.id_ejecucion)
        escritor.texto(sobre.entrada.nombre)
        escritor.texto(sobre.origen.grupo)
        escritor.entero(sobre.origen.instancia, BYTES_U32)
        return escritor.iniciar_bloque()

    def crear_presupuesto_lote(self, entrada, origen, esquema):
        """Reserva el tamaño del sobre y la cabecera para construir un lote por bytes."""
        # Solo se mide el ancho fijo del ID; este sobre no se publica.
        sobre = SobreMensaje(entrada, TipoMensaje.DATOS, origen, Lote(esquema, ()), UUID(int=1))
        _validar_sobre(sobre)
        try:
            escritor = EscritorBinario(
                self._max_bytes_por_mensaje, medir=True,
                comprobar_detencion=self._comprobar_detencion,
            )
            inicio = self._escribir_cabecera_sobre(escritor, sobre)
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
            id_mensaje = UUID(int=lector.entero(BYTES_ID_MENSAJE))
            if id_mensaje.int == 0:
                raise ErrorSerializacion("El identificador del mensaje no puede ser nulo")
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
            sobre = SobreMensaje(entrada, tipo, origen, lote, id_mensaje)
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
            escritor = EscritorBinario(self._max_bytes_por_mensaje,
                                       comprobar_detencion=self._comprobar_detencion)
            escritor.texto(id_ejecucion)
            escritor.texto(clave)
            return escritor.resultado()
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error


def _validar_texto(valor):
    """Rechaza identificadores de texto vacíos o inválidos."""
    if type(valor) is not str or not valor or valor.isspace():
        raise ErrorMensaje("Se esperaba un identificador de texto no vacío")


def _validar_sobre(sobre):
    """Comprueba identidad, origen, tipo y estructura del contenido del sobre."""
    if type(sobre) is not SobreMensaje or type(sobre.entrada) is not Entrada:
        raise ErrorMensaje("Se esperaba un SobreMensaje con Entrada")
    if type(sobre.origen) is not IdentidadProceso or type(sobre.tipo) is not TipoMensaje:
        raise ErrorMensaje("El sobre debe identificar origen y tipo de mensaje")
    if type(sobre.id_mensaje) is not UUID or sobre.id_mensaje.int == 0:
        raise ErrorMensaje("El mensaje debe tener un UUID no nulo")
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
