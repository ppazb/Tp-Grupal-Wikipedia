"""Contrato de comunicación por grupos, independiente del transporte."""

from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional

from ..common.protocol.dto import Contenido, Datos, Entrada, Evento, FinEntrada


class ErrorMiddleware(Exception):
    """Error de comunicación informado."""


class ErrorConexion(ErrorMiddleware):
    """No se pudo establecer o mantener la conexión."""


class ErrorTopologia(ErrorMiddleware):
    """No se pudieron declarar los recursos de comunicación y sus enlaces."""


class ErrorMensaje(ErrorMiddleware):
    """No se pudo codificar, publicar o interpretar un mensaje."""


class ErrorMensajeInvalido(ErrorMensaje):
    """Mensaje estructuralmente inválido, con el contexto que pudo decodificarse."""

    def __init__(self, detalle, *, entrada=None, origen=None, esquema=None):
        super().__init__(detalle)
        self.entrada = entrada
        self.origen = origen
        self.esquema = esquema


class ErrorRecepcion(ErrorMiddleware):
    """No se pudo recibir, confirmar o cancelar la recepción."""


class ErrorSeguimientoEntrada(ErrorMiddleware):
    """Una operación contradice los participantes, cierres o contadores de una entrada."""


class ErrorCapacidad(ErrorMiddleware):
    """Falló el intercambio de permisos de capacidad."""


class OperacionInterrumpida(ErrorMiddleware):
    """Se interrumpió explícitamente una espera de comunicación."""


class PublicacionRechazada(ErrorMiddleware):
    """La publicación excede el límite local o fue rechazada por el broker.

    Un rechazo remoto no identifica necesariamente su causa.
    """


class ErrorPublicacionParcial(ErrorMensaje):
    """Algunas salidas fueron confirmadas antes de fallar otra publicación.

    confirmadas contiene pares (grupo, Entrada), sin conservar los contenidos.
    fallida identifica la salida cuyo envío falló; ante una desconexión no se
    puede asegurar si el broker llegó a aceptarla. La causa conserva ese error.
    """

    def __init__(self, confirmadas, fallida):
        super().__init__("La publicación a varias ramas quedó incompleta")
        self.confirmadas = tuple(confirmadas)
        self.fallida = fallida


class ErrorCierre(ErrorMiddleware):
    """No se pudieron liberar todos los recursos del middleware."""


class ErrorRecursosEjecucion(ErrorMiddleware):
    """No se pudo registrar o liberar un recurso exclusivo de una ejecución."""


class MiddlewareCerrado(ErrorMiddleware):
    """Se intentó operar sobre una instancia cerrada."""


class EntradaFinalizada(ErrorMiddleware):
    """Se intentó publicar datos después de finalizar esa entrada."""


class ComunicacionGrupos(ABC, Generic[Contenido]):
    """Define la comunicación por grupos para gateway y workers.

    Cada instancia se usa secuencialmente desde su proceso propietario.
    La recepción no es reentrante; el procesador puede publicar salidas.
    Los errores de transporte se expresan como ErrorMiddleware,
    conservando su causa.
    """

    @abstractmethod
    def publicar(
        self,
        grupo: str,
        datos: Datos[Contenido],
        *,
        clave_afinidad: Optional[str] = None,
    ) -> None:
        """Publica datos hacia un grupo lógico, esperando capacidad disponible.

        Sin clave de afinidad reparte entre consumidores; con clave selecciona un
        destinatario estable durante la ejecución. El retorno indica aceptación
        del transporte, no procesamiento remoto.
        Puede elevar PublicacionRechazada, ErrorCapacidad, ErrorConexion,
        ErrorMensaje, MiddlewareCerrado u OperacionInterrumpida.
        """

    @abstractmethod
    def recibir(self, procesar: Callable[[Datos[Contenido]], None]) -> None:
        """Espera y procesa un evento lógico del grupo receptor.

        Invoca procesar sincrónicamente y confirma solo si retorna correctamente;
        si falla, propaga su excepción original. Una detención permite retornar
        sin entregar otro evento. No garantiza procesamiento exactamente una vez
        ni orden global entre productores.
        Los registros inválidos se notifican y los válidos se procesan. Puede
        elevar ErrorConexion, ErrorMensaje, ErrorRecepcion o MiddlewareCerrado.
        """

    @abstractmethod
    def detener_recepcion(self) -> None:
        """Solicita terminar la espera, sin cerrar la conexión ni publicar EOF.

        Es idempotente. Deja terminar y confirmar el procesamiento en curso.
        Las llamadas posteriores a recibir retornan sin entregar eventos.
        La integración con señales debe realizarse fuera del manejador de señal.
        """

    @abstractmethod
    def liberar_ejecucion(self, id_ejecucion: str) -> None:
        """Libera el estado y los recursos locales exclusivos de una ejecución terminada.

        Requiere haber completado el procesamiento, las confirmaciones y las salidas.
        Conserva los recursos compartidos. Repetir una liberación exitosa no tiene
        efecto; ante fallos intenta las demás limpiezas, conserva las pendientes y
        eleva ErrorRecursosEjecucion. No conserva un historial de ejecuciones.
        """

    @abstractmethod
    def publicar_en_ramas(self, envios) -> None:
        """Publica salidas acotadas en orden, esperando capacidad por cada destino.

        Una espera conserva la salida actual y no repite las confirmadas. Un
        error posterior a un envío confirmado eleva ErrorPublicacionParcial.
        """

    @abstractmethod
    def registrar_recurso(self, id_ejecucion: str, nombre: str, liberar: Callable[[], None]) -> None:
        """Registra una limpieza idempotente para un recurso exclusivo de la ejecución."""

    @abstractmethod
    def interrumpir(self) -> None:
        """Interrumpe esperas de recepción y publicación sin confirmar trabajo pendiente."""

    @abstractmethod
    def cerrar(self) -> None:
        """Libera los recursos locales e impide nuevas operaciones de comunicación.

        Repetir un cierre exitoso no tiene efecto. Si una limpieza falla, intenta
        las restantes y eleva ErrorCierre. Se invoca secuencialmente, después de
        detener la recepción; no publica EOF.
        """


class Middleware(ComunicacionGrupos[Contenido]):
    """Extiende la comunicación por grupos con cierres lógicos de entradas."""

    @abstractmethod
    def recibir(self, procesar: Callable[[Evento[Contenido]], None]) -> None:
        """Entrega datos o FinEntrada una vez consolidados los aportes y cierres."""

    @abstractmethod
    def finalizar_entrada(self, grupo: str, entrada: Entrada) -> None:
        """Declara el fin de los aportes de este productor a un grupo y entrada.

        Se invoca después del último aporte, incluso si no hubo datos. Es
        idempotente tras confirmación por productor, grupo y entrada. Anuncia el
        total de lotes a la coordinadora del destino y bloquea nuevos aportes.
        No implica que los demás productores terminaron ni que los datos ya
        fueron procesados.
        Puede elevar ErrorConexion, ErrorMensaje o MiddlewareCerrado.
        """
