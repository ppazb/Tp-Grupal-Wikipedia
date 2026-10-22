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


class ErrorRecepcion(ErrorMiddleware):
    """No se pudo recibir, confirmar o cancelar la recepción."""


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


class Middleware(ABC, Generic[Contenido]):
    """Define la comunicación por grupos para gateway y workers.

    Cada instancia se usa secuencialmente desde su proceso propietario.
    La recepción no es reentrante; el procesador puede publicar y finalizar
    salidas. Los errores de transporte se expresan como ErrorMiddleware,
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
        """Publica datos hacia un grupo lógico e informa rechazos de admisión.

        Sin clave de afinidad reparte entre consumidores; con clave selecciona un
        destinatario estable durante la ejecución. El retorno indica aceptación
        del transporte, no procesamiento remoto.
        Puede elevar PublicacionRechazada, ErrorConexion, ErrorMensaje,
        MiddlewareCerrado o EntradaFinalizada.
        """

    @abstractmethod
    def recibir(self, procesar: Callable[[Evento[Contenido]], None]) -> None:
        """Espera y procesa un evento lógico del grupo receptor.

        Invoca procesar sincrónicamente y confirma solo si retorna correctamente;
        si falla, propaga su excepción original. Una detención permite retornar
        sin entregar otro evento. No garantiza procesamiento exactamente una vez
        ni orden global entre productores.
        FinEntrada representa el cierre consolidado de los datos y productores
        de una entrada. Puede elevar ErrorConexion, ErrorMensaje, ErrorRecepcion
        o MiddlewareCerrado.
        """

    @abstractmethod
    def detener_recepcion(self) -> None:
        """Solicita terminar la espera, sin cerrar la conexión ni publicar EOF.

        Es idempotente. Deja terminar y confirmar el procesamiento en curso.
        Las llamadas posteriores a recibir retornan sin entregar eventos.
        La integración con señales debe realizarse fuera del manejador de señal.
        """

    @abstractmethod
    def finalizar_entrada(self, grupo: str, entrada: Entrada) -> None:
        """Declara el fin de los aportes de este productor a un grupo y entrada.

        Se invoca después del último aporte, incluso si no hubo datos. Es
        idempotente por productor, grupo y entrada, y alcanza a todas las
        instancias destinatarias. No implica que los demás productores terminaron.
        Puede elevar ErrorConexion, ErrorMensaje o MiddlewareCerrado.
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
    def cerrar(self) -> None:
        """Libera los recursos locales e impide nuevas operaciones de comunicación.

        Repetir un cierre exitoso no tiene efecto. Si una limpieza falla, intenta
        las restantes y eleva ErrorCierre. Se invoca secuencialmente, después de
        detener la recepción; no publica EOF.
        """
