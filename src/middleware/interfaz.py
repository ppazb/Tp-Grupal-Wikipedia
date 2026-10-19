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


class MiddlewareCerrado(ErrorMiddleware):
    """Se intentó operar sobre una instancia cerrada."""


class EntradaFinalizada(ErrorMiddleware):
    """Se intentó publicar datos después de finalizar esa entrada."""


class Middleware(ABC, Generic[Contenido]):
    """Interfaz que utilizan gateway y workers mediante inyección de dependencia.

    Cada instancia pertenece a un proceso productor y a un grupo receptor lógico.
    Su construcción concreta configura identidad, topología y serialización; el
    negocio no conoce direcciones, réplicas, colas ni conexiones del broker.

    Cada proceso crea y usa su propia instancia; las conexiones no se comparten
    ni se heredan entre procesos. Las operaciones son secuenciales, sin requerir
    threads ni permitir recepción reentrante. El escalado utiliza procesos
    independientes, en una o varias computadoras. El procesador puede publicar
    y finalizar salidas.
    Las implementaciones traducen errores de transporte a ErrorMiddleware y
    conservan su causa mediante ``raise ... from ...``.
    """

    @abstractmethod
    def publicar(
        self,
        grupo: str,
        datos: Datos[Contenido],
        *,
        clave_afinidad: Optional[str] = None,
    ) -> None:
        """Publica datos hacia un grupo lógico, con contrapresión si es necesario.

        Sin clave, el middleware distribuye el trabajo entre consumidores. Una
        clave solicita un destinatario estable dentro de ese grupo durante la
        ejecución; el negocio no calcula índices de réplicas. El algoritmo y la
        topología quedan encapsulados. El contenido debe admitir el serializador
        configurado y contener solo los campos necesarios para el destinatario.

        El retorno indica aceptación del transporte, no procesamiento remoto.
        Puede elevar ErrorConexion, ErrorMensaje, MiddlewareCerrado o
        EntradaFinalizada para la combinación de grupo y entrada ya finalizada.
        """

    @abstractmethod
    def recibir(self, procesar: Callable[[Evento[Contenido]], None]) -> None:
        """Espera y procesa un único evento lógico del grupo receptor configurado.

        Bloquea sin sondeo activo hasta disponer de un evento. Ejecuta el
        procesador sincrónicamente y confirma su entrega solo si este retorna
        correctamente. Si el procesador falla, propaga su excepción original y
        no confirma el evento como procesado; la política de recuperación queda
        a cargo de la implementación. No garantiza procesamiento exactamente una
        vez ni orden global entre productores.

        Oculta los mensajes internos de coordinación. Solo entrega FinEntrada
        después de procesar todos los datos y cierres requeridos de esa entrada,
        incluso si no hubo datos. Las otras entradas y ejecuciones siguen activas.
        Puede elevar ErrorConexion, ErrorMensaje o MiddlewareCerrado.
        """

    @abstractmethod
    def finalizar_entrada(self, grupo: str, entrada: Entrada) -> None:
        """Declara que este productor no enviará más datos a ese grupo y entrada.

        Se llama después de publicar todos sus aportes, aun cuando no haya ninguno.
        Es idempotente para el mismo productor, grupo y entrada. Su retorno no
        significa que los demás productores hayan terminado. La implementación
        propaga y consolida los cierres según la topología, incluyendo todas las
        réplicas destinatarias; no requiere una clave de afinidad del negocio.
        Puede elevar ErrorConexion, ErrorMensaje o MiddlewareCerrado.
        """

    @abstractmethod
    def cerrar(self) -> None:
        """Libera los recursos locales; repetir un cierre exitoso no tiene efecto.

        No publica EOF ni declara completadas las entradas pendientes. Tras el
        cierre, publicar, recibir y finalizar_entrada elevan MiddlewareCerrado.
        Si un recurso falla, intenta liberar los restantes y eleva ErrorCierre.
        No se invoca concurrentemente con otras operaciones; la interrupción de
        una espera y su integración con SIGTERM se resuelven en otra parte, sin llamar
        directamente a Pika desde un manejador de señales.
        """
