"""Contrato de comunicación por grupos, independiente del transporte."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Generic, Optional, TYPE_CHECKING, Union
from uuid import UUID

from src.common.protocol.dto import Contenido, Datos, Entrada, EnvioDatos, Evento, IdentidadProceso

if TYPE_CHECKING:
    from src.middleware.procesamiento import ProcesadorRecuperable


@dataclass(frozen=True)
class ContextoRecepcion:
    """Identidad del lote activo y aviso de un intento registrado anteriormente."""

    id_mensaje: UUID
    entrada: Entrada
    origen: IdentidadProceso
    reintento: bool


class ErrorMiddleware(Exception):
    """Error de comunicación informado."""


class ErrorPersistencia(ErrorMiddleware):
    """El registro durable de comunicación no pudo completar una operación."""


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
    """La publicación fue rechazada por el broker.

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
    def publicar(self, envio: EnvioDatos[Contenido]) -> None:
        """Publica el envío identificado y espera capacidad; el retorno confirma aceptación del broker.

        Conserva el ID en reintentos y selecciona reparto o afinidad según el envío."""

    @abstractmethod
    def recibir(self, procesar: Callable[[Datos[Contenido]], None]) -> None:
        """Entrega datos válidos al procesador y confirma al retornar correctamente.

        El procesador debe dejar efectos durables e idempotentes; las reentregas ya
        completadas no ejecutan nuevamente el negocio. Propaga los errores originales."""

    @property
    def entrega_actual(self) -> ContextoRecepcion:
        """Expone el ID durante el callback para persistir efectos idempotentes."""
        raise NotImplementedError('El transporte debe exponer la identidad de la entrega activa')

    @abstractmethod
    def detener_recepcion(self) -> None:
        """Cancela la espera de nuevas entregas y deja finalizar el procesamiento activo."""

    @abstractmethod
    def liberar_ejecucion(self, id_ejecucion: str) -> None:
        """Libera recursos locales de una ejecución con procesamiento y salidas completos.

        El llamador garantiza resultados durables. Conserva la deduplicación y los
        recursos compartidos; intenta las demás limpiezas si alguna falla."""

    @abstractmethod
    def publicar_en_ramas(self, envios) -> None:
        """Persiste las ramas y las publica en orden esperando capacidad por destino.

        Conserva los IDs y las confirmaciones; informa ErrorPublicacionParcial si
        falla una salida después de haber confirmado otras."""

    @abstractmethod
    def registrar_recurso(self, id_ejecucion: str, nombre: str, liberar: Callable[[], None]) -> None:
        """Registra una limpieza idempotente para un recurso exclusivo de la ejecución."""

    @abstractmethod
    def interrumpir(self) -> None:
        """Interrumpe esperas de recepción y publicación sin confirmar trabajo pendiente."""

    def compactar_estado(self) -> None:
        """Reduce los historiales conservando pendientes e identidades completadas.

        Requiere que las operaciones locales hayan retornado. No borra recursos
        del negocio ni autoriza reutilizar IDs; no equivale al cierre de EOF.
        """
        raise NotImplementedError('El transporte debe implementar su compactación')

    def restablecer(self) -> None:
        """Reconstruye el transporte y recupera pendientes fuera de operaciones activas.

        No vuelve a ejecutar callbacks de negocio. Si falla, debe reintentarse
        explícitamente o cerrarse la instancia antes de admitir trabajo nuevo.
        No revierte una interrupción ni un cierre solicitado por la aplicación.
        """
        raise NotImplementedError('El transporte debe implementar su restablecimiento')

    @abstractmethod
    def cerrar_comunicacion(self) -> None:
        """Libera los recursos de comunicación sin publicar EOF; repetirlo es seguro.

        Intenta todas las limpiezas y propaga ErrorCierre si alguna falla."""


class Middleware(ComunicacionGrupos[Contenido]):
    """Extiende la comunicación por grupos con cierres lógicos de entradas."""

    @abstractmethod
    def crear_presupuesto_lote(self, entrada, esquema):
        """Calcula los bytes disponibles para registros descontando el sobre y la cabecera."""

    @abstractmethod
    def recibir(
        self, procesar: Union[Callable[[Evento[Contenido]], None], 'ProcesadorRecuperable'], *,
        al_finalizar: Optional[Callable[[Entrada], None]] = None,
    ) -> None:
        """Procesa datos o el cierre consolidado de una entrada y confirma después del callback.

        Con al_finalizar, entrega allí los cierres; sin él, los entrega a procesar.
        ProcesadorRecuperable exige al_finalizar y confirma datos mediante su operación
        durable. Todas las réplicas deben consumir controles aunque no reciban datos."""

    @abstractmethod
    def entradas_pendientes(self, id_ejecucion: str) -> frozenset[str]:
        """Devuelve las entradas configuradas sin cierre consolidado local.

        Incluye entradas sin actividad y excluye la del finalizador activo; no implica
        que las publicaciones de los finalizadores hayan terminado."""

    @abstractmethod
    def finalizar_entrada(self, grupo: str, entrada: Entrada) -> None:
        """Publica al coordinador de destino el total final de lotes de esta entrada.

        Admite cero aportes, impide nuevos datos y conserva el cierre en reintentos.
        Cada productor cierra por separado cada destino y entrada."""
