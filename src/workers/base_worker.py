"""Ciclo secuencial de recepción y liberación de ejecuciones para workers."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
import logging

from src.common.protocol.dto import Entrada
from src.common.runtime.apagado import Apagado
from src.middleware.rechazos import ENTRADA_ERRORES
from src.middleware.interfaz import OperacionInterrumpida


REGISTRO = logging.getLogger(__name__)


def comprobar_apagado(apagado):
    """Interrumpe el trabajo cooperativamente cuando el proceso pidió apagar."""
    if apagado is not None and apagado.solicitado:
        raise OperacionInterrumpida("Procesamiento detenido por apagado")


@contextmanager
def entorno_worker(configuracion, *, limpiar=None):
    """Abre señales y transporte; consume solo interrupciones solicitadas por apagado."""
    from src.middleware.rabbitmq import MiddlewareRabbitMQ

    iniciado = False
    with Apagado() as apagado:
        if limpiar is not None:
            apagado.registrar_limpieza(limpiar)
        try:
            with MiddlewareRabbitMQ(configuracion, apagado=apagado) as middleware:
                iniciado = True
                yield middleware, apagado
        except OperacionInterrumpida:
            if not iniciado or not apagado.solicitado:
                raise
            REGISTRO.info("Worker detenido por solicitud de apagado")



class Worker(ABC):
    """Conecta callbacks de negocio con un middleware proporcionado por el proceso.

    El propietario abre y cierra el middleware. La detención se solicita mediante
    detener(), fuera de un manejador de señal, para terminar el ciclo sin polling.
    """

    # Grupos que reciben las notificaciones de rechazo de este worker. Se cierran al terminar la
    # ejecución: el destino no la da por completa hasta recibir el EOF de todos sus productores.
    destinos_errores = ()

    def __init__(self, middleware):
        self._middleware = middleware
        self._detenido = False
        self._completada = None
        self._apagado = None
        self._limpiezas = []
        self._cerrado = False

    def ejecutar(self, apagado=None):
        """Consume entradas y libera recursos al terminar, fallar o recibir una señal."""
        self._apagado = apagado
        try:
            while not self._detenido and not self._cerrado:
                self.comprobar_detencion()
                self._completada = None
                self._middleware.recibir(self._procesar, al_finalizar=self._finalizar)
                if self._completada is not None:
                    self._middleware.liberar_ejecucion(self._completada)
        except BaseException:
            try:
                self.cerrar()
            except Exception:
                REGISTRO.exception("Falló la limpieza del worker; se conserva el error original")
            raise
        else:
            self.cerrar()

    def comprobar_detencion(self):
        """Interrumpe el negocio en un punto seguro si se solicitó apagar el proceso."""
        comprobar_apagado(self._apagado)

    def registrar_limpieza(self, operacion):
        """Registra una limpieza local, sin publicar resultados ni cierres de entradas."""
        if self._cerrado:
            raise RuntimeError("El worker ya está cerrado")
        if not callable(operacion):
            raise TypeError("La limpieza debe ser invocable")
        self._limpiezas.append(operacion)

    def cerrar(self):
        """Libera todos los recursos locales una sola vez, sin emitir EOF."""
        if self._cerrado:
            return
        self._cerrado = self._detenido = True
        errores = []
        for operacion in self._limpiezas:
            try:
                operacion()
            except Exception as error:
                errores.append(error)
        self._limpiezas.clear()
        if errores:
            raise ExceptionGroup("Falló la limpieza del worker", errores)

    def _procesar(self, datos):
        self.comprobar_detencion()
        self.procesar(datos)
        self.comprobar_detencion()

    def detener(self):
        """Detiene nuevas recepciones y deja completar el callback en curso."""
        self._detenido = True
        self._middleware.detener_recepcion()

    def _finalizar(self, entrada):
        self.comprobar_detencion()
        self.finalizar_entrada(entrada)
        self.comprobar_detencion()
        if not self._middleware.entradas_pendientes(entrada.id_ejecucion):
            for destino in self.destinos_errores:
                self._middleware.finalizar_entrada(destino, Entrada(entrada.id_ejecucion, ENTRADA_ERRORES))
            self._completada = entrada.id_ejecucion

    @abstractmethod
    def procesar(self, datos):
        """Procesa los registros de una entrada."""

    @abstractmethod
    def finalizar_entrada(self, entrada):
        """Emite resultados pendientes y cierra las salidas que completó esta entrada."""


def destinos_errores(configuracion):
    """Grupos a los que el proceso local envía notificaciones de rechazo."""
    grupo = configuracion.proceso.grupo
    return tuple(sorted({
        ruta.destino for ruta in configuracion.rutas
        if ruta.origen == grupo and ruta.entrada == ENTRADA_ERRORES
    }))


class WorkerBase(Worker):
    """Abre el middleware desde configuración y adapta los callbacks de los nodos."""

    def __init__(self, ruta_configuracion):
        from src.middleware.cargador_configuracion import cargar_configuracion

        self.configuracion = cargar_configuracion(ruta_configuracion)
        self.middleware = None
        super().__init__(self.middleware)
        self._clientes_vistos = set()
        self.registrar_limpieza(self._clientes_vistos.clear)
        self.destinos_errores = destinos_errores(self.configuracion)

    def iniciar(self):
        """Abre el transporte con apagado común y registra la limpieza del worker."""
        with entorno_worker(self.configuracion, limpiar=self.cerrar) as (middleware, apagado):
            self.middleware = self._middleware = middleware
            self.ejecutar(apagado)

    def anunciar_cliente(self, nombre, id_ejecucion):
        """Registra en el log la primera vez que llegan datos de una ejecución a este worker."""
        if id_ejecucion not in self._clientes_vistos:
            self._clientes_vistos.add(id_ejecucion)
            REGISTRO.info("%s: llegó un nuevo cliente (ejecución '%s')", nombre, id_ejecucion)

    def finalizar_entrada(self, entrada):
        """Delega la finalización de negocio al callback del nodo."""
        self.al_finalizar(entrada)
        self._clientes_vistos.discard(entrada.id_ejecucion)

    @abstractmethod
    def al_finalizar(self, entrada):
        """Emite pendientes y cierra las salidas correspondientes a la entrada."""
