"""Ciclo secuencial de recepción y liberación de ejecuciones para workers."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
import logging
import zlib
from uuid import NAMESPACE_URL, uuid5

from src.common.protocol.dto import Entrada, EnvioDatos
from src.common.runtime.apagado import Apagado
from src.middleware.rechazos import ENTRADA_ERRORES
from src.middleware.interfaz import OperacionInterrumpida


REGISTRO = logging.getLogger(__name__)


def instancia_de(clave, cantidad):
    """Réplica (0..cantidad-1) que le corresponde a una clave; igual en todos los procesos.

    No usa hash() de Python, que cambia entre procesos por PYTHONHASHSEED.
    """
    return zlib.crc32(clave.encode("utf-8")) % cantidad


def id_de_salidas(entrega):
    """id_entrada de las salidas de un lote: identifica al lote recibido completo.

    Usa el id_entrada y el indice del lote: dos lotes hermanos (mismo id_entrada, distinto
    indice) producen salidas con ids distintos. Es determinístico: reprocesar el mismo
    lote da el mismo id.
    """
    return str(uuid5(NAMESPACE_URL, f"{entrega.id_entrada}:{entrega.indice}"))


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

    # Valores por defecto de clase: permiten usar envio() sin pasar por __init__ (tests).
    _middleware = None
    _indice_salida = 0

    def __init__(self, middleware):
        self._middleware = middleware
        self._detenido = False
        self._completada = None
        self._apagado = None
        self._limpiezas = []
        self._cerrado = False
        self._clientes_vistos = set()
        self._indice_salida = 0
        self.registrar_limpieza(self._clientes_vistos.clear)

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

    def anunciar_cliente(self, nombre, id_ejecucion):
        """Registra en el log la primera vez que llegan datos de una ejecución a este worker."""
        if id_ejecucion not in self._clientes_vistos:
            self._clientes_vistos.add(id_ejecucion)
            REGISTRO.info("%s: llegó un nuevo cliente (ejecución '%s')", nombre, id_ejecucion)

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

    def id_lote_actual(self):
        """ID (id_entrada) del lote de entrada que se está procesando."""
        entrega = getattr(self._middleware, 'entrega_actual', None)
        if entrega is None:
            raise RuntimeError('No hay un lote en procesamiento')
        return entrega.id_entrada

    def _grupo_propio(self):
        """Grupo de este proceso."""
        configuracion = getattr(self, 'configuracion', None)
        if configuracion is not None:
            return configuracion.proceso.grupo
        return self._middleware.identidad.grupo

    def envio(self, grupo, datos, *, clave_afinidad=None, instancia=None):
        """Arma una salida con identidad determinística: (id_entrada, indice).

        id_entrada identifica al lote de entrada que se procesa completo (ver id_de_salidas) o,
        al cerrar una entrada, es 'eof:<grupo>:<id_ejecucion>' (el grupo evita que dos grupos
        que publican al cierre compartan ID). indice cuenta las salidas de ese id_entrada
        en el orden en que se arman, así que reprocesar el mismo lote en el mismo
        orden produce los mismos identificadores.
        """
        entrega = getattr(self._middleware, 'entrega_actual', None)
        if entrega is not None:
            id_entrada = id_de_salidas(entrega)
        else:
            id_entrada = f'eof:{self._grupo_propio()}:{datos.entrada.id_ejecucion}'
        indice = self._indice_salida
        self._indice_salida += 1
        return EnvioDatos.nuevo(
            grupo, datos, clave_afinidad,
            id_entrada=id_entrada, indice=indice, instancia=instancia,
        )

    def _procesar(self, datos):
        self.comprobar_detencion()
        self._indice_salida = 0
        self.procesar(datos)
        self.comprobar_detencion()

    def detener(self):
        """Detiene nuevas recepciones y deja completar el callback en curso."""
        self._detenido = True
        self._middleware.detener_recepcion()

    def _finalizar(self, entrada):
        self.comprobar_detencion()
        self._indice_salida = 0
        self.finalizar_entrada(entrada)
        self.comprobar_detencion()
        if not self._middleware.entradas_pendientes(entrada.id_ejecucion):
            self._clientes_vistos.discard(entrada.id_ejecucion)
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
        self.destinos_errores = destinos_errores(self.configuracion)

    def iniciar(self):
        """Abre el transporte con apagado común y registra la limpieza del worker."""
        with entorno_worker(self.configuracion, limpiar=self.cerrar) as (middleware, apagado):
            self.middleware = self._middleware = middleware
            self.ejecutar(apagado)

    def finalizar_entrada(self, entrada):
        """Delega la finalización de negocio al callback del nodo."""
        self.al_finalizar(entrada)

    @abstractmethod
    def al_finalizar(self, entrada):
        """Emite pendientes y cierra las salidas correspondientes a la entrada."""
