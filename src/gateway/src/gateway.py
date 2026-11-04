from contextlib import ExitStack
from selectors import DefaultSelector, EVENT_READ
import logging
import os
import socket
import threading
import time
import uuid
from dataclasses import replace

from ...common.external_protocol import TipoFrame, decodificar_filas, enviar_frame, recibir_frame
from ...common.protocol.dto import Datos, EnvioDatos, Entrada, IdentidadProceso
from ...common.protocol.registros import Esquema, Lote
from ...middleware.cargador_configuracion import cargar_configuracion
from ...middleware.configuracion import ErrorConfiguracion
from ...middleware.interfaz import ErrorMensaje, ErrorMiddleware, OperacionInterrumpida
from ...middleware.rabbitmq import MiddlewareRabbitMQ
from .articulos import articulo_desde_fila
from .resultados import ReceptorResultados, RegistroSesiones, SesionCliente

LOG = logging.getLogger('gateway')

# Configuración desde el entorno 
MIDDLEWARE_CONFIG = os.environ["MIDDLEWARE_CONFIG"]
GATEWAY_GROUP = os.environ["GATEWAY_GROUP"]
GATEWAY_INSTANCE = int(os.environ["GATEWAY_INSTANCE"])
GATEWAY_ERROR_ACK_SECONDS = float(os.environ["GATEWAY_ERROR_ACK_SECONDS"])
# grupo de la topología al que los workers envían los resultados
RESULTS_GROUP = os.environ["GATEWAY_RESULTS_GROUP"]
RESULTS_INSTANCE = int(os.environ["GATEWAY_RESULTS_INSTANCE"])
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


MAX_AVISOS_FILA = 20  # tope de warnings por filas descartadas en una ejecucion
BYTE_8 = 8  

# Rutas salientes de artículos: el problema se abre en tramos y cada uno recibe el lote.
#ej para gateway: filter_2020 y 2021
def _rutas_articulos(configuracion):

    try:
        configuracion.obtener_grupo(GATEWAY_GROUP)
    
    except ErrorConfiguracion as error:
        raise ErrorConfiguracion(f"El grupo {GATEWAY_GROUP!r} debe declararse en la topologia") from error
    
    rutas = [
        ruta for ruta in configuracion.rutas
        if ruta.origen == GATEWAY_GROUP
        and Esquema.ARTICULO in ruta.esquemas
    ]
    
    if not rutas:
        raise ErrorConfiguracion("El gateway requiere al menos una ruta saliente de artículos")
    
    return tuple(rutas)

class PublicadorMiddleware:
    """Mantiene un middleware por hilo de cliente, abierto y cerrado por su dueño."""

    def __init__(self, configuracion, rutas, detener):
        self._configuracion = configuracion
        self._rutas = rutas
        self._detener = detener
        self._local = threading.local()

    def _obtener(self):
        if self._detener.is_set():
            raise OperacionInterrumpida("El gateway se está deteniendo")
        if not hasattr(self._local, 'middleware'):
            with ExitStack() as recursos:
                aviso = recursos.enter_context(self._detener.aviso())
                middleware = recursos.enter_context(MiddlewareRabbitMQ(self._configuracion, apagado=aviso))
                self._local.middleware = middleware
                self._local.recursos = recursos.pop_all()
        return self._local.middleware

    def publicar(self, id_ejecucion, lote):
        self._obtener().publicar_en_ramas(tuple(
            EnvioDatos(ruta.destino, Datos(Entrada(id_ejecucion, ruta.entrada), lote))
            for ruta in self._rutas
        ))

    def finalizar(self, id_ejecucion):
        middleware = self._obtener()
        for ruta in self._rutas:
            middleware.finalizar_entrada(ruta.destino, Entrada(id_ejecucion, ruta.entrada))
        self.cerrar_cliente()

    def interrumpir(self):
        self._detener.set()

    def cerrar_cliente(self):
        """Libera la conexión del hilo actual sin emitir EOF; admite repetir el cierre."""
        recursos = getattr(self._local, 'recursos', None)
        if recursos is not None:
            del self._local.middleware, self._local.recursos
            recursos.close()


class ServidorGateway:
    """
    Acepta clientes con sockets TCP (un hilo por conexión) y deriva lotes al middleware.

    """

    def __init__(self, host, puerto, max_bytes, rutas, publicador, detener, espera_ack_error, sesiones=None, apagado=None):
        
        self._apagado = apagado
        self._vigilante = None
        self._direccion = (host, puerto)
        self._max_bytes = max_bytes
        self._rutas = rutas
        self._publicador = publicador
        self._detener = detener
        self._espera_ack_error = espera_ack_error
        self._sesiones = sesiones if sesiones is not None else RegistroSesiones()
        self._lock_clientes = threading.Lock()
        self._clientes = set()
        self._hilos = set()
        self._socket = None

    @property
    def direccion(self):
        if self._socket is None:
            return self._direccion
        
        return self._socket.getsockname()

    def ejecutar(self):
        try:
            self._aceptar()
        
        finally:
            self._socket = None
            # ExitStack intenta todos los pasos aunque falle una limpieza previa.
            with ExitStack() as limpieza:
                if self._vigilante is not None and self._vigilante.ident is not None:
                    limpieza.callback(self._vigilante.join)
                limpieza.callback(self._esperar_clientes)
                limpieza.callback(self._cerrar_clientes)
                limpieza.callback(self._sesiones.cerrar_todas)
                limpieza.callback(self._publicador.interrumpir)
                limpieza.callback(self._detener.set)

    def _aceptar(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            self._socket = servidor
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(self._direccion)
            servidor.listen()
            self._vigilante = threading.Thread(
                target=self._despertar_accept_al_detener,
                args=(servidor,),
                name="gateway-stop-watcher",
            )
            self._vigilante.start()
            LOG.info(
                "Gateway escuchando en %s:%d; artículos -> %s",
                *servidor.getsockname(),
                ", ".join(f"{ruta.destino}/{ruta.entrada}" for ruta in self._rutas),
            )
            with DefaultSelector() as selector:
                servidor.setblocking(False)
                selector.register(servidor, EVENT_READ)
                if self._apagado is not None:
                    selector.register(self._apagado.fileno(), EVENT_READ)
                while not self._detener.is_set():
                    if self._apagado is not None and self._apagado.solicitado:
                        self._detener.set()
                        break
                    disponibles = selector.select()
                    if self._apagado is not None and self._apagado.solicitado:
                        self._detener.set()
                        break
                    if self._detener.is_set():
                        break
                    if not any(clave.fileobj is servidor for clave, _ in disponibles):
                        continue
                    try:
                        cliente, direccion = servidor.accept()
                    except BlockingIOError:
                        continue
                    cliente.setblocking(True)
                    self._lanzar_cliente(cliente, direccion)

    # Despierta el selector de aceptación cuando un hilo solicita la detención.
    def _despertar_accept_al_detener(self, servidor):
        self._detener.wait()
        try:
            servidor.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # el socket ya estaba cerrado

    #se lanza un hilo por cada cliente que se conecta 
    def _lanzar_cliente(self, cliente, direccion):
        hilo = threading.Thread(target=self._atender_cliente, args=(cliente, direccion),
            name=f"gateway-client-{direccion[0]}:{direccion[1]}",
        )

        with self._lock_clientes:
            terminados = {h for h in self._hilos if not h.is_alive()}
            for terminado in terminados:
                terminado.join()
            self._hilos.difference_update(terminados)
            self._clientes.add(cliente)
            self._hilos.add(hilo)
        
        try:
            hilo.start()
        
        except Exception:
            # no se pudo iniciar el hilo: cerrar el socket y sacar de la lista de clientes
            with self._lock_clientes:
                self._clientes.discard(cliente)
                self._hilos.discard(hilo)
            cliente.close()
            raise

    # una vez creado el hilo, se atiende al cliente 
    def _atender_cliente(self, cliente, direccion):
        try:
            self._procesar_cliente(cliente)
        
        except (InterruptedError, OperacionInterrumpida):
            if not self._detener.is_set():
                LOG.exception("Se interrumpió una conexión sin solicitud de apagado")
                self._detener.set()
        except (OSError, ValueError, ErrorMiddleware, RuntimeError) as error:
            LOG.warning("Se rechazo la conexion %s: %s", direccion, error)
            self._notificar_error(cliente, direccion, str(error).encode("utf-8"))
        
        except Exception:
            LOG.exception("Falló el procesamiento de la conexion de %s", direccion)
            self._notificar_error(cliente, direccion, b"Error interno del gateway")
        
        finally:
            try:
                self._publicador.cerrar_cliente()
            except Exception:
                LOG.exception("No se pudo cerrar el publicador del cliente")
                self._detener.set()
            try:
                cliente.shutdown(socket.SHUT_RDWR)
            
            except OSError:
                pass
            cliente.close()
            
            with self._lock_clientes:
                self._clientes.discard(cliente)

    # Envia ERROR y espera el ACK del cliente (o su cierre) hasta limite de tiempo
    def _notificar_error(self, cliente, direccion, mensaje):
        if self._detener.is_set():
            return
        
        try:
            enviar_frame(cliente, TipoFrame.ERROR, mensaje[:self._max_bytes], self._max_bytes)
            limite = time.monotonic() + self._espera_ack_error
            cliente.settimeout(self._espera_ack_error)
            
            while time.monotonic() < limite:
                frame = recibir_frame(cliente, self._max_bytes, permitir_cierre=True, interrumpido=lambda: True)

                # podria tener otro frame que ya habia sido mandado pero solo me interesa el ACK
                if frame is None or frame[0] is TipoFrame.ACK:
                    return
                
        except (OSError, ValueError):
            LOG.debug("No se pudo completar la notificación de error a %s", direccion)
        except Exception:
            LOG.exception("Falló la notificación de error a %s", direccion)

    def _procesar_cliente(self, cliente):
        # El gateway asigna la ejecución: un uuid nuevo por conexion para no repetir
        id_ejecucion = str(uuid.uuid4())
        inicio = time.monotonic()
        # Se registra antes de publicar: los resultados pueden llegar mientras el cliente sigue enviando
        sesion = SesionCliente(cliente, self._max_bytes)
        self._sesiones.registrar(id_ejecucion, sesion)
        try:
            self._atender_ejecucion(cliente, sesion, id_ejecucion, inicio)
        finally:
            self._sesiones.quitar(id_ejecucion)

    def _atender_ejecucion(self, cliente, sesion, id_ejecucion, inicio):
        cantidad_lotes = 0
        filas_descartadas = 0
        filas_totales = 0
        # Un cliente real se anuncia con su primer frame: el healthcheck abren y cierran
        # el socket sin enviar nada y no deben contarse como clientes
        anunciado = False
        
        # El socket es bloqueante y sin timeout: se duerme en recv hasta que llega
        # el siguiente frame. El apagado despierta este recv cerrando el socket.
        
        while not self._detener.is_set():
            try:
                frame = recibir_frame(cliente, self._max_bytes, permitir_cierre=True)
            
            except ConnectionError:
                frame = None  
            
            self._comprobar_detencion()

            # Se deconecto sin enviar eof
            if frame is None:
                self._cerrar_por_desconexion(id_ejecucion, cantidad_lotes)
                return

            tipo, cuerpo = frame

            if not anunciado:
                anunciado = True
                LOG.info("Ejecución %s: llegó un cliente", id_ejecucion)

            # Es dato
            if tipo is TipoFrame.DATOS:
                
                descartadas, totales = self._procesar_datos(id_ejecucion, cuerpo, filas_descartadas)
                
                filas_descartadas += descartadas
                filas_totales += totales
                cantidad_lotes += 1

                # ACK: se confirma la recepción del lote
                sesion.enviar(TipoFrame.ACK, cantidad_lotes.to_bytes(BYTE_8, "big"))
                
                continue

            #solo puedo recibir dato o eof
            if tipo is not TipoFrame.EOF:
                raise ValueError("Tipo de frame externo no admitido")

            # EOF: se espera que el cuerpo contenga la cantidad de lotes recibidos
            self._verificar_eof(cuerpo, cantidad_lotes)
            
            self._publicador.finalizar(id_ejecucion)
            
            sesion.enviar(TipoFrame.ACK, cantidad_lotes.to_bytes(BYTE_8, "big"))
            
            LOG.info("Ejecución %s: EOF recibido a los %.1f s; %d lotes, %d filas (%d descartadas), publicados en %s",
                id_ejecucion, time.monotonic() - inicio, cantidad_lotes, filas_totales, filas_descartadas,
                ", ".join(ruta.destino for ruta in self._rutas),
            )
            self._esperar_resultados(sesion, id_ejecucion, inicio)
            return

    # La conexión sigue abierta hasta que el cliente recibió todos los resultados
    def _esperar_resultados(self, sesion, id_ejecucion, inicio):
        sesion.completa.wait()
        if sesion.rota:
            LOG.warning("Ejecución %s: se corta la espera de resultados", id_ejecucion)
            return
        self._comprobar_detencion()
        sesion.enviar(TipoFrame.FIN, b"")
        LOG.info("Ejecución %s: resultados entregados al cliente. Ejecución de inicio a fin: %.1f s",
                 id_ejecucion, time.monotonic() - inicio)

    # Si el cliente se cayo sin eof, cierra la entrada para que los workers no esperen mas datos
    def _cerrar_por_desconexion(self, id_ejecucion, cantidad_lotes):
        if not cantidad_lotes or self._detener.is_set():
            return  
        
        LOG.warning("Cliente desconectado antes de enviar EOF: %s; se publica el EOF tras %d lotes",id_ejecucion, cantidad_lotes)
        
        self._publicador.finalizar(id_ejecucion)

    #Arma los artículos del lote crudo y los publica; devuelve (descartadas, totales)
    def _procesar_datos(self, id_ejecucion, cuerpo, descartadas_previas):
        filas = decodificar_filas(cuerpo, comprobar_detencion=self._comprobar_detencion)
        articulos = []
        descartadas = 0
        
        for indice, fila in enumerate(filas):
            self._comprobar_detencion()
            try:
                articulos.append(articulo_desde_fila(fila, indice))
            
            except ValueError as error:
                descartadas += 1
                if descartadas_previas + descartadas <= MAX_AVISOS_FILA:
                    LOG.warning("Ejecución %s: fila descartada: %s", id_ejecucion, error)
        
        if articulos:
            self._publicar_articulos(id_ejecucion, articulos)
        
        return descartadas, len(filas)

    # Publica los artículos; un lote que no entra en un mensaje se divide a la mitad
    def _publicar_articulos(self, id_ejecucion, articulos):
        pendientes = [articulos]
        
        while pendientes:
            self._comprobar_detencion()
            lote = pendientes.pop(0)
            
            try:
                self._publicador.publicar(id_ejecucion, Lote(Esquema.ARTICULO, tuple(lote)))
            
            except ErrorMensaje:

                # un rechazo por tamaño no deja publicaciones parciales
                if len(lote) == 1:
                    raise
                
                mitad = len(lote) // 2
                pendientes[0:0] = (lote[:mitad], lote[mitad:])

    def _comprobar_detencion(self):
        if self._detener.is_set():
            raise InterruptedError("El gateway se está deteniendo")

    @staticmethod
    def _verificar_eof(cuerpo, cantidad_lotes):
        if len(cuerpo) != BYTE_8 or int.from_bytes(cuerpo, "big") != cantidad_lotes:
            raise ValueError("La cantidad de lotes anunciada por EOF no coincide")

    def _cerrar_clientes(self):
        with self._lock_clientes:
            clientes = tuple(self._clientes)
        
        for cliente in clientes:
            try:
                cliente.shutdown(socket.SHUT_RDWR)  # despierta el recv bloqueado
            except OSError:
                pass

    def _esperar_clientes(self):
        with self._lock_clientes:
            hilos = tuple(self._hilos)
        
        for hilo in hilos:
            hilo.join()


def _iniciar_receptor(configuracion, sesiones, detener):
    """Abre, consume y cierra el middleware de resultados dentro de su hilo dueño."""
    grupo = configuracion.obtener_grupo(RESULTS_GROUP)
    if not 0 <= RESULTS_INSTANCE < grupo.instancias:
        raise ErrorConfiguracion(f"GATEWAY_RESULTS_INSTANCE está fuera del grupo {RESULTS_GROUP}")
    configuracion = replace(configuracion, proceso=IdentidadProceso(RESULTS_GROUP, RESULTS_INSTANCE))
    errores = []

    def recibir():
        try:
            with detener.aviso() as aviso:
                with MiddlewareRabbitMQ(configuracion, apagado=aviso,
                                        informar_error=lambda datos: receptor.procesar(datos)) as middleware:
                    receptor = ReceptorResultados(middleware, sesiones, configuracion.limites.max_bytes_por_mensaje)
                    receptor.ejecutar(aviso)
        except OperacionInterrumpida:
            if not detener.is_set():
                errores.append(RuntimeError("Se interrumpió el receptor sin solicitar el apagado"))
                detener.set()
        except Exception as error:
            errores.append(error)
            LOG.exception("Falló la recepción de resultados; se detiene el gateway")
            detener.set()

    hilo = threading.Thread(target=recibir, name="gateway-results")
    hilo.start()
    return hilo, errores


def ejecutar_gateway(detener, apagado):
    configuracion = cargar_configuracion(MIDDLEWARE_CONFIG)
    rutas = _rutas_articulos(configuracion)
    grupo = configuracion.obtener_grupo(GATEWAY_GROUP)
    if not 0 <= GATEWAY_INSTANCE < grupo.instancias:
        raise ErrorConfiguracion(f"GATEWAY_INSTANCE está fuera del grupo {GATEWAY_GROUP}")
    if GATEWAY_ERROR_ACK_SECONDS <= 0:
        raise ValueError("GATEWAY_ERROR_ACK_SECONDS debe ser mayor que cero")

    sesiones = RegistroSesiones()
    publicador_config = replace(configuracion, proceso=IdentidadProceso(GATEWAY_GROUP, GATEWAY_INSTANCE))
    publicador = PublicadorMiddleware(publicador_config, rutas, detener)
    hilo, errores = _iniciar_receptor(configuracion, sesiones, detener)
    try:
        servidor = ServidorGateway(SERVER_HOST, SERVER_PORT, configuracion.limites.max_bytes_por_mensaje,
            rutas, publicador, detener, GATEWAY_ERROR_ACK_SECONDS, sesiones, apagado,
        )
        servidor.ejecutar()
    finally:
        detener.set()
        hilo.join()
    if errores:
        raise ExceptionGroup("Falló el receptor de resultados del gateway", errores)
    return 0
