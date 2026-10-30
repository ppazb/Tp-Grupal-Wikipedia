import logging
import os
import socket
import struct
import threading
import time
import uuid
from dataclasses import replace

from ...common.external_protocol import TipoFrame, decodificar_filas, enviar_frame, recibir_frame
from ...common.protocol.dto import Datos, EnvioDatos, Entrada, IdentidadProceso
from ...common.protocol.registros import Esquema, Lote
from ...middleware.cargador_configuracion import cargar_configuracion
from ...middleware.configuracion import ErrorConfiguracion
from ...middleware.interfaz import ErrorMensaje, ErrorMiddleware
from ...middleware.rabbitmq import MiddlewareRabbitMQ
from .articulos import articulo_desde_fila

# se agregan defaults, cuando hagamos el merge sacamos los defaults ya que 
# lo correcto es q levante error si no esta en yml
LOG = logging.getLogger('gateway')
MIDDLEWARE_CONFIG = os.environ.get("MIDDLEWARE_CONFIG", "config/config.json")
GATEWAY_GROUP = os.environ.get("GATEWAY_GROUP", "gateway")
GATEWAY_INSTANCE = int(os.environ.get("GATEWAY_INSTANCE", "0"))
GATEWAY_ARTICLE_ENTRY = os.environ.get("GATEWAY_ARTICLE_ENTRY")
GATEWAY_ERROR_ACK_SECONDS = float(os.environ.get("GATEWAY_ERROR_ACK_SECONDS", "5"))
SERVER_HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "5555"))
MAX_AVISOS_FILA = 20  # tope de warnings por filas descartadas en una ejecución

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
        and (GATEWAY_ARTICLE_ENTRY is None or ruta.entrada == GATEWAY_ARTICLE_ENTRY)
    ]
    
    if not rutas:
        raise ErrorConfiguracion("El gateway requiere al menos una ruta saliente de artículos")
    
    return tuple(rutas)

class PublicadorMiddleware:
    """Publica en el middleware desde el hilo de cada cliente.
    El middleware no admite dos publicaciones a la vez, 
    asiq se usa lock. Cada hilo publica su propio lote y vuelve cuando se publicó, 
    por eso el ACK al cliente sale después de publicar.
    """

    def __init__(self, configuracion, rutas):
        self._rutas = rutas
        self._lock = threading.Lock()
        self._middleware = MiddlewareRabbitMQ(configuracion)

    def publicar(self, id_ejecucion, lote):
        with self._lock:
            self._middleware.publicar_en_ramas(tuple(
                EnvioDatos(ruta.destino, Datos(Entrada(id_ejecucion, ruta.entrada), lote))
                for ruta in self._rutas
            ))

    def finalizar(self, id_ejecucion):
        with self._lock:
            for ruta in self._rutas:
                self._middleware.finalizar_entrada(ruta.destino, Entrada(id_ejecucion, ruta.entrada))

    def interrumpir(self):
        # Sin lock: tiene q poder despertar a un publicar() que está esperando capacidad
        try:
            self._middleware.interrumpir()
        except ErrorMiddleware:
            LOG.debug("El middleware ya estaba cerrado al interrumpir")

    def cerrar(self):
        with self._lock:
            self._middleware.cerrar()



class ServidorGateway:
    """
    Acepta clientes con sockets TCP (un hilo por conexión) y deriva lotes al middleware.

    """

    def __init__(self, host, puerto, max_bytes, rutas, publicador, detener, espera_ack_error):
        
        self._direccion = (host, puerto)
        self._max_bytes = max_bytes
        self._rutas = rutas
        self._publicador = publicador
        self._detener = detener
        self._espera_ack_error = espera_ack_error
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
            self._detener.set()
            self._socket = None
            self._publicador.interrumpir()
            self._cerrar_clientes()
            self._esperar_clientes()

    def _aceptar(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            self._socket = servidor
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(self._direccion)
            servidor.listen()
            threading.Thread(
                target=self._despertar_accept_al_detener,
                args=(servidor,),
                name="gateway-stop-watcher",
                daemon=True,
            ).start()
            LOG.info(
                "Gateway escuchando en %s:%d; artículos -> %s",
                *servidor.getsockname(),
                ", ".join(f"{ruta.destino}/{ruta.entrada}" for ruta in self._rutas),
            )
            while not self._detener.is_set():
                try:
                    cliente, direccion = servidor.accept()
                except OSError:
                    if self._detener.is_set():
                        break
                    raise
                self._lanzar_cliente(cliente, direccion)

    # Espera la señal de detención y hace fallar el accept() bloqueado.
    def _despertar_accept_al_detener(self, servidor):
        self._detener.wait()
        try:
            servidor.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # el socket ya estaba cerrado

    #se lanza un hilo por cada cliente que se conecta 
    def _lanzar_cliente(self, cliente, direccion):
        hilo = threading.Thread(target=self._atender_cliente, args=(cliente, direccion),
            name=f"gateway-client-{direccion[0]}:{direccion[1]}",daemon=True,
        )

        with self._lock_clientes:
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
        
        except (OSError, ValueError, ErrorMiddleware, RuntimeError) as error:
            LOG.warning("Se rechazo la conexion %s: %s", direccion, error)
            self._notificar_error(cliente, direccion, str(error).encode("utf-8"))
        
        except Exception:
            LOG.exception("Falló el procesamiento de la conexion de %s", direccion)
            self._notificar_error(cliente, direccion, b"Error interno del gateway")
        
        finally:
            try:
                cliente.shutdown(socket.SHUT_RDWR)
            
            except OSError:
                pass
            cliente.close()
            
            with self._lock_clientes:
                self._clientes.discard(cliente)
                self._hilos.discard(threading.current_thread())

    # Envia ERROR y espera el ACK del cliente (o su cierre) hasta limite de tiempo
    def _notificar_error(self, cliente, direccion, mensaje):
        
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
        cantidad_lotes = 0
        filas_descartadas = 0
        filas_totales = 0
        
        # El socket es bloqueante y sin timeout: se duerme en recv hasta que llega
        # el siguiente frame. El apagado despierta este recv cerrando el socket.
        
        while not self._detener.is_set():
            try:
                frame = recibir_frame(cliente, self._max_bytes, permitir_cierre=True)
            
            except ConnectionError:
                frame = None  
            
            # Se deconecto sin enviar eof
            if frame is None:
                self._cerrar_por_desconexion(id_ejecucion, cantidad_lotes)
                return

            tipo, cuerpo = frame

            # Es dato
            if tipo is TipoFrame.DATOS:
                
                descartadas, totales = self._procesar_datos(id_ejecucion, cuerpo, filas_descartadas)
                
                filas_descartadas += descartadas
                filas_totales += totales
                cantidad_lotes += 1

                # ACK: se confirma la recepción del lote
                enviar_frame(cliente, TipoFrame.ACK, struct.pack("!Q", cantidad_lotes), self._max_bytes)
                
                continue

            #solo puedo recibir dato o eof
            if tipo is not TipoFrame.EOF:
                raise ValueError("Tipo de frame externo no admitido")

            # EOF: se espera que el cuerpo contenga la cantidad de lotes recibidos
            self._verificar_eof(cuerpo, cantidad_lotes)
            
            self._publicador.finalizar(id_ejecucion)
            
            enviar_frame(cliente, TipoFrame.ACK,struct.pack("!Q", cantidad_lotes), self._max_bytes)
            
            LOG.info("Ejecución %s admitida: %d lotes, %d filas (%d descartadas), publicados en %s",
                id_ejecucion, cantidad_lotes, filas_totales, filas_descartadas,", ".join(ruta.destino for ruta in self._rutas),
            )
            return

    # Si el cliente se cayo sin eof, cierra la entrada para que los workers no esperen mas datos
    def _cerrar_por_desconexion(self, id_ejecucion, cantidad_lotes):
        if not cantidad_lotes or self._detener.is_set():
            return  
        
        LOG.warning("Cliente desconectado antes de enviar EOF: %s; se publica el EOF tras %d lotes",id_ejecucion, cantidad_lotes)
        
        self._publicador.finalizar(id_ejecucion)

    #Arma los artículos del lote crudo y los publica; devuelve (descartadas, totales)
    def _procesar_datos(self, id_ejecucion, cuerpo, descartadas_previas):
        filas = decodificar_filas(cuerpo)
        articulos = []
        descartadas = 0
        
        for indice, fila in enumerate(filas):
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
            lote = pendientes.pop(0)
            
            try:
                self._publicador.publicar(id_ejecucion, Lote(Esquema.ARTICULO, tuple(lote)))
            
            except ErrorMensaje:

                # un rechazo por tamaño no deja publicaciones parciales
                if len(lote) == 1:
                    raise
                
                mitad = len(lote) // 2
                pendientes[0:0] = (lote[:mitad], lote[mitad:])

    @staticmethod
    def _verificar_eof(cuerpo, cantidad_lotes):
        if len(cuerpo) != BYTE_8 or struct.unpack("!Q", cuerpo)[0] != cantidad_lotes:
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


def ejecutar_gateway(detener):
    configuracion = cargar_configuracion(MIDDLEWARE_CONFIG)
    rutas = _rutas_articulos(configuracion)
    grupo = configuracion.obtener_grupo(GATEWAY_GROUP)
    
    if not 0 <= GATEWAY_INSTANCE < grupo.instancias:
        raise ErrorConfiguracion("GATEWAY_INSTANCE está fuera del grupo gateway")
    
    if GATEWAY_ERROR_ACK_SECONDS <= 0:
        raise ValueError("GATEWAY_ERROR_ACK_SECONDS debe ser mayor que cero")

    configuracion = replace(configuracion,proceso=IdentidadProceso(GATEWAY_GROUP, GATEWAY_INSTANCE))
    publicador = PublicadorMiddleware(configuracion, rutas)
    servidor = ServidorGateway(SERVER_HOST, SERVER_PORT, configuracion.limites.max_bytes_por_mensaje,
        rutas, publicador, detener,GATEWAY_ERROR_ACK_SECONDS,
    )
    
    try:
        servidor.ejecutar()
        return 0
    finally:
        publicador.cerrar()