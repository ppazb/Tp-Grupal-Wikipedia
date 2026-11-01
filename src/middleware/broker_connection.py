"""Ciclo de vida de la conexión y el canal internos del transporte RabbitMQ."""

import logging
import os

import pika

from .apagado_broker import ApagadoBroker
from .configuracion import ConfiguracionBroker, ErrorConfiguracion
from .interfaz import ErrorCierre, ErrorConexion, MiddlewareCerrado, OperacionInterrumpida


REGISTRO = logging.getLogger(__name__)
INTENTOS_CONEXION = 1


class ConexionBroker:
    """Administra una conexión y un canal usados secuencialmente por un proceso.

    Crear la instancia después de iniciar el proceso hijo. El canal es de uso
    interno de los manejadores de transporte; no debe entregarse al negocio.
    Un cierre o una apertura fallida impiden reutilizar esta instancia.
    """

    def __init__(self, configuracion: ConfiguracionBroker, *, apagado=None):
        self._apagado = apagado
        self._enlace_apagado = None
        self._configuracion = configuracion
        self._proceso_propietario = os.getpid()
        self._conexion = None
        self._canal = None
        self._canales_adicionales = []
        self._cerrada = False

    def abrir(self) -> None:
        """Abre conexión y canal; repetirlo mientras ambos estén abiertos no hace nada.

        Si falla la creación del canal, cierra la conexión ya adquirida. Si falla
        también esa limpieza, la registra sin reemplazar la causa de apertura.
        El despertar por señales se conecta tras establecer la conexión; esa
        primera conexión conserva los timeouts configurados del broker.
        """
        self._validar_proceso()
        if self._cerrada:
            raise MiddlewareCerrado("La conexión del middleware ya fue cerrada")
        if self._conexion is not None:
            self._validar_abierta()
            return
        try:
            self.comprobar_apagado()
            parametros = self._crear_parametros()
            self._conexion = pika.BlockingConnection(parametros)
            if self._apagado is not None:
                self._enlace_apagado = ApagadoBroker(self._conexion, self._apagado)
            self._canal = self._conexion.channel()
        except BaseException as error:
            # También libera recursos ante una interrupción durante la apertura,
            # pero conserva KeyboardInterrupt/SystemExit sin traducirlos.
            self._cerrar_tras_error()
            if not isinstance(error, Exception) or isinstance(error, (ErrorConfiguracion, OperacionInterrumpida)):
                raise
            raise ErrorConexion("No se pudo abrir la conexión y el canal de RabbitMQ") from error

    @property
    def canal(self):
        """Devuelve el canal abierto tras validar el proceso propietario y la conexión."""
        self._validar_proceso()
        self._validar_abierta()
        self.comprobar_apagado()
        return self._canal

    def crear_canal(self):
        """Abre un canal de uso interno cuyo cierre pertenece a esta conexión."""
        self._validar_proceso()
        self._validar_abierta()
        self.comprobar_apagado()
        try:
            canal = self._conexion.channel()
            self._canales_adicionales.append(canal)
            return canal
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorConexion("No se pudo abrir un canal adicional") from error

    def cerrar(self) -> None:
        """Cierra primero el canal y luego la conexión, incluso si el primero falla.

        Registra cada fallo y eleva ErrorCierre con la primera causa. Si queda
        un recurso abierto, conserva su referencia para otro intento de cierre.
        No publica EOF, borra colas ni permite volver a abrir esta instancia.
        """
        self._validar_proceso()
        self._cerrada = True
        if self._enlace_apagado is not None:
            self._enlace_apagado.cerrar()
        errores = []
        recursos = [("canal adicional", canal) for canal in self._canales_adicionales]
        recursos.extend((("canal", self._canal), ("conexión", self._conexion)))
        for nombre, recurso in recursos:
            if recurso is None:
                continue
            try:
                if recurso.is_open:
                    recurso.close()
                if not recurso.is_closed:
                    raise ErrorCierre(f"El recurso {nombre} no quedó cerrado")
            except Exception as error:
                REGISTRO.exception("No se pudo cerrar el recurso %s de RabbitMQ", nombre)
                errores.append(error)
        # Cerrar la conexión también cierra sus canales. No se pierden referencias
        # a recursos que permanezcan abiertos después de un error.
        if self._conexion is not None and self._conexion.is_closed:
            self._conexion = None
            self._canal = None
            self._canales_adicionales.clear()
        elif self._canal is not None and self._canal.is_closed:
            self._canal = None
        self._canales_adicionales = [
            canal for canal in self._canales_adicionales if not canal.is_closed
        ]
        if errores:
            raise ErrorCierre("Falló el cierre de recursos de RabbitMQ; ver registro") from errores[0]

    def comprobar_apagado(self):
        """Rechaza trabajo nuevo tras SIGTERM sin impedir las limpiezas internas."""
        if self._enlace_apagado is not None:
            self._enlace_apagado.comprobar()
        if self._apagado is not None and self._apagado.solicitado:
            raise OperacionInterrumpida("La comunicación se detuvo por apagado")

    def preparar_limpieza(self):
        """Retira un aviso de apagado pendiente antes de limpiar una operación fallida."""
        self._validar_proceso()
        if self._apagado is not None and self._apagado.solicitado:
            if self._enlace_apagado is not None:
                self._enlace_apagado.cerrar()

    def programar(self, operacion) -> None:
        """Notifica al bucle de E/S para ejecutar una operación al despachar eventos.

        No debe llamarse desde un manejador de señales: Pika adquiere locks internos.
        """
        self._validar_proceso()
        self._validar_abierta()
        try:
            self._conexion.add_callback_threadsafe(operacion)
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorConexion("No se pudo notificar al bucle de recepción") from error

    def _crear_parametros(self):
        configuracion = self._configuracion
        try:
            return pika.ConnectionParameters(
                host=configuracion.host,
                port=configuracion.puerto,
                virtual_host=configuracion.virtual_host,
                credentials=pika.PlainCredentials(
                    configuracion.usuario, configuracion.contrasena,
                ),
                heartbeat=configuracion.latido_segundos,
                socket_timeout=configuracion.timeout_conexion_segundos,
                stack_timeout=configuracion.timeout_conexion_segundos,
                blocked_connection_timeout=configuracion.timeout_bloqueo_segundos,
                connection_attempts=INTENTOS_CONEXION,
            )
        except (TypeError, ValueError) as error:
            raise ErrorConfiguracion("Parámetros de conexión no admitidos por Pika") from error

    def _validar_proceso(self):
        if os.getpid() != self._proceso_propietario:
            raise ErrorConexion("La conexión no puede utilizarse desde otro proceso")

    def _validar_abierta(self):
        if self._cerrada:
            raise MiddlewareCerrado("La conexión del middleware ya fue cerrada")
        if self._conexion is None or self._canal is None:
            raise ErrorConexion("La conexión y el canal todavía no fueron abiertos")
        if not self._conexion.is_open or not self._canal.is_open:
            raise ErrorConexion("La conexión o el canal dejaron de estar abiertos")

    def _cerrar_tras_error(self):
        try:
            self.cerrar()
        except ErrorCierre:
            # cerrar ya registró las causas; se conserva la excepción original
            # del bloque o de la apertura en lugar de reemplazarla por el cierre.
            REGISTRO.error("La limpieza falló durante el manejo de otro error")

    def __enter__(self):
        self.abrir()
        return self

    def __exit__(self, tipo_error, error, traza):
        if tipo_error is None:
            self.cerrar()
        else:
            self._cerrar_tras_error()
        return False
