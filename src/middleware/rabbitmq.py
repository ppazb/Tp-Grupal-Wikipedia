"""Comunicación por grupos con conexión, capacidad y recursos administrados."""

import logging
import os

from ..common.protocol.dto import EnvioDatos
from ..common.protocol.registros import Esquema, Lote
from .broker_connection import ConexionBroker
from .capacidad import CapacidadDatos
from .configuracion import ErrorConfiguracion
from .interfaz import (
    ComunicacionGrupos, ErrorCierre, ErrorConexion, ErrorMiddleware, ErrorRecepcion,
    ErrorRecursosEjecucion, MiddlewareCerrado,
    OperacionInterrumpida,
)
from .publicador import PublicadorDatos
from .receptor import ReceptorDatos
from .rechazos import ENTRADA_ERRORES
from .recursos_ejecucion import RecursosPorEjecucion
from .topologia import TopologiaGrupos


REGISTRO = logging.getLogger(__name__)


class MiddlewareRabbitMQ(ComunicacionGrupos[Lote]):
    """Abre y administra la comunicación de un proceso desde su configuración.

    Espera capacidad antes de publicar. Los errores de registros se envían por la
    ruta errores; un sumidero recibe un manejador que los entrega a su aplicación.
    Su uso como contexto cierra conexión y recursos incluso ante excepciones.
    """

    def __init__(self, configuracion, *, informar_error=None):
        self._proceso_propietario = os.getpid()
        self._conexion = ConexionBroker(configuracion.broker)
        self._recursos = RecursosPorEjecucion()
        self._cerrada = False
        self._cerrando = False
        self._publicando = False
        self._recibiendo = False
        self._interrumpida = False
        self._receptor = None
        self._capacidad = None
        self._publicador = None
        topologia = TopologiaGrupos(configuracion)
        notificador = self._resolver_notificador(configuracion, informar_error, topologia)
        try:
            self._conexion.abrir()
            topologia.declarar(self._conexion)
            self._capacidad = CapacidadDatos(self._conexion, topologia)
            self._publicador = PublicadorDatos(
                configuracion, self._conexion, capacidad=self._capacidad, topologia=topologia,
            )
            if topologia.colas_recepcion() is not None:
                self._receptor = ReceptorDatos(
                    configuracion, self._conexion, informar_error=notificador,
                    capacidad=self._capacidad, topologia=topologia,
                )
        except BaseException:
            self._cerrar_tras_error()
            raise

    def _resolver_notificador(self, configuracion, informar_error, topologia):
        if informar_error is not None:
            if not callable(informar_error):
                raise ErrorConfiguracion("El manejador de errores debe ser invocable")
            return informar_error
        rutas = [
            ruta for ruta in configuracion.rutas
            if ruta.origen == configuracion.proceso.grupo
            and ruta.entrada == ENTRADA_ERRORES and Esquema.ERROR_REGISTRO in ruta.esquemas
        ]
        if len(rutas) > 1:
            raise ErrorConfiguracion("El grupo debe tener un único destino de notificaciones")
        if not rutas:
            if topologia.colas_recepcion() is not None:
                raise ErrorConfiguracion("El sumidero requiere un manejador de errores")
            return None
        return lambda datos: self.publicar(rutas[0].destino, datos)

    def publicar(self, grupo, datos, *, clave_afinidad=None):
        """Publica un lote tras obtener capacidad en su destino."""
        self.publicar_en_ramas((EnvioDatos(grupo, datos, clave_afinidad),))

    def publicar_en_ramas(self, envios):
        """Publica salidas en orden sin volver a emitir ramas confirmadas al esperar."""
        self._validar_abierta()
        if self._interrumpida:
            raise OperacionInterrumpida("La comunicación fue interrumpida")
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite reentrancia")
        self._publicando = True
        try:
            self._publicador.publicar_en_ramas(envios)
        finally:
            self._publicando = False

    def finalizar_entrada(self, grupo, entrada):
        """Publica el cierre del productor después de sus lotes confirmados."""
        self._validar_abierta()
        if self._interrumpida:
            raise OperacionInterrumpida("La comunicación fue interrumpida")
        if self._publicando:
            raise ErrorMiddleware("No se puede cerrar una entrada durante una publicación")
        self._publicando = True
        try:
            self._publicador.finalizar_entrada(grupo, entrada)
        finally:
            self._publicando = False

    def recibir(self, procesar):
        """Procesa una entrega y confirma después de completar sus salidas."""
        self._validar_abierta()
        if self._interrumpida:
            raise OperacionInterrumpida("La comunicación fue interrumpida")
        if self._receptor is None:
            raise ErrorRecepcion("El grupo no tiene entradas configuradas")
        if self._recibiendo or self._publicando:
            raise ErrorRecepcion("No se puede iniciar una recepción durante otra operación")
        self._recibiendo = True
        try:
            self._receptor.recibir(procesar)
        finally:
            self._recibiendo = False

    def detener_recepcion(self):
        """Detiene nuevas entregas sin interrumpir el procesamiento en curso."""
        self._validar_abierta()
        if self._receptor is not None:
            self._receptor.detener_recepcion()

    def interrumpir(self):
        """Notifica al bucle para cancelar esperas; no debe invocarse desde una señal."""
        self._validar_abierta()
        if self._interrumpida:
            return
        self.detener_recepcion()
        self._conexion.programar(self._capacidad.interrumpir)
        self._interrumpida = True

    def registrar_recurso(self, id_ejecucion, nombre, liberar):
        """Registra la limpieza de un recurso exclusivo de una ejecución."""
        self._validar_abierta()
        self._recursos.registrar(id_ejecucion, nombre, liberar)

    def liberar_ejecucion(self, id_ejecucion):
        """Libera una ejecución sin modificar colas ni recursos compartidos."""
        self._validar_abierta()
        if self._recibiendo or self._publicando:
            raise ErrorRecursosEjecucion("No se puede liberar una ejecución durante una operación")
        self._recursos.liberar_ejecucion(id_ejecucion)
        self._publicador.liberar_ejecucion(id_ejecucion)

    def cerrar(self):
        """Intenta todas las limpiezas y el cierre de canales y conexión."""
        if os.getpid() != self._proceso_propietario:
            raise ErrorConexion("El middleware no puede cerrarse desde otro proceso")
        if self._recibiendo or self._publicando or self._cerrando:
            raise ErrorCierre("El cierre requiere que hayan retornado las operaciones activas")
        self._cerrada = True
        self._cerrando = True
        errores = []
        try:
            if self._publicador is not None:
                self._publicador.descartar_seguimiento()
            for liberar in (self._recursos.cerrar, self._conexion.cerrar):
                try:
                    liberar()
                except BaseException as error:
                    REGISTRO.exception("Falló una limpieza del middleware")
                    errores.append(error)
        finally:
            self._cerrando = False
        if errores:
            for error in errores:
                if not isinstance(error, Exception):
                    raise error
            raise ErrorCierre("Falló el cierre del middleware; ver registro") from errores[0]

    def _validar_abierta(self):
        if self._cerrada:
            raise MiddlewareCerrado("El middleware ya fue cerrado")
        self._conexion.canal

    def _cerrar_tras_error(self):
        try:
            self.cerrar()
        except ErrorCierre:
            REGISTRO.error("Falló la limpieza del middleware; se conserva el error original")

    def __enter__(self):
        self._validar_abierta()
        return self

    def __exit__(self, tipo_error, error, traza):
        if tipo_error is None:
            self.cerrar()
        else:
            self._cerrar_tras_error()
        return False
