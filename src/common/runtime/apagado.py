"""Solicitud de apagado por señales y limpieza secuencial fuera de sus manejadores."""

import logging
import os
import signal
from selectors import DefaultSelector, EVENT_READ
import socket
import threading


REGISTRO = logging.getLogger(__name__)
SENALES_APAGADO = (signal.SIGTERM, signal.SIGINT)
BYTES_LECTURA = 4096


class Apagado:
    """Notifica al bucle propietario mediante un descriptor legible.

    Se usa en el hilo principal de un único proceso. El bucle debe observar
    fileno() y llamar a procesar() cuando sea legible. Antes del cierre debe
    retirar ese descriptor de su selector. Las limpiezas se ejecutan
    en el orden registrado, una sola vez, incluso si otra falla. No crea hilos.
    Posee el descriptor global de despertar de señales mientras está abierto;
    no admite otro propietario simultáneo de ese descriptor.
    """

    def __init__(self):
        self._proceso = os.getpid()
        self._solicitado = False
        self._cerrado = False
        self._abierto = False
        self._lectura = None
        self._escritura = None
        self._anteriores = []
        self._limpiezas = []
        self._despertar_anterior = -1

    @property
    def solicitado(self):
        """Indica si se recibió una señal o una solicitud explícita de apagado."""
        return self._solicitado

    def registrar_limpieza(self, operacion):
        """Agrega una operación al final de la secuencia de limpieza."""
        self._validar_propietario()
        if self._cerrado:
            raise RuntimeError('El apagado ya se cerró')
        if not callable(operacion):
            raise TypeError('La limpieza debe ser invocable')
        self._limpiezas.append(operacion)

    def abrir(self):
        """Registra señales y un socket no bloqueante; revierte aperturas parciales."""
        self._validar_propietario()
        if self._cerrado:
            raise RuntimeError('El apagado ya se cerró')
        if self._abierto:
            return
        try:
            self._lectura, self._escritura = socket.socketpair()
            self._lectura.setblocking(False)
            self._escritura.setblocking(False)
            anterior = signal.set_wakeup_fd(self._escritura.fileno(), warn_on_full_buffer=False)
            if anterior != -1:
                signal.set_wakeup_fd(anterior)
                raise RuntimeError('Otro componente ya administra el despertar por señales')
            self._abierto = True
            for numero in SENALES_APAGADO:
                previo = signal.signal(numero, self._manejar_senal)
                self._anteriores.append((numero, previo))
        except BaseException:
            self._liberar_notificacion()
            raise

    def _manejar_senal(self, numero, marco):
        # Python escribe el aviso mediante set_wakeup_fd; aquí no se usan locks ni E/S.
        self._solicitado = True

    def solicitar(self):
        """Solicita apagado y despierta al bucle; se invoca fuera del manejador."""
        self._validar_propietario()
        if self._cerrado or self._solicitado:
            return
        if not self._abierto:
            raise RuntimeError('El apagado no está abierto')
        self._solicitado = True
        try:
            if self._escritura.send(b'\0') != 1:
                raise OSError('No se pudo escribir el aviso de apagado')
        except BlockingIOError:
            # Un socket lleno ya es legible: existe un aviso pendiente para el bucle.
            pass

    def fileno(self):
        """Devuelve el descriptor que el bucle debe observar para detectar el apagado."""
        self._validar_propietario()
        if not self._abierto or self._cerrado:
            raise RuntimeError('El apagado no está abierto')
        return self._lectura.fileno()

    def esperar(self, segundos):
        """Espera hasta el plazo o una solicitud, sin consumir el aviso ni ejecutar limpiezas."""
        descriptor = self.fileno()
        if segundos < 0:
            raise ValueError('El plazo de espera no puede ser negativo')
        if not self._solicitado:
            with DefaultSelector() as selector:
                selector.register(descriptor, EVENT_READ)
                selector.select(timeout=segundos)
        return self._solicitado

    def procesar(self):
        """Consume avisos disponibles y ejecuta el cierre si se solicitó detener."""
        self._validar_propietario()
        if self._cerrado:
            return
        if not self._abierto:
            raise RuntimeError('El apagado no está abierto')
        self.consumir_avisos()
        if self._solicitado:
            self.cerrar()

    def consumir_avisos(self):
        """Drena notificaciones sin ejecutar limpiezas dentro del bucle de un transporte."""
        self._validar_propietario()
        if not self._abierto or self._cerrado:
            raise RuntimeError('El apagado no está abierto')
        while True:
            try:
                if not self._lectura.recv(BYTES_LECTURA):
                    raise OSError('Se cerró inesperadamente el socket de notificación')
            except BlockingIOError:
                break

    def cerrar(self):
        """Intenta cada limpieza una vez y libera señales y sockets, aun con errores.

        Los errores ordinarios se agrupan al terminar; otra llamada no repite
        las operaciones fallidas ni las completadas.
        """
        self._validar_propietario()
        if self._cerrado:
            return
        self._cerrado = True
        errores = []
        for operacion in self._limpiezas:
            try:
                operacion()
            except Exception as error:
                errores.append(error)
        self._limpiezas.clear()
        try:
            self._liberar_notificacion()
        except Exception as error:
            errores.append(error)
        if errores:
            raise ExceptionGroup('Falló la secuencia de apagado', errores)

    def _liberar_notificacion(self):
        for numero, anterior in reversed(self._anteriores):
            signal.signal(numero, anterior)
        self._anteriores.clear()
        if self._abierto:
            signal.set_wakeup_fd(self._despertar_anterior)
            self._abierto = False
        for recurso in (self._lectura, self._escritura):
            if recurso is not None:
                recurso.close()
        self._lectura = self._escritura = None

    def _validar_propietario(self):
        if os.getpid() != self._proceso or threading.current_thread() is not threading.main_thread():
            raise RuntimeError('El apagado debe usarse en el hilo principal del proceso propietario')

    def __enter__(self):
        self.abrir()
        return self

    def __exit__(self, tipo, error, traza):
        try:
            self.cerrar()
        except Exception:
            if error is None:
                raise
            REGISTRO.exception('Falló el apagado durante el manejo de otro error')
        return False
