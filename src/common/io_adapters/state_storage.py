"""WAL común: escritura durable y recuperación incremental de operaciones binarias."""

import fcntl
import logging
import os
from pathlib import Path
from threading import RLock

from .archivos import escribir_completo
from .formato_wal import (
    ColaIncompleta, ErrorWAL, MAX_LONGITUD, MAX_SECUENCIA, WALOcupado,
    delimitar_registro, leer_registro,
)


REGISTRO = logging.getLogger(__name__)


class WAL:
    """Conserva operaciones completas en un archivo exclusivo de una instancia lógica.

    El directorio padre debe existir en almacenamiento persistente. Antes de
    registrar se exige recuperar, aplicando las operaciones al estado del dueño.
    Cada contenido es una unidad atómica de recuperación y su tamaño está acotado
    por max_bytes_por_registro. El WAL no interpreta ni deduplica sus bytes.
    Las llamadas se serializan entre hilos; no se admite heredar la instancia a
    otro proceso. Cerrar libera el archivo, sin eliminar los datos persistidos.
    """

    def __init__(self, ruta, *, max_bytes_por_registro, comprobar_detencion=None):
        if type(max_bytes_por_registro) is not int or not 0 < max_bytes_por_registro <= MAX_LONGITUD:
            raise ValueError('max_bytes_por_registro debe ser un entero positivo representable en u32')
        self._ruta = Path(ruta)
        self._max_bytes = max_bytes_por_registro
        self._comprobar_detencion = comprobar_detencion
        self._pid = os.getpid()
        self._mutex = RLock()
        self._archivo = None
        self._recuperado = False
        self._operando = False
        self._fallido = False
        self._siguiente = 1
        self._abrir()

    def _abrir(self):
        try:
            self._comprobar()
            self._archivo = self._ruta.open('a+b', buffering=0)
            try:
                fcntl.flock(self._archivo.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise WALOcupado(f'El WAL ya está en uso: {self._ruta}') from error
            # fsync del archivo no persiste por sí solo su entrada en el directorio.
            os.fsync(self._archivo.fileno())
            descriptor = os.open(self._ruta.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except BaseException:
            self._cerrar_tras_error()
            raise

    def recuperar(self, aplicar):
        """Aplica registros en orden y recorta solo una cola incompleta.

        aplicar recibe un RegistroWAL por vez y no debe llamar al mismo WAL.
        Se invoca una sola vez por apertura, sobre un estado por reconstruir.
        Si falla, el dueño debe descartar su reconstrucción parcial y reabrir.
        Devuelve la cantidad de operaciones recuperadas, sin reunirlas en memoria.
        """
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
            if self._recuperado:
                raise ErrorWAL('El WAL ya fue recuperado en esta apertura')
            if not callable(aplicar):
                raise TypeError('aplicar debe ser invocable')
            self._operando = True
            try:
                self._archivo.seek(0)
                self._siguiente = self._recorrer(aplicar)
                os.fsync(self._archivo.fileno())
                self._recuperado = True
                return self._siguiente - 1
            except BaseException:
                self._fallido = True
                raise
            finally:
                self._operando = False

    def _recorrer(self, aplicar):
        secuencia = 1
        while True:
            self._comprobar()
            inicio = self._archivo.tell()
            try:
                registro = leer_registro(self._archivo, secuencia, self._max_bytes, self._comprobar_detencion)
            except ColaIncompleta:
                self._archivo.truncate(inicio)
                self._archivo.seek(inicio)
                REGISTRO.warning('Se descartó una escritura incompleta al final de %s, offset %s', self._ruta, inicio)
                return secuencia
            if registro is None:
                return secuencia
            aplicar(registro)
            secuencia += 1

    def registrar(self, contenido):
        """Agrega una operación y devuelve su secuencia después de fsync.

        Un error no implica que el registro esté ausente: se debe cerrar y
        recuperar antes de decidir un reintento. Los IDs lógicos pertenecen al
        contenido; la secuencia devuelta solo identifica la posición en este WAL.
        """
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
            if not self._recuperado:
                raise ErrorWAL('Recuperar el WAL antes de registrar operaciones')
            if type(contenido) is not bytes:
                raise TypeError('El contenido del WAL debe ser bytes')
            if len(contenido) > self._max_bytes:
                raise ErrorWAL('El contenido supera max_bytes_por_registro')
            if self._siguiente > MAX_SECUENCIA:
                raise ErrorWAL('Se agotó la secuencia u64 del WAL')
            self._comprobar()
            cabecera, cierre = delimitar_registro(self._siguiente, contenido)
            self._operando = True
            try:
                for parte in (cabecera, contenido, cierre):
                    escribir_completo(self._archivo, parte, self._comprobar_detencion)
                os.fsync(self._archivo.fileno())
                secuencia = self._siguiente
                self._siguiente += 1
                return secuencia
            except BaseException:
                # No se agrega otra operación detrás de una escritura incierta.
                self._fallido = True
                raise
            finally:
                self._operando = False

    def _validar_proceso(self):
        if os.getpid() != self._pid:
            raise ErrorWAL('Abrir un WAL propio después de crear el proceso')

    def _validar_disponible(self):
        if self._archivo is None or self._archivo.closed:
            raise ErrorWAL('El WAL está cerrado')
        if self._fallido:
            raise ErrorWAL('El WAL falló; cerrar y recuperar antes de continuar')
        if self._operando:
            raise ErrorWAL('Las operaciones del WAL no admiten reentrancia')

    def _comprobar(self):
        if self._comprobar_detencion is not None:
            self._comprobar_detencion()

    def cerrar(self):
        """Libera el descriptor y su bloqueo; admite llamadas repetidas."""
        self._validar_proceso()
        with self._mutex:
            if self._operando:
                raise ErrorWAL('No se puede cerrar el WAL desde una operación activa')
            if self._archivo is not None:
                self._archivo.close()

    def _cerrar_tras_error(self):
        try:
            self.cerrar()
        except Exception:
            REGISTRO.exception('Falló el cierre del WAL; se conserva el error original')

    def __enter__(self):
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
        return self

    def __exit__(self, tipo, error, traza):
        if tipo is None:
            self.cerrar()
        else:
            self._cerrar_tras_error()
        return False
