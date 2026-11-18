"""WAL común: escritura durable y recuperación incremental de operaciones binarias."""

import fcntl
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from threading import RLock
from uuid import uuid4

from .archivos import ArchivoTruncado, escribir_completo, leer_completo
from .formato_wal import (
    ColaIncompleta, ErrorWAL, MAX_LONGITUD, MAX_SECUENCIA, WALOcupado,
    delimitar_registro, leer_registro,
)


REGISTRO = logging.getLogger(__name__)
FIRMA_COMPACTACION = b'WALC'
BYTES_GENERACION = 16


@dataclass(frozen=True)
class PuntoWAL:
    """Posición de un estado ya reconstruido sobre el mismo archivo durable."""

    dispositivo: int
    inodo: int
    posicion: int
    siguiente: int
    generacion: bytes | None = None


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
        self._bloqueo = None
        self._recuperado = False
        self._operando = False
        self._fallido = False
        self._siguiente = 1
        self._generacion = None
        self._inicio = 0
        self._abrir()

    def _abrir(self):
        try:
            self._comprobar()
            self._bloqueo = self._ruta.with_name(self._ruta.name + '.lock').open('a+b', buffering=0)
            try:
                fcntl.flock(self._bloqueo.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise WALOcupado(f'El WAL ya está en uso: {self._ruta}') from error
            self._archivo = self._ruta.open('a+b', buffering=0)
            self._leer_generacion()
            # Un temporal de una compactación interrumpida nunca es el historial activo.
            self._ruta.with_name(self._ruta.name + '.compactando').unlink(missing_ok=True)
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

    def _leer_generacion(self):
        """Identifica reemplazos aunque el sistema de archivos reutilice un inodo."""
        self._archivo.seek(0)
        try:
            firma = leer_completo(self._archivo, len(FIRMA_COMPACTACION), admitir_fin=True)
        except ArchivoTruncado:
            firma = None  # La recuperación recortará el primer registro incompleto.
        if firma == FIRMA_COMPACTACION:
            try:
                self._generacion = bytes(leer_completo(self._archivo, BYTES_GENERACION))
            except ArchivoTruncado as error:
                raise ErrorWAL('Cabecera de compactación incompleta') from error
            self._inicio = len(FIRMA_COMPACTACION) + BYTES_GENERACION
        else:
            self._generacion, self._inicio = None, 0
        self._archivo.seek(self._inicio)

    def recuperar(self, aplicar, *, desde=None):
        """Aplica registros en orden y recorta solo una cola incompleta.

        aplicar recibe un RegistroWAL por vez y no debe llamar al mismo WAL.
        desde permite continuar un estado que el dueño conservó junto con ese
        punto; sin él se reconstruye desde el primer registro.
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
                inicio, secuencia = self._inicio, 1
                if desde is not None:
                    estado = os.fstat(self._archivo.fileno())
                    if (type(desde) is not PuntoWAL
                            or desde.generacion != self._generacion
                            or (desde.dispositivo, desde.inodo) != (estado.st_dev, estado.st_ino)
                            or not self._inicio <= desde.posicion <= estado.st_size
                            or desde.siguiente < 1):
                        raise ErrorWAL('El punto de recuperación no pertenece al historial disponible')
                    inicio, secuencia = desde.posicion, desde.siguiente
                self._archivo.seek(inicio)
                self._siguiente = self._recorrer(aplicar, secuencia)
                os.fsync(self._archivo.fileno())
                self._recuperado = True
                return self._siguiente - 1
            except BaseException:
                self._fallido = True
                raise
            finally:
                self._operando = False

    @property
    def punto_recuperacion(self):
        """Devuelve el punto durable asociado al estado reconstruido por el dueño."""
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
            if not self._recuperado:
                raise ErrorWAL('Recuperar el WAL antes de obtener una posición')
            estado = os.fstat(self._archivo.fileno())
            return PuntoWAL(estado.st_dev, estado.st_ino, self._archivo.tell(), self._siguiente, self._generacion)

    def _recorrer(self, aplicar, secuencia):
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

    def conserva_punto(self, punto):
        """Indica si un índice incremental corresponde al archivo vigente."""
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
            estado = os.fstat(self._archivo.fileno())
            return (type(punto) is PuntoWAL
                    and punto.generacion == self._generacion
                    and (punto.dispositivo, punto.inodo) == (estado.st_dev, estado.st_ino)
                    and self._inicio <= punto.posicion <= estado.st_size)

    def compactar(self, contenidos):
        """Reemplaza el historial por registros equivalentes bajo el bloqueo estable.

        El iterable se escribe incrementalmente. Antes del reemplazo una caída
        conserva el original; después queda el compacto completo. Un error exige
        cerrar y recuperar para resolver qué versión quedó instalada.
        """
        self._validar_proceso()
        with self._mutex:
            self._validar_disponible()
            if not self._recuperado:
                raise ErrorWAL('Recuperar antes de compactar')
            self._operando = True
            temporal = self._ruta.with_name(self._ruta.name + '.compactando')
            try:
                siguiente = 1
                generacion = uuid4().bytes
                with temporal.open('wb', buffering=0) as archivo:
                    escribir_completo(archivo, FIRMA_COMPACTACION + generacion, self._comprobar_detencion)
                    for contenido in contenidos:
                        self._comprobar()
                        if type(contenido) is not bytes or len(contenido) > self._max_bytes:
                            raise ErrorWAL('Contenido inválido al compactar')
                        if siguiente > MAX_SECUENCIA:
                            raise ErrorWAL('Se agotó la secuencia al compactar')
                        cabecera, cierre = delimitar_registro(siguiente, contenido)
                        for parte in (cabecera, contenido, cierre):
                            escribir_completo(archivo, parte, self._comprobar_detencion)
                        siguiente += 1
                    os.fsync(archivo.fileno())
                self._comprobar()
                os.replace(temporal, self._ruta)
                descriptor = os.open(self._ruta.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                self._archivo.close()
                self._archivo = self._ruta.open('a+b', buffering=0)
                self._archivo.seek(0, os.SEEK_END)
                self._siguiente = siguiente
                self._generacion = generacion
                self._inicio = len(FIRMA_COMPACTACION) + BYTES_GENERACION
            except BaseException:
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
            self._fallido = True
            try:
                if self._archivo is not None:
                    self._archivo.close()
            finally:
                if self._bloqueo is not None:
                    self._bloqueo.close()

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
