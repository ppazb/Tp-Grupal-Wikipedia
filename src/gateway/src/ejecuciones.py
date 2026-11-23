"""
Estado durable de las ejecuciones del gateway.

Por ejecución hay dos logs (WAL con CRC y fsync), cada uno en su subdirectorio de WAL_DIR:

control/<id>.log : INICIO, LOTE(n), EOF, ENTRADA_CERRADA(nombre), COMPLETA, ABANDONADA, FIN
resultados/<id>.log : un registro por resultado aceptado: clave de deduplicación + lote ya codificado

La secuencia de un resultado (la que viaja en RESULTADO) es su posición en el log de resultados,
empezando en 1. Una ejecución terminada (FIN o ABANDONADA) se anota en el registro de cerradas
de cada WalsPorEjecucion y se borran sus logs.

Cada ejecución tiene su propia condición: protege su estado y sus logs, y despierta a quien espera
resultados nuevos. Nunca se publica al middleware ni se envía al socket con ella tomada.
"""

import logging
import os
import threading
import time
import uuid

from ...workers.wal_ejecuciones import WalsPorEjecucion

LOG = logging.getLogger('gateway')

SUBDIRECTORIO_CONTROL = "control"
SUBDIRECTORIO_RESULTADOS = "resultados"

_INICIO = b"I"
_LOTE = b"L"
_EOF = b"E"
_ENTRADA_CERRADA = b"N"
_COMPLETA = b"C"
_ABANDONADA = b"A"
_FIN = b"F"

_BYTES_NUMERO = 8
_BYTES_LARGO = 4


class EjecucionCerrada(ValueError):
    """La ejecución ya terminó (FIN) o fue abandonada."""


def _codificar_resultado_guardado(clave, contenido):
    nombre, id_entrada, indice = clave
    nombre = nombre.encode("utf-8")
    id_entrada = id_entrada.encode("utf-8")
    return b"".join((
        len(nombre).to_bytes(_BYTES_LARGO, "big"), nombre,
        len(id_entrada).to_bytes(_BYTES_LARGO, "big"), id_entrada,
        indice.to_bytes(_BYTES_LARGO, "big"),
        contenido,
    ))


def _decodificar_resultado_guardado(registro):
    """Devuelve (clave, contenido) de un registro del log de resultados."""
    vista = memoryview(registro)
    posicion = 0

    def tomar(cantidad):
        nonlocal posicion
        if posicion + cantidad > len(vista):
            raise ValueError("Registro de resultado truncado")
        tomado = vista[posicion:posicion + cantidad]
        posicion += cantidad
        return tomado

    nombre = bytes(tomar(int.from_bytes(tomar(_BYTES_LARGO), "big"))).decode("utf-8")
    id_entrada = bytes(tomar(int.from_bytes(tomar(_BYTES_LARGO), "big"))).decode("utf-8")
    indice = int.from_bytes(tomar(_BYTES_LARGO), "big")
    return (nombre, id_entrada, indice), bytes(vista[posicion:])


class Ejecucion:
    """Estado de una ejecución. Los métodos que modifican persisten antes de cambiar la memoria."""

    def __init__(self, registro, id_ejecucion):
        self._registro = registro
        self.id = id_ejecucion
        self.inicio = time.monotonic()
        self.condicion = threading.Condition()
        # Serializa el procesamiento de lotes de la ejecución sin tomar la condición mientras se publica
        self.lock_lotes = threading.Lock()
        self.lotes_procesados = 0
        self.eof_recibido = False
        self.completa = False
        self.cerrada = False
        self.entradas_cerradas = set()
        self.sesion = None
        self._claves = set()
        self._posiciones = []

    # CONTROL:

    def _escribir_control(self, etiqueta, cuerpo=b""):
        self._registro.control.agregar(self.id, etiqueta + cuerpo)

    def _comprobar_abierta(self):
        if self.cerrada:
            raise EjecucionCerrada(f"La ejecución {self.id} ya terminó")

    def registrar_lote(self, numero):
        with self.condicion:
            self._comprobar_abierta()
            self._escribir_control(_LOTE, numero.to_bytes(_BYTES_NUMERO, "big"))
            self.lotes_procesados = numero

    def registrar_eof(self):
        with self.condicion:
            self._comprobar_abierta()
            if not self.eof_recibido:
                self._escribir_control(_EOF)
                self.eof_recibido = True

    def registrar_entrada_cerrada(self, nombre):
        """Persiste el cierre consolidado de una entrada; devuelve True si con él la ejecución quedó completa."""
        with self.condicion:
            if self.cerrada:
                return False
            if nombre not in self.entradas_cerradas:
                self._escribir_control(_ENTRADA_CERRADA, nombre.encode("utf-8"))
                self.entradas_cerradas.add(nombre)
            return self._completar_si_corresponde()

    def _completar_si_corresponde(self):
        esperadas = self._registro.entradas_esperadas
        if self.completa or not esperadas or not esperadas <= self.entradas_cerradas:
            return False
        self._escribir_control(_COMPLETA)
        self.completa = True
        self.condicion.notify_all()
        return True

    # resultados:

    def ya_vista(self, clave):
        with self.condicion:
            return clave in self._claves

    def agregar_resultado(self, clave, contenido):
        """Persiste un resultado y despierta a los emisores. False si es un duplicado o la ejecución terminó."""
        with self.condicion:
            if self.cerrada or clave in self._claves:
                return False
            posicion = self._registro.resultados.agregar(self.id, _codificar_resultado_guardado(clave, contenido))
            self._claves.add(clave)
            self._posiciones.append(posicion)
            self.condicion.notify_all()
            return True

    def _cargar_resultado(self, clave, posicion):
        self._claves.add(clave)
        self._posiciones.append(posicion)

    @property
    def cantidad_resultados(self):
        return len(self._posiciones)

    def leer_resultado(self, secuencia):
        """Contenido del resultado de esa secuencia (desde 1); el llamador debe tener la condición."""
        _, contenido = _decodificar_resultado_guardado(
            self._registro.resultados.leer_en(self.id, self._posiciones[secuencia - 1]))
        return contenido

    # conexion

    def adjuntar(self, sesion):
        """Asocia la conexión del cliente; la conexión anterior, si hay, deja de recibir resultados."""
        with self.condicion:
            self._comprobar_abierta()
            anterior, self.sesion = self.sesion, sesion
            sesion.al_romperse(self.despertar)
            if anterior is not None:
                anterior.marcar_rota()

    def desadjuntar(self, sesion):
        """Suelta la conexión si sigue siendo la vigente; devuelve si lo era."""
        with self.condicion:
            if self.sesion is not sesion:
                return False
            self.sesion = None
            return True

    def despertar(self):
        with self.condicion:
            self.condicion.notify_all()

    # cierre

    def _cerrar(self, etiqueta):
        with self.condicion:
            if self.cerrada:
                return False
            self._escribir_control(etiqueta)
            self.cerrada = True
            self._registro.cerrar_logs(self.id)
            sesion = self.sesion
            self.condicion.notify_all()
        if sesion is not None and etiqueta == _ABANDONADA:
            sesion.marcar_rota()
        return True

    def finalizar(self):
        """Todos los resultados fueron entregados: anota FIN y borra los logs."""
        return self._cerrar(_FIN)

    def abandonar(self):
        """El cliente no volverá: anota ABANDONADA y borra los logs (con sus resultados)."""
        return self._cerrar(_ABANDONADA)


class RegistroEjecuciones:
    """Ejecuciones del gateway, recuperadas del disco al crearse el registro."""

    def __init__(self, directorio, entradas_esperadas):
        self.entradas_esperadas = frozenset(entradas_esperadas)
        self.control = WalsPorEjecucion(os.path.join(directorio, SUBDIRECTORIO_CONTROL))
        self.resultados = WalsPorEjecucion(os.path.join(directorio, SUBDIRECTORIO_RESULTADOS))
        self._lock = threading.Lock()
        self._ejecuciones = {}
        self.recuperadas = []
        self._cerrado = False
        self._recuperar()

    def _recuperar(self):
        """Reconstruye las ejecuciones en curso. Se hace antes de aceptar conexiones y de recibir resultados."""
        con_control = set()
        for id_ejecucion, registros in self.control.recuperar():
            con_control.add(id_ejecucion)
            ejecucion = Ejecucion(self, id_ejecucion)
            iniciada, terminada = self._leer_control(ejecucion, registros)
            if terminada or not iniciada:
                self.cerrar_logs(id_ejecucion)
                continue
            for registro, posicion in self.resultados.iterar_con_posiciones(id_ejecucion):
                clave, _ = _decodificar_resultado_guardado(registro)
                ejecucion._cargar_resultado(clave, posicion)
            # Si los cierres de todas las entradas ya estaban escritos, la ejecución estaba completa
            with ejecucion.condicion:
                ejecucion._completar_si_corresponde()
            self._ejecuciones[id_ejecucion] = ejecucion
            self.recuperadas.append(ejecucion)
            LOG.info("Ejecución %s recuperada: %d lotes, EOF=%s, %d resultados, completa=%s",
                     id_ejecucion, ejecucion.lotes_procesados, ejecucion.eof_recibido,
                     ejecucion.cantidad_resultados, ejecucion.completa)
        # Logs de resultados cuya ejecución ya no tiene control (caída entre un cierre y el otro)
        for id_ejecucion in self.resultados.en_curso():
            if id_ejecucion not in con_control and not self.resultados.esta_cerrada(id_ejecucion):
                self.resultados.cerrar_ejecucion(id_ejecucion)

    @staticmethod
    def _leer_control(ejecucion, registros):
        iniciada = terminada = False
        for registro in registros:
            etiqueta, cuerpo = registro[:1], registro[1:]
            if etiqueta == _INICIO:
                iniciada = True
            elif etiqueta == _LOTE:
                ejecucion.lotes_procesados = max(ejecucion.lotes_procesados, int.from_bytes(cuerpo, "big"))
            elif etiqueta == _EOF:
                ejecucion.eof_recibido = True
            elif etiqueta == _ENTRADA_CERRADA:
                ejecucion.entradas_cerradas.add(cuerpo.decode("utf-8"))
            elif etiqueta == _COMPLETA:
                ejecucion.completa = True
            elif etiqueta in (_ABANDONADA, _FIN):
                terminada = True
            else:
                raise ValueError(f"Registro de control desconocido en la ejecución {ejecucion.id}")
        return iniciada, terminada

    def crear(self):
        """Nueva ejecución con id asignado por el gateway; queda anotada antes de informarla."""
        ejecucion = Ejecucion(self, str(uuid.uuid4()))
        ejecucion._escribir_control(_INICIO)
        with self._lock:
            if self._cerrado:
                raise InterruptedError("El registro de ejecuciones está cerrado")
            self._ejecuciones[ejecucion.id] = ejecucion
        return ejecucion

    def obtener(self, id_ejecucion):
        """La ejecución en curso o None si es desconocida, terminó o fue abandonada."""
        with self._lock:
            return self._ejecuciones.get(id_ejecucion)

    def cerrar_logs(self, id_ejecucion):
        """Registra la ejecución como cerrada y borra sus logs; admite repetirse tras una caída."""
        with self._lock:
            self._ejecuciones.pop(id_ejecucion, None)
            self.resultados.cerrar_ejecucion(id_ejecucion)
            self.control.cerrar_ejecucion(id_ejecucion)

    def cerrar_sesiones(self):
        """Despierta a los emisores cuando el gateway se detiene; el estado queda en disco."""
        with self._lock:
            ejecuciones = tuple(self._ejecuciones.values())
        for ejecucion in ejecuciones:
            with ejecucion.condicion:
                sesion = ejecucion.sesion
            if sesion is not None:
                sesion.marcar_rota()

    def cerrar(self):
        with self._lock:
            self._cerrado = True
            self.control.cerrar()
            self.resultados.cerrar()
