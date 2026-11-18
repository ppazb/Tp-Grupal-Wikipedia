"""Reservas lógicas por cola, compartidas entre productores y consumidores."""

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from ..common.io_adapters.state_storage import WAL, PuntoWAL
from ..common.io_adapters.formato_wal import ErrorWAL, RegistroWAL
from .interfaz import ErrorCapacidad
from .salidas_durables import crear_directorio_durable


RESERVAR = 1
CONSUMIR = 2
CANCELAR = 3
RESERVA_CONSUMIDA = 4
RESERVA_CANCELADA = 5
BYTES_ID = 16
BYTES_HUELLA = 32
MAX_BYTES_REGISTRO = 1 + BYTES_ID + BYTES_HUELLA


class EstadoReserva(Enum):
    """Distingue una reserva nueva, un envío incierto y un lote ya consumido."""

    NUEVA = 1
    PENDIENTE = 2
    COMPLETADA = 3


@dataclass
class _EstadoCola:
    limite: int
    mensajes: dict = field(default_factory=dict)
    ocupados: int = 0
    punto: PuntoWAL | None = None

    def aplicar(self, registro):
        contenido = registro.contenido
        if len(contenido) not in (1 + BYTES_ID, MAX_BYTES_REGISTRO):
            raise ErrorCapacidad('Registro de capacidad con longitud inválida')
        tipo = contenido[0]
        identificador = UUID(bytes=contenido[1:1 + BYTES_ID])
        if identificador.int == 0:
            raise ErrorCapacidad('Identificador de reserva nulo')
        anterior = self.mensajes.get(identificador)
        if tipo in (RESERVA_CONSUMIDA, RESERVA_CANCELADA) and len(contenido) == MAX_BYTES_REGISTRO:
            if anterior is not None:
                raise ErrorCapacidad('Reserva compacta repetida')
            estado = CONSUMIR if tipo == RESERVA_CONSUMIDA else CANCELAR
            self.mensajes[identificador] = (contenido[1 + BYTES_ID:], estado)
        elif tipo == RESERVAR and len(contenido) == MAX_BYTES_REGISTRO:
            huella = contenido[1 + BYTES_ID:]
            if anterior is not None and (anterior[0] != huella or anterior[1] != CANCELAR):
                raise ErrorCapacidad('Reserva repetida o con otro contenido')
            if self.ocupados >= self.limite:
                raise ErrorCapacidad('El historial excede la capacidad configurada')
            self.mensajes[identificador] = (huella, RESERVAR)
            self.ocupados += 1
        elif tipo in (CONSUMIR, CANCELAR) and len(contenido) == 1 + BYTES_ID:
            if anterior is None or anterior[1] != RESERVAR:
                raise ErrorCapacidad('Devolución sin reserva pendiente')
            self.mensajes[identificador] = (anterior[0], tipo)
            self.ocupados -= 1
        else:
            raise ErrorCapacidad('Operación de capacidad inválida')


class RegistroReservas:
    """Mantiene el límite y las resoluciones por ID bajo un turno por cola."""

    def __init__(self, configuracion, conexion, topologia, turnos):
        self._directorio = crear_directorio_durable(
            Path(configuracion.directorio_recepcion) / 'capacidad',
        )
        self._limite = configuracion.limites.max_mensajes_por_cola
        self._conexion = conexion
        self._topologia = topologia
        self._turnos = turnos
        self._estados = {}

    @contextmanager
    def abrir(self, cola):
        """Recupera el estado compartido de una cola y conserva su exclusión."""
        turno = self._topologia.cola_turno_capacidad(cola)
        with self._turnos.adquirir(turno):
            try:
                with WAL(self._directorio / f'{cola}.wal',
                         max_bytes_por_registro=MAX_BYTES_REGISTRO,
                         comprobar_detencion=self._conexion.comprobar_apagado) as wal:
                    estado = self._estados.setdefault(cola, _EstadoCola(self._limite))
                    if estado.punto is not None and not wal.conserva_punto(estado.punto):
                        estado = self._estados[cola] = _EstadoCola(self._limite)
                    wal.recuperar(estado.aplicar, desde=estado.punto)
                    yield ReservaAbierta(estado, wal)
                    estado.punto = wal.punto_recuperacion
            except (OSError, ErrorWAL) as error:
                self._estados.pop(cola, None)
                raise ErrorCapacidad('No se pudo recuperar el registro compartido de capacidad') from error
            except BaseException:
                self._estados.pop(cola, None)
                raise

    def compactar(self):
        """Resume cada cola bajo exclusión, sin eliminar reservas ni resoluciones."""
        for cola in self._topologia.colas_datos():
            with self.abrir(cola) as reserva:
                reserva.compactar()

    def cerrar(self):
        """Descarta índices locales sin borrar reservas ni resoluciones durables."""
        self._estados.clear()


class ReservaAbierta:
    """Opera sobre el WAL mientras el registro conserva el turno de la cola."""

    def __init__(self, estado, wal):
        self._estado = estado
        self._wal = wal

    @property
    def disponibles(self):
        return self._estado.limite - self._estado.ocupados

    def adquirir(self, identificador, cuerpo):
        """Reserva un lote nuevo o recupera su estado sin cobrar otro lugar."""
        anterior = self._consultar(identificador, cuerpo)
        if anterior is not None:
            if anterior[1] == CONSUMIR:
                return EstadoReserva.COMPLETADA
            if anterior[1] == RESERVAR:
                return EstadoReserva.PENDIENTE
        if not self.disponibles:
            return None
        self._registrar(bytes([RESERVAR]) + identificador.bytes + sha256(cuerpo).digest())
        return EstadoReserva.NUEVA

    def validar_entrega(self, identificador, cuerpo):
        """Comprueba que los bytes recibidos pertenecen a una reserva real."""
        anterior = self._consultar(identificador, cuerpo)
        if anterior is None or anterior[1] == CANCELAR:
            raise ErrorCapacidad('La entrega no tiene una reserva durable válida')

    def devolver(self, identificador, *, consumida):
        """Libera una reserva una sola vez, conservando la identidad del lote."""
        anterior = self._estado.mensajes.get(identificador)
        if anterior is None:
            raise ErrorCapacidad('No existe la reserva que se intenta devolver')
        tipo = CONSUMIR if consumida else CANCELAR
        if anterior[1] == tipo:
            return
        if anterior[1] != RESERVAR:
            raise ErrorCapacidad('La devolución contradice una resolución anterior')
        self._registrar(bytes([tipo]) + identificador.bytes)

    def compactar(self):
        """Conserva una operación por ID con su estado final y huella."""
        self._wal.compactar(self._registros_compactos())

    def _registros_compactos(self):
        tipos = {RESERVAR: RESERVAR, CONSUMIR: RESERVA_CONSUMIDA, CANCELAR: RESERVA_CANCELADA}
        for identificador, (huella, estado) in self._estado.mensajes.items():
            yield bytes([tipos[estado]]) + identificador.bytes + huella

    def _consultar(self, identificador, cuerpo):
        if type(identificador) is not UUID or identificador.int == 0 or type(cuerpo) is not bytes:
            raise ErrorCapacidad('La reserva requiere el ID y los bytes del lote')
        anterior = self._estado.mensajes.get(identificador)
        if anterior is not None and anterior[0] != sha256(cuerpo).digest():
            raise ErrorCapacidad('Se reutilizó una reserva con otro contenido')
        return anterior

    def _registrar(self, contenido):
        try:
            self._wal.registrar(contenido)
        except (ErrorWAL, OSError) as error:
            raise ErrorCapacidad('No se pudo persistir la reserva de capacidad') from error
        # La misma interpretación se usa durante la recuperación y la operación.
        self._estado.aplicar(RegistroWAL(0, contenido))
