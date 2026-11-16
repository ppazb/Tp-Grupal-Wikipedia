"""Registro compartido y particionado de identidades recibidas y completadas."""

from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from ..common.io_adapters.state_storage import WAL, PuntoWAL
from ..common.io_adapters.formato_wal import ErrorWAL
from .interfaz import ContextoRecepcion, ErrorPersistencia, ErrorRecepcion
from .salidas_durables import crear_directorio_durable


INICIAR = 1
COMPLETAR = 2
BYTES_ID = 16
BYTES_HUELLA = 32
MAX_BYTES_REGISTRO = 1 + BYTES_ID + BYTES_HUELLA


@dataclass
class _Particion:
    """Índice reconstruido y posición durable del historial que lo respalda."""

    registros: dict = field(default_factory=dict)
    punto: PuntoWAL | None = None

    def aplicar(self, registro):
        contenido = registro.contenido
        if len(contenido) < 1 + BYTES_ID:
            raise ErrorPersistencia('Registro de recepción incompleto')
        tipo = contenido[0]
        identificador = UUID(bytes=contenido[1:1 + BYTES_ID])
        if tipo == INICIAR and len(contenido) == MAX_BYTES_REGISTRO:
            if identificador in self.registros:
                raise ErrorPersistencia('Inicio repetido en el registro de recepción')
            self.registros[identificador] = (contenido[1 + BYTES_ID:], False)
        elif tipo == COMPLETAR and len(contenido) == 1 + BYTES_ID:
            anterior = self.registros.get(identificador)
            if anterior is None or anterior[1]:
                raise ErrorPersistencia('Finalización sin recepción pendiente')
            self.registros[identificador] = (anterior[0], True)
        else:
            raise ErrorPersistencia('Operación inválida en el registro de recepción')


class RecepcionesDurables:
    """Serializa cada partición y reconoce mensajes completados por cualquier réplica.

    Las réplicas comparten el directorio, la cantidad de particiones y sus turnos.
    El callback debe dejar sus efectos durables antes de confirmar; un inicio
    incompleto requiere que el negocio recupere o repita esos efectos por ID.
    """

    def __init__(self, configuracion, conexion, topologia, turnos):
        self._directorio = crear_directorio_durable(
            Path(configuracion.directorio_recepcion) / configuracion.proceso.grupo,
        )
        self._proceso = configuracion.proceso
        self._cantidad = configuracion.particiones_recepcion
        self._conexion = conexion
        self._topologia = topologia
        self._turnos = turnos
        self._particiones = {}
        self._actual = None
        self._wal = None
        self._particion = None
        self._completada = False

    @property
    def actual(self):
        """Devuelve la identidad estable que el negocio usa para recuperar sus efectos."""
        if self._actual is None:
            raise ErrorRecepcion('No hay un lote en procesamiento')
        return self._actual

    @contextmanager
    def recibir(self, sobre, cuerpo):
        """Conserva el turno del lote y entrega si ya fue completado durablemente."""
        if self._actual is not None:
            raise ErrorRecepcion('El registro de recepción no admite reentrancia')
        numero = sobre.id_mensaje.int % self._cantidad
        cola = self._topologia.cola_turno_recepcion(self._proceso.grupo, numero)
        with self._turnos.adquirir(cola):
            error_callback = None
            try:
                with WAL(self._directorio / f'recepciones-{numero}.wal',
                         max_bytes_por_registro=MAX_BYTES_REGISTRO,
                         comprobar_detencion=self._conexion.comprobar_apagado) as wal:
                    particion = self._particiones.setdefault(numero, _Particion())
                    wal.recuperar(particion.aplicar, desde=particion.punto)
                    huella = sha256(cuerpo).digest()
                    anterior = particion.registros.get(sobre.id_mensaje)
                    if anterior is not None and anterior[0] != huella:
                        raise ErrorPersistencia('El mismo ID de recepción tiene otro contenido u origen')
                    if anterior is None:
                        wal.registrar(bytes([INICIAR]) + sobre.id_mensaje.bytes + huella)
                        particion.registros[sobre.id_mensaje] = (huella, False)
                    self._actual = ContextoRecepcion(
                        sobre.id_mensaje, sobre.entrada, sobre.origen, anterior is not None,
                    )
                    self._wal, self._particion = wal, particion
                    self._completada = anterior is not None and anterior[1]
                    try:
                        yield self._completada
                    except BaseException as error:
                        error_callback = error
                        raise
                    particion.punto = wal.punto_recuperacion
            except (OSError, ErrorWAL) as error:
                self._particiones.pop(numero, None)
                if error is error_callback:
                    raise
                raise ErrorPersistencia('Falló el registro compartido de recepción') from error
            except BaseException:
                # La memoria pudo avanzar antes de un error: se reconstruye desde cero.
                self._particiones.pop(numero, None)
                raise
            finally:
                self._actual = self._wal = self._particion = None
                self._completada = False

    def confirmar(self):
        """Persiste que el negocio completó el lote antes del ACK de su entrega."""
        actual = self.actual
        if self._completada:
            raise ErrorRecepcion('El lote ya fue completado')
        self._conexion.comprobar_apagado()
        try:
            self._wal.registrar(bytes([COMPLETAR]) + actual.id_mensaje.bytes)
        except (OSError, ErrorWAL) as error:
            raise ErrorPersistencia('No se pudo persistir la finalización del lote') from error
        huella, _ = self._particion.registros[actual.id_mensaje]
        self._particion.registros[actual.id_mensaje] = (huella, True)
        self._completada = True

    def cerrar(self):
        """Descarta índices locales; conserva el historial compartido para reentregas."""
        if self._actual is not None:
            raise ErrorRecepcion('No se puede cerrar durante el procesamiento de un lote')
        self._particiones.clear()
