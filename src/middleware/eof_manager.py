"""Seguimiento local de entradas, cierres productores y lotes por ejecución."""

from dataclasses import dataclass, field

from ..common.protocol.dto import Entrada, IdentidadProceso
from ..common.protocol.primitivas import BYTES_U64
from .interfaz import ErrorSeguimientoEntrada


MAXIMO_LOTES = 18446744073709551615


@dataclass(frozen=True)
class EstadoEntrada:
    """Vista inmutable de una entrada; los contadores de trabajo son de esta réplica.

    Los cierres corresponden a productores de todo el grupo. Su total no demuestra
    que las demás réplicas hayan procesado los aportes anunciados.
    """

    productores_esperados: frozenset[IdentidadProceso]
    cierres: tuple[tuple[IdentidadProceso, int], ...]
    lotes_recibidos: int
    lotes_resueltos: int

    @property
    def productores_pendientes(self):
        return self.productores_esperados - {productor for productor, _ in self.cierres}

    @property
    def total_esperado(self):
        """Total anunciado al reunir todos los cierres; None si todavía faltan."""
        if self.productores_pendientes:
            return None
        return sum(total for _, total in self.cierres)

    @property
    def lotes_pendientes(self):
        """Lotes registrados localmente cuya resolución todavía no terminó."""
        return self.lotes_recibidos - self.lotes_resueltos


@dataclass
class _TrabajoEntrada:
    cierres: dict[IdentidadProceso, int] = field(default_factory=dict)
    total_anunciado: int = 0
    lotes_recibidos: int = 0
    lotes_resueltos: int = 0


class SeguimientoEntradas:
    """Administra las entradas del grupo local separadas por ejecución.

    Se utiliza secuencialmente dentro del proceso propietario. Registra cada lote
    al iniciar su procesamiento y lo resuelve una sola vez al terminar. No guarda
    contenidos ni identifica duplicados de datos. El llamador autoriza la limpieza
    cuando el estado ya no participa de la coordinación.
    """

    def __init__(self, configuracion):
        grupo = configuracion.proceso.grupo
        configuracion.obtener_grupo(grupo)
        entradas = {ruta.entrada for ruta in configuracion.rutas if ruta.destino == grupo}
        self._productores = {
            entrada: configuracion.productores_entrada(grupo, entrada)
            for entrada in sorted(entradas)
        }
        self._ejecuciones = {}

    def registrar_ejecucion(self, id_ejecucion):
        """Crea todas las entradas esperadas, incluso sin datos; repetirlo conserva el estado."""
        self._validar_ejecucion(id_ejecucion)
        if id_ejecucion not in self._ejecuciones:
            self._ejecuciones[id_ejecucion] = {
                entrada: _TrabajoEntrada() for entrada in self._productores
            }

    def registrar_lote(self, entrada, productor):
        """Registra un lote local, admitiendo datos que llegaron después de su EOF de control."""
        estado = self._estado(entrada)
        self._validar_productor(entrada.nombre, productor)
        nuevo_total = estado.lotes_recibidos + 1
        self._validar_cantidad(nuevo_total)
        if len(estado.cierres) == len(self._productores[entrada.nombre]):
            if nuevo_total > estado.total_anunciado:
                raise ErrorSeguimientoEntrada("Los lotes locales exceden el total anunciado")
        estado.lotes_recibidos = nuevo_total

    def resolver_lote(self, entrada):
        """Marca la resolución de un lote local previamente registrado."""
        estado = self._estado(entrada)
        if estado.lotes_resueltos == estado.lotes_recibidos:
            raise ErrorSeguimientoEntrada("No hay lotes locales pendientes de resolución")
        estado.lotes_resueltos += 1

    def registrar_cierre(self, entrada, productor, total_lotes):
        """Registra un total final por productor; un cierre idéntico repetido no altera el estado."""
        estado = self._estado(entrada)
        self._validar_productor(entrada.nombre, productor)
        self._validar_cantidad(total_lotes)
        if productor in estado.cierres:
            if estado.cierres[productor] != total_lotes:
                raise ErrorSeguimientoEntrada("El productor anunció dos totales finales distintos")
            return
        nuevo_total = estado.total_anunciado + total_lotes
        self._validar_cantidad(nuevo_total)
        ultimo = len(estado.cierres) + 1 == len(self._productores[entrada.nombre])
        if ultimo and nuevo_total < estado.lotes_recibidos:
            raise ErrorSeguimientoEntrada("El total final es menor que los lotes recibidos localmente")
        estado.cierres[productor] = total_lotes
        estado.total_anunciado = nuevo_total

    def consultar(self, entrada):
        """Devuelve una copia inmutable del seguimiento de la entrada."""
        estado = self._estado(entrada)
        cierres = tuple(sorted(
            estado.cierres.items(), key=lambda elemento: (elemento[0].grupo, elemento[0].instancia),
        ))
        return EstadoEntrada(
            self._productores[entrada.nombre], cierres,
            estado.lotes_recibidos, estado.lotes_resueltos,
        )

    def liberar_ejecucion(self, id_ejecucion):
        """Elimina el seguimiento sin lotes locales pendientes; no modifica otras ejecuciones."""
        self._validar_ejecucion(id_ejecucion)
        entradas = self._ejecuciones.get(id_ejecucion)
        if entradas is None:
            return
        if any(estado.lotes_recibidos != estado.lotes_resueltos for estado in entradas.values()):
            raise ErrorSeguimientoEntrada("La ejecución tiene lotes locales pendientes")
        del self._ejecuciones[id_ejecucion]

    def _estado(self, entrada):
        if type(entrada) is not Entrada:
            raise ErrorSeguimientoEntrada("Se esperaba una Entrada")
        self._validar_ejecucion(entrada.id_ejecucion)
        if type(entrada.nombre) is not str or entrada.nombre not in self._productores:
            raise ErrorSeguimientoEntrada("La entrada no pertenece al grupo receptor")
        if entrada.id_ejecucion not in self._ejecuciones:
            raise ErrorSeguimientoEntrada("La ejecución no está registrada")
        return self._ejecuciones[entrada.id_ejecucion][entrada.nombre]

    def _validar_productor(self, entrada, productor):
        if (type(productor) is not IdentidadProceso or type(productor.grupo) is not str
                or type(productor.instancia) is not int
                or productor not in self._productores[entrada]):
            raise ErrorSeguimientoEntrada("El productor no pertenece a la entrada")

    @staticmethod
    def _validar_ejecucion(id_ejecucion):
        if type(id_ejecucion) is not str or not id_ejecucion.strip():
            raise ErrorSeguimientoEntrada("La ejecución debe tener un identificador no vacío")

    @staticmethod
    def _validar_cantidad(cantidad):
        if type(cantidad) is not int or not 0 <= cantidad <= MAXIMO_LOTES:
            raise ErrorSeguimientoEntrada("La cantidad de lotes debe ser un entero u64")
