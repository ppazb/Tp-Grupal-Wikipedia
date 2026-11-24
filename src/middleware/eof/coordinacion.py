"""Cierre local de entradas: cada réplica decide por sí sola cuándo terminó una entrada.

Cada productor anuncia, por EOF, cuántos lotes envió a esta réplica. La entrada está
completa cuando todos sus productores cerraron y la réplica resolvió exactamente esa
cantidad de lotes de cada uno. No hay coordinadora ni mensajes entre réplicas.
"""

from dataclasses import dataclass, field

from ...common.protocol.dto import Entrada, FinEntrada, IdentidadProceso, TipoMensaje
from .ventana import VentanaLotes
from ..persistencia.coordinacion import CambioEOF, RegistroEOF
from ..interfaz import ErrorMensajeInvalido, ErrorSeguimientoEntrada


MAXIMO_LOTES = (1 << 64) - 1  # Mayor cantidad representable por un entero u64.


@dataclass
class _EstadoEntrada:
    resueltos: dict = field(default_factory=dict)   # productor -> lotes resueltos
    ventanas: dict = field(default_factory=dict)    # productor -> VentanaLotes
    cierres: dict = field(default_factory=dict)     # productor -> total anunciado
    fin_entregado: bool = False


class CierreLocal:
    """Cierre de entradas de un grupo sin esperas activas ni llamadas al negocio.

    Cada instancia pertenece a un proceso y se utiliza secuencialmente. Todo cambio se
    persiste en el WAL de EOF antes de aplicarse en memoria.
    """

    def __init__(self, configuracion, *, directorio, registros_por_foto=None):
        self._proceso = configuracion.proceso
        self._k = configuracion.limites.ventana_lotes
        self._productores = {
            ruta.entrada: configuracion.productores_entrada(self._proceso.grupo, ruta.entrada)
            for ruta in configuracion.rutas if ruta.destino == self._proceso.grupo
        }
        self._entradas = {}
        self._retiradas = {}  # id_ejecucion -> {Entrada: {productor: total}}
        argumentos = {} if registros_por_foto is None else {'registros_por_foto': registros_por_foto}
        self._registro = RegistroEOF(directorio, configuracion, **argumentos)
        self._inutilizable = False
        try:
            for cambio, *datos in self._registro.recuperar():
                if cambio is CambioEOF.FOTO:
                    self._entradas = {
                        entrada: _EstadoEntrada(dict(res), dict(ven), dict(cie), fin)
                        for entrada, (res, ven, cie, fin) in datos[0].items()
                    }
                    self._retiradas = datos[1]
                else:
                    self._aplicar_cambio(cambio, *datos)
        except BaseException:
            self._registro.cerrar_registro()
            raise

    # --- datos -----------------------------------------------------------------------------

    def validar_datos(self, sobre):
        """Valida el productor y rechaza lotes nuevos que excedan el cierre anunciado."""
        self._comprobar_disponible()
        if sobre.origen not in self._productores.get(sobre.entrada.nombre, ()):
            raise ErrorMensajeInvalido("El productor no pertenece a la entrada")
        if self.ejecucion_retirada(sobre.entrada.id_ejecucion):
            return
        estado = self._entradas.get(sobre.entrada)
        if estado is None or self.lote_resuelto(sobre):
            return
        cierre = estado.cierres.get(sobre.origen)
        if cierre is not None and estado.resueltos.get(sobre.origen, 0) >= cierre:
            raise ErrorSeguimientoEntrada("Los lotes locales del productor exceden su cierre anunciado")

    def lote_resuelto(self, sobre):
        """Consulta la ventana del productor: si esta réplica ya registró ese lote."""
        self._comprobar_disponible()
        estado = self._entradas.get(sobre.entrada)
        ventana = None if estado is None else estado.ventanas.get(sobre.origen)
        return ventana is not None and ventana.contiene(sobre.id_entrada, sobre.indice)

    def resolver_lote(self, sobre):
        """Registra una sola contribución por lote; un duplicado no cambia nada."""
        self.validar_datos(sobre)
        if self.ejecucion_retirada(sobre.entrada.id_ejecucion) or self.lote_resuelto(sobre):
            return
        self._registrar_cambio(CambioEOF.LOTE_RESUELTO, sobre.entrada, productor=sobre.origen,
                               id_entrada=sobre.id_entrada, indice=sobre.indice)

    # --- controles -------------------------------------------------------------------------

    def recibir_control(self, sobre):
        """Persiste el EOF de un productor; devuelve FinEntrada si la entrada quedó completa."""
        self._comprobar_disponible()
        if sobre.tipo is not TipoMensaje.EOF:
            raise ErrorMensajeInvalido("Solo se aceptan controles EOF")
        if sobre.origen not in self._productores.get(sobre.entrada.nombre, ()):
            raise ErrorMensajeInvalido("El EOF no pertenece a un productor de la entrada")
        total = sobre.contenido.total_lotes
        if type(total) is not int or not 0 <= total <= MAXIMO_LOTES:
            raise ErrorMensajeInvalido("El total de lotes debe ser un entero u64")
        entrada = sobre.entrada
        if self.ejecucion_retirada(entrada.id_ejecucion):
            self._validar_control_tardio(sobre)
            return None
        estado = self._obtener_o_crear(entrada)
        anterior = estado.cierres.get(sobre.origen)
        if anterior is None:
            if total < estado.resueltos.get(sobre.origen, 0):
                raise ErrorSeguimientoEntrada("El productor anunció menos lotes que los resueltos localmente")
            self._registrar_cambio(CambioEOF.CIERRE_PRODUCTOR, entrada, productor=sobre.origen, valor=total)
        elif anterior != total:
            raise ErrorSeguimientoEntrada("El productor anunció dos totales finales distintos")
        return self.finalizacion_pendiente(entrada)

    def entrada_completa(self, entrada):
        """True si todos los productores cerraron y esta réplica resolvió exactamente su total."""
        estado = self._entradas.get(entrada)
        if estado is None:
            return False
        productores = self._productores.get(entrada.nombre, ())
        return bool(productores) and all(
            productor in estado.cierres
            and estado.resueltos.get(productor, 0) == estado.cierres[productor]
            for productor in productores
        )

    def finalizacion_pendiente(self, entrada):
        """FinEntrada si la entrada está completa y su negocio todavía no terminó."""
        self._comprobar_disponible()
        estado = self._entradas.get(entrada)
        if estado is not None and not estado.fin_entregado and self.entrada_completa(entrada):
            return FinEntrada(entrada)
        return None

    def siguiente_finalizacion_pendiente(self):
        """Primera entrada completa cuyo callback de fin todavía no terminó durablemente."""
        self._comprobar_disponible()
        for entrada in self._entradas:
            pendiente = self.finalizacion_pendiente(entrada)
            if pendiente is not None:
                return pendiente
        return None

    def confirmar_fin(self, entrada):
        """Registra que el negocio terminó de atender el cierre local de la entrada."""
        self._comprobar_disponible()
        if not self.entrada_completa(entrada):
            raise ErrorSeguimientoEntrada("La entrada todavía no está completa")
        if not self._entradas[entrada].fin_entregado:
            self._registrar_cambio(CambioEOF.FIN_ENTREGADO, entrada)

    # --- ejecuciones -----------------------------------------------------------------------

    def validar_liberacion(self, id_ejecucion):
        """Exige atender todas las entradas configuradas de una ejecución con actividad."""
        self._comprobar_disponible()
        if self.ejecucion_retirada(id_ejecucion):
            return
        if self._entradas_sin_fin(id_ejecucion):
            raise ErrorSeguimientoEntrada("La ejecución tiene cierres locales pendientes")

    def entradas_pendientes(self, id_ejecucion, *, finalizando=None):
        """Devuelve entradas sin finalización atendida, exceptuando el callback actual."""
        self._comprobar_disponible()
        if self.ejecucion_retirada(id_ejecucion):
            return frozenset()
        return frozenset(
            nombre for nombre in self._entradas_sin_fin(id_ejecucion)
            if Entrada(id_ejecucion, nombre) != finalizando
        )

    def liberar_ejecucion(self, id_ejecucion):
        """Persiste el retiro y reemplaza el detalle por una foto con los cierres finales."""
        self.validar_liberacion(id_ejecucion)
        if not self.ejecucion_retirada(id_ejecucion):
            self._registrar_cambio(CambioEOF.LIBERAR_EJECUCION, Entrada(id_ejecucion, ""))
        self._escribir_foto()

    def ejecucion_retirada(self, id_ejecucion):
        """Consulta la evidencia durable que impide reabrir una ejecución terminada."""
        self._comprobar_disponible()
        return id_ejecucion in self._retiradas

    def ejecuciones_retiradas(self):
        """Entrega las identidades retiradas para completar su limpieza al reiniciar."""
        self._comprobar_disponible()
        return tuple(self._retiradas)

    def ejecuciones_pendientes_de_retiro(self):
        """Identifica ejecuciones con todos sus finalizadores atendidos y sin marca de retiro."""
        self._comprobar_disponible()
        ejecuciones = {entrada.id_ejecucion for entrada in self._entradas}
        return tuple(ejecucion for ejecucion in ejecuciones if not self._entradas_sin_fin(ejecucion))

    def cerrar_registro(self):
        """Libera el WAL de coordinación sin borrar el estado recuperable."""
        self._inutilizable = True
        self._registro.cerrar_registro()

    # --- internos --------------------------------------------------------------------------

    def _entradas_sin_fin(self, id_ejecucion):
        sin_fin = set()
        for nombre in self._productores:
            estado = self._entradas.get(Entrada(id_ejecucion, nombre))
            if estado is None or not estado.fin_entregado:
                sin_fin.add(nombre)
        return sin_fin

    def _validar_control_tardio(self, sobre):
        """Acepta repeticiones del cierre y rechaza totales contradictorios sin reabrirlo."""
        cierres = self._retiradas[sobre.entrada.id_ejecucion].get(sobre.entrada, {})
        if cierres.get(sobre.origen) != sobre.contenido.total_lotes:
            raise ErrorSeguimientoEntrada('El EOF contradice el cierre de una ejecución retirada')

    def _obtener_o_crear(self, entrada):
        return self._entradas.setdefault(entrada, _EstadoEntrada())

    def _comprobar_disponible(self):
        """Rechaza operaciones después del cierre o de un error de persistencia."""
        if self._inutilizable:
            raise ErrorSeguimientoEntrada('El estado de EOF requiere cerrar y recuperar la instancia')

    def _registrar_cambio(self, cambio, entrada, *, productor=None, valor=0, id_entrada=None, indice=0):
        """Sincroniza una transición, la aplica en memoria y compacta cada N registros."""
        self._comprobar_disponible()
        try:
            self._registro.registrar(cambio, entrada, productor=productor, valor=valor,
                                     id_entrada=id_entrada, indice=indice)
            self._aplicar_cambio(cambio, entrada, productor, valor, id_entrada, indice)
            if self._registro.requiere_foto:
                self._escribir_foto()
        except BaseException:
            self._inutilizable = True
            raise

    def _escribir_foto(self):
        try:
            self._registro.escribir_foto(
                {entrada: (estado.resueltos, estado.ventanas, estado.cierres, estado.fin_entregado)
                 for entrada, estado in self._entradas.items()},
                self._retiradas,
            )
        except BaseException:
            self._inutilizable = True
            raise

    def _aplicar_cambio(self, cambio, entrada, productor, valor, id_entrada, indice):
        """Reconstruye una transición local sin ejecutar el negocio."""
        if cambio is CambioEOF.LIBERAR_EJECUCION:
            self._retiradas[entrada.id_ejecucion] = {
                clave: dict(estado.cierres) for clave, estado in self._entradas.items()
                if clave.id_ejecucion == entrada.id_ejecucion
            }
            self._entradas = {clave: estado for clave, estado in self._entradas.items()
                              if clave.id_ejecucion != entrada.id_ejecucion}
            return
        estado = self._obtener_o_crear(entrada)
        if cambio is CambioEOF.LOTE_RESUELTO:
            ventana = estado.ventanas.get(productor)
            if ventana is None:
                ventana = estado.ventanas[productor] = VentanaLotes(self._k)
            if not ventana.contiene(id_entrada, indice):
                ventana.registrar(id_entrada, indice)
                estado.resueltos[productor] = estado.resueltos.get(productor, 0) + 1
        elif cambio is CambioEOF.CIERRE_PRODUCTOR:
            estado.cierres[productor] = valor
        elif cambio is CambioEOF.FIN_ENTREGADO:
            estado.fin_entregado = True
