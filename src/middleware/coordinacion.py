"""Barrera por entrada basada en cierres productores y avances acumulados de réplicas."""

from dataclasses import dataclass, field

from ..common.protocol.dto import (
    AvanceReplica, Entrada, FinEntrada, IdentidadProceso, PrepararCierre, TipoMensaje,
)
from .eof_manager import SeguimientoEntradas
from .interfaz import ErrorMensajeInvalido, ErrorSeguimientoEntrada


@dataclass
class _Barrera:
    total: int | None = None
    preparacion_enviada: bool = False
    preparada: bool = False
    avances: dict[int, int] = field(default_factory=dict)
    total_resuelto: int = 0
    ultimo_avance: int = -1
    fin_enviado: bool = False
    fin_recibido: bool = False
    fin_entregado: bool = False


class CoordinacionCierre:
    """Coordina entradas de un grupo sin esperas activas ni llamadas al negocio.

    Cada instancia pertenece a un proceso y se utiliza secuencialmente. emitir
    confirma la publicación de controles. Los avances se envían al preparar la
    barrera y cuando se resuelve un lote posterior, nunca durante la ingesta previa.
    """

    def __init__(self, configuracion, topologia, emitir):
        self._proceso = configuracion.proceso
        self._cantidad = configuracion.obtener_grupo(self._proceso.grupo).instancias
        self._topologia = topologia
        self._emitir = emitir
        self._seguimiento = SeguimientoEntradas(configuracion)
        self._productores = {
            ruta.entrada: configuracion.productores_entrada(self._proceso.grupo, ruta.entrada)
            for ruta in configuracion.rutas if ruta.destino == self._proceso.grupo
        }
        self._barreras = {}

    def validar_datos(self, sobre):
        """Comprueba que un lote pertenezca a una entrada todavía abierta del grupo."""
        if sobre.origen not in self._productores.get(sobre.entrada.nombre, ()):
            raise ErrorMensajeInvalido("El productor no pertenece a la entrada")
        estado = self._barreras.get(sobre.entrada)
        if estado is not None and estado.fin_recibido:
            raise ErrorSeguimientoEntrada("Se recibieron datos después del cierre consolidado")

    def resolver_lote(self, sobre):
        """Cuenta un lote tras resolver su entrega y las salidas o rechazos asociados."""
        self.validar_datos(sobre)
        estado = self._estado(sobre.entrada)
        # El receptor procesa secuencialmente: durante el trabajo del negocio no
        # evalúa controles. Un lote sin resolver no aporta al contador de la barrera.
        self._seguimiento.registrar_lote(sobre.entrada, sobre.origen)
        self._seguimiento.resolver_lote(sobre.entrada)
        if estado.preparada:
            self._informar_avance(sobre.entrada, estado)

    def recibir_control(self, sobre):
        """Aplica un control válido y devuelve FinEntrada cuando el cierre es entregable."""
        self._validar_control(sobre)
        entrada = sobre.entrada
        estado = self._estado(entrada)
        if sobre.tipo is TipoMensaje.EOF:
            self._recibir_eof(sobre, estado)
        elif sobre.tipo is TipoMensaje.PREPARAR_CIERRE:
            self._preparar(entrada, estado, sobre.contenido.total_lotes)
        elif sobre.tipo is TipoMensaje.AVANCE_REPLICA:
            self._recibir_avance(sobre, estado)
        else:
            if not estado.preparada:
                raise ErrorSeguimientoEntrada("El cierre consolidado requiere preparación")
            estado.fin_recibido = True
            if not estado.fin_entregado:
                return FinEntrada(entrada)
        return None

    def confirmar_fin(self, entrada):
        """Registra que el negocio terminó de atender el cierre local de la entrada."""
        estado = self._barreras[entrada]
        if not estado.fin_recibido:
            raise ErrorSeguimientoEntrada("La entrada no recibió su cierre consolidado")
        estado.fin_entregado = True

    def validar_liberacion(self, id_ejecucion):
        """Exige atender todas las entradas configuradas de una ejecución con actividad."""
        estados = [self._barreras.get(Entrada(id_ejecucion, nombre)) for nombre in self._productores]
        if any(estado is not None for estado in estados) and any(
            estado is None or not estado.fin_entregado for estado in estados
        ):
            raise ErrorSeguimientoEntrada("La ejecución tiene cierres locales pendientes")

    def entradas_pendientes(self, id_ejecucion):
        """Devuelve entradas sin cierre consolidado local, incluidas las que no tuvieron actividad."""
        pendientes = set()
        for nombre in self._productores:
            estado = self._barreras.get(Entrada(id_ejecucion, nombre))
            if estado is None or not estado.fin_recibido:
                pendientes.add(nombre)
        return frozenset(pendientes)

    def liberar_ejecucion(self, id_ejecucion):
        """Libera los contadores y barreras de una ejecución con cierres locales atendidos."""
        self.validar_liberacion(id_ejecucion)
        self._seguimiento.liberar_ejecucion(id_ejecucion)
        self._barreras = {entrada: estado for entrada, estado in self._barreras.items()
                          if entrada.id_ejecucion != id_ejecucion}

    def _estado(self, entrada):
        self._seguimiento.registrar_ejecucion(entrada.id_ejecucion)
        return self._barreras.setdefault(entrada, _Barrera())

    def _coordinadora(self, entrada):
        return self._topologia.instancia_coordinadora(self._proceso.grupo, entrada)

    def _validar_control(self, sobre):
        if sobre.entrada.nombre not in self._productores:
            raise ErrorMensajeInvalido("El control no corresponde a una entrada local")
        coordinadora = self._coordinadora(sobre.entrada)
        if sobre.tipo is TipoMensaje.EOF:
            valido = (sobre.origen in self._productores[sobre.entrada.nombre]
                      and self._proceso.instancia == coordinadora)
        elif sobre.tipo is TipoMensaje.AVANCE_REPLICA:
            valido = (sobre.origen.grupo == self._proceso.grupo
                      and 0 <= sobre.origen.instancia < self._cantidad
                      and self._proceso.instancia == coordinadora)
        elif sobre.tipo in (TipoMensaje.PREPARAR_CIERRE, TipoMensaje.FIN_ENTRADA):
            valido = sobre.origen == IdentidadProceso(self._proceso.grupo, coordinadora)
        else:
            valido = False
        if not valido:
            raise ErrorMensajeInvalido("El tipo, origen o destinatario del control no es válido")

    def _recibir_eof(self, sobre, estado):
        entrada = sobre.entrada
        self._seguimiento.registrar_cierre(entrada, sobre.origen, sobre.contenido.total_lotes)
        total = self._seguimiento.consultar(entrada).total_esperado
        if total is not None and not estado.preparacion_enviada:
            self._emitir(TipoMensaje.PREPARAR_CIERRE, entrada, PrepararCierre(total))
            estado.total = total
            estado.preparacion_enviada = True

    def _preparar(self, entrada, estado, total):
        if estado.total is not None and estado.total != total:
            raise ErrorSeguimientoEntrada("La barrera recibió dos totales diferentes")
        if self._seguimiento.consultar(entrada).lotes_resueltos > total:
            raise ErrorSeguimientoEntrada("Los lotes locales superan el total de la barrera")
        estado.total = total
        estado.preparada = True
        self._informar_avance(entrada, estado)

    def _informar_avance(self, entrada, estado):
        resueltos = self._seguimiento.consultar(entrada).lotes_resueltos
        if resueltos > estado.total:
            raise ErrorSeguimientoEntrada("Los lotes locales superan el total de la barrera")
        if resueltos != estado.ultimo_avance:
            self._emitir(TipoMensaje.AVANCE_REPLICA, entrada, AvanceReplica(resueltos),
                         self._coordinadora(entrada))
            estado.ultimo_avance = resueltos

    def _recibir_avance(self, sobre, estado):
        if not estado.preparacion_enviada:
            raise ErrorSeguimientoEntrada("Se recibió un avance antes de preparar la barrera")
        instancia = sobre.origen.instancia
        nuevo = sobre.contenido.lotes_resueltos
        anterior = estado.avances.get(instancia, 0)
        if nuevo < anterior:
            return
        total = estado.total_resuelto - anterior + nuevo
        if total > estado.total:
            raise ErrorSeguimientoEntrada("Las réplicas resolvieron más lotes que los anunciados")
        estado.avances[instancia] = nuevo
        estado.total_resuelto = total
        if len(estado.avances) == self._cantidad and total == estado.total and not estado.fin_enviado:
            self._emitir(TipoMensaje.FIN_ENTRADA, sobre.entrada, None)
            estado.fin_enviado = True
