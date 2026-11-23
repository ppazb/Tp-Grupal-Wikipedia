"""Barrera por entrada basada en cierres productores y avances acumulados de réplicas."""

from dataclasses import dataclass, field

from ..common.protocol.dto import (
    AvanceReplica, Entrada, FinEntrada, IdentidadProceso, PrepararCierre, TipoMensaje,
)
from .eof_manager import SeguimientoEntradas
from .persistencia_eof import CambioEOF, RegistroEOF
from .interfaz import ErrorMensajeInvalido, ErrorSeguimientoEntrada


@dataclass
class _Barrera:
    lotes_resueltos: set[tuple[IdentidadProceso, str, int]] = field(default_factory=set)
    resueltos_por_productor: dict[IdentidadProceso, int] = field(default_factory=dict)
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

    def __init__(self, configuracion, topologia, emitir, *, directorio):
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
        self._registro = RegistroEOF(directorio, configuracion)
        self._inutilizable = False
        try:
            for cambio in self._registro.recuperar():
                self._aplicar_cambio(*cambio)
        except BaseException:
            self._registro.cerrar_registro()
            raise

    def validar_datos(self, sobre):
        """Valida el productor y admite reentregas conocidas de una entrada cerrada."""
        self._comprobar_disponible()
        if sobre.origen not in self._productores.get(sobre.entrada.nombre, ()):
            raise ErrorMensajeInvalido("El productor no pertenece a la entrada")
        estado = self._barreras.get(sobre.entrada)
        if estado is not None and estado.fin_recibido and not self.lote_resuelto(sobre):
            raise ErrorSeguimientoEntrada("Se recibieron datos después del cierre consolidado")
        if estado is not None and not self.lote_resuelto(sobre):
            cierres = dict(self._seguimiento.consultar(sobre.entrada).cierres)
            total = cierres.get(sobre.origen)
            if total is not None and estado.resueltos_por_productor.get(sobre.origen, 0) >= total:
                raise ErrorSeguimientoEntrada("Los lotes locales del productor exceden su cierre anunciado")

    def lote_resuelto(self, sobre):
        """Consulta si esta réplica ya registró la resolución durable de la entrada identificada."""
        self._comprobar_disponible()
        estado = self._barreras.get(sobre.entrada)
        identidad = (sobre.origen, sobre.id_entrada, sobre.indice)
        return estado is not None and identidad in estado.lotes_resueltos

    def resolver_lote(self, sobre):
        """Registra una sola contribución por lote y completa el aviso de avance correspondiente."""
        self.validar_datos(sobre)
        estado = self._estado(sobre.entrada)
        # El receptor procesa secuencialmente: durante el trabajo del negocio no
        # evalúa controles. Un lote sin resolver no aporta al contador de la barrera.
        if not self.lote_resuelto(sobre):
            self._seguimiento.validar_lote(sobre.entrada, sobre.origen)
            self._registrar_cambio(CambioEOF.LOTE_RESUELTO, sobre.entrada, productor=sobre.origen,
                                  id_entrada=sobre.id_entrada, indice=sobre.indice)
        if estado.preparada:
            self._informar_avance(sobre.entrada, estado)

    def recibir_control(self, sobre):
        """Persiste controles nuevos y reconoce repeticiones por entrada, tipo y emisor."""
        self._comprobar_disponible()
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
            if not estado.fin_recibido:
                self._registrar_cambio(CambioEOF.FIN_RECIBIDO, entrada)
            if not estado.fin_entregado:
                return FinEntrada(entrada)
        return None

    def confirmar_fin(self, entrada):
        """Registra que el negocio terminó de atender el cierre local de la entrada."""
        self._comprobar_disponible()
        estado = self._barreras[entrada]
        if not estado.fin_recibido:
            raise ErrorSeguimientoEntrada("La entrada no recibió su cierre consolidado")
        if not estado.fin_entregado:
            self._registrar_cambio(CambioEOF.FIN_ENTREGADO, entrada)

    def reanudar_controles(self):
        """Completa publicaciones pendientes a partir de las transiciones recuperadas."""
        self._comprobar_disponible()
        for entrada, estado in self._barreras.items():
            es_coordinadora = self._proceso.instancia == self._coordinadora(entrada)
            if es_coordinadora:
                self._enviar_preparacion_pendiente(entrada, estado)
            if estado.preparada:
                self._informar_avance(entrada, estado)
            if es_coordinadora:
                self._enviar_fin_pendiente(entrada, estado)

    def validar_liberacion(self, id_ejecucion):
        """Exige atender todas las entradas configuradas de una ejecución con actividad."""
        self._comprobar_disponible()
        estados = [self._barreras.get(Entrada(id_ejecucion, nombre)) for nombre in self._productores]
        if any(estado is not None for estado in estados) and any(
            estado is None or not estado.fin_entregado for estado in estados
        ):
            raise ErrorSeguimientoEntrada("La ejecución tiene cierres locales pendientes")

    def entradas_pendientes(self, id_ejecucion):
        """Devuelve entradas sin cierre consolidado local, incluidas las que no tuvieron actividad."""
        self._comprobar_disponible()
        pendientes = set()
        for nombre in self._productores:
            estado = self._barreras.get(Entrada(id_ejecucion, nombre))
            if estado is None or not estado.fin_recibido:
                pendientes.add(nombre)
        return frozenset(pendientes)

    def liberar_ejecucion(self, id_ejecucion):
        """Libera los contadores y barreras de una ejecución con cierres locales atendidos."""
        self.validar_liberacion(id_ejecucion)
        self._registrar_cambio(CambioEOF.LIBERAR_EJECUCION, Entrada(id_ejecucion, ""))

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
        """Conserva el total único del productor y prepara al reunir todos los cierres."""
        entrada = sobre.entrada
        total_productor = sobre.contenido.total_lotes
        if total_productor < estado.resueltos_por_productor.get(sobre.origen, 0):
            raise ErrorSeguimientoEntrada("El productor anunció menos lotes que los resueltos localmente")
        if self._seguimiento.validar_cierre(entrada, sobre.origen, total_productor):
            self._registrar_cambio(CambioEOF.CIERRE_PRODUCTOR, entrada,
                                  productor=sobre.origen, valor=total_productor)
        self._enviar_preparacion_pendiente(entrada, estado)

    def _enviar_preparacion_pendiente(self, entrada, estado):
        """Publica la preparación al reunir los cierres si todavía no confirmó su envío."""
        total = self._seguimiento.consultar(entrada).total_esperado
        if total is not None and not estado.preparacion_enviada:
            self._emitir(TipoMensaje.PREPARAR_CIERRE, entrada, PrepararCierre(total))
            self._registrar_cambio(CambioEOF.PREPARACION_ENVIADA, entrada, valor=total)

    def _preparar(self, entrada, estado, total):
        """Persiste la preparación una sola vez y completa el aviso de avance local."""
        if estado.total is not None and estado.total != total:
            raise ErrorSeguimientoEntrada("La barrera recibió dos totales diferentes")
        if self._seguimiento.consultar(entrada).lotes_resueltos > total:
            raise ErrorSeguimientoEntrada("Los lotes locales superan el total de la barrera")
        if not estado.preparada:
            self._registrar_cambio(CambioEOF.PREPARADA, entrada, valor=total)
        self._informar_avance(entrada, estado)

    def _informar_avance(self, entrada, estado):
        resueltos = self._seguimiento.consultar(entrada).lotes_resueltos
        if resueltos > estado.total:
            raise ErrorSeguimientoEntrada("Los lotes locales superan el total de la barrera")
        if resueltos != estado.ultimo_avance:
            self._emitir(TipoMensaje.AVANCE_REPLICA, entrada, AvanceReplica(resueltos),
                         self._coordinadora(entrada))
            self._registrar_cambio(CambioEOF.AVANCE_ENVIADO, entrada, valor=resueltos)

    def _recibir_avance(self, sobre, estado):
        """Conserva el mayor avance de cada réplica y evalúa el cierre con esos valores."""
        if not estado.preparacion_enviada:
            raise ErrorSeguimientoEntrada("Se recibió un avance antes de preparar la barrera")
        instancia = sobre.origen.instancia
        nuevo = sobre.contenido.lotes_resueltos
        anterior = estado.avances.get(instancia, 0)
        # Una reentrega antigua no retrocede el contador ni impide reintentar
        # el FIN si su publicación anterior quedó interrumpida.
        nuevo = max(nuevo, anterior)
        total = estado.total_resuelto - anterior + nuevo
        if total > estado.total:
            raise ErrorSeguimientoEntrada("Las réplicas resolvieron más lotes que los anunciados")
        if instancia not in estado.avances or nuevo != anterior:
            self._registrar_cambio(CambioEOF.AVANCE_RECIBIDO, sobre.entrada,
                                  instancia=instancia, valor=nuevo)
        self._enviar_fin_pendiente(sobre.entrada, estado)

    def _enviar_fin_pendiente(self, entrada, estado):
        """Publica FIN cuando todas las réplicas cubren el total y falta confirmar su envío."""
        if (estado.preparacion_enviada and len(estado.avances) == self._cantidad
                and estado.total_resuelto == estado.total and not estado.fin_enviado):
            self._emitir(TipoMensaje.FIN_ENTRADA, entrada, None)
            self._registrar_cambio(CambioEOF.FIN_ENVIADO, entrada)

    def cerrar_registro(self):
        """Libera el WAL de coordinación sin borrar el estado recuperable."""
        self._inutilizable = True
        self._registro.cerrar_registro()

    def _comprobar_disponible(self):
        """Rechaza operaciones después del cierre o de un error de persistencia."""
        if self._inutilizable:
            raise ErrorSeguimientoEntrada('El estado de EOF requiere cerrar y recuperar la instancia')

    def _registrar_cambio(self, cambio, entrada, *, productor=None, valor=0, instancia=0,
                         id_entrada=None, indice=0):
        """Sincroniza una transición y la aplica al seguimiento en memoria."""
        self._comprobar_disponible()
        try:
            self._registro.registrar(cambio, entrada, productor=productor, valor=valor,
                                     instancia=instancia, id_entrada=id_entrada, indice=indice)
            self._aplicar_cambio(cambio, entrada, productor, valor, instancia, id_entrada, indice)
        except BaseException:
            self._inutilizable = True
            raise

    def _aplicar_cambio(self, cambio, entrada, productor, valor, instancia, id_entrada, indice):
        """Reconstruye una transición local sin publicar mensajes ni ejecutar el negocio."""
        if cambio is CambioEOF.LIBERAR_EJECUCION:
            self._seguimiento.liberar_ejecucion(entrada.id_ejecucion)
            self._barreras = {clave: estado for clave, estado in self._barreras.items()
                              if clave.id_ejecucion != entrada.id_ejecucion}
            return
        estado = self._estado(entrada)
        if cambio is CambioEOF.LOTE_RESUELTO:
            identidad = (productor, id_entrada, indice)
            if identidad not in estado.lotes_resueltos:
                self._seguimiento.registrar_lote(entrada, productor)
                self._seguimiento.resolver_lote(entrada)
                estado.lotes_resueltos.add(identidad)
                estado.resueltos_por_productor[productor] = estado.resueltos_por_productor.get(productor, 0) + 1
        elif cambio is CambioEOF.CIERRE_PRODUCTOR:
            self._seguimiento.registrar_cierre(entrada, productor, valor)
        elif cambio is CambioEOF.PREPARACION_ENVIADA:
            estado.total = valor
            estado.preparacion_enviada = True
        elif cambio is CambioEOF.PREPARADA:
            estado.total = valor
            estado.preparada = True
        elif cambio is CambioEOF.AVANCE_ENVIADO:
            estado.ultimo_avance = valor
        elif cambio is CambioEOF.AVANCE_RECIBIDO:
            estado.total_resuelto += valor - estado.avances.get(instancia, 0)
            estado.avances[instancia] = valor
        elif cambio is CambioEOF.FIN_ENVIADO:
            estado.fin_enviado = True
        elif cambio is CambioEOF.FIN_RECIBIDO:
            estado.fin_recibido = True
        elif cambio is CambioEOF.FIN_ENTREGADO:
            estado.fin_entregado = True
