"""Seguimiento durable de publicaciones y cierres por ejecución del productor."""

from dataclasses import dataclass, field
from enum import IntEnum
import fcntl
from pathlib import Path

from src.common.io_adapters.wal import WAL
from src.common.protocol.primitivas import EscritorBinario, LectorBinario, BYTES_U8, BYTES_U32, BYTES_U64
from .interfaz import EntradaFinalizada, ErrorMensaje, ErrorSeguimientoEntrada


MAX_BYTES_REGISTRO = 16_000_000
MAXIMO_LOTES = 18446744073709551615


class CambioPublicacion(IntEnum):
    CONFIGURACION = 0
    INICIAR_ENVIO = 1
    CONFIRMAR_ENVIO = 2
    INICIAR_CIERRE = 3
    CONFIRMAR_CIERRE = 4


@dataclass
class PublicacionRegistrada:
    """Conserva la huella del envío y su confirmación durable."""

    huella: bytes
    confirmada: bool = False


@dataclass
class SalidaRegistrada:
    """Agrupa identidades, totales y cierre de un destino y entrada."""

    publicaciones: dict = field(default_factory=dict)
    total: int = 0
    pendientes: int = 0
    cierre_iniciado: bool = False
    cierre_confirmado: bool = False


def _configuracion_persistida(configuracion, id_ejecucion):
    """Codifica la identidad y las rutas que determinan los destinatarios del productor."""
    escritor = EscritorBinario(MAX_BYTES_REGISTRO)
    escritor.entero(CambioPublicacion.CONFIGURACION.value, BYTES_U8)
    escritor.texto(id_ejecucion)
    escritor.texto(configuracion.proceso.grupo)
    escritor.entero(configuracion.proceso.instancia, BYTES_U32)
    escritor.entero(configuracion.obtener_grupo(configuracion.proceso.grupo).instancias, BYTES_U32)
    rutas = sorted((ruta for ruta in configuracion.rutas if ruta.origen == configuracion.proceso.grupo),
                   key=lambda ruta: (ruta.destino, ruta.entrada, ruta.nombre))
    escritor.entero(len(rutas), BYTES_U32)
    for ruta in rutas:
        escritor.texto(ruta.nombre)
        escritor.texto(ruta.destino)
        escritor.texto(ruta.entrada)
        escritor.entero(configuracion.obtener_grupo(ruta.destino).instancias, BYTES_U32)
        escritor.entero(len(ruta.esquemas), BYTES_U32)
        for esquema in sorted(ruta.esquemas, key=lambda esquema: esquema.value):
            escritor.entero(esquema.value, BYTES_U32)
    return escritor.resultado()


def ruta_registro(directorio, id_ejecucion):
    """Obtiene una ruta de archivo sin interpretar el ID de ejecución como un directorio."""
    if type(id_ejecucion) is not str or not id_ejecucion.strip():
        raise ErrorMensaje('La ejecución debe tener una identidad no vacía')
    return Path(directorio) / f'{id_ejecucion.encode("utf-8").hex()}.wal'


class PublicacionesEjecucion:
    """Recupera un WAL exclusivo de ejecución y conserva las identidades ya confirmadas."""

    def __init__(self, directorio, configuracion, id_ejecucion):
        ruta = ruta_registro(directorio, id_ejecucion)
        ruta.parent.mkdir(parents=True, exist_ok=True)
        self._bloqueo = ruta.with_suffix('.lock').open('ab')
        self._wal = None
        self._inutilizable = False
        self._salidas = {}
        try:
            # Impide que dos publicadores del gateway recuperen y modifiquen la misma ejecución.
            fcntl.flock(self._bloqueo.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._wal = WAL(ruta)
            self._recuperar(_configuracion_persistida(configuracion, id_ejecucion))
        except BaseException:
            self.cerrar_registro()
            raise

    def _recuperar(self, configuracion):
        """Valida la configuración y reconstruye las salidas sin efectuar publicaciones."""
        registros = self._wal.iterar()
        try:
            cabecera = next(registros, None)
            if cabecera is None:
                self._wal.agregar(configuracion)
                return
            if cabecera != configuracion:
                raise ErrorSeguimientoEntrada('La configuración de salidas no coincide con su WAL')
            for datos in registros:
                lector = LectorBinario(datos)
                cambio = CambioPublicacion(lector.entero(BYTES_U8))
                salida = (lector.texto(), lector.texto())
                identidad = None
                huella = None
                if cambio in (CambioPublicacion.INICIAR_ENVIO, CambioPublicacion.CONFIRMAR_ENVIO):
                    identidad = (lector.texto(), lector.entero(BYTES_U32))
                if cambio is CambioPublicacion.INICIAR_ENVIO:
                    huella = lector.bloque()
                if cambio is CambioPublicacion.INICIAR_CIERRE:
                    total = lector.entero(BYTES_U64)
                    if total != self.consultar_salida(salida).total:
                        raise ErrorSeguimientoEntrada('El total del cierre no coincide con las publicaciones')
                lector.finalizar()
                self._aplicar(cambio, salida, identidad, huella)
        finally:
            registros.close()

    def consultar_salida(self, salida):
        """Obtiene el seguimiento de un grupo y una entrada dentro de esta ejecución."""
        if self._inutilizable:
            raise ErrorSeguimientoEntrada('Las publicaciones requieren cerrar y recuperar la instancia')
        return self._salidas.setdefault(salida, SalidaRegistrada())

    def validar_envio(self, salida, identidad, huella):
        """Rechaza identidades reutilizadas con otro contenido o destino y salidas nuevas tras el cierre."""
        estado = self.consultar_salida(salida)
        anterior = estado.publicaciones.get(identidad)
        if anterior is not None and anterior.huella != huella:
            raise ErrorMensaje('La identidad de la publicación ya pertenece a otro contenido o destino')
        if anterior is None:
            if estado.cierre_iniciado:
                raise EntradaFinalizada('El productor ya inició el cierre de esa salida')
            if estado.total + estado.pendientes >= MAXIMO_LOTES:
                raise ErrorMensaje('La cantidad de lotes excede el rango u64')
        return anterior

    def iniciar_envio(self, salida, identidad, huella):
        """Registra el intento antes de enviarlo; devuelve si ya estaba confirmado."""
        anterior = self.validar_envio(salida, identidad, huella)
        if anterior is None:
            self._registrar(CambioPublicacion.INICIAR_ENVIO, salida, identidad, huella)
            return False
        return anterior.confirmada

    def confirmar_envio(self, salida, identidad):
        """Suma una publicación una sola vez después de su confirmación por RabbitMQ."""
        estado = self.consultar_salida(salida)
        if not estado.publicaciones[identidad].confirmada:
            self._registrar(CambioPublicacion.CONFIRMAR_ENVIO, salida, identidad)

    def iniciar_cierre(self, salida):
        """Fija el total del EOF y rechaza cerrarlo si quedan envíos sin confirmar."""
        estado = self.consultar_salida(salida)
        if estado.pendientes:
            raise ErrorSeguimientoEntrada('No se puede publicar EOF: hay publicaciones pendientes de confirmar')
        if not estado.cierre_iniciado:
            self._registrar(CambioPublicacion.INICIAR_CIERRE, salida)
        return estado

    def confirmar_cierre(self, salida):
        """Conserva la confirmación del EOF para no volver a enviarlo al repetir la finalización."""
        if not self.consultar_salida(salida).cierre_confirmado:
            self._registrar(CambioPublicacion.CONFIRMAR_CIERRE, salida)

    def _registrar(self, cambio, salida, identidad=None, huella=None):
        """Persiste una transición antes de aplicarla en memoria."""
        estado = self.consultar_salida(salida)
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(cambio.value, BYTES_U8)
        for texto in salida:
            escritor.texto(texto)
        if identidad is not None:
            escritor.texto(identidad[0])
            escritor.entero(identidad[1], BYTES_U32)
        if cambio is CambioPublicacion.INICIAR_ENVIO:
            escritor.binario(huella)
        if cambio is CambioPublicacion.INICIAR_CIERRE:
            escritor.entero(estado.total, BYTES_U64)
        try:
            self._wal.agregar(escritor.resultado())
            self._aplicar(cambio, salida, identidad, huella)
        except BaseException:
            self._inutilizable = True
            raise

    def _aplicar(self, cambio, salida, identidad, huella):
        """Reconstruye intentos, confirmaciones y cierres de una salida."""
        estado = self.consultar_salida(salida)
        if cambio is CambioPublicacion.INICIAR_ENVIO:
            if identidad not in estado.publicaciones:
                estado.publicaciones[identidad] = PublicacionRegistrada(bytes(huella))
                estado.pendientes += 1
        elif cambio is CambioPublicacion.CONFIRMAR_ENVIO:
            publicacion = estado.publicaciones[identidad]
            if not publicacion.confirmada:
                publicacion.confirmada = True
                estado.total += 1
                estado.pendientes -= 1
        elif cambio is CambioPublicacion.INICIAR_CIERRE:
            if estado.pendientes:
                raise ErrorSeguimientoEntrada('El WAL contiene un cierre con publicaciones pendientes')
            estado.cierre_iniciado = True
        elif cambio is CambioPublicacion.CONFIRMAR_CIERRE:
            if not estado.cierre_iniciado:
                raise ErrorSeguimientoEntrada('El WAL confirma un cierre que no fue iniciado')
            estado.cierre_confirmado = True
        else:
            raise ErrorSeguimientoEntrada('Transición de publicación desconocida')

    def validar_liberacion(self):
        """Rechaza liberar índices de salidas sin cierre confirmado."""
        if self._inutilizable:
            raise ErrorSeguimientoEntrada('Las publicaciones requieren recuperar la instancia')
        if any(estado.pendientes or not estado.cierre_confirmado for estado in self._salidas.values()):
            raise ErrorSeguimientoEntrada('La ejecución tiene publicaciones o cierres pendientes')

    def cerrar_registro(self):
        """Cierra el WAL y libera su exclusión sin borrar el historial."""
        self._inutilizable = True
        try:
            if self._wal is not None:
                self._wal.cerrar()
        finally:
            self._bloqueo.close()
