"""Seguimiento durable de lo publicado y de los cierres por ejecución del productor.

Por cada (grupo, entrada, instancia destinataria) se guarda un contador de lotes
confirmados y una ventana de los últimos K ids: el estado tiene tamaño fijo y no crece
con el dataset. Cada cierto número de registros el WAL se reemplaza por una foto.
"""

from dataclasses import dataclass
from enum import IntEnum
import fcntl
from pathlib import Path

from src.common.io_adapters.wal import WAL
from src.common.protocol.primitivas import EscritorBinario, LectorBinario, BYTES_U8, BYTES_U32, BYTES_U64
from ..eof.ventana import VentanaLotes
from ..interfaz import EntradaFinalizada, ErrorMensaje, ErrorSeguimientoEntrada


MAX_BYTES_REGISTRO = 16_000_000  # 16 MB por registro binario; no limita el WAL completo.
MAXIMO_LOTES = (1 << 64) - 1  # Mayor cantidad representable por un entero u64.
REGISTROS_POR_FOTO = 10_000  # Cada tantos registros el WAL se reemplaza por cabecera + foto.


class CambioPublicacion(IntEnum):
    CONFIGURACION = 0
    ENVIO_CONFIRMADO = 1
    CIERRE_CONFIRMADO = 2
    FOTO = 3
    RETIRAR_EJECUCION = 4


@dataclass
class _Destino:
    """Lotes confirmados hacia una instancia y los últimos ids enviados."""

    ventana: VentanaLotes
    enviados: int = 0


def _codificar_configuracion_productor(configuracion, id_ejecucion):
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
    """Recupera un WAL exclusivo de ejecución y conserva contadores, ventanas y cierres.

    Un destino se identifica con la tupla (grupo, entrada, instancia).
    """

    def __init__(self, directorio, configuracion, id_ejecucion, *, registros_por_foto=REGISTROS_POR_FOTO):
        ruta = ruta_registro(directorio, id_ejecucion)
        ruta.parent.mkdir(parents=True, exist_ok=True)
        self._bloqueo = ruta.with_suffix('.lock').open('ab')
        self._wal = None
        self._inutilizable = False
        self._k = configuracion.limites.ventana_lotes
        self._registros_por_foto = registros_por_foto
        self._destinos = {}  # (grupo, entrada, instancia) -> _Destino
        self._cierres = set()  # (grupo, entrada) con EOF confirmado en todas las instancias
        self._salidas = set()  # (grupo, entrada) a las que se publicó algún lote
        self._registros_desde_foto = 0
        self._retirada = False
        try:
            # Impide que dos publicadores del gateway recuperen y modifiquen la misma ejecución.
            fcntl.flock(self._bloqueo.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._wal = WAL(ruta)
            self._recuperar(_codificar_configuracion_productor(configuracion, id_ejecucion))
        except BaseException:
            self.cerrar_registro()
            raise

    # consultas: lectura de estado, no cambian nada
    def ya_confirmado(self, destino, id_entrada, indice):
        """True si el lote ya consta como enviado a ese destino (según su ventana)."""
        self._comprobar_disponible()
        estado = self._destinos.get(tuple(destino))
        return estado is not None and estado.ventana.contiene(id_entrada, indice)

    def total(self, grupo, entrada, instancia):
        """Cantidad de lotes confirmados hacia esa instancia."""
        self._comprobar_disponible()
        estado = self._destinos.get((grupo, entrada, instancia))
        return 0 if estado is None else estado.enviados

    def cierre_confirmado(self, grupo, entrada):
        """True si el EOF de la salida ya fue confirmado en todas las instancias."""
        self._comprobar_disponible()
        return (grupo, entrada) in self._cierres

    # transiciones: funciones pára cambiar estado

    def confirmar_envio(self, destino, id_entrada, indice):
        """Suma un lote confirmado por el broker, salvo que ya esté en la ventana."""
        self._comprobar_disponible()
        destino = tuple(destino)
        if self._retirada:
            raise EntradaFinalizada('La ejecución fue retirada; no admite nuevas publicaciones ni reenvíos')
        if self.ya_confirmado(destino, id_entrada, indice):
            return
        if (destino[0], destino[1]) in self._cierres:
            raise EntradaFinalizada('El productor ya cerró esa salida')
        grupo, entrada, instancia = destino
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(CambioPublicacion.ENVIO_CONFIRMADO.value, BYTES_U8)
        escritor.texto(grupo)
        escritor.texto(entrada)
        escritor.entero(instancia, BYTES_U32)
        escritor.texto(id_entrada)
        escritor.entero(indice, BYTES_U32)
        self._persistir(escritor.resultado(), self._aplicar_envio, destino, id_entrada, indice)

    def confirmar_cierre(self, grupo, entrada):
        """Conserva la confirmación del EOF para no volver a enviarlo al repetir la finalización."""
        self._comprobar_disponible()
        if (grupo, entrada) in self._cierres:
            return
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(CambioPublicacion.CIERRE_CONFIRMADO.value, BYTES_U8)
        escritor.texto(grupo)
        escritor.texto(entrada)
        self._persistir(escritor.resultado(), self._cierres.add, (grupo, entrada))

    def validar_liberacion(self):
        """Rechaza liberar salidas con lotes publicados y sin cierre confirmado."""
        self._comprobar_disponible()
        if self._salidas - self._cierres:
            raise ErrorSeguimientoEntrada('La ejecución tiene cierres pendientes')

    def retirar_ejecucion(self):
        """Sustituye el WAL por cabecera + totales + marca de retiro, sin ids de lotes."""
        self.validar_liberacion()
        if self._retirada:
            return
        try:
            cabecera = self._leer_cabecera()
            self._wal.reemplazar(self._registros_de_retiro(cabecera))
            self._retirada = True
            for estado in self._destinos.values():
                estado.ventana = VentanaLotes(self._k)
            self._registros_desde_foto = 0
        except BaseException:
            self._inutilizable = True
            raise

    def cerrar_registro(self):
        """Cierra el WAL y libera su exclusión sin borrar el historial."""
        self._inutilizable = True
        try:
            if self._wal is not None:
                self._wal.cerrar()
        finally:
            self._bloqueo.close()

    #  persistencia: metodos para persistir en WAL

    def _comprobar_disponible(self):
        if self._inutilizable:
            raise ErrorSeguimientoEntrada('Las publicaciones requieren cerrar y recuperar la instancia')

    def _persistir(self, registro, aplicar, *argumentos):
        """Persiste una transición antes de aplicarla en memoria y compacta cada N registros."""
        try:
            self._wal.agregar(registro)
            aplicar(*argumentos)
            self._registros_desde_foto += 1
            if self._registros_desde_foto >= self._registros_por_foto:
                self._escribir_foto()
        except BaseException:
            self._inutilizable = True
            raise

    def _aplicar_envio(self, destino, id_entrada, indice):
        estado = self._destinos.get(destino)
        if estado is None:
            estado = self._destinos[destino] = _Destino(VentanaLotes(self._k))
        if estado.ventana.contiene(id_entrada, indice):
            return
        if estado.enviados >= MAXIMO_LOTES:
            raise ErrorMensaje('La cantidad de lotes excede el rango u64')
        estado.ventana.registrar(id_entrada, indice)
        estado.enviados += 1
        self._salidas.add((destino[0], destino[1]))

    def _leer_cabecera(self):
        registros = self._wal.iterar()
        try:
            return next(registros)
        finally:
            registros.close()

    def _codificar_foto(self, con_ventanas=True):
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(CambioPublicacion.FOTO.value, BYTES_U8)
        escritor.entero(len(self._destinos), BYTES_U32)
        for (grupo, entrada, instancia), estado in sorted(self._destinos.items()):
            escritor.texto(grupo)
            escritor.texto(entrada)
            escritor.entero(instancia, BYTES_U32)
            escritor.entero(estado.enviados, BYTES_U64)
            escritor.binario(estado.ventana.serializar() if con_ventanas else VentanaLotes(self._k).serializar())
        escritor.entero(len(self._cierres), BYTES_U32)
        for grupo, entrada in sorted(self._cierres):
            escritor.texto(grupo)
            escritor.texto(entrada)
        escritor.entero(len(self._salidas), BYTES_U32)
        for grupo, entrada in sorted(self._salidas):
            escritor.texto(grupo)
            escritor.texto(entrada)
        return escritor.resultado()

    def _escribir_foto(self):
        """Reemplaza el WAL por la cabecera y una foto del estado actual."""
        cabecera = self._leer_cabecera()
        self._wal.reemplazar((cabecera, self._codificar_foto()))
        self._registros_desde_foto = 0

    def _registros_de_retiro(self, cabecera):
        yield cabecera
        yield self._codificar_foto(con_ventanas=False)
        yield CambioPublicacion.RETIRAR_EJECUCION.value.to_bytes(BYTES_U8, 'big')

    def _aplicar_foto(self, lector):
        self._destinos = {}
        for _ in range(lector.entero(BYTES_U32)):
            grupo, entrada = lector.texto(), lector.texto()
            instancia = lector.entero(BYTES_U32)
            enviados = lector.entero(BYTES_U64)
            ventana = VentanaLotes.deserializar(lector.bloque(), self._k)
            self._destinos[(grupo, entrada, instancia)] = _Destino(ventana, enviados)
        self._cierres = {(lector.texto(), lector.texto()) for _ in range(lector.entero(BYTES_U32))}
        self._salidas = {(lector.texto(), lector.texto()) for _ in range(lector.entero(BYTES_U32))}
        lector.finalizar()

    def _recuperar(self, configuracion):
        """Valida la configuración y reconstruye el estado sin efectuar publicaciones."""
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
                if self._retirada:
                    raise ErrorSeguimientoEntrada('El WAL contiene cambios posteriores al retiro')
                self._registros_desde_foto += 1
                if cambio is CambioPublicacion.FOTO:
                    self._aplicar_foto(lector)
                    self._registros_desde_foto = 0
                elif cambio is CambioPublicacion.RETIRAR_EJECUCION:
                    lector.finalizar()
                    self.validar_liberacion()
                    self._retirada = True
                elif cambio is CambioPublicacion.ENVIO_CONFIRMADO:
                    destino = (lector.texto(), lector.texto(), lector.entero(BYTES_U32))
                    id_entrada, indice = lector.texto(), lector.entero(BYTES_U32)
                    lector.finalizar()
                    self._aplicar_envio(destino, id_entrada, indice)
                elif cambio is CambioPublicacion.CIERRE_CONFIRMADO:
                    salida = (lector.texto(), lector.texto())
                    lector.finalizar()
                    self._cierres.add(salida)
                else:
                    raise ErrorSeguimientoEntrada('Transición de publicación desconocida')
        finally:
            registros.close()
