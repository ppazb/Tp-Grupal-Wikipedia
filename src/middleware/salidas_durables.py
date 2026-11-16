"""Publicaciones pendientes y confirmaciones reconstruibles desde un WAL."""

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import os
import logging
from uuid import UUID, uuid4
from urllib.parse import quote

from ..common.io_adapters.state_storage import WAL
from ..common.io_adapters.formato_wal import ErrorWAL
from ..common.protocol.dto import SobreMensaje
from ..common.protocol.primitivas import EscritorBinario, LectorBinario
from .interfaz import ErrorPersistencia
from .topologia import DestinoPublicacion, MAX_BYTES_NOMBRE_AMQP


INICIAR = 1
AGREGAR = 2
COMPLETAR = 3
CONFIRMAR = 4
LIBERAR = 5
INDIVIDUAL = 6
REGISTRO = logging.getLogger(__name__)
BYTES_ID = 16
# Cuatro nombres AMQP con prefijos, etiquetas y longitud del cuerpo.
BYTES_METADATOS = 4 * (MAX_BYTES_NOMBRE_AMQP + 4) + BYTES_ID + 16


@dataclass(frozen=True)
class Publicacion:
    """Sobre y destino exactos de una salida, conservados durante sus reintentos."""

    grupo: str
    destino: DestinoPublicacion
    cuerpo: bytes
    sobre: SobreMensaje

    @property
    def id_mensaje(self):
        return self.sobre.id_mensaje


def crear_directorio_durable(ruta):
    """Crea directorios faltantes y sincroniza sus entradas antes de usarlos."""
    ruta = Path(ruta).absolute()
    faltantes = []
    actual = ruta
    while not actual.exists():
        faltantes.append(actual)
        actual = actual.parent
    for directorio in reversed(faltantes):
        directorio.mkdir(exist_ok=True)
        descriptor = os.open(directorio.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return ruta


class SalidasDurables:
    """Registra grupos de salidas antes de enviarlos y retiene solo cuerpos pendientes.

    INICIAR/AGREGAR/COMPLETAR hace visible el conjunto completo de ramas. Un grupo
    incompleto al recuperar se descarta: ninguna de sus ramas pudo enviarse.
    Las huellas de salidas confirmadas permiten reconocer IDs ya publicados.
    """

    def __init__(self, configuracion, codificador, aplicar, liberar, ambito, comprobar):
        self._codificador = codificador
        self._aplicar = aplicar
        self._liberar = liberar
        self._proceso = configuracion.proceso
        self._maximo = configuracion.limites.max_bytes_por_mensaje + BYTES_METADATOS
        self._pendientes = {}
        self._huellas = {}
        self._grupo = None
        self._provisionales = []
        self._fallido = False
        self._wal = None
        if type(ambito) is not str or not ambito or '\x00' in ambito:
            raise ErrorPersistencia('El ámbito de publicación debe ser texto no vacío sin NUL')
        # El directorio identifica al servicio; el ámbito separa sus publicadores.
        # El prefijo y el escape impiden interpretar el ámbito como una ruta.
        nombre = ('publicaciones.wal' if ambito == 'instancia'
                  else f'publicaciones-{quote(ambito, safe="")}.wal')
        ruta = Path(configuracion.directorio_persistencia) / nombre
        try:
            crear_directorio_durable(ruta.parent)
            self._wal = WAL(ruta, max_bytes_por_registro=self._maximo,
                            comprobar_detencion=comprobar)
            self._wal.recuperar(self._recuperar)
            if self._grupo is not None:
                REGISTRO.warning("Se descartó un grupo de salidas sin cierre durable: %s", self._grupo)
            self._grupo = None
            self._provisionales.clear()
        except BaseException:
            if self._wal is not None:
                try:
                    self._wal.cerrar()
                except Exception:
                    REGISTRO.exception("Falló el cierre del registro de salidas")
            raise

    def _codificar(self, publicacion):
        escritor = EscritorBinario(self._maximo)
        escritor.entero(AGREGAR, 1)
        for nombre in (publicacion.grupo, publicacion.destino.exchange,
                       publicacion.destino.clave, publicacion.destino.cola or ''):
            if len(nombre.encode('utf-8')) > MAX_BYTES_NOMBRE_AMQP:
                raise ErrorPersistencia('Nombre de publicación demasiado largo')
            escritor.texto(nombre)
        escritor.binario(publicacion.cuerpo)
        return escritor.resultado()

    def _decodificar(self, lector):
        grupo, exchange, clave, cola = (lector.texto() for _ in range(4))
        cuerpo = bytes(lector.bloque())
        lector.finalizar()
        resultado = self._codificador.decodificar(cuerpo)
        if resultado.errores_por_registro or resultado.sobre.origen != self._proceso:
            raise ErrorPersistencia('La publicación almacenada no corresponde a un sobre válido del productor')
        return Publicacion(grupo, DestinoPublicacion(exchange, clave, cola or None), cuerpo, resultado.sobre)

    def preparar(self, publicaciones):
        """Persiste todas las ramas como conjunto antes de habilitar su transmisión."""
        self._validar()
        nuevas = []
        vistas = {}
        for publicacion in publicaciones:
            contenido = self._codificar(publicacion)
            huella = sha256(contenido).digest()
            anterior = vistas.get(publicacion.id_mensaje, self._huellas.get(publicacion.id_mensaje))
            if anterior is not None:
                if anterior != huella:
                    raise ErrorPersistencia('Se reutilizó un ID con otro destino o contenido')
                continue
            vistas[publicacion.id_mensaje] = huella
            nuevas.append((publicacion, contenido, huella))
        if not nuevas:
            return
        if len(nuevas) == 1:
            self._registrar(bytes([INDIVIDUAL]) + nuevas[0][1][1:])
        else:
            grupo = uuid4().bytes
            self._registrar(bytes([INICIAR]) + grupo)
            for _, contenido, _ in nuevas:
                self._registrar(contenido)
            self._registrar(bytes([COMPLETAR]) + grupo)
        try:
            for publicacion, _, huella in nuevas:
                self._incorporar(publicacion, huella)
        except BaseException:
            self._fallido = True
            raise

    def _incorporar(self, publicacion, huella):
        if publicacion.id_mensaje in self._huellas:
            raise ErrorPersistencia('ID repetido dentro del historial de salidas')
        self._huellas[publicacion.id_mensaje] = huella
        self._pendientes[publicacion.id_mensaje] = publicacion
        self._aplicar(publicacion, False)

    def confirmar(self, publicacion):
        """Persiste la confirmación del broker antes de retirar el cuerpo pendiente."""
        self._registrar(bytes([CONFIRMAR]) + publicacion.id_mensaje.bytes)
        self._resolver(publicacion.id_mensaje)

    def _resolver(self, identificador):
        if identificador not in self._pendientes:
            raise ErrorPersistencia('Confirmación sin una publicación pendiente')
        publicacion = self._pendientes[identificador]
        try:
            self._aplicar(publicacion, True)
            del self._pendientes[identificador]
        except BaseException:
            self._fallido = True
            raise

    def pendientes(self):
        """Devuelve las salidas pendientes en orden de registro."""
        self._validar()
        return tuple(self._pendientes.values())

    def contiene(self, identificador):
        return identificador in self._huellas

    def liberar(self, ejecucion):
        """Registra el fin del seguimiento local sin eliminar huellas de mensajes."""
        if any(p.sobre.entrada.id_ejecucion == ejecucion for p in self.pendientes()):
            raise ErrorPersistencia('No se puede liberar una ejecución con publicaciones pendientes')
        escritor = EscritorBinario(self._maximo)
        escritor.entero(LIBERAR, 1)
        escritor.texto(ejecucion)
        self._registrar(escritor.resultado())
        self._liberar(ejecucion)

    def _recuperar(self, registro):
        lector = LectorBinario(registro.contenido)
        tipo = lector.entero(1)
        if tipo == INDIVIDUAL:
            self._grupo = None
            self._provisionales.clear()
            publicacion = self._decodificar(lector)
            huella = sha256(bytes([AGREGAR]) + registro.contenido[1:]).digest()
            self._incorporar(publicacion, huella)
            return
        if tipo == AGREGAR:
            if self._grupo is None:
                raise ErrorPersistencia('Salida sin grupo de publicación')
            publicacion = self._decodificar(lector)
            self._provisionales.append((publicacion, sha256(registro.contenido).digest()))
            return
        if tipo == LIBERAR:
            ejecucion = lector.texto()
            lector.finalizar()
            self._liberar(ejecucion)
            return
        identificador = UUID(int=lector.entero(BYTES_ID))
        lector.finalizar()
        if tipo == INICIAR:
            self._grupo = identificador
            self._provisionales.clear()
        elif tipo == COMPLETAR:
            if self._grupo != identificador:
                raise ErrorPersistencia('Cierre de grupo de publicación inconsistente')
            for publicacion, huella in self._provisionales:
                self._incorporar(publicacion, huella)
            self._provisionales.clear()
            self._grupo = None
        elif tipo == CONFIRMAR:
            self._resolver(identificador)
        else:
            raise ErrorPersistencia('Operación de salidas desconocida')

    def _validar(self):
        if self._fallido:
            raise ErrorPersistencia('El registro de salidas falló; cerrar y recuperar')

    def _registrar(self, contenido):
        self._validar()
        try:
            self._wal.registrar(contenido)
        except (ErrorWAL, OSError) as error:
            self._fallido = True
            raise ErrorPersistencia("No se pudo persistir el estado de publicación") from error
        except BaseException:
            self._fallido = True
            raise

    def cerrar(self):
        """Cierra el WAL conservando todas las publicaciones registradas."""
        if self._wal is not None:
            self._wal.cerrar()
        self._fallido = True
        self._pendientes.clear()
        self._huellas.clear()
        self._provisionales.clear()
