"""Publicación confirmada de datos hacia grupos lógicos."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from uuid import UUID, uuid4

import pika

from ..common.protocol.dto import CierreProductor, Datos, Entrada, EnvioDatos, SobreMensaje, TipoMensaje
from ..common.protocol.primitivas import ErrorSerializacion, EscritorBinario
from ..common.io_adapters.formato_wal import ErrorWAL
from .broker_connection import ConexionBroker
from .capacidad import CABECERA_CAPACIDAD, CABECERA_RESERVA, VERSION_CAPACIDAD, CapacidadDatos
from .reservas_durables import EstadoReserva
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion
from .interfaz import (
    EntradaFinalizada, ErrorConexion, ErrorMensaje, ErrorMiddleware, ErrorPublicacionParcial,
    PublicacionRechazada, OperacionInterrumpida, ErrorPersistencia,
)
from .topologia import TopologiaGrupos
from .salidas_durables import Publicacion, SalidasDurables


MAXIMO_LOTES = 18446744073709551615


@dataclass
class _EstadoSalida:
    total: int = 0
    cierre_iniciado: bool = False
    cierre_confirmado: bool = False
    id_cierre: UUID = field(default_factory=uuid4)


class PublicadorDatos:
    """Reserva capacidad y publica lotes secuencialmente con confirmación.

    Cada Datos viaja como un mensaje sujeto al límite de tamaño configurado.
    Un NACK eleva PublicacionRechazada; la salida se conserva para recuperarla.
    El llamador administra el cierre de la conexión, incluso ante errores.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, capacidad=None, topologia=None, ambito,
    ):
        self._configuracion = configuracion
        self._conexion = conexion
        self._topologia = topologia or TopologiaGrupos(configuracion)
        self._codificador = CodificadorMensajes(
            configuracion.limites.max_bytes_por_mensaje,
            comprobar_detencion=conexion.comprobar_apagado,
        )
        if topologia is None:
            self._topologia.declarar(conexion)
        self._capacidad = capacidad or CapacidadDatos(configuracion, conexion, self._topologia)
        self._publicando = False
        self._salidas = {}
        try:
            conexion.canal.confirm_delivery()
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudieron habilitar las confirmaciones") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudieron habilitar las confirmaciones") from error
        try:
            self._durables = SalidasDurables(
                configuracion, self._codificador, self._seguir_salida, self._liberar_seguimiento,
                ambito, conexion.comprobar_apagado, self._guardar_seguimiento, self._recuperar_seguimiento,
            )
        except (ErrorWAL, OSError, ErrorSerializacion) as error:
            raise ErrorPersistencia("No se pudo recuperar el registro de salidas") from error

    def publicar(self, envio: EnvioDatos) -> None:
        """Publica un envío identificado conservando su destino, contenido e ID."""
        self.publicar_en_ramas((envio,))

    def publicar_en_ramas(self, envios: Sequence[EnvioDatos]) -> None:
        """Valida y publica una secuencia de salidas, en el orden recibido.

        Recibe una secuencia de envíos a las ramas definidas por el llamador.
        Recupera pendientes antes de admitir trabajo nuevo y codifica todas las
        salidas antes de registrarlas. Si un envío falla después de otros confirmados, eleva
        ErrorPublicacionParcial con esas salidas y la causa del fallo.
        Las ramas quedan registradas antes del primer envío. Repetir un ID
        confirmado no publica otra vez; un resultado incierto se reenvía con su ID.
        """
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        if not isinstance(envios, Sequence):
            raise ErrorMensaje("Las salidas deben ser una secuencia")
        self.recuperar_pendientes()
        self._publicando = True
        try:
            preparados = [self._preparar(envio) for envio in envios]
            self._enviar_preparados(preparados)
        finally:
            self._publicando = False

    def finalizar_entrada(self, grupo, entrada):
        """Confirma un EOF con el total publicado; admite cero e ignora cierres ya confirmados.

        Desde el primer intento de cierre no admite nuevos datos. Un rechazo
        explícito permite repetir el cierre.
        """
        if self._publicando:
            raise ErrorMiddleware("No se puede cerrar una entrada durante una publicación")
        self.recuperar_pendientes()
        sobre = SobreMensaje.nuevo(entrada, TipoMensaje.EOF, self._configuracion.proceso, CierreProductor(0))
        self._codificador.codificar(sobre)
        instancia = self._topologia.instancia_coordinadora(grupo, entrada)
        destino = self._topologia.destino_control(grupo, entrada.nombre, instancia)
        estado = self._salidas.setdefault((grupo, entrada), _EstadoSalida())
        if estado.cierre_confirmado:
            return
        sobre = SobreMensaje(entrada, TipoMensaje.EOF, self._configuracion.proceso,
                             CierreProductor(estado.total), estado.id_cierre)
        publicacion = Publicacion(grupo, destino, self._codificador.codificar(sobre), sobre)
        self._publicando = True
        try:
            self._durables.preparar((publicacion,))
            self._transmitir(publicacion)
        finally:
            self._publicando = False

    def liberar_ejecucion(self, id_ejecucion):
        """Elimina contadores y cierres de una ejecución cuya limpieza autorizó el llamador."""
        self._durables.liberar(id_ejecucion)

    def validar_liberacion(self, id_ejecucion):
        """Verifica que la ejecución no conserve publicaciones sin confirmar."""
        self._durables.validar_liberacion(id_ejecucion)

    def compactar(self):
        """Compacta las publicaciones sin modificar los totales ni sus identidades."""
        self._durables.compactar()

    def _guardar_seguimiento(self, maximo):
        for (grupo, entrada), estado in self._salidas.items():
            escritor = EscritorBinario(maximo)
            for texto in (grupo, entrada.id_ejecucion, entrada.nombre):
                escritor.texto(texto)
            escritor.entero(estado.total, 8)
            escritor.entero(int(estado.cierre_iniciado), 1)
            escritor.entero(int(estado.cierre_confirmado), 1)
            escritor.agregar(estado.id_cierre.bytes)
            yield escritor.resultado()

    def _recuperar_seguimiento(self, lector):
        grupo = lector.texto()
        entrada = Entrada(lector.texto(), lector.texto())
        total, iniciado, confirmado = lector.entero(8), lector.entero(1), lector.entero(1)
        identificador = UUID(int=lector.entero(16))
        lector.finalizar()
        self._configuracion.obtener_ruta(self._configuracion.proceso.grupo, grupo, entrada.nombre)
        if iniciado not in (0, 1) or confirmado not in (0, 1) or confirmado > iniciado or not identificador.int:
            raise ErrorPersistencia('Seguimiento compacto inválido')
        self._salidas[(grupo, entrada)] = _EstadoSalida(total, bool(iniciado), bool(confirmado), identificador)

    def _liberar_seguimiento(self, id_ejecucion):
        self._salidas = {
            identidad: estado for identidad, estado in self._salidas.items()
            if identidad[1].id_ejecucion != id_ejecucion
        }

    def publicar_control(self, sobre, instancia=None):
        """Publica un control identificado sin reservar capacidad ni cambiar su ID."""
        if not isinstance(sobre, SobreMensaje) or sobre.tipo not in (
            TipoMensaje.PREPARAR_CIERRE, TipoMensaje.AVANCE_REPLICA, TipoMensaje.FIN_ENTRADA,
        ):
            raise ErrorMensaje("El tipo no corresponde a coordinación entre réplicas")
        if sobre.origen != self._configuracion.proceso:
            raise ErrorMensaje("El control debe pertenecer al productor local")
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        self.recuperar_pendientes()
        cuerpo = self._codificador.codificar(sobre)
        destino = self._topologia.destino_coordinacion(sobre.entrada.nombre, instancia)
        self._publicando = True
        try:
            publicacion = Publicacion(self._configuracion.proceso.grupo, destino, cuerpo, sobre)
            self._durables.preparar((publicacion,))
            if any(p.id_mensaje == sobre.id_mensaje for p in self._durables.pendientes()):
                self._transmitir(publicacion)
        finally:
            self._publicando = False

    def cerrar(self):
        """Cierra el registro durable sin descartar publicaciones pendientes."""
        self._durables.cerrar()
        self._salidas.clear()

    def recuperar_pendientes(self):
        """Reenvía en orden las publicaciones sin confirmación durable."""
        if self._publicando:
            raise ErrorMiddleware("No se puede recuperar durante una publicación")
        self._publicando = True
        try:
            for publicacion in self._durables.pendientes():
                self._transmitir(publicacion)
        finally:
            self._publicando = False

    def _seguir_salida(self, publicacion, confirmada):
        sobre = publicacion.sobre
        if sobre.tipo not in (TipoMensaje.DATOS, TipoMensaje.EOF):
            return
        estado = self._salidas.setdefault((publicacion.grupo, sobre.entrada), _EstadoSalida())
        if sobre.tipo is TipoMensaje.DATOS:
            if not confirmada:
                if estado.cierre_iniciado or estado.total == MAXIMO_LOTES:
                    raise ErrorMensaje("Historial de datos incompatible con el cierre de salida")
                estado.total += 1
        else:
            if sobre.contenido.total_lotes != estado.total:
                raise ErrorMensaje("El cierre persistido no coincide con el total de salida")
            estado.id_cierre = sobre.id_mensaje
            estado.cierre_iniciado = True
            estado.cierre_confirmado = confirmada

    def _transmitir(self, publicacion):
        self._publicar_cuerpo(publicacion.destino, publicacion.cuerpo,
                             identificador=publicacion.id_mensaje,
                             reservar_capacidad=publicacion.sobre.tipo is TipoMensaje.DATOS)
        self._durables.confirmar(publicacion)

    def _enviar_preparados(self, preparados):
        self._durables.preparar(preparados)
        confirmadas = []
        pendientes = {p.id_mensaje for p in self._durables.pendientes()}
        for publicacion in preparados:
            identidad = (publicacion.grupo, publicacion.sobre.entrada)
            try:
                if publicacion.id_mensaje in pendientes:
                    self._transmitir(publicacion)
                    pendientes.remove(publicacion.id_mensaje)
            except OperacionInterrumpida:
                raise
            except ErrorMiddleware as error:
                if confirmadas:
                    raise ErrorPublicacionParcial(confirmadas, identidad) from error
                raise
            confirmadas.append(identidad)

    def _preparar(self, envio):
        if not isinstance(envio, EnvioDatos) or not isinstance(envio.datos, Datos):
            raise ErrorMensaje("Cada salida debe ser un EnvioDatos con Datos")
        if type(envio.grupo) is not str or not envio.grupo.strip():
            raise ErrorMensaje("El grupo destinatario debe ser texto no vacío")
        sobre = SobreMensaje(
            entrada=envio.datos.entrada,
            tipo=TipoMensaje.DATOS,
            origen=self._configuracion.proceso,
            contenido=envio.datos.contenido,
            id_mensaje=envio.id_mensaje,
        )
        cuerpo = self._codificador.codificar(sobre)
        estado = self._salidas.get((envio.grupo, sobre.entrada))
        if estado is not None and estado.cierre_iniciado and not self._durables.contiene(sobre.id_mensaje):
            raise EntradaFinalizada("El productor ya inició el cierre de esa salida")
        ruta = self._configuracion.obtener_ruta(
            sobre.origen.grupo, envio.grupo, sobre.entrada.nombre,
        )
        if sobre.contenido.esquema not in ruta.esquemas:
            raise ErrorMensaje("El esquema no está admitido en la ruta de salida")
        instancia = self._instancia_destinataria(envio)
        destino = self._topologia.destino_datos(
            envio.grupo, sobre.entrada.nombre, instancia,
        )
        return Publicacion(envio.grupo, destino, cuerpo, sobre)

    def _instancia_destinataria(self, envio):
        clave = envio.clave_afinidad
        if clave is None:
            return None
        if type(clave) is not str or not clave:
            raise ErrorMensaje("La clave de afinidad debe ser texto no vacío")
        identificador = self._codificador.codificar_afinidad(
            envio.datos.entrada.id_ejecucion, clave,
        )
        # hash() de Python cambia entre procesos. SHA-256 fija la asignación para
        # todos los productores mientras la cantidad de instancias permanezca fija.
        huella = hashlib.sha256(identificador).digest()
        cantidad = self._configuracion.obtener_grupo(envio.grupo).instancias
        return int.from_bytes(huella, byteorder="big") % cantidad

    def _publicar_cuerpo(self, destino, cuerpo, *, identificador, reservar_capacidad=True):
        reserva = None
        if reservar_capacidad:
            reserva = self._capacidad.adquirir(destino.cola, identificador, cuerpo)
            if reserva is EstadoReserva.COMPLETADA:
                return
        # Una interrupción conserva la reserva: la salida ya está en el WAL.
        canal = self._conexion.canal
        try:
            canal.basic_publish(
                exchange=destino.exchange,
                routing_key=destino.clave,
                body=cuerpo,
                mandatory=True,
                properties=pika.BasicProperties(
                    headers={CABECERA_CAPACIDAD: VERSION_CAPACIDAD,
                             CABECERA_RESERVA: identificador.hex} if reservar_capacidad else None,
                    content_type="application/octet-stream",
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                ),
            )
        except pika.exceptions.UnroutableError as error:
            if reserva is EstadoReserva.NUEVA:
                self._capacidad.devolver(destino.cola, identificador, consumida=False)
            raise ErrorMensaje("El mensaje no llegó a una cola destinataria") from error
        except pika.exceptions.NackError as error:
            if reserva is EstadoReserva.NUEVA:
                self._capacidad.devolver(destino.cola, identificador, consumida=False)
            # Un NACK puede indicar una cola llena, pero no identifica la causa.
            raise PublicacionRechazada("El broker no admitió la publicación") from error
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudo confirmar la publicación") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudo publicar el mensaje") from error
