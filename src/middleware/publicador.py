"""Publicación confirmada de datos hacia grupos lógicos."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional

import pika

from ..common.protocol.dto import CierreProductor, Datos, EnvioDatos, SobreMensaje, TipoMensaje
from ..common.protocol.primitivas import BYTES_U64
from .broker_connection import ConexionBroker
from .capacidad import CABECERA_CAPACIDAD, VERSION_CAPACIDAD, CapacidadDatos
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion
from .interfaz import (
    EntradaFinalizada, ErrorConexion, ErrorMensaje, ErrorMiddleware, ErrorPublicacionParcial,
    PublicacionRechazada,
)
from .topologia import TopologiaGrupos


MAXIMO_LOTES = 18446744073709551615


@dataclass
class _EstadoSalida:
    total: int = 0
    cierre_iniciado: bool = False
    cierre_confirmado: bool = False


class PublicadorDatos:
    """Reserva capacidad y publica lotes secuencialmente con confirmación.

    Cada Datos viaja como un mensaje sujeto al límite de tamaño configurado.
    Un NACK eleva PublicacionRechazada, sin reintentos automáticos.
    El llamador administra el cierre de la conexión, incluso ante errores.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, capacidad=None, topologia=None,
    ):
        self._configuracion = configuracion
        self._conexion = conexion
        self._topologia = topologia or TopologiaGrupos(configuracion)
        self._codificador = CodificadorMensajes(configuracion.limites.max_bytes_por_mensaje)
        if topologia is None:
            self._topologia.declarar(conexion)
        self._capacidad = capacidad or CapacidadDatos(conexion, self._topologia)
        self._publicando = False
        self._salidas = {}
        try:
            conexion.canal.confirm_delivery()
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudieron habilitar las confirmaciones") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudieron habilitar las confirmaciones") from error

    def publicar(
        self, grupo: str, datos: Datos, *, clave_afinidad: Optional[str] = None,
    ) -> None:
        """Envía a reparto o a la instancia correspondiente a la clave de afinidad.

        La afinidad usa la ejecución y la clave lógica; todos los registros de
        un lote con afinidad deben pertenecer a esa misma clave. Sin afinidad,
        la cola compartida reparte lotes completos entre los consumidores.
        """
        self.publicar_en_ramas((EnvioDatos(grupo, datos, clave_afinidad),))

    def publicar_en_ramas(self, envios: Sequence[EnvioDatos]) -> None:
        """Valida y publica una secuencia de salidas, en el orden recibido.

        Recibe una secuencia de envíos a las ramas definidas por el llamador.
        Codifica todas las salidas antes de enviar la primera; una secuencia vacía
        no publica nada. Si un envío falla después de otros confirmados, eleva
        ErrorPublicacionParcial con esas salidas y la causa del fallo.
        Repetir el conjunto puede duplicar las salidas confirmadas.
        """
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        if not isinstance(envios, Sequence):
            raise ErrorMensaje("Las salidas deben ser una secuencia")
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
        # Codificar primero valida la identidad incluso cuando no hubo datos.
        sobre = SobreMensaje(entrada, TipoMensaje.EOF, self._configuracion.proceso, CierreProductor(0))
        self._codificador.codificar(sobre)
        instancia = self._topologia.instancia_coordinadora(grupo, entrada)
        destino = self._topologia.destino_control(grupo, entrada.nombre, instancia)
        estado = self._salidas.setdefault((grupo, entrada), _EstadoSalida())
        if estado.cierre_confirmado:
            return
        cuerpo = self._codificador.codificar(SobreMensaje(
            entrada, TipoMensaje.EOF, self._configuracion.proceso, CierreProductor(estado.total),
        ))
        estado.cierre_iniciado = True
        self._publicando = True
        try:
            self._publicar_cuerpo(destino, cuerpo, reservar_capacidad=False)
            estado.cierre_confirmado = True
        finally:
            self._publicando = False

    def liberar_ejecucion(self, id_ejecucion):
        """Elimina contadores y cierres de una ejecución cuya limpieza autorizó el llamador."""
        self._salidas = {
            identidad: estado for identidad, estado in self._salidas.items()
            if identidad[1].id_ejecucion != id_ejecucion
        }

    def publicar_control(self, tipo, entrada, contenido, instancia=None):
        """Publica coordinación del grupo local sin reservar capacidad de datos."""
        if tipo not in (TipoMensaje.PREPARAR_CIERRE, TipoMensaje.AVANCE_REPLICA, TipoMensaje.FIN_ENTRADA):
            raise ErrorMensaje("El tipo no corresponde a coordinación entre réplicas")
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        cuerpo = self._codificador.codificar(SobreMensaje(
            entrada, tipo, self._configuracion.proceso, contenido,
        ))
        destino = self._topologia.destino_coordinacion(entrada.nombre, instancia)
        self._publicando = True
        try:
            self._publicar_cuerpo(destino, cuerpo, reservar_capacidad=False)
        finally:
            self._publicando = False

    def descartar_seguimiento(self):
        """Libera todos los contadores locales al cerrar el middleware, sin emitir EOF."""
        self._salidas.clear()

    def _enviar_preparados(self, preparados):
        confirmadas = []
        for identidad, destino, cuerpo in preparados:
            estado = self._salidas.setdefault(identidad, _EstadoSalida())
            try:
                if estado.total == MAXIMO_LOTES:
                    raise ErrorMensaje("La cantidad de lotes excede el rango u64")
                self._publicar_cuerpo(destino, cuerpo)
            except ErrorMiddleware as error:
                if confirmadas:
                    raise ErrorPublicacionParcial(confirmadas, identidad) from error
                raise
            estado.total += 1
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
        )
        cuerpo = self._codificador.codificar(sobre)
        estado = self._salidas.get((envio.grupo, sobre.entrada))
        if estado is not None and estado.cierre_iniciado:
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
        return (envio.grupo, sobre.entrada), destino, cuerpo

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

    def _publicar_cuerpo(self, destino, cuerpo, *, reservar_capacidad=True):
        if reservar_capacidad:
            self._capacidad.adquirir(destino.cola)
        try:
            self._conexion.canal.basic_publish(
                exchange=destino.exchange,
                routing_key=destino.clave,
                body=cuerpo,
                mandatory=True,
                properties=pika.BasicProperties(
                    headers={CABECERA_CAPACIDAD: VERSION_CAPACIDAD} if reservar_capacidad else None,
                    content_type="application/octet-stream",
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                ),
            )
        except pika.exceptions.UnroutableError as error:
            if reservar_capacidad:
                self._capacidad.devolver(destino.cola)
            raise ErrorMensaje("El mensaje no llegó a una cola destinataria") from error
        except pika.exceptions.NackError as error:
            if reservar_capacidad:
                self._capacidad.devolver(destino.cola)
            # Un NACK puede indicar una cola llena, pero no identifica la causa.
            raise PublicacionRechazada("El broker no admitió la publicación") from error
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudo confirmar la publicación") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudo publicar el mensaje") from error
