"""Publicación confirmada de datos hacia grupos lógicos."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pika

from ...common.protocol.dto import CierreProductor, Datos, EnvioDatos, SobreMensaje, TipoMensaje
from ...common.protocol.primitivas import EscritorBinario
from .conexion import ConexionBroker
from .capacidad import CABECERA_CAPACIDAD, VERSION_CAPACIDAD, CapacidadDatos
from ..mensajes.codificacion import CodificadorMensajes
from ..configuracion.modelos import ConfiguracionComunicacion
from ..interfaz import (
    ErrorConexion, ErrorMensaje, ErrorMiddleware, ErrorPublicacionParcial,
    PublicacionRechazada, OperacionInterrumpida,
)
from .topologia import TopologiaGrupos
from ..persistencia.publicaciones import PublicacionesEjecucion, ruta_registro


@dataclass(frozen=True)
class _EnvioPreparado:
    salida: tuple
    identidad: tuple
    destino: object
    cuerpo: bytes
    huella: bytes


MAX_BYTES_DIRECCION = 4096  # Presupuesto para codificar exchange, clave y cola en la huella.


class PublicadorDatos:
    """Reserva capacidad y publica lotes secuencialmente con confirmación.

    Cada Datos viaja como un mensaje sujeto al límite de tamaño configurado.
    Un NACK eleva PublicacionRechazada, sin reintentos automáticos.
    El llamador administra el cierre de la conexión, incluso ante errores.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, directorio, capacidad=None, topologia=None,
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
        self._capacidad = capacidad or CapacidadDatos(conexion, self._topologia)
        self._publicando = False
        self._directorio = Path(directorio) / "publicaciones"
        self._registros = {}
        try:
            conexion.canal.confirm_delivery()
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudieron habilitar las confirmaciones") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudieron habilitar las confirmaciones") from error

    def publicar(self, envio: EnvioDatos) -> None:
        """Publica un envío identificado conservando su destino, contenido e ID."""
        self.publicar_en_ramas((envio,))

    def publicar_en_ramas(self, envios: Sequence[EnvioDatos]) -> None:
        """Valida y registra todas las salidas, publica las pendientes e informa confirmaciones parciales."""
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        if not isinstance(envios, Sequence):
            raise ErrorMensaje("Las salidas deben ser una secuencia")
        self._publicando = True
        try:
            preparados = [self._preparar_envio_identificado(envio) for envio in envios]
            identidades = {}
            for preparado in preparados:
                clave = (preparado.salida, preparado.identidad)
                if clave in identidades and identidades[clave] != preparado.huella:
                    raise ErrorMensaje("La misma identidad tiene dos contenidos o destinos en la publicación")
                identidades[clave] = preparado.huella
            for preparado in preparados:
                grupo, entrada = preparado.salida
                self._obtener_registro_ejecucion(entrada.id_ejecucion).iniciar_envio(
                    (grupo, entrada.nombre), preparado.identidad, preparado.huella,
                )
            self._publicar_envios_pendientes(preparados)
        finally:
            self._publicando = False

    def finalizar_entrada(self, grupo, entrada):
        """Fija el total de lotes y confirma su EOF; admite cero resultados y omite cierres confirmados."""
        if self._publicando:
            raise ErrorMiddleware("No se puede cerrar una entrada durante una publicación")
        # Codificar primero valida la identidad incluso cuando no hubo datos.
        id_cierre = f"eof:{entrada.id_ejecucion}"
        sobre = SobreMensaje(entrada, TipoMensaje.EOF, self._configuracion.proceso,
                             CierreProductor(0), id_cierre, 0)
        self._codificador.codificar(sobre)
        instancia = self._topologia.instancia_coordinadora(grupo, entrada)
        destino = self._topologia.destino_control(grupo, entrada.nombre, instancia)
        registro = self._obtener_registro_ejecucion(entrada.id_ejecucion)
        salida = (grupo, entrada.nombre)
        estado = registro.iniciar_cierre(salida)
        if estado.cierre_confirmado:
            return
        cuerpo = self._codificador.codificar(SobreMensaje(
            entrada, TipoMensaje.EOF, self._configuracion.proceso, CierreProductor(estado.total),
            id_cierre, 0,
        ))
        self._publicando = True
        try:
            self._publicar_cuerpo_binario(destino, cuerpo, reservar_capacidad=False)
            registro.confirmar_cierre(salida)
        finally:
            self._publicando = False

    def validar_liberacion(self, id_ejecucion):
        """Exige que todas las salidas conocidas hayan confirmado su EOF."""
        registro = self._registros.get(id_ejecucion)
        if registro is None and ruta_registro(self._directorio, id_ejecucion).exists():
            registro = self._obtener_registro_ejecucion(id_ejecucion)
        if registro is not None:
            registro.validar_liberacion()

    def liberar_ejecucion(self, id_ejecucion):
        """Conserva los cierres finales y retira el detalle de lotes antes de cerrar el archivo."""
        self.validar_liberacion(id_ejecucion)
        registro = self._obtener_registro_ejecucion(id_ejecucion)
        registro.retirar_ejecucion()
        registro.cerrar_registro()
        del self._registros[id_ejecucion]

    def publicar_control(self, sobre, instancia=None):
        """Publica un control identificado sin reservar capacidad ni cambiar su ID."""
        if not isinstance(sobre, SobreMensaje) or sobre.tipo not in (
            TipoMensaje.PREPARAR_CIERRE, TipoMensaje.AVANCE_REPLICA, TipoMensaje.FIN_ENTRADA,
        ):
            raise ErrorMensaje("El tipo no corresponde a coordinación entre réplicas")
        if self._publicando:
            raise ErrorMiddleware("La publicación no admite llamadas reentrantes")
        cuerpo = self._codificador.codificar(sobre)
        destino = self._topologia.destino_coordinacion(sobre.entrada.nombre, instancia)
        self._publicando = True
        try:
            self._publicar_cuerpo_binario(destino, cuerpo, reservar_capacidad=False)
        finally:
            self._publicando = False

    def cerrar_registros(self):
        """Cierra todos los WAL abiertos sin borrar publicaciones ni emitir EOF."""
        errores = []
        for registro in self._registros.values():
            try:
                registro.cerrar_registro()
            except Exception as error:
                errores.append(error)
        self._registros.clear()
        if errores:
            raise ExceptionGroup("Falló el cierre del seguimiento de publicaciones", errores)

    def _obtener_registro_ejecucion(self, id_ejecucion):
        """Recupera el seguimiento de una ejecución antes de publicar o cerrar sus salidas."""
        if id_ejecucion not in self._registros:
            self._registros[id_ejecucion] = PublicacionesEjecucion(
                self._directorio, self._configuracion, id_ejecucion,
            )
        return self._registros[id_ejecucion]

    def _publicar_envios_pendientes(self, preparados):
        """Publica intentos pendientes y registra sus confirmaciones de manera individual."""
        confirmadas = []
        for preparado in preparados:
            grupo, entrada = preparado.salida
            registro = self._obtener_registro_ejecucion(entrada.id_ejecucion)
            salida = (grupo, entrada.nombre)
            try:
                anterior = registro.validar_envio(salida, preparado.identidad, preparado.huella)
                if not anterior.confirmada:
                    self._publicar_cuerpo_binario(preparado.destino, preparado.cuerpo)
                    registro.confirmar_envio(salida, preparado.identidad)
            except OperacionInterrumpida:
                raise
            except ErrorMiddleware as error:
                if confirmadas:
                    raise ErrorPublicacionParcial(confirmadas, preparado.salida) from error
                raise
            confirmadas.append(preparado.salida)

    def _preparar_envio_identificado(self, envio):
        """Valida y codifica el envío, resuelve su destino y calcula su huella."""
        if not isinstance(envio, EnvioDatos) or not isinstance(envio.datos, Datos):
            raise ErrorMensaje("Cada salida debe ser un EnvioDatos con Datos")
        if type(envio.grupo) is not str or not envio.grupo.strip():
            raise ErrorMensaje("El grupo destinatario debe ser texto no vacío")
        sobre = SobreMensaje(
            entrada=envio.datos.entrada,
            tipo=TipoMensaje.DATOS,
            origen=self._configuracion.proceso,
            contenido=envio.datos.contenido,
            id_entrada=envio.id_entrada,
            indice=envio.indice,
        )
        cuerpo = self._codificador.codificar(sobre)
        ruta = self._configuracion.obtener_ruta(
            sobre.origen.grupo, envio.grupo, sobre.entrada.nombre,
        )
        if sobre.contenido.esquema not in ruta.esquemas:
            raise ErrorMensaje("El esquema no está admitido en la ruta de salida")
        instancia = self._seleccionar_instancia_destinataria(envio)
        destino = self._topologia.destino_datos(
            envio.grupo, sobre.entrada.nombre, instancia,
        )
        direccion = EscritorBinario(MAX_BYTES_DIRECCION)
        for campo in (destino.exchange, destino.clave, destino.cola):
            direccion.texto(campo)
        huella = hashlib.sha256(direccion.resultado() + cuerpo).digest()
        identidad = (envio.id_entrada, envio.indice)
        self._obtener_registro_ejecucion(sobre.entrada.id_ejecucion).validar_envio(
            (envio.grupo, sobre.entrada.nombre), identidad, huella,
        )
        return _EnvioPreparado((envio.grupo, sobre.entrada), identidad, destino, cuerpo, huella)

    def _seleccionar_instancia_destinataria(self, envio):
        """Resuelve la instancia explícita o por afinidad; devuelve None para reparto."""
        clave = envio.clave_afinidad
        if envio.instancia is not None:
            # La instancia se elige directamente: no se hashea ni se combina con una clave.
            if clave is not None:
                raise ErrorMensaje("La instancia y la clave de afinidad no se pueden combinar")
            cantidad = self._configuracion.obtener_grupo(envio.grupo).instancias
            if type(envio.instancia) is not int or not 0 <= envio.instancia < cantidad:
                raise ErrorMensaje("La instancia destinataria no pertenece al grupo")
            return envio.instancia
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

    def _publicar_cuerpo_binario(self, destino, cuerpo, *, reservar_capacidad=True):
        """Envía datos con capacidad transaccional o controles con confirmación del broker."""
        propiedades = pika.BasicProperties(
            headers={CABECERA_CAPACIDAD: VERSION_CAPACIDAD} if reservar_capacidad else None,
            content_type="application/octet-stream",
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        )
        if reservar_capacidad:
            self._capacidad.publicar_dato(destino, cuerpo, propiedades)
            return
        canal = self._conexion.canal
        try:
            canal.basic_publish(
                exchange=destino.exchange,
                routing_key=destino.clave,
                body=cuerpo,
                mandatory=True,
                properties=propiedades,
            )
        except pika.exceptions.UnroutableError as error:
            raise ErrorMensaje("El mensaje no llegó a una cola destinataria") from error
        except pika.exceptions.NackError as error:
            # Un NACK puede indicar una cola llena, pero no identifica la causa.
            raise PublicacionRechazada("El broker no admitió la publicación") from error
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudo confirmar la publicación") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudo publicar el mensaje") from error
