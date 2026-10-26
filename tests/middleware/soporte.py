"""Datos, procesos y observación del transporte exclusivos de las pruebas."""

from dataclasses import replace
from pathlib import Path

from src.common.protocol.dto import Datos, Entrada, IdentidadProceso
from src.common.protocol.registros import (
    CodigoError, ConteoPalabra, ErrorRegistro, Esquema, Lote, Palabra,
)
from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from tests.soporte import observar_reserva


CONFIGURACION = Path(__file__).with_name('config.json')


def configurar(virtual_host, grupo='ocurrencias', instancia=0):
    base = cargar_configuracion(CONFIGURACION)
    return replace(
        base, broker=replace(base.broker, virtual_host=virtual_host),
        proceso=IdentidadProceso(grupo, instancia),
    )


def palabras(ejecucion, *valores):
    return Datos(Entrada(ejecucion, 'palabras'), Lote(
        Esquema.PALABRA, tuple(Palabra(valor) for valor in valores),
    ))


def conteo(valor):
    return Datos(Entrada('cliente', 'conteos'), Lote(
        Esquema.CONTEO_PALABRA, (ConteoPalabra(valor, 1),),
    ))


def aviso(valor):
    registro = ErrorRegistro(
        'palabras', Esquema.PALABRA.value, CodigoError.VALIDACION_NEGOCIO,
        0, None, valor,
    )
    return Datos(Entrada('cliente', 'errores'), Lote(Esquema.ERROR_REGISTRO, (registro,)))


def publicar_en_proceso(configuracion, eventos, envios, reserva_observada):
    with MiddlewareRabbitMQ(configuracion) as middleware:
        observar_reserva(middleware, eventos, reserva_observada)
        middleware.publicar_en_ramas(envios)
        eventos.put('publicado')


def consumir_en_proceso(configuracion, eventos, publicar_salida=False, observar_error=False):
    with MiddlewareRabbitMQ(configuracion) as middleware:
        middleware._conexion.programar(lambda: eventos.put('listo'))
        if publicar_salida or observar_error:
            observar_reserva(middleware, eventos)

        def procesar(datos):
            eventos.put(datos)
            if publicar_salida:
                # Detener nuevas entregas no debe reencolar la que está procesándose.
                middleware._conexion.programar(middleware.detener_recepcion)
                middleware.publicar('top_n', conteo('segundo'))

        middleware.recibir(procesar)
        eventos.put('terminado')
