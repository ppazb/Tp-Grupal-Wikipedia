"""Datos, procesos y observación del transporte exclusivos de las pruebas."""

import base64
from dataclasses import replace
from pathlib import Path
from urllib.request import Request, urlopen

from src.common.protocol.dto import Datos, Entrada, IdentidadProceso
from src.common.protocol.registros import (
    CodigoError, ConteoPalabra, ErrorRegistro, Esquema, Lote, Palabra,
)
from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.rabbitmq import MiddlewareRabbitMQ


PLAZO_ESCENARIO = 45
PLAZO_PROCESO = 10
PLAZO_BLOQUEO = 0.2
CONFIGURACION = Path(__file__).with_name('config.ini')


def configurar(virtual_host, grupo='ocurrencias', instancia=0):
    base = cargar_configuracion(CONFIGURACION)
    return replace(
        base, broker=replace(base.broker, virtual_host=virtual_host),
        proceso=IdentidadProceso(grupo, instancia),
    )


def administrar_virtual_host(configuracion, metodo):
    """Crea o elimina únicamente el virtual host del escenario en el broker de pruebas."""
    broker = configuracion.broker
    credencial = base64.b64encode(f'{broker.usuario}:{broker.contrasena}'.encode()).decode()
    solicitud = Request(
        f'http://{broker.host}:15672/api/vhosts/{broker.virtual_host}',
        method=metodo, headers={'Authorization': f'Basic {credencial}'},
    )
    with urlopen(solicitud, timeout=PLAZO_PROCESO) as respuesta:
        if respuesta.status not in (201, 204):
            raise AssertionError(f'Administración del virtual host: HTTP {respuesta.status}')


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


def observar_reserva(middleware, eventos, numero=1):
    """Notifica desde el bucle de E/S al esperar la reserva indicada, sin sustituirla."""
    adquirir = middleware._capacidad.adquirir
    llamadas = 0

    def observado(cola):
        nonlocal llamadas
        llamadas += 1
        if llamadas == numero:
            middleware._conexion.programar(lambda: eventos.put('esperando'))
        return adquirir(cola)

    middleware._capacidad.adquirir = observado


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
