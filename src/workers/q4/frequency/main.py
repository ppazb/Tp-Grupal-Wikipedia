"""Arranque del proceso de Frecuencia con configuración de comunicación y parciales."""

import argparse

from src.common.protocol.registros import Esquema
from src.middleware.cargador_configuracion import cargar_configuracion
from src.workers.base_worker import destinos_errores
from src.workers.arranque import agregar_argumentos_comunicacion, validar_rutas
from .worker import WorkerFrecuencia


ENTRADA_PALABRAS = 'palabras'
GRUPO_CONTEOS = 'top_n'
ENTRADA_CONTEOS = 'conteos'


def ejecutar():
    """Valida las rutas y ejecuta el worker con una conexión propia del proceso."""
    parser = argparse.ArgumentParser(description='Worker de frecuencias parciales de Q4')
    agregar_argumentos_comunicacion(
        parser, entrada=ENTRADA_PALABRAS, grupo_salida=GRUPO_CONTEOS,
        entrada_salida=ENTRADA_CONTEOS,
    )
    argumentos = parser.parse_args()
    configuracion = cargar_configuracion(argumentos.config)
    validar_rutas(configuracion, argumentos.entrada, argumentos.grupo_salida,
                  argumentos.entrada_salida, esquema_entrada=Esquema.PALABRA,
                  esquema_salida=Esquema.CONTEO_PALABRA)
    from src.middleware.rabbitmq import MiddlewareRabbitMQ

    try:
        with MiddlewareRabbitMQ(configuracion) as middleware:
            worker = WorkerFrecuencia(
                middleware,
                entrada=argumentos.entrada, grupo_salida=argumentos.grupo_salida,
                entrada_salida=argumentos.entrada_salida,
            )
            # Cierra la entrada de errores del gateway al terminar cada ejecución
            worker.destinos_errores = destinos_errores(configuracion)
            worker.ejecutar()
    except KeyboardInterrupt:
        # La salida del contexto libera la conexión tras interrumpir la operación.
        return


if __name__ == '__main__':
    ejecutar()
