"""Arranque del proceso de Frecuencia con configuración de comunicación y parciales."""

import argparse

from src.common.protocol.registros import Esquema
from src.middleware.cargador_configuracion import cargar_configuracion
from src.workers.base_worker import entorno_worker
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
    with entorno_worker(configuracion) as (middleware, apagado):
        worker = WorkerFrecuencia(
            middleware, entrada=argumentos.entrada, grupo_salida=argumentos.grupo_salida,
            entrada_salida=argumentos.entrada_salida,
        )
        apagado.registrar_limpieza(worker.cerrar)
        worker.ejecutar(apagado)


if __name__ == '__main__':
    ejecutar()
