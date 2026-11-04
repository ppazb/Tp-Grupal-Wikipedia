"""Arranque del proceso de Frecuencia con configuración de comunicación y parciales."""

import argparse
import logging

from src.common.protocol.registros import Esquema
from src.middleware.cargador_configuracion import cargar_configuracion
from src.workers.base_worker import destinos_errores, entorno_worker
from src.workers.arranque import agregar_argumentos_comunicacion, validar_rutas
from .worker import WorkerFrecuencia


REGISTRO = logging.getLogger(__name__)


ENTRADA_PALABRAS = 'palabras'
GRUPO_CONTEOS = 'top_n'
ENTRADA_CONTEOS = 'conteos'


def ejecutar():
    """Valida las rutas y ejecuta el worker con una conexión propia del proceso."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
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
    REGISTRO.info('Frecuencia: iniciando proceso %s', configuracion.proceso)
    with entorno_worker(configuracion) as (middleware, apagado):
        worker = WorkerFrecuencia(
            middleware, entrada=argumentos.entrada, grupo_salida=argumentos.grupo_salida,
            entrada_salida=argumentos.entrada_salida,
        )
        worker.destinos_errores = destinos_errores(configuracion)
        apagado.registrar_limpieza(worker.cerrar)
        REGISTRO.info('Frecuencia: listo para recibir %s; salida %s/%s',
                      argumentos.entrada, argumentos.grupo_salida, argumentos.entrada_salida)
        worker.ejecutar(apagado)
    REGISTRO.info('Frecuencia: proceso detenido y recursos liberados')


if __name__ == '__main__':
    try:
        ejecutar()
    except Exception:
        REGISTRO.exception('Frecuencia: el proceso terminó con un error')
        raise
