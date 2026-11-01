"""Arranque de Top N con presupuesto explícito y almacenamiento temporal propio."""

import argparse

from src.common.protocol.registros import Esquema
from src.middleware.cargador_configuracion import cargar_configuracion
from src.workers.base_worker import destinos_errores
from src.workers.arranque import agregar_argumentos_comunicacion, validar_rutas
from .consolidador import ConsolidadorFrecuencias
from .worker import WorkerTopN


ENTRADA_CONTEOS = 'conteos'
GRUPO_RESULTADOS = 'gateway_resultados'
ENTRADA_RESULTADOS = 'q4'


def ejecutar():
    """Valida el despliegue y ejecuta Top N liberando archivos y conexión al salir."""
    parser = argparse.ArgumentParser(description='Worker de Top 20 global de Q4')
    agregar_argumentos_comunicacion(
        parser, entrada=ENTRADA_CONTEOS, grupo_salida=GRUPO_RESULTADOS,
        entrada_salida=ENTRADA_RESULTADOS,
    )
    parser.add_argument('--presupuesto-memoria-bytes', type=int, required=True,
                        help='Obligatorio, sin valor por defecto: presupuesto del buffer '
                             'de consolidación compartido entre ejecuciones')
    parser.add_argument('--directorio-temporal', default=None,
                        help='Opcional: directorio existente para archivos propios; '
                             'si se omite se usa el directorio temporal del sistema')
    argumentos = parser.parse_args()
    configuracion = cargar_configuracion(argumentos.config)
    validar_rutas(configuracion, argumentos.entrada, argumentos.grupo_salida,
                  argumentos.entrada_salida, esquema_entrada=Esquema.CONTEO_PALABRA,
                  esquema_salida=Esquema.CONTEO_PALABRA)
    from src.middleware.rabbitmq import MiddlewareRabbitMQ

    try:
        with ConsolidadorFrecuencias(
            argumentos.presupuesto_memoria_bytes, argumentos.directorio_temporal,
        ) as consolidador, MiddlewareRabbitMQ(configuracion) as middleware:
            worker = WorkerTopN(
                middleware, consolidador, entrada=argumentos.entrada,
                grupo_salida=argumentos.grupo_salida, entrada_salida=argumentos.entrada_salida,
            )
            # Cierra la entrada de errores del gateway al terminar cada ejecución
            worker.destinos_errores = destinos_errores(configuracion)
            worker.ejecutar()
    except KeyboardInterrupt:
        return


if __name__ == '__main__':
    ejecutar()
