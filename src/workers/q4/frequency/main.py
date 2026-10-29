"""Arranque del proceso de Frecuencia con configuración de comunicación y parciales."""

import argparse

from src.common.protocol.registros import Esquema
from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.configuracion import ErrorConfiguracion
from .worker import WorkerFrecuencia


CONFIGURACION_PREDETERMINADA = 'config/config.json'
ENTRADA_PALABRAS = 'palabras'
GRUPO_CONTEOS = 'top_n'
ENTRADA_CONTEOS = 'conteos'


def ejecutar():
    """Valida las rutas y ejecuta el worker con una conexión propia del proceso."""
    parser = argparse.ArgumentParser(description='Worker de frecuencias parciales de Q4')
    parser.add_argument('--config', default=CONFIGURACION_PREDETERMINADA,
                        help='Opcional: archivo JSON de comunicación (por defecto: %(default)s)')
    parser.add_argument('--entrada', default=ENTRADA_PALABRAS,
                        help='Opcional: entrada de palabras (por defecto: %(default)s)')
    parser.add_argument('--grupo-salida', default=GRUPO_CONTEOS,
                        help='Opcional: grupo que recibe los conteos (por defecto: %(default)s)')
    parser.add_argument('--entrada-salida', default=ENTRADA_CONTEOS,
                        help='Opcional: entrada del grupo destino (por defecto: %(default)s)')
    argumentos = parser.parse_args()
    configuracion = cargar_configuracion(argumentos.config)
    validar_rutas(configuracion, argumentos.entrada, argumentos.grupo_salida,
                  argumentos.entrada_salida)
    from src.middleware.rabbitmq import MiddlewareRabbitMQ

    try:
        with MiddlewareRabbitMQ(configuracion) as middleware:
            WorkerFrecuencia(
                middleware,
                entrada=argumentos.entrada, grupo_salida=argumentos.grupo_salida,
                entrada_salida=argumentos.entrada_salida,
            ).ejecutar()
    except KeyboardInterrupt:
        # La salida del contexto libera la conexión tras interrumpir la operación.
        return


def validar_rutas(configuracion, entrada, grupo_salida, entrada_salida):
    """Exige una entrada de palabras y una ruta de salida que admita conteos."""
    grupo = configuracion.proceso.grupo
    entrantes = [ruta for ruta in configuracion.rutas if ruta.destino == grupo]
    if not entrantes or any(
        ruta.entrada != entrada or ruta.esquemas != (Esquema.PALABRA,)
        for ruta in entrantes
    ):
        raise ErrorConfiguracion('Frecuencia requiere una única entrada lógica de PALABRA')
    salida = configuracion.obtener_ruta(grupo, grupo_salida, entrada_salida)
    if Esquema.CONTEO_PALABRA not in salida.esquemas:
        raise ErrorConfiguracion('La salida de Frecuencia debe admitir CONTEO_PALABRA')


if __name__ == '__main__':
    ejecutar()
