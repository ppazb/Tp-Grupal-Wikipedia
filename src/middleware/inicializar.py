"""Inicialización única de la topología y sus permisos antes de iniciar procesos."""

import argparse

import pika

from .broker_connection import ConexionBroker
from .capacidad import COLA_INEXISTENTE, publicar_permiso
from .cargador_configuracion import cargar_configuracion
from .interfaz import ErrorCapacidad
from .queue_handler import declarar_cola
from .topologia import PREFIJO_RECURSOS, TopologiaGrupos


COLA_BLOQUEO = f"{PREFIJO_RECURSOS}.capacidad.inicializador"
COLA_INICIADA = f"{PREFIJO_RECURSOS}.capacidad.iniciada"


def inicializar_capacidad(configuracion):
    """Declara colas vacías y carga permisos una sola vez por despliegue.

    Un bloqueo exclusivo impide inicializadores simultáneos. La marca durable
    impide duplicar permisos, incluso si una inicialización anterior quedó parcial.
    Requiere un virtual host dedicado con la topología de datos vacía.
    """
    topologia = TopologiaGrupos(configuracion)
    with ConexionBroker(configuracion.broker) as conexion:
        try:
            canal = conexion.canal
            canal.queue_declare(queue=COLA_BLOQUEO, exclusive=True, auto_delete=True)
            _comprobar_sin_inicializar(conexion)
            canal.queue_declare(queue=COLA_INICIADA, durable=True)
            topologia.declarar(conexion)
            for cola in topologia.colas_datos():
                estado = canal.queue_declare(queue=cola, passive=True).method
                if estado.message_count or estado.consumer_count:
                    raise ErrorCapacidad("La inicialización requiere colas de datos vacías y sin consumidores")
            canal.confirm_delivery()
            for cola in topologia.colas_datos():
                permisos = topologia.cola_capacidad(cola)
                declarar_cola(canal, permisos, configuracion.limites.max_mensajes_por_cola)
                estado = canal.queue_declare(queue=permisos, passive=True).method
                if estado.message_count or estado.consumer_count:
                    raise ErrorCapacidad("La cola de permisos debe estar vacía y sin consumidores")
                for _ in range(configuracion.limites.max_mensajes_por_cola):
                    publicar_permiso(canal, permisos)
            canal.queue_declare(queue=topologia.marca_capacidad(), durable=True)
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("Falló la inicialización de capacidad; no iniciar los procesos") from error


def _comprobar_sin_inicializar(conexion):
    canal = conexion.crear_canal()
    try:
        canal.queue_declare(queue=COLA_INICIADA, passive=True)
    except pika.exceptions.ChannelClosedByBroker as error:
        if error.reply_code == COLA_INEXISTENTE:
            return
        raise
    else:
        raise ErrorCapacidad("El despliegue ya tiene una inicialización iniciada; no se duplican permisos")
    finally:
        if canal.is_open:
            canal.close()


def main():
    """Carga la configuración indicada y prepara el despliegue antes de los workers."""
    argumentos = argparse.ArgumentParser(description="Inicializar permisos de comunicación")
    argumentos.add_argument("configuracion", help="Ruta al archivo INI del despliegue")
    opciones = argumentos.parse_args()
    inicializar_capacidad(cargar_configuracion(opciones.configuracion))


if __name__ == "__main__":
    main()
