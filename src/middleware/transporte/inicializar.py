"""Inicialización única de la topología y sus permisos antes de iniciar procesos."""

import argparse

import pika

from .conexion import ConexionBroker
from .capacidad import publicar_permiso
from ..configuracion.cargador import cargar_configuracion
from ..interfaz import ErrorCapacidad
from .colas import declarar_cola
from .topologia import PREFIJO_RECURSOS, TopologiaGrupos


COLA_BLOQUEO = f"{PREFIJO_RECURSOS}.capacidad.inicializador"
COLA_INICIADA = f"{PREFIJO_RECURSOS}.capacidad.iniciada"


def inicializar_capacidad(configuracion):
    """Carga permisos y marcas de disponibilidad en una misma transacción.

    Un bloqueo exclusivo impide inicializadores simultáneos. La marca durable
    permite repetir el arranque sin duplicar permisos, incluso después de una caída.
    Requiere un virtual host dedicado con la topología de datos vacía.
    """
    topologia = TopologiaGrupos(configuracion)
    with ConexionBroker(configuracion.broker) as conexion:
        try:
            canal = conexion.canal
            canal.queue_declare(queue=COLA_BLOQUEO, exclusive=True, auto_delete=True)
            canal.queue_declare(queue=COLA_INICIADA, durable=True)
            if _inicializacion_completa(canal, topologia):
                return
            topologia.declarar(conexion)
            for cola in topologia.colas_datos():
                estado = canal.queue_declare(queue=cola, passive=True).method
                if estado.message_count or estado.consumer_count:
                    raise ErrorCapacidad("La inicialización requiere colas de datos vacías y sin consumidores")
            for cola in topologia.colas_datos():
                permisos = topologia.cola_capacidad(cola)
                declarar_cola(canal, permisos, configuracion.limites.max_mensajes_por_cola)
                estado = canal.queue_declare(queue=permisos, passive=True).method
                if estado.message_count or estado.consumer_count:
                    raise ErrorCapacidad("La cola de permisos debe estar vacía y sin consumidores")
            marca = topologia.marca_capacidad()
            estado = canal.queue_declare(queue=marca, durable=True).method
            if estado.message_count or estado.consumer_count:
                raise ErrorCapacidad("La marca de capacidad no coincide con la inicialización")
            canal.tx_select()
            for cola in topologia.colas_datos():
                permisos = topologia.cola_capacidad(cola)
                for _ in range(configuracion.limites.max_mensajes_por_cola):
                    publicar_permiso(canal, permisos)
            publicar_permiso(canal, marca)
            canal.basic_publish(
                exchange="", routing_key=COLA_INICIADA, body=marca.encode('utf-8'),
                mandatory=True, properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                ),
            )
            canal.tx_commit()
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("Falló la inicialización de capacidad; no iniciar los procesos") from error


def _inicializacion_completa(canal, topologia):
    """Consulta la marca sin consumirla y exige la misma configuración del despliegue."""
    entrega, _, cuerpo = canal.basic_get(queue=COLA_INICIADA, auto_ack=False)
    if entrega is None:
        return False
    canal.basic_nack(delivery_tag=entrega.delivery_tag, requeue=True)
    if cuerpo != topologia.marca_capacidad().encode('utf-8'):
        raise ErrorCapacidad("El broker contiene permisos para otra configuración; usar un despliegue limpio")
    estado = canal.queue_declare(queue=topologia.marca_capacidad(), passive=True).method
    if estado.message_count != 1:
        raise ErrorCapacidad("La marca de disponibilidad de capacidad es inconsistente")
    return True


def main():
    """Carga la configuración indicada y prepara el despliegue antes de los workers."""
    argumentos = argparse.ArgumentParser(description="Inicializar permisos de comunicación")
    argumentos.add_argument("configuracion", help="Ruta al archivo JSON del despliegue")
    opciones = argumentos.parse_args()
    inicializar_capacidad(cargar_configuracion(opciones.configuracion))


if __name__ == "__main__":
    main()
