"""Inicialización y recuperación de la topología antes de habilitar los procesos."""

import argparse

import pika

from src.middleware.transporte.conexion import ConexionBroker
from src.middleware.transporte.capacidad import COLA_INEXISTENTE
from src.middleware.transporte.permisos import publicar_permiso
from src.middleware.configuracion.cargador import cargar_configuracion
from src.middleware.interfaz import ErrorCapacidad
from src.middleware.transporte.declaraciones import declarar_cola
from src.middleware.transporte.topologia import PREFIJO_RECURSOS, TopologiaGrupos


COLA_BLOQUEO = f'{PREFIJO_RECURSOS}.capacidad.inicializador'
COLA_INICIADA = f'{PREFIJO_RECURSOS}.capacidad.iniciada'


def inicializar_capacidad(configuracion):
    """Completa una preparación interrumpida sin alterar despliegues habilitados.

    El bloqueo exclusivo serializa inicializadores. La marca final solo existe
    después de confirmar todos los avisos y turnos. Antes de ella no pueden
    abrirse los workers; sus colas auxiliares se reconstruyen si quedó un parcial.
    """
    topologia = TopologiaGrupos(configuracion)
    with ConexionBroker(configuracion.broker) as conexion:
        try:
            canal = conexion.canal
            canal.queue_declare(queue=COLA_BLOQUEO, exclusive=True, auto_delete=True)
            if _existe_cola(conexion, topologia.marca_capacidad()):
                return
            canal.confirm_delivery()
            _validar_preparacion(canal, topologia.marca_capacidad())
            topologia.declarar(conexion)
            for cola in topologia.colas_datos():
                estado = canal.queue_declare(queue=cola, passive=True).method
                if estado.message_count or estado.consumer_count:
                    raise ErrorCapacidad('La inicialización requiere datos vacíos y sin consumidores')
            auxiliares = list(topologia.turnos_recepcion())
            for cola in topologia.colas_datos():
                aviso = topologia.cola_capacidad(cola)
                # Los avisos son intercambiables: conservar el último no pierde trabajo.
                canal.queue_declare(queue=aviso, durable=True, arguments={
                    'x-queue-type': 'classic', 'x-max-length': 1, 'x-overflow': 'drop-head',
                })
                auxiliares.append(aviso)
                turno = topologia.cola_turno_capacidad(cola)
                declarar_cola(canal, turno, 1)
                auxiliares.append(turno)
            for cola in topologia.turnos_recepcion():
                declarar_cola(canal, cola, 1)
            for cola in auxiliares:
                estado = canal.queue_declare(queue=cola, passive=True).method
                if estado.consumer_count:
                    raise ErrorCapacidad('Hay consumidores antes de completar la inicialización')
            for cola in auxiliares:
                canal.queue_purge(queue=cola)
                publicar_permiso(canal, cola)
            canal.queue_declare(queue=topologia.marca_capacidad(), durable=True)
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad('Falló la inicialización; puede reintentarse antes de iniciar los procesos') from error


def _validar_preparacion(canal, marca):
    """Conserva la configuración de una preparación para impedir reintentos incompatibles."""
    canal.queue_declare(queue=COLA_INICIADA, durable=True)
    entrega, _, cuerpo = canal.basic_get(queue=COLA_INICIADA, auto_ack=False)
    esperado = marca.encode('utf-8')
    if entrega is not None:
        canal.basic_nack(delivery_tag=entrega.delivery_tag, requeue=True)
        if cuerpo != esperado:
            raise ErrorCapacidad('La preparación existente pertenece a otra configuración de capacidad')
        return
    canal.basic_publish(
        exchange='', routing_key=COLA_INICIADA, body=esperado, mandatory=True,
        properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
    )


def _existe_cola(conexion, nombre):
    """Consulta la existencia de una cola sin crearla."""
    canal = conexion.crear_canal()
    try:
        canal.queue_declare(queue=nombre, passive=True)
        return True
    except pika.exceptions.ChannelClosedByBroker as error:
        if error.reply_code == COLA_INEXISTENTE:
            return False
        raise
    finally:
        if canal.is_open:
            canal.close()


def main():
    """Carga la configuración indicada y prepara el despliegue antes de los workers."""
    argumentos = argparse.ArgumentParser(description='Inicializar capacidad de comunicación')
    argumentos.add_argument('configuracion', help='Ruta al archivo JSON del despliegue')
    opciones = argumentos.parse_args()
    inicializar_capacidad(cargar_configuracion(opciones.configuracion))


if __name__ == '__main__':
    main()
