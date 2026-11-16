"""Exclusión entre réplicas mediante un turno no confirmado por partición."""

from contextlib import contextmanager
import logging

from .capacidad import PERMISO
from .interfaz import ErrorRecepcion, OperacionInterrumpida


REGISTRO = logging.getLogger(__name__)


class TurnosRecepcion:
    """Mantiene el turno sin ACK; RabbitMQ lo devuelve si desaparece la conexión."""

    def __init__(self, conexion):
        self._conexion = conexion
        self._canal = conexion.crear_canal()
        self._canal.basic_qos(prefetch_count=1, global_qos=True)
        self._consumidor = None
        self._entrega = None
        self._activo = False
        self._detenido = False

    @contextmanager
    def adquirir(self, cola):
        """Espera por eventos y conserva la exclusión hasta salir del contexto."""
        if self._activo:
            raise ErrorRecepcion('La adquisición de turnos no admite reentrancia')
        if self._entrega is not None or self._consumidor is not None:
            raise ErrorRecepcion('Quedó un turno sin liberar; cerrar la conexión')
        if self._detenido:
            raise OperacionInterrumpida('La espera de recepción fue detenida')
        self._activo = True
        try:
            self._conexion.comprobar_apagado()
            self._consumidor = self._canal.basic_consume(
                queue=cola, auto_ack=False, on_message_callback=self._capturar,
            )
            self._canal.start_consuming()
            self._conexion.comprobar_apagado()
            if self._detenido:
                raise OperacionInterrumpida('La espera de recepción fue detenida')
            if self._entrega is None:
                raise ErrorRecepcion('Se canceló la espera sin entregar el turno')
            _, cuerpo = self._entrega
            if cuerpo != PERMISO:
                raise ErrorRecepcion('El turno de recepción es inválido')
            yield
        except BaseException:
            try:
                self._liberar()
            except Exception:
                REGISTRO.exception('Falló la devolución del turno; se conserva el error original')
            raise
        else:
            self._liberar()
        finally:
            self._activo = False

    def _capturar(self, canal, metodo, propiedades, cuerpo):
        self._entrega = (metodo.delivery_tag, cuerpo)
        self._cancelar()

    def _cancelar(self):
        if self._consumidor is not None and self._canal.is_open:
            self._canal.basic_cancel(consumer_tag=self._consumidor)
            self._consumidor = None

    def _liberar(self):
        self._conexion.preparar_limpieza()
        self._cancelar()
        if self._entrega is not None and self._canal.is_open:
            self._canal.basic_nack(delivery_tag=self._entrega[0], requeue=True)
        self._entrega = None

    def detener(self):
        """Cancela una espera sin quitarle el turno a un procesamiento en curso."""
        self._detenido = True
        self._cancelar()
