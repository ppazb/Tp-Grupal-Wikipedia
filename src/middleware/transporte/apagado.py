"""Integra el descriptor de apagado con el selector interno de Pika"""

from ..interfaz import OperacionInterrumpida


class ApagadoBroker:
    """Despierta esperas AMQP sin ejecutar cierres reentrantes dentro de Pika.

    BlockingConnection no expone registro de descriptores. El acceso a su
    implementación SelectConnection queda concentrado en este adaptador.
    El proceso conserva abierto Apagado hasta cerrar el middleware.
    """

    def __init__(self, conexion, apagado):
        self._apagado = apagado
        self._bucle = conexion._impl.ioloop
        self._descriptor = apagado.fileno()
        self._registrado = False
        self.comprobar()
        self._bucle.add_handler(self._descriptor, self._atender_aviso_apagado, self._bucle.READ)
        self._registrado = True

    def comprobar(self):
        """Impide comenzar o confirmar trabajo después de una solicitud de apagado."""
        if self._apagado.solicitado:
            self.desvincular_descriptor()
            raise OperacionInterrumpida('La comunicación se detuvo por apagado')

    def _atender_aviso_apagado(self, _descriptor, _eventos):
        """Consume el aviso del descriptor y propaga la interrupción al bucle de Pika."""
        self._apagado.consumir_avisos()
        if self._apagado.solicitado:
            # Retirar primero permite a los finally cancelar consumidores y cerrar
            # canales sin recibir otra interrupción al despachar eventos de Pika.
            self.desvincular_descriptor()
            self.comprobar()

    def desvincular_descriptor(self):
        """Retira el descriptor sin cerrar el componente de apagado del proceso."""
        if self._registrado:
            self._bucle.remove_handler(self._descriptor)
            self._registrado = False
