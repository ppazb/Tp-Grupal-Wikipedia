"""Medición incremental del espacio para registros de un lote serializado."""

from ..common.protocol.primitivas import ErrorSerializacion, EscritorBinario
from ..common.protocol.serializer import escribir_registro
from .interfaz import ErrorMensaje


class PresupuestoLote:
    """Contabiliza bytes de registros; el creador ya descontó sobre y cabecera.

    No guarda registros ni adquiere permisos del broker. Se utiliza para agregar
    registros cuyo tamaño no cambia después de admitirlos.
    """

    def __init__(self, bytes_disponibles, esquema):
        if type(bytes_disponibles) is not int or bytes_disponibles <= 0:
            raise ErrorMensaje('El sobre y la cabecera no dejan espacio para registros')
        self._capacidad = bytes_disponibles
        self._ocupados = 0
        self._esquema = esquema

    def admitir(self, registro):
        """Reserva sus bytes si cabe; devuelve False sin modificar el estado si falta espacio.

        Un registro inválido o que no cabe ni en un lote nuevo eleva ErrorMensaje.
        """
        try:
            escritor = EscritorBinario(self._capacidad, medir=True)
            escribir_registro(escritor, self._esquema, registro)
        except ErrorSerializacion as error:
            raise ErrorMensaje(str(error)) from error
        if self._ocupados + escritor.posicion > self._capacidad:
            return False
        self._ocupados += escritor.posicion
        return True

    @property
    def completo(self):
        """Indica que no quedan bytes disponibles para otro registro."""
        return self._ocupados == self._capacidad
