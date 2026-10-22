"""Lectura y escritura acotadas de enteros, textos y bytes en orden de red."""


BYTES_U8 = 1
BYTES_U16 = 2
BYTES_U32 = 4
BYTES_U64 = 8
CARACTERES_POR_BLOQUE = 1024


class ErrorSerializacion(ValueError):
    """Un campo o cuerpo no cumple el formato binario o su límite de tamaño."""


def validar_entero(valor, ancho):
    """Comprueba el rango sin signo y rechaza booleanos y conversiones implícitas."""
    if type(valor) is not int or not 0 <= valor < (1 << (8 * ancho)):
        raise ErrorSerializacion("Entero fuera de rango o de tipo incorrecto")


class EscritorBinario:
    """Mide o construye un cuerpo sin superar el máximo de bytes admitido."""

    def __init__(self, max_bytes: int, *, medir: bool = False):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ErrorSerializacion("El límite de bytes debe ser un entero positivo")
        self._max_bytes = max_bytes
        self._cuerpo = None if medir else bytearray()
        self.posicion = 0

    def agregar(self, datos):
        """Agrega bytes después de comprobar el espacio disponible."""
        if self.posicion + len(datos) > self._max_bytes:
            raise ErrorSerializacion("El cuerpo supera el límite de bytes")
        self.posicion += len(datos)
        if self._cuerpo is not None:
            self._cuerpo.extend(datos)

    def entero(self, valor, ancho):
        validar_entero(valor, ancho)
        self.agregar(valor.to_bytes(ancho, "big"))

    def iniciar_bloque(self):
        """Reserva el prefijo u32 de un bloque y devuelve su posición."""
        posicion = self.posicion
        self.entero(0, BYTES_U32)
        return posicion

    def terminar_bloque(self, inicio):
        """Completa el prefijo con la cantidad de bytes escritos en el bloque."""
        longitud = self.posicion - inicio - BYTES_U32
        validar_entero(longitud, BYTES_U32)
        if self._cuerpo is not None:
            self._cuerpo[inicio:inicio + BYTES_U32] = longitud.to_bytes(BYTES_U32, "big")

    def texto(self, valor):
        """Escribe UTF-8 con prefijo u32, convirtiendo por bloques acotados."""
        if type(valor) is not str:
            raise ErrorSerializacion("Se esperaba un texto")
        if len(valor) + BYTES_U32 > self._max_bytes - self.posicion:
            raise ErrorSerializacion("El texto supera el espacio disponible")
        inicio = self.iniciar_bloque()
        try:
            for indice in range(0, len(valor), CARACTERES_POR_BLOQUE):
                self.agregar(valor[indice:indice + CARACTERES_POR_BLOQUE].encode("utf-8"))
        except UnicodeError as error:
            raise ErrorSerializacion("Texto no representable en UTF-8") from error
        self.terminar_bloque(inicio)

    def binario(self, valor):
        if type(valor) is not bytes:
            raise ErrorSerializacion("Se esperaban bytes")
        self.entero(len(valor), BYTES_U32)
        self.agregar(valor)

    def resultado(self):
        """Devuelve los bytes construidos; el modo de medición no produce cuerpo."""
        if self._cuerpo is None:
            raise ErrorSerializacion("Un escritor de medición no produce bytes")
        return bytes(self._cuerpo)


class LectorBinario:
    """Recorre una vista del cuerpo comprobando cada frontera antes de leer."""

    def __init__(self, cuerpo):
        self._cuerpo = memoryview(cuerpo)
        self.posicion = 0

    @property
    def restantes(self):
        return len(self._cuerpo) - self.posicion

    def tomar(self, cantidad):
        if type(cantidad) is not int or cantidad < 0 or cantidad > self.restantes:
            raise ErrorSerializacion("Campo incompleto o longitud inválida")
        inicio = self.posicion
        self.posicion += cantidad
        return self._cuerpo[inicio:self.posicion]

    def entero(self, ancho):
        return int.from_bytes(self.tomar(ancho), "big")

    def bloque(self):
        return self.tomar(self.entero(BYTES_U32))

    def texto(self):
        try:
            return bytes(self.bloque()).decode("utf-8")
        except UnicodeError as error:
            raise ErrorSerializacion("Texto con UTF-8 inválido") from error

    def finalizar(self):
        if self.restantes:
            raise ErrorSerializacion("El cuerpo contiene bytes sobrantes")

    def vista_restante(self):
        """Devuelve una vista sin copiar ni avanzar la posición."""
        return self._cuerpo[self.posicion:]
