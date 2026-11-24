import os
import zlib

from .archivos import escribir_completo

_BYTES_LARGO = 4
_BYTES_CRC = 4
_BYTES_CABECERA = _BYTES_LARGO + _BYTES_CRC


def _crc(bytes_largo, datos):
    # cubre el largo pq crc de 0 es 0, y queremos que el crc de un registro vacío no sea 0
    return zlib.crc32(bytes_largo + datos)


def _empaquetar(datos):
    #crea la cabecera de largo y crc, y la concatena con los datos
    bytes_largo = len(datos).to_bytes(_BYTES_LARGO, 'big')
    bytes_crc = _crc(bytes_largo, datos).to_bytes(_BYTES_CRC, 'big')
    return bytes_largo + bytes_crc + datos


def _leer_registros(archivo):
    # Recorre los registros validos de principio a fin, de a uno (sin cargarlos todos).
    # Entrega (registro, posicion donde termina) y se detiene en el primer registro roto.

    while True:
        cabecera = archivo.read(_BYTES_CABECERA)
        if len(cabecera) < _BYTES_CABECERA:
            break

        bytes_largo = cabecera[:_BYTES_LARGO]
        largo = int.from_bytes(bytes_largo, 'big')
        crc_guardado = int.from_bytes(cabecera[_BYTES_LARGO:], 'big')

        datos = archivo.read(largo)
        #no cumple con el largo esperado, se q el registro esta corrupto
        if len(datos) < largo:
            break

        #si no coincide el crc, se q el registro esta corrupto
        if _crc(bytes_largo, datos) != crc_guardado:
            break

        # si llegamos hasta aca, el registro es valido hasta esa posicion
        yield datos, archivo.tell()


# Cada registro se guarda como [largo 4B][crc32 4B][datos] (el crc es sobre largo y datos).
class WAL:
    def __init__(self, ruta):
        self._ruta = str(ruta)
        self._directorio = os.path.dirname(os.path.abspath(self._ruta))

        #si el directorio no existe, lo creo
        os.makedirs(self._directorio, exist_ok=True)

        # corto si hay registro corrupto
        self._recuperar()

        # El archivo oficial conserva el estado válido si se interrumpió una compactación.
        if os.path.exists(self._ruta + '.compactando'):
            os.unlink(self._ruta + '.compactando')

        self._archivo = open(self._ruta, 'ab') #append

    def agregar(self, datos):
        """Persiste un registro; al finalizar ya está en disco. Devuelve su posición en el archivo."""
        posicion = self._archivo.tell()

        escribir_completo(self._archivo, _empaquetar(bytes(datos)))
        
        #me aseguro que llegue a disco
        self._archivo.flush()
        os.fsync(self._archivo.fileno())
        return posicion

    def leer(self):
        """Devuelve la lista de registros guardados, en orden de escritura."""
        return list(self.iterar())

    def iterar(self):
        """Entrega los registros de a uno, en orden, sin cargar todo el log en memoria.

        Hay que consumirlo antes de seguir agregando."""
        with open(self._ruta, 'rb') as archivo:
            for datos, _ in _leer_registros(archivo):
                yield datos

    def iterar_con_posiciones(self):
        """Como iterar(), pero entrega (registro, posicion) para poder releerlo con leer_en()"""
        with open(self._ruta, 'rb') as archivo:
            inicio = 0
            for datos, fin in _leer_registros(archivo):
                yield datos, inicio
                inicio = fin

    def leer_en(self, posicion):
        """Lee un único registro ya persistido, sin recorrer los anteriores"""
        with open(self._ruta, 'rb') as archivo:
            archivo.seek(posicion)
            for datos, _ in _leer_registros(archivo):
                return datos
        raise ValueError('No hay un registro válido en la posición indicada')

    def cerrar(self):
        self._archivo.close()

    def reemplazar(self, registros):
        """Reemplaza el log por registros completos mediante un archivo temporal sincronizado.

        El llamador excluye otras escrituras y deja de usar la instancia si falla.
        Los registros se recorren incrementalmente; el reemplazo ocurre al terminar.
        """
        ruta_temporal = None
        try:
            ruta_temporal = self._ruta + '.compactando'
            with open(ruta_temporal, 'wb') as temporal:
                for datos in registros:
                    escribir_completo(temporal, _empaquetar(bytes(datos)))
                temporal.flush()
                os.fsync(temporal.fileno())
            os.replace(ruta_temporal, self._ruta)
            self._archivo.close()
            self._archivo = open(self._ruta, 'ab')
            descriptor = os.open(self._directorio, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if ruta_temporal is not None and os.path.exists(ruta_temporal):
                os.unlink(ruta_temporal)

    def _recuperar(self):
        """Corta el archivo despues del ultimo registro valido"""
        if not os.path.exists(self._ruta):
            return

        posicion_valida = 0
        with open(self._ruta, 'rb') as archivo:
            for _, fin in _leer_registros(archivo):
                posicion_valida = fin

        # si el tamaño no coincide, tengo que cortar
        if posicion_valida != os.path.getsize(self._ruta):
            with open(self._ruta, 'r+b') as archivo:
                archivo.truncate(posicion_valida)

                #flusheo y me aseguro que llega a disco
                archivo.flush()
                os.fsync(archivo.fileno())

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cerrar()
