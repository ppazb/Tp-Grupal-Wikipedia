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

        self._archivo = open(self._ruta, 'ab') #append

    def agregar(self, datos):
        """Persiste un registro; al finalizar ya está en disco."""
        
        escribir_completo(self._archivo, _empaquetar(bytes(datos)))
        
        #me aseguro que llegue a disco
        self._archivo.flush()
        os.fsync(self._archivo.fileno())

    def leer(self):
        """Devuelve la lista de registros guardados, en orden de escritura."""
        return list(self.iterar())

    def iterar(self):
        """Entrega los registros de a uno, en orden, sin cargar todo el log en memoria.

        Hay que consumirlo antes de seguir agregando o reemplazar."""
        with open(self._ruta, 'rb') as archivo:
            for datos, _ in _leer_registros(archivo):
                yield datos

    def reemplazar(self, registros):
        """Reescribe el log solo con estos registros, atomico: solo se reemplaza si 
        se pudo escribir todo."""
        temporal = self._ruta + '.tmp'

        # Escribo el contenido nuevo aparte, el original no se toca
        with open(temporal, 'wb') as archivo:
            for datos in registros:
                escribir_completo(archivo, _empaquetar(bytes(datos)))
            
            archivo.flush()
            os.fsync(archivo.fileno())

        # cambio atomico: queda el archivo viejo o el nuevo
        self._archivo.close()
        os.replace(temporal, self._ruta)

        
        self._sincronizar_directorio()

        # sigo agregando sobre el archivo nuevo en append
        self._archivo = open(self._ruta, 'ab')

    def cerrar(self):
        self._archivo.close()

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

    def _sincronizar_directorio(self):
        """fsync del directorio: hace durable el cambio de nombre de os.replace."""
        descriptor = os.open(self._directorio, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.cerrar()
