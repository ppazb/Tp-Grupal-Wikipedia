"""Tramos binarios ordenados y fusión incremental de conteos de palabras."""

from contextlib import ExitStack
from itertools import groupby
from heapq import merge
from struct import Struct

from src.common.protocol.primitivas import BYTES_U64, validar_entero
from src.common.protocol.registros import ConteoPalabra


CABECERA = Struct('!IQ')
BUFFER_ARCHIVO = 64 * 1024


def sumar_cantidades(primera: int, segunda: int) -> int:
    """Suma conteos representables en el DTO de comunicación."""
    resultado = primera + segunda
    validar_entero(resultado, BYTES_U64)
    return resultado


def _escribir_completo(archivo, datos):
    vista = memoryview(datos)
    while vista:
        escritos = archivo.write(vista)
        if escritos is None or escritos <= 0 or escritos > len(vista):
            raise OSError('No se pudo completar la escritura del tramo')
        vista = vista[escritos:]


def _leer_completo(archivo, cantidad, *, admitir_fin=False):
    datos = bytearray()
    while len(datos) < cantidad:
        parte = archivo.read(cantidad - len(datos))
        if not parte:
            if admitir_fin and not datos:
                return None
            raise ValueError('Tramo de conteos truncado')
        datos.extend(parte)
    return datos


def escribir_conteos(ruta, registros, comprobar_detencion=None):
    """Escribe incrementalmente registros ordenados sin conservar el iterable."""
    with ruta.open('xb', buffering=BUFFER_ARCHIVO) as archivo:
        for registro in registros:
            if comprobar_detencion is not None:
                comprobar_detencion()
            palabra = registro.palabra.encode('utf-8')
            _escribir_completo(archivo, CABECERA.pack(len(palabra), registro.cantidad))
            _escribir_completo(archivo, palabra)


def leer_conteos(archivo, max_bytes_palabra, comprobar_detencion=None):
    """Lee un registro a la vez y rechaza longitudes, conteos u orden inválidos."""
    anterior = None
    while True:
        if comprobar_detencion is not None:
            comprobar_detencion()
        cabecera = _leer_completo(archivo, CABECERA.size, admitir_fin=True)
        if cabecera is None:
            return
        longitud, cantidad = CABECERA.unpack(cabecera)
        if not 0 < longitud <= max_bytes_palabra or cantidad == 0:
            raise ValueError('Longitud o cantidad inválida en un tramo de conteos')
        palabra = _leer_completo(archivo, longitud).decode('utf-8')
        if not palabra.strip() or (anterior is not None and palabra <= anterior):
            raise ValueError('El tramo contiene palabras inválidas, repetidas o desordenadas')
        anterior = palabra
        yield ConteoPalabra(palabra, cantidad)


def agrupar_conteos(registros, comprobar_detencion=None):
    """Suma palabras contiguas sin materializar el grupo de aportes."""
    for palabra, aportes in groupby(registros, key=lambda registro: registro.palabra):
        cantidad = 0
        for aporte in aportes:
            if comprobar_detencion is not None:
                comprobar_detencion()
            cantidad = sumar_cantidades(cantidad, aporte.cantidad)
        yield ConteoPalabra(palabra, cantidad)


def fusionar_tramos(primero, segundo, destino, max_bytes_palabra, comprobar_detencion=None):
    """Fusiona dos tramos ordenados usando dos lectores y un escritor."""
    with ExitStack() as recursos:
        lectores = [
            leer_conteos(recursos.enter_context(ruta.open('rb', buffering=BUFFER_ARCHIVO)),
                         max_bytes_palabra, comprobar_detencion)
            for ruta in (primero, segundo)
        ]
        registros = merge(*lectores, key=lambda registro: registro.palabra)
        escribir_conteos(destino, agrupar_conteos(registros, comprobar_detencion), comprobar_detencion)
