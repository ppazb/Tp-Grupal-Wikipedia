import logging
from pathlib import Path

import pyarrow.parquet as parquet


LOG = logging.getLogger(__name__)
EXTENSION_PARQUET = ".parquet"
COLUMNAS_PARQUET = (
    "abstract",
    "date_modified",
    "image",
    "in_language",
    "name",
    "sections",
    "url",
)


def listar_parquets(ruta_directorio):
    """Parquets del directorio, ordenados por nombre para un envio determinista"""
    directorio = Path(ruta_directorio)
    if not directorio.is_dir():
        raise ValueError(f"INPUT_DIR no es un directorio: {ruta_directorio}")
    
    archivos = sorted(
        ruta for ruta in directorio.iterdir()
        if ruta.is_file() and ruta.suffix == EXTENSION_PARQUET
    )
    if not archivos:
        raise ValueError(f"No hay archivos {EXTENSION_PARQUET} en {ruta_directorio}")
    
    return archivos


def _abrir(ruta):
    archivo = parquet.ParquetFile(ruta)
    faltantes = set(COLUMNAS_PARQUET) - set(archivo.schema_arrow.names)
    if faltantes:
        raise ValueError(f"Faltan columnas en {ruta.name}: {', '.join(sorted(faltantes))}")
    return archivo


def iterar_lotes(ruta_directorio, lote_size):
    """
    Recorre todos los Parquet del directorio y entrega listas de filas (dicts) de a lote_size
    lee de a un lote por vez, así que no carga los archivos enteros en memoria
    Valida las columnas de cada archivo antes de empezar a leerlo
    """
    archivos = listar_parquets(ruta_directorio)
    for numero, ruta in enumerate(archivos, start=1):
        archivo = _abrir(ruta)
        LOG.info("Leyendo %s (%d/%d)", ruta.name, numero, len(archivos))
        
        for lote_arrow in archivo.iter_batches(batch_size=lote_size, columns=list(COLUMNAS_PARQUET)):
            filas = lote_arrow.to_pylist()
            if filas:
                yield filas
