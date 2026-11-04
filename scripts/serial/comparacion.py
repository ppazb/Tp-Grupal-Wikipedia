"""Exporta y compara resultados completos, conservando las filas repetidas."""

import csv
import hashlib
from collections import Counter
from pathlib import Path

COLUMNAS = {'q1': ('url', 'fecha'), 'q2': ('nombre', 'url', 'archivo'),
            'q3': ('palabra',), 'q4': ('palabra', 'cantidad'), 'q5': ('url', 'cantidad')}
BLOQUE_BYTES = 64 * 1024


def huella(ruta):
    """Calcula la huella del contenido leyendo por bloques."""
    resumen = hashlib.sha256()
    with Path(ruta).open('rb') as archivo:
        for bloque in iter(lambda: archivo.read(BLOQUE_BYTES), b''):
            resumen.update(bloque)
    return resumen.hexdigest()


def descargar_imagen(url, destino, sesion, timeout):
    """Guarda una imagen completa; un fallo HTTP invalida la referencia."""
    with sesion.get(url, stream=True, timeout=timeout) as respuesta:
        respuesta.raise_for_status()
        with destino.open('wb') as archivo:
            for bloque in respuesta.iter_content(BLOQUE_BYTES):
                if archivo.write(bloque) != len(bloque):
                    raise OSError(f'Escritura incompleta de {destino}')
    if destino.stat().st_size == 0:
        raise ValueError(f'Imagen vacía: {url}')


def exportar(resultados, destino, sesion, timeout):
    """Exporta las consultas y descarga las imágenes seleccionadas por Q2."""
    destino = Path(destino)
    (destino / 'imagenes').mkdir(parents=True)
    for consulta, tabla in resultados.items():
        with (destino / f'{consulta}.csv').open('w', encoding='utf-8', newline='') as archivo:
            escritor = csv.writer(archivo)
            escritor.writerow(COLUMNAS[consulta])
            if consulta == 'q3':
                escritor.writerows((palabra,) for palabra in tabla)
                continue
            for numero, fila in enumerate(tabla.itertuples(index=False, name=None)):
                if consulta == 'q1':
                    fecha, url = fila
                    escritor.writerow((url, fecha.isoformat()))
                elif consulta == 'q2':
                    nombre, url, imagen = fila
                    relativa = ''
                    if isinstance(imagen, str) and imagen:
                        relativa = f'imagenes/{numero}.img'
                        descargar_imagen(imagen, destino / relativa, sesion, timeout)
                    escritor.writerow((nombre, url, relativa))
                else:
                    escritor.writerow(fila)


def leer_filas(directorio, consulta):
    """Normaliza formatos y compara imágenes por contenido, no por nombre de archivo."""
    import pandas as pd

    ruta = directorio / f'{consulta}.csv'
    if not ruta.exists():
        return Counter()  # El cliente no crea un CSV para una consulta sin resultados.
    filas = Counter()
    with ruta.open(encoding='utf-8', newline='') as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != list(COLUMNAS[consulta]):
            raise ValueError(f'Encabezado inesperado: {ruta}')
        for fila in lector:
            if None in fila or any(valor is None for valor in fila.values()):
                raise ValueError(f'Fila incompleta en {ruta}')
            valores = [fila[columna] for columna in COLUMNAS[consulta]]
            if consulta == 'q1':
                valores[1] = pd.Timestamp(valores[1]).isoformat()
            elif consulta in ('q4', 'q5'):
                valores[1] = str(int(valores[1]))
            elif consulta == 'q2' and valores[2]:
                imagen = (directorio / valores[2]).resolve()
                if not imagen.is_relative_to(directorio.resolve()):
                    raise ValueError(f'Imagen fuera del directorio: {imagen}')
                valores[2] = huella(imagen)
            filas[tuple(valores)] += 1
    return filas


def comparar(referencia, clientes, destino):
    """Escribe todas las diferencias y devuelve si cada cliente coincide con la referencia."""
    coincide = True
    with Path(destino).open('w', encoding='utf-8', newline='') as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(('cliente', 'consulta', 'diferencia', 'fila', 'cantidad'))
        for consulta in COLUMNAS:
            esperadas = leer_filas(referencia, consulta)
            for cliente in clientes:
                obtenidas = leer_filas(cliente, consulta)
                faltantes, sobrantes = esperadas - obtenidas, obtenidas - esperadas
                correcto = not faltantes and not sobrantes
                coincide &= correcto
                print(f'{cliente.name} {consulta}: {"OK" if correcto else "DIFERENCIAS"}', flush=True)
                for tipo, diferencias in (('faltante', faltantes), ('sobrante', sobrantes)):
                    for fila, cantidad in diferencias.items():
                        escritor.writerow((cliente.name, consulta, tipo, repr(fila), cantidad))
        for cliente in clientes:
            errores = cliente / 'errores.csv'
            if errores.exists():
                with errores.open(encoding='utf-8', newline='') as registros:
                    lector = csv.reader(registros)
                    next(lector, None)
                    for fila in lector:
                        coincide = False
                        escritor.writerow((cliente.name, 'errores', 'rechazo', repr(fila), 1))
    return coincide
