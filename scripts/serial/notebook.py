"""Ejecuta los cálculos del notebook con los Parquet locales, sin editarlo."""

import ast
import collections
import json
from pathlib import Path

COLUMNAS = ('abstract', 'date_modified', 'image', 'in_language', 'name', 'sections', 'url')


def ejecutar_celda(fuente, contexto, nombre):
    """Captura el objeto de la última expresión, sin depender de su presentación."""
    arbol = ast.parse(fuente, filename=nombre)
    expresion = None
    if arbol.body and isinstance(arbol.body[-1], ast.Expr):
        expresion = ast.Expression(arbol.body.pop().value)
    exec(compile(arbol, nombre, 'exec'), contexto)
    if expresion is not None:
        return eval(compile(expresion, nombre, 'eval'), contexto)


def lotes(archivos, filas_por_lote):
    """Entrega los Parquet en orden, en DataFrames chicos, sin cargar nunca el conjunto completo."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    for ruta in archivos:
        for lote in pq.ParquetFile(ruta).iter_batches(batch_size=filas_por_lote, columns=list(COLUMNAS)):
            yield pd.DataFrame(pa.Table.from_batches([lote]).to_pandas())


def ejecutar_notebook(ruta, entrada, filas_por_lote=100_000):
    """Ejecuta las celdas originales del notebook lote a lote, en un único proceso y con memoria acotada.

    Cada lote hace el papel de `wiki_entry_df` y las celdas 4 a 9 y 10 se ejecutan tal cual sobre él.
    Los aportes de cada lote se combinan de forma exacta: concatenación (Q1, Q2), suma de contadores
    (Q4), unión de conjuntos (Q3) y suma/cantidad para el promedio de Q5.
    """
    import pandas as pd

    documento = json.loads(Path(ruta).read_text(encoding='utf-8'))
    celdas = documento['cells']
    # El adaptador depende de esta organización; un cambio debe revisarse explícitamente.
    marcas = {2: 'kagglehub.dataset_download', 6: '[["date_modified", "url"]]',
              7: 'has_biography_section', 8: 'words_counter.most_common',
              9: 'pd.Series', 12: 'abstract_word_count'}
    if len(celdas) != 13 or any(marca not in ''.join(celdas[i]['source'])
                              for i, marca in marcas.items()):
        raise ValueError('Cambió la estructura del notebook; revisar el adaptador serial')
    archivos = sorted(Path(entrada).glob('*.parquet'))
    if not archivos:
        raise ValueError(f'No hay Parquet en {entrada}')
    fuentes = {i: ''.join(c['source']) for i, c in enumerate(celdas) if c['cell_type'] == 'code'}

    def correr(indice, contexto):
        return ejecutar_celda(fuentes[indice], contexto, f'{ruta}:celda-{indice}')

    contexto = {'__name__': '__referencia_serial__'}
    # kagglehub no hace falta: los datos son locales.
    exec(compile(fuentes[0].replace('import kagglehub\n', ''), f'{ruta}:celda-0', 'exec'), contexto)
    q1, q2, q5 = [], [], []
    contador, ingles, frances = collections.Counter(), set(), set()
    total_antes = cantidad_antes = 0
    print(f'Ejecutando celdas 4 a 10 del notebook por lotes de {filas_por_lote} filas', flush=True)
    procesadas = 0
    for lote in lotes(archivos, filas_por_lote):
        contexto['wiki_entry_df'] = lote
        for indice in (4, 5):
            correr(indice, contexto)
        antes = contexto['wiki_entry_before_2020_df']
        q1.append(correr(6, contexto))
        correr(7, contexto)
        q2.append(antes.loc[antes['has_biography_section'], ['name', 'url', 'image']])
        correr(8, contexto)  # Reinicia words_counter: queda el contador de este lote.
        contador.update(contexto['words_counter'])
        correr(9, contexto)
        ingles |= contexto['english_words']
        frances |= contexto['french_words']
        correr(10, contexto)
        # Mismo cálculo que la celda 11, acumulando total y cantidad en vez de dividir por lote.
        extraer = contexto['extract_non_stopwords_from_abstract']
        for abstract in antes['abstract'].dropna():
            total_antes += len(extraer(abstract))
            cantidad_antes += 1
        # Celda 12 sin el filtro final, que necesita el promedio global; se descartan los conteos
        # en cero porque nunca pueden superar un promedio (>= 0).
        despues = contexto['wiki_entry_after_2020_df']
        despues['abstract_word_count'] = despues['abstract'].apply(
            lambda abstract: len(extraer(abstract)))
        q5.append(despues.loc[despues['abstract_word_count'] > 0, ['url', 'abstract_word_count']])
        procesadas += len(lote)
    promedio = total_antes / cantidad_antes
    print('Before 2020 abstract average:', promedio, flush=True)
    vacio = {'q1': ['date_modified', 'url'], 'q2': ['name', 'url', 'image'],
             'q5': ['url', 'abstract_word_count']}

    def unir(partes, clave):
        return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=vacio[clave])

    resultados = {'q1': unir(q1, 'q1'), 'q2': unir(q2, 'q2')}
    resultados['q4'] = pd.DataFrame(contador.most_common(20), columns=['word', 'count'])
    resultados['q3'] = pd.Series(data=list(ingles & frances))
    q5 = unir(q5, 'q5')
    resultados['q5'] = q5[q5['abstract_word_count'] > promedio]
    return resultados, archivos
