"""Ejecuta los cálculos del notebook con los Parquet locales, sin editarlo."""

import ast
import json
from pathlib import Path

COLUMNAS = ('abstract', 'date_modified', 'image', 'in_language', 'name', 'sections', 'url')
CELDAS_RESULTADO = {6: 'q1', 7: 'q2', 8: 'q4', 9: 'q3', 12: 'q5'}


def ejecutar_celda(fuente, contexto, nombre):
    """Captura el objeto de la última expresión, sin depender de su presentación."""
    arbol = ast.parse(fuente, filename=nombre)
    expresion = None
    if arbol.body and isinstance(arbol.body[-1], ast.Expr):
        expresion = ast.Expression(arbol.body.pop().value)
    exec(compile(arbol, nombre, 'exec'), contexto)
    if expresion is not None:
        return eval(compile(expresion, nombre, 'eval'), contexto)


def ejecutar_notebook(ruta, entrada):
    """Sustituye la celda de carga por los archivos locales y ejecuta las consultas originales."""
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
    contexto = {'__name__': '__referencia_serial__'}
    resultados = {}
    for indice, celda in enumerate(celdas):
        if celda['cell_type'] != 'code':
            continue
        print(f'Ejecutando celda {indice} del notebook', flush=True)
        if indice == 2:
            # El notebook trabaja con un DataFrame completo; conservamos ese comportamiento.
            contexto['wiki_entry_df'] = pd.concat(
                [pd.read_parquet(ruta, columns=list(COLUMNAS)) for ruta in archivos],
                ignore_index=True,
            )
            continue
        resultado = ejecutar_celda(''.join(celda['source']), contexto, f'{ruta}:celda-{indice}')
        if indice in CELDAS_RESULTADO:
            resultados[CELDAS_RESULTADO[indice]] = resultado
    # Q2 añade la identidad URL a las filas ya seleccionadas por el notebook.
    resultados['q2'] = contexto['wiki_entry_before_2020_df'].loc[
        resultados['q2'].index, ['name', 'url', 'image']]
    return resultados, archivos
