"""Ejecuta la referencia serial y compara los CSV e imágenes de todos los clientes."""

import argparse
import json
import logging
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.io_adapters.descargas_http import TIMEOUT_SEGUNDOS
from serial.comparacion import comparar, exportar, huella
from serial.notebook import ejecutar_notebook


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--entrada', type=Path, default=Path('input'), help='Parquet locales (por defecto: input)')
    parser.add_argument('--salidas', type=Path, default=Path('output'), help='Resultados client_* (por defecto: output)')
    parser.add_argument('--notebook', type=Path, default=Path('version-secuencial.ipynb'), help='Notebook original')
    parser.add_argument('--registros', type=Path, default=Path('registros/comparacion-serial'), help='Referencias e informes')
    parser.add_argument('--timeout-http', type=float, default=TIMEOUT_SEGUNDOS,
                        help='Timeout de conexión/lectura de imágenes, en segundos (%(default)s, el mismo que el worker de Q2)')
    args = parser.parse_args()
    if args.timeout_http <= 0:
        parser.error('--timeout-http debe ser positivo')
    clientes = sorted(ruta for ruta in args.salidas.glob('client_*') if ruta.is_dir())
    if not clientes:
        parser.error(f'No hay resultados de clientes en {args.salidas}')
    destino = args.registros / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    destino.mkdir(parents=True)
    informe = {'estado': 'incompleto',
               'clientes': [str(cliente) for cliente in clientes]}
    try:
        informe['notebook_sha256'] = huella(args.notebook)
        archivos = sorted(args.entrada.glob('*.parquet'))
        informe['entrada'] = [{'archivo': str(ruta), 'bytes': ruta.stat().st_size,
                               'modificado_ns': ruta.stat().st_mtime_ns} for ruta in archivos]
        resultados, _ = ejecutar_notebook(args.notebook, args.entrada)
        with requests.Session() as sesion:
            sesion.headers['User-Agent'] = os.environ.get(
                'IMAGE_USER_AGENT', 'TpGrupalWikipedia/1.0 (https://github.com/ppazb/Tp-Grupal-Wikipedia) python-requests')
            descartadas = exportar(resultados, destino / 'referencia', sesion, args.timeout_http)
            informe['descargas_fallidas_q2'] = descartadas
            if descartadas:
                print(f'ADVERTENCIA: la referencia omitió {descartadas} artículos de Q2 por errores de descarga; '
                      'consultar referencia/descargas_fallidas_q2.csv.', flush=True)
        del resultados
        for dato in informe['entrada']:
            estado = Path(dato['archivo']).stat()
            if (estado.st_size, estado.st_mtime_ns) != (dato['bytes'], dato['modificado_ns']):
                raise ValueError('La entrada cambió durante el cálculo serial')
        correcto = comparar(destino / 'referencia', clientes, destino / 'diferencias.csv')
        informe['estado'] = 'coincide' if correcto else 'diferencias'
        return 0 if correcto else 1
    except Exception as error:
        informe['error'] = f'{type(error).__name__}: {error}'
        print(f'No se pudo completar la comparación: {error}', flush=True)
        return 2
    finally:
        (destino / 'informe.json').write_text(json.dumps(informe, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f'Referencia e informe: {destino}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
