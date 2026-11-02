"""Genera entradas incrementales y stopwords locales para el despliegue de prueba."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    base = Path('/prueba')
    opciones = json.loads((base / 'escenario.json').read_text())
    corpus = base / 'nltk/corpora/stopwords'
    corpus.mkdir(parents=True)
    (corpus / 'english').write_text('the\nand\n')
    (corpus / 'french').write_text('le\net\n')
    entrada = base / 'entrada'
    entrada.mkdir()
    # Se reutiliza el mismo archivo por ambos clientes; sus ejecuciones son independientes.
    with pq.ParquetWriter(entrada / 'articulos.parquet', pa.schema([
        ('abstract', pa.string()), ('date_modified', pa.string()),
        ('image', pa.struct([('content_url', pa.string())])),
        ('in_language', pa.struct([('identifier', pa.string())])),
        ('name', pa.string()), ('sections', pa.string()), ('url', pa.string()),
    ])) as escritor:
        for inicio in range(0, opciones['filas_por_cliente'], 256):
            filas = []
            for n in range(inicio, min(inicio + 256, opciones['filas_por_cliente'])):
                filas.append({
                    'abstract': 'hotel the le ' + ' '.join(
                        f'mot{n}_{p}' for p in range(opciones['palabras_por_abstract'])),
                    'date_modified': '2019-01-01' if n % 3 else '2021-01-01',
                    'image': {'content_url': 'http://imagenes:8080/imagen.bin'},
                    'in_language': {'identifier': 'en' if n % 2 else 'fr'},
                    'name': f'Artículo {n}', 'sections': json.dumps([{'name': 'Biography'}]),
                    'url': f'http://wiki/articulo/{n}',
                })
            escritor.write_table(pa.Table.from_pylist(filas, schema=escritor.schema))
    imagenes = base / 'imagenes'
    imagenes.mkdir()
    (imagenes / 'imagen.bin').write_bytes(b'imagen de prueba' * 1024)


if __name__ == '__main__':
    main()
