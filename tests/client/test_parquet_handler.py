
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.client.src.parquet_handler import COLUMNAS_PARQUET, iterar_lotes, listar_parquets


def escribir_parquet(directorio, nombre, nombres_articulos, columnas=COLUMNAS_PARQUET):
    tabla = pa.table({
        columna: nombres_articulos if columna == "name" else ["x"] * len(nombres_articulos)
        for columna in columnas
    })
    pq.write_table(tabla, Path(directorio) / nombre)

"""Tests simples del parquet_handler: arman Parquet chicos en un directorio temporal."""

class TestParquetHandler(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def test_lista_solo_los_parquet_ordenados_por_nombre(self):
        escribir_parquet(self.dir, "b.parquet", ["1"])
        escribir_parquet(self.dir, "a.parquet", ["1"])
        (Path(self.dir) / "notas.txt").write_text("no soy un parquet")
        self.assertEqual([r.name for r in listar_parquets(self.dir)], ["a.parquet", "b.parquet"])

    def test_directorio_inexistente_o_sin_parquets_falla(self):
        with self.assertRaises(ValueError):
            listar_parquets(Path(self.dir) / "no_existe")
        with self.assertRaises(ValueError):
            listar_parquets(self.dir)

    def test_envia_todos_los_archivos_en_orden_y_en_lotes_del_tamano_pedido(self):
        escribir_parquet(self.dir, "a.parquet", ["a1", "a2", "a3"])
        escribir_parquet(self.dir, "b.parquet", ["b1", "b2"])
        lotes = [[f["name"] for f in lote] for lote in iterar_lotes(self.dir, 2)]
        self.assertEqual(lotes, [["a1", "a2"], ["a3"], ["b1", "b2"]])

    def test_archivo_con_columnas_faltantes_falla(self):
        escribir_parquet(self.dir, "a.parquet", ["a1"], columnas=("name", "url"))
        with self.assertRaises(ValueError):
            list(iterar_lotes(self.dir, 2))


if __name__ == "__main__":
    unittest.main()
