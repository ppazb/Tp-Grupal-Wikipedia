import os
import tempfile
import unittest

from src.common.io_adapters.wal import WAL


class TestWAL(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.ruta = os.path.join(self._dir.name, 'log', 'wal.bin')

    def test_recupera_registros_en_orden_tras_reabrir(self):
        with WAL(self.ruta) as wal:
            wal.agregar(b'a')
            wal.agregar(b'')
            wal.agregar(b'ccc')
        with WAL(self.ruta) as wal:
            self.assertEqual(wal.leer(), [b'a', b'', b'ccc'])
            self.assertEqual(list(wal.iterar()), [b'a', b'', b'ccc'])

    def test_descarta_cola_incompleta_y_sigue_escribiendo(self):
        with WAL(self.ruta) as wal:
            wal.agregar(b'uno')
            wal.agregar(b'dos')
        with open(self.ruta, 'r+b') as archivo:
            archivo.truncate(os.path.getsize(self.ruta) - 1)
        with WAL(self.ruta) as wal:
            self.assertEqual(wal.leer(), [b'uno'])
            wal.agregar(b'tres')
            self.assertEqual(wal.leer(), [b'uno', b'tres'])

    def test_descarta_registro_corrupto(self):
        with WAL(self.ruta) as wal:
            wal.agregar(b'uno')
            wal.agregar(b'dos')
        with open(self.ruta, 'r+b') as archivo:
            archivo.seek(-1, os.SEEK_END)
            archivo.write(b'X')
        with WAL(self.ruta) as wal:
            self.assertEqual(wal.leer(), [b'uno'])

    def test_una_cola_de_ceros_no_valida_como_registros_vacios(self):
        with WAL(self.ruta) as wal:
            wal.agregar(b'uno')
        with open(self.ruta, 'ab') as archivo:
            archivo.write(bytes(64))
        with WAL(self.ruta) as wal:
            self.assertEqual(wal.leer(), [b'uno'])


if __name__ == '__main__':
    unittest.main()
