import unittest

from src.common.protocol.dto import Entrada, EntregaActual, IdentidadProceso
from src.workers.deduplicacion import DedupPorOrigen

K = 4

ORIGEN_A = IdentidadProceso('productores', 0)
ORIGEN_B = IdentidadProceso('productores', 1)


def entrega(id_entrada, indice, origen=ORIGEN_A, ejecucion='ej1'):
    return EntregaActual(Entrada(ejecucion, 'entrada'), origen, id_entrada, indice)


class PruebasComunes:
    """Comportamiento común de la deduplicación por (ejecución, origen)."""

    def test_mensaje_nuevo_no_es_duplicado_y_repetido_si(self):
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0)))
        self.dedup.registrar(entrega('L1', 0))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))

    def test_es_duplicado_no_modifica_el_estado(self):
        self.dedup.es_duplicado(entrega('L1', 0))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0)))

    def test_indice_mayor_al_maximo_no_es_duplicado(self):
        self.dedup.registrar(entrega('L1', 1))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 1)))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 2)))

    def test_indices_salteados(self):
        self.dedup.registrar(entrega('L1', 0))
        self.dedup.registrar(entrega('L1', 3))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 2)))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 4)))

    def test_registrar_un_indice_menor_no_baja_el_maximo(self):
        self.dedup.registrar(entrega('L1', 3))
        self.dedup.registrar(entrega('L1', 1))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 3)))

    def test_otro_id_entrada_no_es_duplicado(self):
        self.dedup.registrar(entrega('L1', 0))
        self.assertFalse(self.dedup.es_duplicado(entrega('L2', 0)))

    def test_otra_ejecucion_no_es_duplicado(self):
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej1'))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej2')))


class TestDedupPorOrigen(PruebasComunes, unittest.TestCase):
    def setUp(self):
        self.dedup = DedupPorOrigen(K)

    def test_cerrar_ejecucion_olvida_sus_ids(self):
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej1'))
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej2'))
        self.dedup.cerrar_ejecucion('ej1')
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej1')))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej2')))

    def test_otro_origen_no_es_duplicado(self):
        self.dedup.registrar(entrega('L1', 0, origen=ORIGEN_A))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, origen=ORIGEN_B)))

    def test_recuerda_solo_los_ultimos_k_lotes_por_origen(self):
        for numero in range(K + 1):
            self.dedup.registrar(entrega(f'L{numero}', 0))
        self.assertFalse(self.dedup.es_duplicado(entrega('L0', 0)))  # el más viejo se olvidó
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))
        self.assertTrue(self.dedup.es_duplicado(entrega(f'L{K}', 0)))

    def test_la_ventana_es_por_ejecucion_y_origen(self):
        self.dedup.registrar(entrega('L0', 0, ejecucion='ej1'))
        # otra ejecucion del mismo origen llena su propia ventana: no desplaza a L0 de ej1
        for numero in range(K + 1):
            self.dedup.registrar(entrega(f'X{numero}', 0, ejecucion='ej2'))
        self.assertTrue(self.dedup.es_duplicado(entrega('L0', 0, ejecucion='ej1')))
        self.assertFalse(self.dedup.es_duplicado(entrega('X0', 0, ejecucion='ej2')))  # este si se olvido

    def test_cerrar_ejecucion_libera_sus_ventanas(self):
        self.dedup.registrar(entrega('L1', 0, origen=ORIGEN_A, ejecucion='ej1'))
        self.dedup.registrar(entrega('L1', 0, origen=ORIGEN_B, ejecucion='ej1'))
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej2'))
        self.dedup.cerrar_ejecucion('ej1')
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, origen=ORIGEN_A, ejecucion='ej1')))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, origen=ORIGEN_B, ejecucion='ej1')))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej2')))

    def test_actualizar_un_lote_no_ocupa_otro_lugar(self):
        for numero in range(K):
            self.dedup.registrar(entrega(f'L{numero}', 0))
        self.dedup.registrar(entrega('L0', 1))  # mismo lote, otro índice
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))
        self.assertTrue(self.dedup.es_duplicado(entrega('L0', 1)))


if __name__ == '__main__':
    unittest.main()
