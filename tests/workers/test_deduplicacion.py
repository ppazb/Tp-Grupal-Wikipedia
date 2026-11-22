import unittest

from src.common.protocol.dto import Entrada, EntregaActual, IdentidadProceso
from src.workers.deduplicacion import LOTES_RECORDADOS, DedupDesdeAfinidad, DedupDesdeCompartida

ORIGEN_A = IdentidadProceso('productores', 0)
ORIGEN_B = IdentidadProceso('productores', 1)


def entrega(id_entrada, indice, origen=ORIGEN_A, ejecucion='ej1'):
    return EntregaActual(Entrada(ejecucion, 'entrada'), origen, id_entrada, indice)


class PruebasComunes:
    """Lo que deben cumplir ambas deduplicaciones."""

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


class TestDedupDesdeCompartida(PruebasComunes, unittest.TestCase):
    def setUp(self):
        self.dedup = DedupDesdeCompartida()

    def test_otro_origen_si_es_duplicado(self):
        # con cola compartida otra réplica puede reprocesar el mismo lote
        self.dedup.registrar(entrega('L1', 0, origen=ORIGEN_A))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0, origen=ORIGEN_B)))

    def test_cerrar_ejecucion_olvida_sus_ids(self):
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej1'))
        self.dedup.registrar(entrega('L1', 0, ejecucion='ej2'))
        self.dedup.cerrar_ejecucion('ej1')
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej1')))
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0, ejecucion='ej2')))


class TestDedupDesdeAfinidad(PruebasComunes, unittest.TestCase):
    def setUp(self):
        self.dedup = DedupDesdeAfinidad()

    def test_otro_origen_no_es_duplicado(self):
        self.dedup.registrar(entrega('L1', 0, origen=ORIGEN_A))
        self.assertFalse(self.dedup.es_duplicado(entrega('L1', 0, origen=ORIGEN_B)))

    def test_recuerda_solo_los_ultimos_lotes_por_origen(self):
        for numero in range(LOTES_RECORDADOS + 1):
            self.dedup.registrar(entrega(f'L{numero}', 0))
        self.assertFalse(self.dedup.es_duplicado(entrega('L0', 0)))  # el más viejo se olvidó
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))
        self.assertTrue(self.dedup.es_duplicado(entrega(f'L{LOTES_RECORDADOS}', 0)))

    def test_la_ventana_es_por_ejecucion_y_origen(self):
        self.dedup.registrar(entrega('L0', 0, ejecucion='ej1'))
        # otra ejecucion del mismo origen llena su propia ventana: no desplaza a L0 de ej1
        for numero in range(LOTES_RECORDADOS + 1):
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
        for numero in range(LOTES_RECORDADOS):
            self.dedup.registrar(entrega(f'L{numero}', 0))
        self.dedup.registrar(entrega('L0', 1))  # mismo lote, otro índice
        self.assertTrue(self.dedup.es_duplicado(entrega('L1', 0)))
        self.assertTrue(self.dedup.es_duplicado(entrega('L0', 1)))


if __name__ == '__main__':
    unittest.main()
