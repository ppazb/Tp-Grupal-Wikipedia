import os
import unittest

from src.common.protocol.dto import Entrada
from src.common.protocol.registros import Esquema, Idioma, Lote, PalabraIdioma
from src.workers.q3.intersection.worker import ENTRADA_RESULTADOS, GRUPO_RESULTADOS, IntersectionQ3
from src.workers.wal_ejecuciones import ARCHIVO_CERRADAS
from tests.workers.soporte import PruebaConWal, id_salida

EN, FR = Idioma.INGLES, Idioma.FRANCES
ENTRADA = Entrada('ej1', 'palabras_idioma')


def lote(*pares):
    return Lote(Esquema.PALABRA_IDIOMA, tuple(PalabraIdioma(p, i) for p, i in pares))


class TestIntersectionQ3(PruebaConWal):
    clase_worker = IntersectionQ3
    grupo = 'q3_intersection'

    def palabras(self, worker):
        return [r.palabra for e in worker.middleware.publicados for r in e.datos.contenido.registros]

    def test_solo_devuelve_palabras_presentes_en_ambos_idiomas(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR), ('hotel', EN), ('hotel', FR),
                                            ('solo_en', EN), ('solo_fr', FR)), 'L1')
        self.assertEqual(self.palabras(worker), ['casa', 'hotel'])

    def test_interseccion_entre_lotes_distintos_y_sin_duplicados(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('hotel', EN), ('casa', EN)), 'L1')
        self.entregar(worker, ENTRADA, lote(('hotel', FR), ('hotel', EN), ('hotel', FR)), 'L2')
        self.entregar(worker, ENTRADA, lote(('hotel', FR)), 'L3')
        self.assertEqual(self.palabras(worker), ['hotel'])

    def test_los_resultados_de_un_lote_salen_con_su_id_y_un_indice(self):
        worker = self.nuevo_worker()
        worker.middleware.registros_por_mensaje = 1  # un mensaje por palabra
        self.entregar(worker, ENTRADA, lote(('a', EN), ('a', FR), ('b', EN), ('b', FR)), 'L1')
        self.assertEqual(self.ids(worker), [(id_salida('L1'), 0), (id_salida('L1'), 1)])

    def test_cierra_la_salida_hacia_el_gateway(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN)), 'L1')
        self.cerrar(worker, ENTRADA)
        self.assertEqual(worker.middleware.finalizadas, [(GRUPO_RESULTADOS, Entrada('ej1', ENTRADA_RESULTADOS))])
        self.assertEqual(self.archivos_wal(), [ARCHIVO_CERRADAS])

    def test_lote_repetido_no_republica(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR)), 'L1')
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR)), 'L1')
        self.assertEqual(self.palabras(worker), ['casa'])

    def test_tras_reiniciar_recupera_el_estado_sin_republicar(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('hotel', EN), ('hotel', FR)), 'L1')
        self.caer(worker)

        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('hotel', EN), ('hotel', FR)), 'L1')  # reentrega
        self.assertEqual(self.palabras(worker), [])
        self.entregar(worker, ENTRADA, lote(('casa', FR), ('hotel', FR)), 'L2')
        self.assertEqual(self.palabras(worker), ['casa'])  # hotel ya se había enviado

    def test_caida_antes_del_wal_republica_con_los_mismos_ids(self):
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR)), 'L1')
        primeros = self.ids(worker)
        # Se simula que el WAL no llegó a escribirse: un worker nuevo sobre un directorio vacío
        self.caer(worker)
        for nombre in self.archivos_wal():
            os.remove(os.path.join(self.wal, nombre))
        worker = self.nuevo_worker()
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR)), 'L1')
        self.assertEqual(self.ids(worker), primeros)

    def test_ejecucion_cerrada_descarta_lotes(self):
        worker = self.nuevo_worker()
        self.cerrar(worker, ENTRADA)
        self.entregar(worker, ENTRADA, lote(('casa', EN), ('casa', FR)), 'L9')
        self.assertEqual(self.palabras(worker), [])


if __name__ == '__main__':
    unittest.main()
