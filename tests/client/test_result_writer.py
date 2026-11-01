import os
import tempfile
import unittest

from src.client.src.result_writer import EscritorResultados
from src.common.protocol.registros import Esquema, FragmentoImagen, Lote, ResultadoQ1


class TestEscritorResultados(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.escritor = EscritorResultados(self.dir)

    def tearDown(self):
        self.escritor.cerrar()
        self._tmp.cleanup()

    def leer(self, ruta):
        with open(os.path.join(self.dir, ruta), encoding="utf-8") as archivo:
            return archivo.read()

    def test_resultados_tabulares_van_a_un_archivo_por_entrada(self):
        self.escritor.escribir("resultados_finales_q1", Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("u1", "f1"),)))
        self.escritor.escribir("resultados_finales_q1", Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("u2", "f2"),)))
        self.escritor.cerrar()
        self.assertEqual(self.leer("q1.csv"), "url,fecha\nu1,f1\nu2,f2\n")

    def test_imagen_se_reconstruye_con_fragmentos_desordenados_y_repetidos(self):
        def frag(desplazamiento, datos):
            return Lote(Esquema.FRAGMENTO_IMAGEN, (FragmentoImagen("t1", "Ada", "u/ada", 6, desplazamiento, datos),))

        self.escritor.escribir("resultados_finales_q2", frag(3, b"def"))
        self.escritor.escribir("resultados_finales_q2", frag(3, b"def"))  # reentrega
        self.escritor.escribir("resultados_finales_q2", frag(0, b"abc"))
        self.escritor.cerrar()
        ruta = os.path.join(self.dir, "imagenes", "t1.img")
        with open(ruta, "rb") as archivo:
            self.assertEqual(archivo.read(), b"abcdef")
        self.assertEqual(self.leer("q2.csv"), "nombre,url,archivo\nAda,u/ada,imagenes/t1.img\n")

    def test_articulo_sin_imagen_figura_con_archivo_vacio(self):
        sin_imagen = FragmentoImagen("t2", "Bob", "u/bob", 0, 0, b"")
        self.escritor.escribir("resultados_finales_q2", Lote(Esquema.FRAGMENTO_IMAGEN, (sin_imagen,)))
        self.escritor.cerrar()
        self.assertEqual(self.leer("q2.csv"), "nombre,url,archivo\nBob,u/bob,\n")
        self.assertEqual(os.listdir(os.path.join(self.dir, "imagenes")), [])

    def test_al_iniciar_se_borran_resultados_de_una_ejecucion_anterior(self):
        self.escritor.escribir("resultados_finales_q1", Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("u", "f"),)))
        self.escritor.escribir("resultados_finales_q2", Lote(
            Esquema.FRAGMENTO_IMAGEN, (FragmentoImagen("t3", "Ada", "u/ada", 3, 0, b"abc"),)))
        self.escritor.cerrar()

        nuevo = EscritorResultados(self.dir)  # misma carpeta, otra ejecución
        nuevo.cerrar()
        self.assertEqual(sorted(os.listdir(self.dir)), ["imagenes"])
        self.assertEqual(os.listdir(os.path.join(self.dir, "imagenes")), [])


if __name__ == "__main__":
    unittest.main()
