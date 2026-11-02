import unittest

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Idioma, Lote, PalabraIdioma
from src.workers.q3.intersection.worker import ENTRADA_RESULTADOS, IntersectionQ3

EN, FR = Idioma.INGLES, Idioma.FRANCES


class _Presupuesto:
    def admitir(self, palabra):
        return True


class _MiddlewareFalso:
    def __init__(self):
        self.publicados = []
        self.finalizadas = []

    def crear_presupuesto_lote(self, entrada, esquema):
        return _Presupuesto()

    def publicar(self, grupo, datos):
        self.publicados.append(datos)

    def finalizar_entrada(self, grupo, entrada):
        self.finalizadas.append((grupo, entrada))


class TestIntersectionQ3(unittest.TestCase):

    def setUp(self):
        # Se evita __init__ para no abrir conexión con RabbitMQ
        self.worker = object.__new__(IntersectionQ3)
        self.worker.middleware = _MiddlewareFalso()
        self.worker._ejecuciones = {}
        self.worker._clientes_vistos = set()
        self.entrada = Entrada("ej1", "q3_entrada")

    def enviar(self, *pares):
        registros = tuple(PalabraIdioma(palabra, idioma) for palabra, idioma in pares)
        self.worker.procesar(Datos(self.entrada, Lote(Esquema.PALABRA_IDIOMA, registros)))

    def resultado(self):
        self.worker.al_finalizar(self.entrada)
        return sorted(p.palabra for d in self.worker.middleware.publicados for p in d.contenido.registros)

    def test_solo_devuelve_palabras_presentes_en_ambos_idiomas(self):
        self.enviar(("casa", EN), ("house", EN), ("maison", FR), ("casa", FR), ("hotel", EN), ("hotel", FR))
        self.assertEqual(self.resultado(), ["casa", "hotel"])

    def test_palabras_de_un_solo_idioma_no_se_devuelven(self):
        self.enviar(("only", EN), ("only", EN), ("seulement", FR))
        self.assertEqual(self.resultado(), [])

    def test_interseccion_entre_lotes_distintos_y_sin_duplicados(self):
        self.enviar(("hotel", EN), ("hotel", EN))
        self.enviar(("hotel", FR), ("hotel", FR))
        self.enviar(("hotel", EN), ("hotel", FR))
        self.assertEqual(self.resultado(), ["hotel"])

    def test_cierra_la_salida_hacia_el_gateway(self):
        self.enviar(("a", EN), ("a", FR))
        self.worker.al_finalizar(self.entrada)
        self.assertEqual(self.worker.middleware.finalizadas,
                         [("gateway_resultados", Entrada("ej1", ENTRADA_RESULTADOS))])


if __name__ == "__main__":
    unittest.main()
