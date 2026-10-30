"""Tests simples del manejador de mensajes del cliente (no necesitan RabbitMQ ni red)."""

import unittest

from src.client.src.message_handler import LoteDemasiadoGrande, ManejadorMensajes, fila_plana
from src.common.external_protocol import decodificar_filas


def una_fila(**cambios):
    fila = {
        "abstract": "Un resumen",
        "date_modified": None,
        "image": {"content_url": "http://img/x.png"},
        "in_language": {"identifier": "en"},
        "name": "Articulo",
        "sections": '[{"name": "Abstract"}, {"name": "History"}]',
        "url": "http://wiki/Articulo",
    }
    fila.update(cambios)
    return fila


class TestMessageHandler(unittest.TestCase):

    def test_sections_en_json_devuelve_los_nombres(self):
        self.assertEqual(fila_plana(una_fila())["sections"], ("Abstract", "History"))

    def test_sections_nulo_o_json_invalido_queda_vacio(self):
        self.assertEqual(fila_plana(una_fila(sections=None))["sections"], ())
        self.assertEqual(fila_plana(una_fila(sections="esto no es json"))["sections"], ())

    def test_fila_plana_aplana_structs_y_nulos(self):
        plana = fila_plana(una_fila())
        self.assertEqual(plana["image_content_url"], "http://img/x.png")
        self.assertEqual(plana["in_language"], "en")
        self.assertEqual(plana["date_modified"], "") # None viaja como texto vacío

    def test_lote_serializado_se_puede_decodificar(self):
        cuerpo = ManejadorMensajes(65536).serializar_lote([una_fila(name="A"), una_fila(name="B")])
        filas = decodificar_filas(cuerpo)
        self.assertEqual([f["name"] for f in filas], ["A", "B"])
        self.assertEqual(filas[0]["sections"], ("Abstract", "History"))

    def test_lote_que_no_entra_levanta_lote_demasiado_grande(self):
        with self.assertRaises(LoteDemasiadoGrande):
            ManejadorMensajes(50).serializar_lote([una_fila()])

    def test_eof_son_8_bytes_con_la_cantidad_de_lotes(self):
        manejador = ManejadorMensajes(65536)
        manejador.lote_confirmado()
        manejador.lote_confirmado()
        self.assertEqual(manejador.serializar_eof(), (2).to_bytes(8, "big"))

    def test_validar_ack(self):
        manejador = ManejadorMensajes(65536)
        self.assertTrue(manejador.validar_ack((3).to_bytes(8, "big"), 3))
        self.assertFalse(manejador.validar_ack((4).to_bytes(8, "big"), 3)) # cantidad distinta
        self.assertFalse(manejador.validar_ack(b"\x03", 3))  # largo incorrecto


if __name__ == "__main__":
    unittest.main()
