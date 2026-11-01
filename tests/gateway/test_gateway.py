"""Pruebas básicas del gateway: sockets reales, publicador falso (no necesitan RabbitMQ)."""

import os
import re
import socket
import threading
import time
import unittest
from pathlib import Path

CONFIG = Path(__file__).parents[2] / "config" / "config.json"
COMPOSE = Path(__file__).parents[2] / "docker-compose.yml"


def _entorno_del_gateway_en_el_compose():
    """Variables `- CLAVE=valor` del servicio gateway del docker-compose."""
    bloque = re.search(r"^  gateway:\n(.*?)(?=^  \w)", COMPOSE.read_text(), re.S | re.M).group(1)
    return dict(re.findall(r"^\s+- (\w+)=\s*(\S+)", bloque, re.M))


# El gateway lee su configuración del entorno al importarse: se usa la misma que define el compose
for _clave, _valor in _entorno_del_gateway_en_el_compose().items():
    os.environ.setdefault(_clave, _valor)

from src.common.external_protocol import (
    CAMPOS_FILA, TipoFrame, codificar_filas, decodificar_resultado, enviar_frame, recibir_frame,
)
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Idioma, Lote, ResultadoQ1
from src.gateway.src import gateway
from src.gateway.src.articulos import articulo_desde_fila
from src.gateway.src.gateway import ServidorGateway
from src.gateway.src.resultados import ReceptorResultados, RegistroSesiones
from src.middleware.cargador_configuracion import cargar_configuracion

MAX_BYTES = 65536


def fila(**cambios):
    datos = {
        "abstract": "resumen", "date_modified": "2019-05-01T10:00:00", "image_content_url": "",
        "in_language": "en", "name": "Articulo", "sections": ("Abstract",), "url": "http://wiki/a",
    }
    datos.update(cambios)
    assert set(datos) == set(CAMPOS_FILA)
    return datos


class PublicadorFalso:
    """
    Guarda lo que el gateway publicaría en el middleware.

    Al cerrar una ejecución simula un pipeline instantáneo: entrega los resultados
    configurados y da la ejecución por completa.
    """

    def __init__(self, sesiones, resultados=()):
        self.lotes = []      # (id_ejecucion, lote)
        self.cerradas = []   # id_ejecucion
        self._sesiones = sesiones
        self._resultados = resultados  # lotes que se entregan al cliente antes de completar

    def publicar(self, id_ejecucion, lote):
        self.lotes.append((id_ejecucion, lote))

    def finalizar(self, id_ejecucion):
        self.cerradas.append(id_ejecucion)
        sesion = self._sesiones.obtener(id_ejecucion)
        if sesion is None:
            return
        for lote in self._resultados:
            nombre = "resultados_finales_q2" if lote.esquema is Esquema.FRAGMENTO_IMAGEN else "resultados_finales_q1"
            ReceptorFalso(self._sesiones).procesar(Datos(Entrada(id_ejecucion, nombre), lote))
        sesion.completa.set()

    def interrumpir(self):
        pass


class ReceptorFalso(ReceptorResultados):
    """El receptor real sin middleware: solo se usa procesar()."""

    def __init__(self, sesiones):
        super().__init__(None, sesiones, MAX_BYTES)


class TestArticulos(unittest.TestCase):

    def test_fila_valida_se_convierte_en_articulo(self):
        articulo = articulo_desde_fila(fila(in_language="fr"), 0)
        self.assertEqual(articulo.idioma, Idioma.FRANCES)
        self.assertEqual(articulo.url, "http://wiki/a")

    def test_idioma_o_campos_requeridos_invalidos_se_rechazan(self):
        for cambio in ({"in_language": "es"}, {"url": ""}, {"name": " "}, {"date_modified": ""}):
            with self.assertRaises(ValueError, msg=str(cambio)):
                articulo_desde_fila(fila(**cambio), 0)


class TestConfiguracion(unittest.TestCase):

    def test_el_config_tiene_una_ruta_de_articulos_desde_el_gateway(self):
        rutas = gateway._rutas_articulos(cargar_configuracion(CONFIG))
        self.assertEqual([(r.origen, r.destino) for r in rutas], [(gateway.GATEWAY_GROUP, "date_router")])
        self.assertIn(Esquema.ARTICULO, rutas[0].esquemas)


class TestServidor(unittest.TestCase):

    def setUp(self):
        self.recibidos = []  # (entrada, lote) de los RESULTADO vistos por respuesta()
        self.sesiones = RegistroSesiones()
        self.publicador = PublicadorFalso(self.sesiones, self.resultados())
        self.detener = threading.Event()
        self.servidor = ServidorGateway(
            "127.0.0.1", 0, MAX_BYTES, (), self.publicador, self.detener, 1, self.sesiones,
        )
        self.hilo = threading.Thread(target=self.servidor.ejecutar, daemon=True)
        self.hilo.start()
        self.clientes = []

    def resultados(self):
        return ()

    def tearDown(self):
        for cliente in self.clientes:
            cliente.close()
        self.detener.set()
        self.hilo.join(5)

    def conectar(self):
        limite = time.monotonic() + 3
        while True:
            try:
                direccion = self.servidor.direccion
                cliente = socket.create_connection(direccion, timeout=3)
                break
            except OSError:
                if time.monotonic() > limite:
                    raise
                time.sleep(0.02)
        self.clientes.append(cliente)
        return cliente

    def enviar_lote(self, cliente, filas):
        enviar_frame(cliente, TipoFrame.DATOS, codificar_filas(filas, MAX_BYTES), MAX_BYTES)

    def enviar_eof(self, cliente, cantidad):
        enviar_frame(cliente, TipoFrame.EOF, cantidad.to_bytes(8, "big"), MAX_BYTES)

    def respuesta(self, cliente):
        """Próxima respuesta que no sea un resultado: estos pueden llegar entre los ACK."""
        while True:
            tipo, cuerpo = recibir_frame(cliente, MAX_BYTES)
            if tipo is not TipoFrame.RESULTADO:
                break
            self.recibidos.append(decodificar_resultado(cuerpo))
        return tipo, (int.from_bytes(cuerpo, "big") if tipo is TipoFrame.ACK else cuerpo.decode())

    def test_lotes_y_eof_se_confirman_y_se_publican(self):
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila(), fila(name="B")])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.enviar_lote(cliente, [fila()])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 2))
        self.enviar_eof(cliente, 2)
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 2))

        self.assertEqual([len(lote.registros) for _, lote in self.publicador.lotes], [2, 1])
        self.assertEqual({id_ for id_, _ in self.publicador.lotes}, set(self.publicador.cerradas))
        self.assertEqual(len(self.publicador.cerradas), 1)

    def test_fila_invalida_se_descarta_y_el_resto_se_publica(self):
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila(), fila(in_language="es"), fila(url="")])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.assertEqual(len(self.publicador.lotes[0][1].registros), 1)

    def test_eof_con_cantidad_incorrecta_devuelve_error_y_no_cierra(self):
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 5)
        self.assertEqual(self.respuesta(cliente)[0], TipoFrame.ERROR)
        self.assertEqual(self.publicador.cerradas, [])

    def test_frame_no_admitido_devuelve_error(self):
        cliente = self.conectar()
        enviar_frame(cliente, TipoFrame.ACK, b"", MAX_BYTES)
        self.assertEqual(self.respuesta(cliente)[0], TipoFrame.ERROR)

    def test_cliente_que_se_cae_sin_eof_cierra_la_entrada(self):
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila()])
        self.respuesta(cliente)
        cliente.close()
        limite = time.monotonic() + 3
        while not self.publicador.cerradas and time.monotonic() < limite:
            time.sleep(0.02)
        self.assertEqual(len(self.publicador.cerradas), 1)

    def test_cada_conexion_tiene_su_propia_ejecucion(self):
        for _ in range(2):
            cliente = self.conectar()
            self.enviar_lote(cliente, [fila()])
            self.respuesta(cliente)
            self.enviar_eof(cliente, 1)
            self.respuesta(cliente)
        self.assertEqual(len(set(self.publicador.cerradas)), 2)


class TestResultados(TestServidor):
    """Los resultados vuelven, en lotes, por la conexión de la ejecución que los originó."""

    LOTES = (
        Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("http://wiki/a", "2019"), ResultadoQ1("http://wiki/b", "2018"))),
        Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("http://wiki/c", "2017"),)),
    )

    def resultados(self):
        return self.LOTES

    def test_resultados_se_reenvian_por_lote_y_cierran_con_fin(self):
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))

        while True:
            tipo, cuerpo = recibir_frame(cliente, MAX_BYTES)
            if tipo is TipoFrame.FIN:
                break
            self.assertIs(tipo, TipoFrame.RESULTADO)
            self.recibidos.append(decodificar_resultado(cuerpo))
        self.assertEqual(self.recibidos, [("resultados_finales_q1", lote) for lote in self.LOTES])

    def test_cada_cliente_recibe_solo_los_suyos(self):
        sesion_ajena = type("S", (), {
            "enviar": lambda *a: self.fail("resultado a otro cliente"), "marcar_rota": lambda *a: None,
        })()
        self.sesiones.registrar("otra-ejecucion", sesion_ajena)
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.respuesta(cliente)
        recibir_frame(cliente, MAX_BYTES)  # FIN: los resultados ya los vio respuesta()
        self.assertEqual(len(self.recibidos), len(self.LOTES))

    def test_cliente_con_resultados_pendientes_no_se_cierra_antes_del_fin(self):
        self.publicador.finalizar = lambda id_ejecucion: self.publicador.cerradas.append(id_ejecucion)
        cliente = self.conectar()
        self.enviar_lote(cliente, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.respuesta(cliente)
        cliente.settimeout(0.3)
        with self.assertRaises(InterruptedError):  # sin datos: vence el timeout y se consulta interrumpido
            recibir_frame(cliente, MAX_BYTES, interrumpido=lambda: True)


if __name__ == "__main__":
    unittest.main()
