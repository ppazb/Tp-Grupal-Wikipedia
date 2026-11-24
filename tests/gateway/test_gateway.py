"""Pruebas básicas del gateway: sockets reales, publicador falso (no necesitan RabbitMQ)."""

import os
import re
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

CONFIG = Path(__file__).parents[2] / "config" / "config.json"
COMPOSE = Path(__file__).parents[2] / "config" / "compose.plantilla.yml"


def _entorno_del_gateway_en_el_compose():
    """Variables `- CLAVE=valor` del servicio gateway del docker-compose."""
    bloque = re.search(r"^  gateway:\n(.*?)(?=^  \w)", COMPOSE.read_text(), re.S | re.M).group(1)
    return dict(re.findall(r"^\s+- (\w+)=\s*(\S+)", bloque, re.M))


from src.common.external_protocol import (
    CAMPOS_FILA, TipoFrame, codificar_datos, codificar_reanudar, decodificar_reanudado,
    decodificar_resultado, decodificar_sesion, enviar_frame, recibir_frame,
)
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Idioma, Lote, ResultadoQ1
# El módulo captura su configuración al importarse, sin contaminar los otros escenarios.
with patch.dict(os.environ, _entorno_del_gateway_en_el_compose()):
    from src.gateway.src import gateway
    from src.gateway.src.articulos import articulo_desde_fila
    from src.gateway.src.ejecuciones import RegistroEjecuciones
    from src.gateway.src.gateway import ServidorGateway
    from src.gateway.src.resultados import ReceptorResultados
from src.middleware.cargador_configuracion import cargar_configuracion

MAX_BYTES = 65536
ENTRADA_RESULTADOS = "resultados_finales_q1"


def fila(**cambios):
    datos = {
        "abstract": "resumen", "date_modified": "2019-05-01T10:00:00", "image_content_url": "",
        "in_language": "en", "name": "Articulo", "sections": ("Abstract",), "url": "http://wiki/a",
    }
    datos.update(cambios)
    assert set(datos) == set(CAMPOS_FILA)
    return datos


class PublicadorFalso:
    """Guarda lo que el gateway publicaría en el middleware."""

    def __init__(self):
        self.lotes = []      # (id_ejecucion, numero_lote, indice, lote)
        self.cerradas = []   # id_ejecucion

    def publicar(self, id_ejecucion, numero_lote, indice, lote):
        self.lotes.append((id_ejecucion, numero_lote, indice, lote))

    def finalizar(self, id_ejecucion):
        self.cerradas.append(id_ejecucion)

    def interrumpir(self):
        pass

    def cerrar_cliente(self):
        pass


class MiddlewareFalso:
    """Lo mínimo del middleware que usa el receptor: la entrega que se está procesando."""

    def __init__(self):
        self.entrega_actual = None

    def entregar(self, id_entrada, indice):
        self.entrega_actual = SimpleNamespace(id_entrada=id_entrada, indice=indice)


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

    def test_el_compose_da_un_volumen_de_wal_al_gateway(self):
        entorno = _entorno_del_gateway_en_el_compose()
        self.assertEqual(entorno["WAL_DIR"], "/app/data/wal")
        self.assertIn("wal_gateway:/app/data/wal", COMPOSE.read_text())


class GatewayEnPruebas(unittest.TestCase):
    """Sockets reales, publicador falso y WAL en un directorio temporal: no necesitan RabbitMQ."""

    ESPERA_REANUDACION = 60

    def setUp(self):
        self.temporal = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporal.cleanup)
        self.clientes = []
        self.servidores = []
        self.publicador = PublicadorFalso()
        self.levantar()

    def levantar(self):
        """Arranca un gateway sobre el mismo WAL: el anterior, si lo hay, 'se cayó' sin abandonar nada."""
        self.registro = RegistroEjecuciones(self.temporal.name, {ENTRADA_RESULTADOS})
        self.detener = threading.Event()
        self.servidor = ServidorGateway(
            "127.0.0.1", 0, MAX_BYTES, (), self.publicador, self.detener, 1, self.registro, self.ESPERA_REANUDACION,
        )
        self.hilo = threading.Thread(target=self.servidor.ejecutar, daemon=True)
        self.hilo.start()
        self.servidores.append((self.detener, self.hilo, self.registro))
        self.receptor = ReceptorResultados(MiddlewareFalso(), self.registro, MAX_BYTES)

    def caer(self):
        """Detiene el gateway como un apagado: el estado queda en disco."""
        self.detener.set()
        self.hilo.join(5)
        self.assertFalse(self.hilo.is_alive())
        self.registro.cerrar()

    def tearDown(self):
        for cliente in self.clientes:
            cliente.close()
        for detener, hilo, registro in self.servidores:
            detener.set()
            hilo.join(5)
        self.registro.cerrar()

    def conectar(self):
        limite = time.monotonic() + 3
        while True:
            try:
                cliente = socket.create_connection(self.servidor.direccion, timeout=3)
                break
            except OSError:
                if time.monotonic() > limite:
                    raise
                time.sleep(0.02)
        self.clientes.append(cliente)
        return cliente

    def iniciar(self, cliente):
        enviar_frame(cliente, TipoFrame.INICIO, b"", MAX_BYTES)
        tipo, cuerpo = recibir_frame(cliente, MAX_BYTES)
        self.assertIs(tipo, TipoFrame.SESION)
        return decodificar_sesion(cuerpo)

    def reanudar(self, cliente, id_ejecucion, ultima_secuencia=0):
        enviar_frame(cliente, TipoFrame.REANUDAR, codificar_reanudar(id_ejecucion, ultima_secuencia, MAX_BYTES), MAX_BYTES)
        return recibir_frame(cliente, MAX_BYTES)

    def enviar_lote(self, cliente, numero, filas):
        enviar_frame(cliente, TipoFrame.DATOS, codificar_datos(numero, filas, MAX_BYTES), MAX_BYTES)

    def enviar_eof(self, cliente, cantidad):
        enviar_frame(cliente, TipoFrame.EOF, cantidad.to_bytes(8, "big"), MAX_BYTES)

    def respuesta(self, cliente):
        tipo, cuerpo = recibir_frame(cliente, MAX_BYTES)
        return tipo, (int.from_bytes(cuerpo, "big") if tipo is TipoFrame.ACK else cuerpo.decode())

    def entregar_resultado(self, id_ejecucion, lote, id_entrada, indice=0):
        self.receptor._middleware.entregar(id_entrada, indice)
        self.receptor.procesar(Datos(Entrada(id_ejecucion, ENTRADA_RESULTADOS), lote))

    def cerrar_entrada(self, id_ejecucion):
        self.receptor.finalizar_entrada(Entrada(id_ejecucion, ENTRADA_RESULTADOS))

    def esperar(self, condicion):
        limite = time.monotonic() + 3
        while not condicion() and time.monotonic() < limite:
            time.sleep(0.02)
        self.assertTrue(condicion())


class TestRecepcionDeLotes(GatewayEnPruebas):

    def test_lotes_y_eof_se_confirman_y_se_publican_con_ids_repetibles(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila(), fila(name="B")])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.enviar_lote(cliente, 2, [fila()])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 2))
        self.enviar_eof(cliente, 2)
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 2))

        self.assertEqual([(i, n, ind, len(lote.registros)) for i, n, ind, lote in self.publicador.lotes],
                         [(id_ejecucion, 1, 0, 2), (id_ejecucion, 2, 0, 1)])
        self.assertEqual(self.publicador.cerradas, [id_ejecucion])

    def test_lote_ya_procesado_se_confirma_sin_publicar_de_nuevo(self):
        cliente = self.conectar()
        self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.enviar_lote(cliente, 1, [fila()])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.assertEqual(len(self.publicador.lotes), 1)

    def test_lote_fuera_de_secuencia_devuelve_error(self):
        cliente = self.conectar()
        self.iniciar(cliente)
        self.enviar_lote(cliente, 3, [fila()])
        self.assertEqual(self.respuesta(cliente)[0], TipoFrame.ERROR)
        self.assertEqual(self.publicador.lotes, [])

    def test_fila_invalida_se_descarta_y_el_resto_se_publica(self):
        cliente = self.conectar()
        self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila(), fila(in_language="es"), fila(url="")])
        self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.assertEqual(len(self.publicador.lotes[0][3].registros), 1)

    def test_eof_con_cantidad_incorrecta_devuelve_error_y_el_cliente_se_da_por_perdido(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 5)
        self.assertEqual(self.respuesta(cliente)[0], TipoFrame.ERROR)
        # el EOF no se aceptó: la entrada se cierra recién al abandonar la ejecución
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)
        self.assertEqual(self.publicador.cerradas, [id_ejecucion])

    def test_eof_repetido_solo_se_confirma(self):
        cliente = self.conectar()
        self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        for _ in range(2):
            self.enviar_eof(cliente, 1)
            self.assertEqual(self.respuesta(cliente), (TipoFrame.ACK, 1))
        self.assertEqual(len(self.publicador.cerradas), 1)

    def test_primer_frame_que_no_es_inicio_ni_reanudar_devuelve_error(self):
        cliente = self.conectar()
        enviar_frame(cliente, TipoFrame.ACK, b"", MAX_BYTES)
        self.assertEqual(self.respuesta(cliente)[0], TipoFrame.ERROR)

    def test_cada_conexion_tiene_su_propia_ejecucion(self):
        ids = set()
        for _ in range(2):
            cliente = self.conectar()
            ids.add(self.iniciar(cliente))
            self.enviar_lote(cliente, 1, [fila()])
            self.respuesta(cliente)
            self.enviar_eof(cliente, 1)
            self.respuesta(cliente)
        self.assertEqual(len(ids), 2)
        self.assertEqual(set(self.publicador.cerradas), ids)

    def test_cliente_que_se_cae_sin_eof_cierra_la_entrada_y_abandona(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        cliente.close()
        self.esperar(lambda: self.publicador.cerradas)
        self.assertEqual(self.publicador.cerradas, [id_ejecucion])
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)

    def test_cliente_que_se_cae_antes_de_enviar_lotes_se_abandona_sin_cerrar_entrada(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        cliente.close()
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)
        self.assertEqual(self.publicador.cerradas, [])


class TestReanudacion(GatewayEnPruebas):

    def test_reanudar_informa_lotes_procesados_y_eof(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        for numero in (1, 2):
            self.enviar_lote(cliente, numero, [fila()])
            self.respuesta(cliente)
        self.caer()
        self.assertEqual(self.publicador.cerradas, [], "Un apagado no debe cerrar la entrada")

        self.levantar()
        nuevo = self.conectar()
        tipo, cuerpo = self.reanudar(nuevo, id_ejecucion)
        self.assertIs(tipo, TipoFrame.REANUDADO)
        self.assertEqual(decodificar_reanudado(cuerpo), (2, False))

    def test_tras_reanudar_un_lote_reenviado_no_se_republica_y_se_sigue_con_el_siguiente(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.caer()

        self.levantar()
        nuevo = self.conectar()
        self.reanudar(nuevo, id_ejecucion)
        self.enviar_lote(nuevo, 1, [fila()])  # el cliente no vio el ACK: reenvía
        self.assertEqual(self.respuesta(nuevo), (TipoFrame.ACK, 1))
        self.enviar_lote(nuevo, 2, [fila()])
        self.assertEqual(self.respuesta(nuevo), (TipoFrame.ACK, 2))
        self.assertEqual([(n, ind) for _, n, ind, _ in self.publicador.lotes], [(1, 0), (2, 0)])

    def test_eof_recibido_sobrevive_a_la_caida(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.respuesta(cliente)
        self.caer()

        self.levantar()
        nuevo = self.conectar()
        tipo, cuerpo = self.reanudar(nuevo, id_ejecucion)
        self.assertEqual(decodificar_reanudado(cuerpo), (1, True))
        self.enviar_eof(nuevo, 1)
        self.assertEqual(self.respuesta(nuevo), (TipoFrame.ACK, 1))
        self.assertEqual(len(self.publicador.cerradas), 1, "El EOF no debe publicarse dos veces")

    def test_reanudar_una_ejecucion_desconocida_devuelve_error(self):
        cliente = self.conectar()
        self.assertEqual(self.reanudar(cliente, "no-existe")[0], TipoFrame.ERROR)

    def test_cliente_que_reanuda_ya_no_se_abandona_al_vencer_la_espera(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.caer()
        self.levantar()
        nuevo = self.conectar()
        self.reanudar(nuevo, id_ejecucion)
        self.assertIsNotNone(self.registro.obtener(id_ejecucion))


class TestAbandonoPorFaltaDeReanudacion(GatewayEnPruebas):
    ESPERA_REANUDACION = 0.2

    def test_ejecucion_recuperada_sin_cliente_se_abandona_y_cierra_su_entrada(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.caer()
        self.assertEqual(self.publicador.cerradas, [])

        self.levantar()
        self.esperar(lambda: self.publicador.cerradas == [id_ejecucion])
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)
        nuevo = self.conectar()
        self.assertEqual(self.reanudar(nuevo, id_ejecucion)[0], TipoFrame.ERROR)


class TestResultados(GatewayEnPruebas):
    """Los resultados se guardan en disco y vuelven, numerados, por la conexión vigente de la ejecución."""

    LOTES = (
        Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("http://wiki/a", "2019"), ResultadoQ1("http://wiki/b", "2018"))),
        Lote(Esquema.RESULTADO_Q1, (ResultadoQ1("http://wiki/c", "2017"),)),
    )

    def leer_resultados(self, cliente, cantidad):
        recibidos = []
        while len(recibidos) < cantidad:
            tipo, cuerpo = recibir_frame(cliente, MAX_BYTES)
            self.assertIs(tipo, TipoFrame.RESULTADO)
            recibidos.append(decodificar_resultado(cuerpo))
        return recibidos

    def test_resultados_se_reenvian_numerados_y_cierran_con_fin(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.respuesta(cliente)
        for indice, lote in enumerate(self.LOTES):
            self.entregar_resultado(id_ejecucion, lote, f"salida-{indice}")
        self.cerrar_entrada(id_ejecucion)

        self.assertEqual(self.leer_resultados(cliente, 2),
                         [(1, ENTRADA_RESULTADOS, self.LOTES[0]), (2, ENTRADA_RESULTADOS, self.LOTES[1])])
        self.assertIs(recibir_frame(cliente, MAX_BYTES)[0], TipoFrame.FIN)
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)

    def test_resultado_duplicado_se_descarta(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.entregar_resultado(id_ejecucion, self.LOTES[0], "salida", 0)
        self.entregar_resultado(id_ejecucion, self.LOTES[0], "salida", 0)  # reproceso de un worker
        self.entregar_resultado(id_ejecucion, self.LOTES[1], "salida", 1)  # mismo lote, otro índice
        self.assertEqual(self.registro.obtener(id_ejecucion).cantidad_resultados, 2)

    def test_resultados_de_una_ejecucion_desconocida_se_descartan(self):
        self.entregar_resultado("no-existe", self.LOTES[0], "salida")  # no debe fallar

    def test_cada_cliente_recibe_solo_los_suyos(self):
        uno, otro = self.conectar(), self.conectar()
        id_uno, id_otro = self.iniciar(uno), self.iniciar(otro)
        self.entregar_resultado(id_uno, self.LOTES[0], "salida")
        self.assertEqual(self.leer_resultados(uno, 1), [(1, ENTRADA_RESULTADOS, self.LOTES[0])])
        otro.settimeout(0.3)
        with self.assertRaises(InterruptedError):  # sin datos: vence el timeout y se consulta interrumpido
            recibir_frame(otro, MAX_BYTES, interrumpido=lambda: True)
        self.assertEqual(self.registro.obtener(id_otro).cantidad_resultados, 0)

    def test_tras_la_caida_se_reenvia_desde_la_secuencia_siguiente_a_la_informada(self):
        cliente = self.conectar()
        id_ejecucion = self.iniciar(cliente)
        self.enviar_lote(cliente, 1, [fila()])
        self.respuesta(cliente)
        self.enviar_eof(cliente, 1)
        self.respuesta(cliente)
        self.entregar_resultado(id_ejecucion, self.LOTES[0], "salida", 0)
        self.assertEqual(len(self.leer_resultados(cliente, 1)), 1)
        self.caer()

        self.levantar()
        self.entregar_resultado(id_ejecucion, self.LOTES[1], "salida", 1)  # llega mientras el cliente reconecta
        self.cerrar_entrada(id_ejecucion)
        nuevo = self.conectar()
        tipo, cuerpo = self.reanudar(nuevo, id_ejecucion, ultima_secuencia=1)
        self.assertEqual(decodificar_reanudado(cuerpo), (1, True))
        self.assertEqual(self.leer_resultados(nuevo, 1), [(2, ENTRADA_RESULTADOS, self.LOTES[1])])
        self.assertIs(recibir_frame(nuevo, MAX_BYTES)[0], TipoFrame.FIN)

    def test_ejecucion_completa_al_recuperar_manda_todo_lo_pendiente_y_fin(self):
        id_ejecucion = self.registro.crear().id  # el cliente está caído: nada se envía hasta que reanude
        for indice, lote in enumerate(self.LOTES):
            self.entregar_resultado(id_ejecucion, lote, "salida", indice)
        self.cerrar_entrada(id_ejecucion)
        self.caer()

        self.levantar()
        self.assertTrue(self.registro.obtener(id_ejecucion).completa)
        nuevo = self.conectar()
        self.reanudar(nuevo, id_ejecucion, ultima_secuencia=0)
        self.assertEqual([secuencia for secuencia, _, _ in self.leer_resultados(nuevo, 2)], [1, 2])
        self.assertIs(recibir_frame(nuevo, MAX_BYTES)[0], TipoFrame.FIN)
        self.esperar(lambda: self.registro.obtener(id_ejecucion) is None)

    def test_entradas_cerradas_se_recuperan_y_completan_la_ejecucion(self):
        """Si el gateway cae tras persistir el cierre de todas las entradas, al volver la ejecución está completa."""
        id_ejecucion = self.registro.crear().id  # sin cliente conectado: no se envía FIN
        self.cerrar_entrada(id_ejecucion)
        self.caer()
        self.levantar()
        recuperada = self.registro.obtener(id_ejecucion)
        self.assertEqual(recuperada.entradas_cerradas, {ENTRADA_RESULTADOS})
        self.assertTrue(recuperada.completa)


if __name__ == "__main__":
    unittest.main()
