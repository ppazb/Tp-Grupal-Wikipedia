"""Escenarios de referencia intacta, comparación por cliente e imágenes diferentes."""

import hashlib
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from comparar_serial import main
from serial.comparacion import comparar, descargar_imagen, exportar
from serial.notebook import ejecutar_notebook

RAIZ = Path(__file__).resolve().parents[2]


class ImagenLocal(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/imagen':
            self.send_error(404)
            return
        contenido = b'imagen de referencia\x00\xff'
        self.send_response(200)
        self.send_header('Content-Length', str(len(contenido)))
        self.end_headers()
        self.wfile.write(contenido)

    def log_message(self, *_):
        pass


class ComparacionSerial(unittest.TestCase):
    def test_notebook_intacto_con_parquets_locales_y_varios_clientes(self):
        """Ejecuta las cinco consultas originales y detecta aportes e imágenes alterados."""
        servidor = ThreadingHTTPServer(('127.0.0.1', 0), ImagenLocal)
        hilo = threading.Thread(target=servidor.serve_forever)
        hilo.start()
        try:
            with tempfile.TemporaryDirectory() as temporal:
                raiz = Path(temporal)
                entrada = raiz / 'entrada'
                entrada.mkdir()
                url_imagen = f'http://127.0.0.1:{servidor.server_port}/imagen'
                filas = [
                    ('en', '2019-01-01', 'zebra zebra orchid', 'uno', url_imagen),
                    ('fr', '2019-01-01', 'zebra orchid', 'dos', None),
                    ('en', '2022-01-01', 'zebra orchid violet tulip', 'tres', None),
                ]
                for numero, (idioma, fecha, abstract, url, imagen) in enumerate(filas):
                    pd.DataFrame([dict(abstract=abstract, date_modified=pd.Timestamp(fecha),
                        in_language={'identifier': idioma}, image={'content_url': imagen},
                        name=url, url=url, sections=json.dumps([{'name': 'Biography'}]))]).to_parquet(
                            entrada / f'{numero}.parquet')
                notebook = RAIZ / 'version-secuencial.ipynb'
                antes = hashlib.sha256(notebook.read_bytes()).hexdigest()
                resultados, archivos = ejecutar_notebook(notebook, entrada)
                self.assertEqual(len(archivos), 3)
                self.assertEqual(antes, hashlib.sha256(notebook.read_bytes()).hexdigest())
                self.assertEqual(dict(resultados['q4'].itertuples(index=False, name=None)),
                                 {'zebra': 3, 'orchid': 3, 'violet': 1, 'tulip': 1})
                self.assertEqual(list(resultados['q5'].itertuples(index=False, name=None)), [('tres', 4)])
                self.assertEqual(set(resultados['q3']), {'zebra', 'orchid'})
                referencia = raiz / 'referencia'
                with requests.Session() as sesion:
                    exportar(resultados, referencia, sesion, 5)
                import shutil
                clientes = [raiz / 'client_0', raiz / 'client_1']
                for cliente in clientes:
                    shutil.copytree(referencia, cliente)
                self.assertTrue(comparar(referencia, clientes, raiz / 'diferencias.csv'))
                argumentos = ['comparar_serial', '--entrada', str(entrada), '--salidas', str(raiz),
                              '--notebook', str(notebook), '--registros', str(raiz / 'registros')]
                with patch.object(sys, 'argv', argumentos):
                    self.assertEqual(main(), 0)
                # Una fila duplicada debe detectarse aunque el conjunto de valores coincida.
                with (clientes[0] / 'q4.csv').open('a') as archivo:
                    archivo.write('zebra,3\n')
                (clientes[1] / 'imagenes/0.img').write_bytes(b'otra imagen')
                self.assertFalse(comparar(referencia, clientes, raiz / 'diferencias.csv'))
                diferencias = (raiz / 'diferencias.csv').read_text()
                self.assertIn('client_0,q4,sobrante', diferencias)
                self.assertIn('client_1,q2,faltante', diferencias)
                with patch.object(sys, 'argv', argumentos):
                    self.assertEqual(main(), 1)
                with patch.object(sys, 'argv', argumentos + ['--notebook', str(raiz / 'ausente.ipynb')]):
                    self.assertEqual(main(), 2)
                with requests.Session() as sesion:
                    resultados['q2'].iloc[0, 2] = f'http://127.0.0.1:{servidor.server_port}/error'
                    parcial = raiz / 'referencia_con_descarte'
                    self.assertEqual(exportar(resultados, parcial, sesion, 5), 1)
                    self.assertTrue((parcial / 'q5.csv').exists())
                    self.assertFalse((parcial / 'imagenes/0.img').exists())
                    self.assertIn('404', (parcial / 'descargas_fallidas_q2.csv').read_text())
                    self.assertNotIn('uno', (parcial / 'q2.csv').read_text())
                    # Un cliente que sí recibió la imagen debe seguir mostrando una diferencia.
                    self.assertFalse(comparar(parcial, [clientes[1]], raiz / 'diferencias.csv'))
                    self.assertIn('q2,sobrante', (raiz / 'diferencias.csv').read_text())
                    import shutil
                    sin_imagen = raiz / 'client_sin_imagen'
                    shutil.copytree(parcial, sin_imagen)
                    self.assertTrue(comparar(parcial, [sin_imagen], raiz / 'diferencias.csv'))
        finally:
            servidor.shutdown()
            servidor.server_close()
            hilo.join()

    def test_consultas_vacias_y_rechazos_no_se_ocultan(self):
        with tempfile.TemporaryDirectory() as temporal:
            raiz = Path(temporal)
            referencia, cliente = raiz / 'referencia', raiz / 'client_0'
            referencia.mkdir()
            cliente.mkdir()
            self.assertTrue(comparar(referencia, [cliente], raiz / 'diferencias.csv'))
            (cliente / 'errores.csv').write_text('detalle\nregistro inválido\n')
            self.assertFalse(comparar(referencia, [cliente], raiz / 'diferencias.csv'))
            (cliente / 'q1.csv').write_text('columnas,incorrectas\n')
            with self.assertRaises(ValueError):
                comparar(referencia, [cliente], raiz / 'diferencias.csv')

    def test_descarga_interrumpida_reintenta_sin_conservar_bytes_parciales(self):
        def fragmentos_interrumpidos(_):
            yield b'parcial'
            raise requests.ConnectionError('conexión cortada durante la imagen')

        respuesta_fallida = MagicMock()
        respuesta_fallida.__enter__.return_value = respuesta_fallida
        respuesta_fallida.iter_content.side_effect = fragmentos_interrumpidos
        respuesta_correcta = MagicMock()
        respuesta_correcta.__enter__.return_value = respuesta_correcta
        respuesta_correcta.iter_content.return_value = [b'imagen completa']
        sesion = MagicMock()
        with tempfile.TemporaryDirectory() as temporal:
            destino = Path(temporal) / 'imagen'
            sesion.get.side_effect = [respuesta_fallida, respuesta_correcta]
            descargar_imagen('http://local/imagen', destino, sesion, 5)
            self.assertEqual(destino.read_bytes(), b'imagen completa')
            self.assertEqual(sesion.get.call_count, 2)
            sesion.get.side_effect = [respuesta_fallida, respuesta_fallida]
            with self.assertRaises(requests.ConnectionError):
                descargar_imagen('http://local/imagen', destino, sesion, 5)
            self.assertFalse(destino.exists())
            # Un fallo local de disco no es un descarte por indisponibilidad de Wikimedia.
            sesion.get.side_effect = None
            sesion.get.return_value = respuesta_correcta
            with patch.object(Path, 'open', side_effect=OSError('sin espacio')):
                with self.assertRaises(OSError):
                    descargar_imagen('http://local/imagen', destino, sesion, 5)
