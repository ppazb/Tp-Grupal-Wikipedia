"""Servidor HTTP local y observación de recursos para escenarios del descargador."""

from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import requests

from src.common.runtime.apagado import Apagado
from tests.soporte import PLAZO_ESCENARIO, PLAZO_PROCESO


IMAGEN = b'imagen de prueba' * 3000


def servir_imagen(configuracion, eventos, modo, liberar):
    """Atiende una petición real, pudiendo retener cabeceras o cuerpo hasta el cierre."""
    class Manejador(BaseHTTPRequestHandler):
        def do_GET(self):
            eventos.put('peticion')
            if modo == 'cabeceras':
                if not liberar.wait(PLAZO_ESCENARIO):
                    raise AssertionError('No se liberó el servidor HTTP de pruebas')
                return
            self.send_response(503 if modo == 'reintento' else 200)
            self.send_header('Content-Length', str(len(IMAGEN) if modo != 'reintento' else 0))
            self.end_headers()
            if modo == 'cuerpo':
                if not liberar.wait(PLAZO_ESCENARIO):
                    raise AssertionError('No se liberó el cuerpo HTTP de pruebas')
            elif modo != 'reintento':
                self.wfile.write(IMAGEN)

        def log_message(self, formato, *argumentos):
            pass

    with HTTPServer(('127.0.0.1', 0), Manejador) as servidor:
        servidor.timeout = PLAZO_PROCESO
        eventos.put(servidor.server_address[1])
        servidor.handle_request()


class ObservadorHTTP:
    """Comprueba cierres reales de sesión y respuestas, y observa la espera de reintento."""

    def __init__(self, modo, eventos):
        self.modo = modo
        self.eventos = eventos
        self.sesiones_cerradas = []
        self.respuestas = []
        self.intentos = 0

    def instalar(self, recursos):
        obtener = requests.Session.get
        cerrar = requests.Session.close
        esperar = Apagado.esperar

        def obtener_observado(sesion, *argumentos, **opciones):
            self.intentos += 1
            respuesta = obtener(sesion, *argumentos, **opciones)
            self.respuestas.append(respuesta)
            if self.modo == 'http_cuerpo':
                leer = respuesta.raw.read

                def leer_observado(*argumentos, **opciones):
                    self.eventos.put('leyendo')
                    return leer(*argumentos, **opciones)

                recursos.enter_context(patch.object(respuesta.raw, 'read', leer_observado))
            return respuesta

        def cerrar_observado(sesion):
            cerrar(sesion)
            self.sesiones_cerradas.append(sesion)

        def esperar_observado(apagado, segundos):
            self.eventos.put('reintentando')
            # Una espera larga demuestra que el descriptor la despierta antes de vencer.
            return esperar(apagado, PLAZO_ESCENARIO)

        recursos.enter_context(patch.object(requests.Session, 'get', obtener_observado))
        recursos.enter_context(patch.object(requests.Session, 'close', cerrar_observado))
        if self.modo == 'http_reintento':
            recursos.enter_context(patch.object(Apagado, 'esperar', esperar_observado))

    def comprobar(self, worker):
        assert self.sesiones_cerradas == [worker._sesion], 'No se cerró la sesión HTTP una sola vez'
        assert all(respuesta.raw.closed for respuesta in self.respuestas), 'Quedó una respuesta HTTP abierta'
        if self.modo.startswith('http_') or self.modo in ('descarga', 'fragmentos'):
            assert self.intentos == 1, 'Se reintentó una descarga después de SIGTERM'
