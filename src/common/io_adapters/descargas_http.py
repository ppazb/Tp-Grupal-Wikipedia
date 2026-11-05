"""Ritmo de descargas y reintentos de errores temporales de un servidor HTTP."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
import math
import os
from threading import Event
import time

import requests

LOG = logging.getLogger(__name__)
ESTADOS_TEMPORALES = {408, 425, 429, 500, 502, 503, 504}
ESTADOS_AUSENTES = {404, 410}
# Política de descarga única: la usan el worker de Q2 y la referencia serial, para que
# reintenten y esperen igual. Cada valor se puede cambiar con su variable IMAGE_*.
INTERVALO_SEGUNDOS = 0.2        # IMAGE_INTERVAL_SECONDS: pausa mínima entre pedidos (por réplica)
REINTENTO_SEGUNDOS = 10.0       # IMAGE_RETRY_SECONDS: primera espera tras un fallo temporal
MAX_RETROCESO_SEGUNDOS = 30.0   # IMAGE_MAX_BACKOFF_SECONDS: tope de la espera creciente
BYTES_POR_SEGUNDO = 2_500_000   # IMAGE_BYTES_PER_SECOND: total entre réplicas (Wikimedia pide <= 25 Mbps)
MAX_REINTENTOS = 3              # IMAGE_MAX_RETRIES: reintentos después del primer intento
TIMEOUT_SEGUNDOS = 15           # timeout de conexión y lectura de cada pedido
MAX_REPLICAS_DESCARGA = 2


class DescargaAgotada(Exception):
    """Se agotaron los reintentos permitidos de una descarga."""


def maximo_reintentos():
    """Lee IMAGE_MAX_RETRIES; sin definir, usa MAX_REINTENTOS."""
    valor = os.environ.get('IMAGE_MAX_RETRIES')
    if valor is None:
        return MAX_REINTENTOS
    reintentos = int(valor)
    if reintentos < 0:
        raise ValueError('IMAGE_MAX_RETRIES no puede ser negativo')
    return reintentos


def positivo(nombre, defecto):
    """Lee una duración o tasa positiva y finita del entorno."""
    valor = float(os.environ.get(nombre, defecto))
    if not math.isfinite(valor) or valor <= 0:
        raise ValueError(f'{nombre} debe ser positivo y finito')
    return valor


def demora_servidor(valor):
    """Interpreta Retry-After como segundos o fecha HTTP, sin acortar el plazo."""
    if not valor:
        return 0.0
    try:
        segundos = float(valor)
        if math.isfinite(segundos):
            return max(0.0, segundos)
    except ValueError:
        try:
            fecha = parsedate_to_datetime(valor)
            return max(0.0, (fecha - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    LOG.warning('Retry-After inválido: %r; se aplica el retroceso local', valor)
    return 0.0


class DescargadorHTTP:
    """Procesa una descarga por vez y retiene el trabajo durante los rechazos temporales.

    La operación recibe una respuesta abierta y debe consumirla antes de retornar.
    La espera inyectada permite interrumpir el ritmo y los reintentos por SIGTERM.
    """

    def __init__(self, esperar=None, comprobar=None, replicas=1):
        if type(replicas) is not int or not 1 <= replicas <= MAX_REPLICAS_DESCARGA:
            raise ValueError('Las descargas directas de Wikimedia admiten entre 1 y 2 réplicas concurrentes')
        self._esperar = esperar if esperar is not None else Event().wait
        self._comprobar = comprobar if comprobar is not None else lambda: None
        self._intervalo = positivo('IMAGE_INTERVAL_SECONDS', INTERVALO_SEGUNDOS) * replicas
        self._reintento = positivo('IMAGE_RETRY_SECONDS', REINTENTO_SEGUNDOS)
        self._max_retroceso = max(self._reintento, positivo('IMAGE_MAX_BACKOFF_SECONDS', MAX_RETROCESO_SEGUNDOS))
        self._tasa = positivo('IMAGE_BYTES_PER_SECOND', BYTES_POR_SEGUNDO) / replicas
        self._max_reintentos = maximo_reintentos()
        self._proxima = 0.0

    def esperar(self, segundos):
        """Espera sin sondeo y comprueba cancelación antes y después del bloqueo."""
        self._comprobar()
        if segundos > 0:
            self._esperar(segundos)
        self._comprobar()

    def bloques(self, respuesta, cantidad):
        """Entrega bloques limitando la velocidad media de lectura de la respuesta."""
        for bloque in respuesta.iter_content(cantidad):
            self.esperar(len(bloque) / self._tasa)
            yield bloque

    def descargar(self, sesion, url, timeout, consumir):
        """Reintenta 429, fallos transitorios HTTP y cortes de conexión; tras el tope lanza DescargaAgotada."""
        retroceso = self._reintento
        intento = 0
        while True:
            self.esperar(max(0.0, self._proxima - time.monotonic()))
            intento += 1
            demora = 0.0
            # El intervalo se cuenta desde el inicio del pedido, no desde el fin de la descarga,
            # así la lectura del cuerpo no suma tiempo extra entre pedidos.
            self._proxima = time.monotonic() + self._intervalo
            try:
                with sesion.get(url, stream=True, timeout=timeout) as respuesta:
                    self._comprobar()
                    if respuesta.status_code in ESTADOS_TEMPORALES:
                        demora = demora_servidor(respuesta.headers.get('Retry-After'))
                    respuesta.raise_for_status()
                    return consumir(respuesta)
            except requests.HTTPError as error:
                estado = error.response.status_code if error.response is not None else None
                if estado not in ESTADOS_TEMPORALES:
                    raise
                LOG.warning('HTTP %s en %s (intento %d)', estado, url, intento)
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as error:
                LOG.warning('Descarga interrumpida de %s (intento %d): %s', url, intento, error)
            if intento > self._max_reintentos:
                raise DescargaAgotada(f'{url}: {self._max_reintentos} reintentos agotados')
            demora = max(demora, retroceso, self._intervalo)
            LOG.warning('Se conserva la imagen pendiente; reintento en %.1f segundos: %s', demora, url)
            self.esperar(demora)
            retroceso = min(retroceso * 2, self._max_retroceso)
