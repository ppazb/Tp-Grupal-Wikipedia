"""Arranques reales instrumentados para observar el apagado sin modificar la implementación."""

from contextlib import ExitStack
import importlib
import json
import logging
import multiprocessing
import os
from pathlib import Path
import signal
import sys
from tempfile import TemporaryDirectory
import threading
from unittest.mock import patch
from unittest import TestCase

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import (
    Articulo, Candidato, CantidadPalabras, ConteoPalabra, Esquema, Idioma, Lote,
    Palabra, ReferenciaHistorica, ResultadoQ1, TextoArticulo,
)
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.middleware.capacidad import CapacidadDatos
from src.workers.base_worker import Worker
from tests.soporte import PLAZO_PROCESO
from .computo import ObservadorComputo

MODULOS = {
    'date_router': 'date_router', 'q1_filter_lang': 'q1.filter_lang',
    'q1_formatter': 'q1.formatter', 'word_occurrences': 'word_occurrences',
    'frecuencias': 'q4.frequency', 'top_n': 'q4.top_n',
    'q5_average': 'q5.average', 'q5_comparator': 'q5.comparator',
}


def entrada_prueba(grupo):
    """Devuelve un lote válido y el grupo que lo publica en la topología del escenario."""
    opciones = {
        'date_router': ('gateway', 'articulos_crudos', Esquema.ARTICULO,
                        (Articulo('url', 'nombre', '2019', Idioma.INGLES, 'tree tree forêt', (), ''),)),
        'q1_filter_lang': ('date_router', 'historicos_q1', Esquema.REFERENCIA_HISTORICA,
                           (ReferenciaHistorica('url', '2019', Idioma.INGLES),)),
        'q1_formatter': ('q1_filter_lang', 'filtrados_ing_q1', Esquema.RESULTADO_Q1,
                         (ResultadoQ1('url', '2019'),)),
        'word_occurrences': ('date_router', 'textos_historicos', Esquema.TEXTO_ARTICULO,
                             (TextoArticulo('url', Idioma.INGLES, 'tree tree forêt'),)),
        'frecuencias': ('word_occurrences', 'palabras', Esquema.PALABRA, (Palabra('tree'),)),
        'top_n': ('frecuencias', 'conteos', Esquema.CONTEO_PALABRA,
                  tuple(ConteoPalabra(f'mot{n}', 2) for n in range(16))),
        'q5_average': ('word_occurrences', 'cantidades_historicas', Esquema.CANTIDAD_PALABRAS,
                       (CantidadPalabras(12),)),
        'q5_comparator': ('word_occurrences', 'candidatos', Esquema.CANDIDATO,
                          (Candidato('url', 12),)),
    }
    fuente, entrada, esquema, registros = opciones[grupo]
    return fuente, Datos(Entrada('cliente', entrada), Lote(esquema, registros))


def comprobar_recursos(worker, disco, anteriores):
    """Comprueba recursos antes de que termine el intérprete y su limpieza automática."""
    assert worker._cerrado
    conexion = worker._middleware._conexion
    assert conexion._conexion is None and conexion._canal is None
    assert not conexion._canales_adicionales
    apagado = conexion._apagado
    assert apagado._lectura is None and apagado._escritura is None
    assert all(signal.getsignal(numero) == anterior for numero, anterior in anteriores.items())
    assert not list(disco.iterdir()), 'Quedaron temporales propios del worker'
    for nombre in ('_acumuladores', '_resumenes'):
        assert not getattr(worker, nombre, {}), f'Quedó estado en {nombre}'
    if hasattr(worker, '_parciales'):
        assert not worker._parciales._presupuestos
        assert not worker._parciales._acumulador._frecuencias
    if hasattr(worker, '_consolidador'):
        assert not worker._consolidador._buffer
        assert not worker._consolidador._ejecuciones
    worker.cerrar()
    worker._middleware.cerrar()


def preparar_archivos(configuracion, temporal):
    """Prepara configuración aislada, temporales y stopwords mínimas sin descargas."""
    base = Path(temporal)
    config = json.loads(Path(__file__).with_name('config.json').read_text())
    config['broker']['virtual_host'] = configuracion.broker.virtual_host
    config['proceso'] = {'grupo': configuracion.proceso.grupo, 'instancia': configuracion.proceso.instancia}
    config['grupos'] = {grupo.nombre: grupo.instancias for grupo in configuracion.grupos}
    config['limites']['max_mensajes_por_cola'] = configuracion.limites.max_mensajes_por_cola
    archivo = base / 'config.json'
    archivo.write_text(json.dumps(config))
    disco = base / 'disco'
    disco.mkdir()
    nltk = base / 'nltk'
    corpus = nltk / 'corpora' / 'stopwords'
    corpus.mkdir(parents=True)
    (corpus / 'english').write_text('the\nand\n')
    (corpus / 'french').write_text('le\net\n')
    return archivo, disco, nltk


def ejecutar_worker(configuracion, eventos, modo, continuar):
    """Ejecuta el main real y retiene solo el punto observado por el escenario."""
    logging.getLogger("pika").setLevel(logging.WARNING)
    grupo = configuracion.proceso.grupo
    anteriores = {numero: signal.getsignal(numero) for numero in (signal.SIGTERM, signal.SIGINT)}
    instancias = []
    ejecutar_original = Worker.ejecutar
    procesar_original = Worker._procesar
    abrir_original = MiddlewareRabbitMQ.__init__
    adquirir_original = CapacidadDatos.adquirir

    def ejecutar_observado(worker, apagado=None):
        instancias.append(worker)
        return ejecutar_original(worker, apagado)

    def abrir_observado(middleware, *args, **kwargs):
        abrir_original(middleware, *args, **kwargs)
        if modo == 'inactivo':
            middleware._conexion.programar(lambda: eventos.put('esperando'))

    def adquirir_observado(capacidad, cola):
        capacidad._conexion.programar(lambda: eventos.put('sin_capacidad'))
        return adquirir_original(capacidad, cola)

    def procesar_observado(worker, datos):
        procesar_original(worker, datos)
        if modo == 'activo':
            if grupo in ('top_n', 'q5_comparator'):
                assert any(p.is_file() for p in disco.rglob('*')), 'No se ejercitó almacenamiento'
            eventos.put('procesado_sin_ack')
            if not continuar.wait(PLAZO_PROCESO):
                raise AssertionError('El padre no liberó el callback observado')
            worker.comprobar_detencion()

    with TemporaryDirectory(prefix='sigterm-worker-') as temporal, ExitStack() as recursos:
        archivo, disco, nltk = preparar_archivos(configuracion, temporal)
        recursos.enter_context(patch.dict(os.environ, {'TEMP_DIR': str(disco), 'NLTK_DATA': str(nltk)}))
        modulo = importlib.import_module(f'src.workers.{MODULOS[grupo]}.main')
        argumentos = ['worker', str(archivo)]
        if grupo in ('frecuencias', 'top_n'):
            argumentos = ['worker', '--config', str(archivo)]
        if grupo == 'top_n':
            argumentos += ['--presupuesto-memoria-bytes', '1400', '--directorio-temporal', str(disco)]
        recursos.enter_context(patch.object(sys, 'argv', argumentos))
        recursos.enter_context(patch.object(Worker, 'ejecutar', ejecutar_observado))
        recursos.enter_context(patch.object(Worker, '_procesar', procesar_observado))
        recursos.enter_context(patch.object(MiddlewareRabbitMQ, '__init__', abrir_observado))
        if modo == 'capacidad':
            recursos.enter_context(patch.object(CapacidadDatos, 'adquirir', adquirir_observado))
        ObservadorComputo(modo, eventos, continuar).instalar(recursos)
        with TestCase().assertNoLogs('src.middleware', level='ERROR'):
            (modulo.ejecutar if grupo in ('frecuencias', 'top_n') else modulo.main)()
        assert len(instancias) == 1
        comprobar_recursos(instancias[0], disco, anteriores)
    # La cola de observación crea su propio hilo; se drena y une antes del control.
    eventos.put('limpio')
    eventos.close()
    eventos.join_thread()
    assert not multiprocessing.active_children(), 'Quedaron procesos hijos vivos'
    assert threading.enumerate() == [threading.main_thread()], 'Quedaron hilos vivos'
    hijos = Path(f'/proc/self/task/{os.getpid()}/children')
    assert not hijos.read_text().strip(), 'Quedaron hijos del proceso en Linux'
