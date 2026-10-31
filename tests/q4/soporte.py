"""Fuentes sintéticas y procesos reales de Q4 para escenarios con RabbitMQ."""

from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, Palabra
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.workers.q4.frequency.worker import WorkerFrecuencia
from src.workers.q4.top_n.consolidador import ConsolidadorFrecuencias
from src.workers.q4.top_n.worker import WorkerTopN

STOPWORDS = frozenset(('the', 'and', 'le', 'et'))
PRESUPUESTO_CONSOLIDACION = 1400


def serial(abstracts):
    """Cuenta presencia por abstract y ordena todos los totales de la muestra pequeña."""
    frecuencias = Counter()
    for abstract in abstracts:
        frecuencias.update(set(abstract.split()) - STOPWORDS)
    return sorted(frecuencias.items(), key=lambda par: (-par[1], par[0]))[:20]


def rechazar_error(error):
    raise AssertionError(f'Rechazo inesperado: {error}')


def producir(configuracion, eventos, clientes, numero):
    """Representa Ocurrencia Palabras con tokens controlados, sin analizar texto natural."""
    with MiddlewareRabbitMQ(configuracion, informar_error=rechazar_error) as middleware:
        for indice in range(max(map(len, clientes.values()), default=0)):
            if indice % 2 != numero:
                continue
            for cliente, abstracts in clientes.items():
                if indice >= len(abstracts):
                    continue
                # La fuente conserva una presencia por abstract, pero no entre abstracts.
                vistos = set()
                for palabra in abstracts[indice].split():
                    if palabra in STOPWORDS or palabra in vistos:
                        continue
                    vistos.add(palabra)
                    middleware.publicar('frecuencias', Datos(
                        Entrada(cliente, 'palabras'),
                        Lote(Esquema.PALABRA, (Palabra(palabra),)),
                    ))
        for cliente in clientes:
            middleware.finalizar_entrada('frecuencias', Entrada(cliente, 'palabras'))
    eventos.put(('producido',))


def ejecutar_worker(worker, eventos, clientes):
    """Detiene el proceso auxiliar al finalizar cada ejecución una única vez."""
    finalizar = worker.finalizar_entrada
    cierres = set()

    def finalizar_observado(entrada):
        if entrada.id_ejecucion in cierres or entrada.id_ejecucion not in clientes:
            raise AssertionError('Cierre duplicado o desconocido')
        finalizar(entrada)
        cierres.add(entrada.id_ejecucion)
        if cierres == set(clientes):
            worker.detener()

    worker.finalizar_entrada = finalizar_observado
    eventos.put(('listo',))
    worker.ejecutar()
    if cierres != set(clientes):
        raise AssertionError('El worker terminó sin cerrar todas las ejecuciones')


def frecuencia(configuracion, eventos, clientes):
    with MiddlewareRabbitMQ(configuracion, informar_error=rechazar_error) as middleware:
        worker = WorkerFrecuencia(middleware, entrada='palabras',
                                  grupo_salida='top_n', entrada_salida='conteos')
        ejecutar_worker(worker, eventos, clientes)
    eventos.put(('terminado',))


def top_n(configuracion, eventos, clientes):
    with TemporaryDirectory(prefix='prueba-q4-') as temporal:
        with ConsolidadorFrecuencias(PRESUPUESTO_CONSOLIDACION, temporal) as consolidador:
            with MiddlewareRabbitMQ(configuracion, informar_error=rechazar_error) as middleware:
                worker = WorkerTopN(middleware, consolidador, entrada='conteos',
                                    grupo_salida='sumidero', entrada_salida='q4')
                ejecutar_worker(worker, eventos, clientes)
                # Tras los cierres se borran los archivos, aun con el consolidador abierto.
                if any(ruta.is_file() for ruta in Path(temporal).rglob('*')):
                    raise AssertionError('Quedaron archivos de ejecuciones terminadas')
            tramos = consolidador._secuencia
        if list(Path(temporal).iterdir()):
            raise AssertionError('No se eliminó el directorio del consolidador')
    eventos.put(('terminado', tramos))


def consumir(configuracion, eventos, clientes):
    resultados = {cliente: [] for cliente in clientes}
    lotes = Counter()
    cierres = set()
    with MiddlewareRabbitMQ(configuracion, informar_error=rechazar_error) as middleware:
        def procesar(datos):
            cliente = datos.entrada.id_ejecucion
            if datos.entrada.nombre != 'q4' or cliente in cierres:
                raise AssertionError('Salida fuera de entrada o posterior al cierre')
            if datos.contenido.esquema is not Esquema.CONTEO_PALABRA:
                raise AssertionError('Esquema de resultado incorrecto')
            resultados[cliente].extend((r.palabra, r.cantidad) for r in datos.contenido.registros)
            lotes[cliente] += 1

        def finalizar(entrada):
            if entrada.nombre != 'q4' or entrada.id_ejecucion in cierres:
                raise AssertionError('Cierre duplicado o entrada incorrecta')
            cierres.add(entrada.id_ejecucion)

        eventos.put(('listo',))
        while cierres != set(clientes):
            middleware.recibir(procesar, al_finalizar=finalizar)
        for cliente in clientes:
            middleware.liberar_ejecucion(cliente)
    eventos.put(('resultado', resultados, dict(lotes), cierres))
