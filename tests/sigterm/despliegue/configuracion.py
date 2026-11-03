"""Construye un despliegue aislado con la topología real e identidades por réplica."""

import json
import os
from pathlib import Path

IMAGEN = 'wikipedia-pruebas:local'
MODULOS = {
    'date_router': 'date_router', 'word_occurrences': 'word_occurrences',
    'q1_filter_lang': 'q1.filter_lang', 'q1_formatter': 'q1.formatter',
    'q2_filter_bio': 'q2.filter_bio', 'q2_image_downloader': 'q2.image_downloader',
    'q3_intersection': 'q3.intersection', 'frecuencias': 'q4.frequency',
    'top_n': 'q4.top_n', 'q5_average': 'q5.average', 'q5_comparator': 'q5.comparator',
}


def servicio(base, nombre, comando, entorno=None, observado=True):
    temporal = base / 'temporales' / nombre
    temporal.mkdir(parents=True)
    entrada = ['python', '-m', 'tests.sigterm.despliegue.proceso'] if observado else ['python']
    return {
        'image': IMAGEN, 'user': f'{os.getuid()}:{os.getgid()}',
        'entrypoint': entrada, 'command': comando,
        'volumes': [f'{base}:/prueba', f'{temporal}:/temporales'],
        'environment': {
            'NODO_PRUEBA': nombre, 'NLTK_DATA': '/prueba/nltk', 'TEMP_DIR': '/temporales',
            'WIKI_MW_BROKER_HOST': 'rabbitmq', **(entorno or {}),
        },
        'restart': 'no',
    }


def crear(base, raiz, opciones):
    config = json.loads((raiz / 'config/config.json').read_text())
    esperados = set(MODULOS) | {'gateway', 'gateway_resultados'}
    if set(config['grupos']) != esperados:
        raise ValueError('Actualizar el escenario para cubrir los grupos de la topología actual')
    config['grupos'] = {g: opciones['replicas'].get(g, 1) for g in config['grupos']}
    for limite in ('max_bytes_por_mensaje', 'max_mensajes_por_cola'):
        config['limites'][limite] = opciones[limite]
    (base / 'config.json').write_text(json.dumps(config, indent=2))
    (base / 'escenario.json').write_text(json.dumps(opciones))
    servicios = infraestructura(base, config)
    dependencias = {'rabbitmq': {'condition': 'service_healthy'},
                    'inicializador': {'condition': 'service_completed_successfully'}}
    aplicaciones = []
    for grupo, modulo in MODULOS.items():
        for instancia in range(config['grupos'][grupo]):
            nombre = f'{grupo}-{instancia}'
            comando = [f'src.workers.{modulo}.main']
            if grupo in ('frecuencias', 'top_n'):
                comando += ['--config', '/prueba/config.json']
            else:
                comando += ['/prueba/config.json']
            if grupo == 'top_n':
                comando += ['--presupuesto-memoria-bytes', str(opciones['presupuesto_top_n_bytes']),
                            '--directorio-temporal', '/temporales']
            servicios[nombre] = servicio(base, nombre, comando, {
                'WIKI_MW_PROCESO_GRUPO': grupo, 'WIKI_MW_PROCESO_INSTANCIA': str(instancia),
                'BATCH_SIZE_Q5': str(opciones['candidatos_por_escritura']),
            })
            servicios[nombre]['depends_on'] = dict(dependencias)
            if grupo == 'q2_image_downloader':
                servicios[nombre]['depends_on']['imagenes'] = {'condition': 'service_healthy'}
            aplicaciones.append(nombre)
    agregar_gateway_clientes(base, servicios, dependencias, opciones)
    aplicaciones += ['gateway', 'cliente-0', 'cliente-1']
    ruta = base / 'compose.json'
    ruta.write_text(json.dumps({'services': servicios, 'volumes': {'broker': {}}}, indent=2))
    return ruta, aplicaciones


def infraestructura(base, config):
    broker = config['broker']
    return {
        'rabbitmq': {
            'image': 'rabbitmq:4.1-management', 'volumes': ['broker:/var/lib/rabbitmq'],
            'environment': {'RABBITMQ_DEFAULT_USER': broker['usuario'],
                            'RABBITMQ_DEFAULT_PASS': broker['contrasena'],
                            'RABBITMQ_DEFAULT_VHOST': broker['virtual_host']},
            'healthcheck': {'test': ['CMD', 'rabbitmq-diagnostics', '-q', 'check_running'],
                            'interval': '5s', 'timeout': '10s', 'retries': 12},
        },
        'preparador': servicio(base, 'preparador',
                              ['-m', 'tests.sigterm.despliegue.preparar'], observado=False),
        'inicializador': {
            **servicio(base, 'inicializador',
                       ['-m', 'src.middleware.inicializar', '/prueba/config.json'], observado=False),
            'depends_on': {'rabbitmq': {'condition': 'service_healthy'}},
        },
        'imagenes': {
            'init': True,
            **servicio(base, 'imagenes', ['-m', 'http.server', '8080', '--bind', '0.0.0.0',
                                         '--directory', '/prueba/imagenes'], observado=False),
            'healthcheck': {'test': ['CMD', 'python', '-c',
                                    "from urllib.request import urlopen; urlopen('http://localhost:8080/imagen.bin').close()"],
                            'interval': '2s', 'timeout': '3s', 'retries': 20},
        },
    }


def agregar_gateway_clientes(base, servicios, dependencias, opciones):
    servicios['gateway'] = {
        **servicio(base, 'gateway', ['src.gateway.main'], {
            'MIDDLEWARE_CONFIG': '/prueba/config.json', 'GATEWAY_GROUP': 'gateway',
            'GATEWAY_INSTANCE': '0', 'GATEWAY_ERROR_ACK_SECONDS': '10',
            'GATEWAY_RESULTS_GROUP': 'gateway_resultados', 'GATEWAY_RESULTS_INSTANCE': '0',
            'SERVER_HOST': '0.0.0.0', 'SERVER_PORT': '5555',
        }),
        'depends_on': dict(dependencias),
        'healthcheck': {'test': ['CMD', 'python', '-c',
                                "import socket; socket.create_connection(('localhost', 5555), 2).close()"],
                        'interval': '2s', 'timeout': '3s', 'retries': 30},
    }
    for numero in range(2):
        nombre = f'cliente-{numero}'
        servicios[nombre] = {
            **servicio(base, nombre, ['src.client.main'], {
                'INPUT_DIR': '/prueba/entrada', 'OUTPUT_DIR': '/prueba/resultados',
                'CLIENT_ID': str(numero), 'BATCH_SIZE': str(opciones['filas_por_lote']),
                'MAX_FRAME_BYTES': str(opciones['max_bytes_por_mensaje']),
                'SERVER_HOST': 'gateway', 'SERVER_PORT': '5555',
                'CONNECT_TIMEOUT_SECONDS': '10', 'SOCKET_TIMEOUT_SECONDS': '30',
            }),
            'depends_on': {'gateway': {'condition': 'service_healthy'}},
        }
