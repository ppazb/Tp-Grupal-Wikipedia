"""Genera servicios con identidades únicas a partir de los grupos configurados."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
from tempfile import NamedTemporaryFile


RAIZ = Path(__file__).resolve().parents[1]
PLANTILLA = RAIZ / 'config/compose.plantilla.yml'
CONFIGURACION = RAIZ / 'config/config.json'
SALIDA = RAIZ / 'docker-compose.yaml'
GRUPOS_UNICOS = ('gateway', 'gateway_resultados', 'q5_average', 'q5_comparator')
PERFIL_CLIENTES = 'clientes'


def leer_plantilla():
    """Usa Compose para resolver YAML, anclas y rutas sin abrir conexión al broker."""
    resultado = subprocess.run(
        ['docker', 'compose', '--project-directory', str(RAIZ),
         '-f', str(PLANTILLA), 'config', '--format', 'json'],
        check=True, capture_output=True, text=True,
    )
    return json.loads(resultado.stdout)


def validar_grupos(configuracion, servicios):
    """Exige cantidades positivas y un servicio plantilla para cada grupo worker."""
    if not isinstance(configuracion, dict):
        raise ValueError('La configuración debe ser un objeto JSON')
    grupos = configuracion.get('grupos')
    if not isinstance(grupos, dict) or not grupos:
        raise ValueError('La configuración debe contener un objeto grupos no vacío')
    workers = {}
    for nombre, servicio in servicios.items():
        grupo = servicio.get('environment', {}).get('WIKI_MW_PROCESO_GRUPO')
        if grupo is not None:
            if grupo in workers:
                raise ValueError(f'Hay más de una plantilla para el grupo {grupo}')
            workers[grupo] = nombre
    esperados = set(workers) | {'gateway', 'gateway_resultados'}
    if set(grupos) != esperados:
        raise ValueError(f'Grupos sin plantilla: {sorted(set(grupos) - esperados)}; '
                         f'grupos faltantes: {sorted(esperados - set(grupos))}')
    for grupo, cantidad in grupos.items():
        if type(cantidad) is not int or cantidad < 1:
            raise ValueError(f'La cantidad de instancias de {grupo} debe ser un entero positivo')
        if grupo in GRUPOS_UNICOS and cantidad != 1:
            raise ValueError(f'{grupo} solo admite una instancia con la implementación actual')
    return workers


def configurar_servicios(documento, configuracion, ruta):
    """Monta el JSON elegido y sincroniza broker y límite de frames de los clientes."""
    broker = configuracion['broker']
    limite = configuracion['limites']['max_bytes_por_mensaje']
    if type(limite) is not int or limite < 1:
        raise ValueError('max_bytes_por_mensaje debe ser un entero positivo')
    puerto = broker['puerto']
    if type(puerto) is not int or not 1 <= puerto <= 65535:
        raise ValueError('El puerto del broker debe estar entre 1 y 65535')
    for servicio in documento['services'].values():
        if 'deploy' in servicio:
            # Compose normaliza placement vacío, pero luego lo advierte como opción de Swarm.
            servicio['deploy'] = {k: v for k, v in servicio['deploy'].items() if v != {}}
        entorno = servicio.setdefault('environment', {})
        for volumen in servicio.get('volumes', []):
            if volumen['target'] == '/app/config':
                volumen.update(source=str(ruta), target='/app/config/config.json')
        if 'WIKI_MW_BROKER_HOST' in entorno:
            entorno['WIKI_MW_BROKER_HOST'] = 'rabbitmq'
        if 'CLIENT_ID' in entorno:
            entorno['MAX_FRAME_BYTES'] = str(limite)
            servicio['profiles'] = [PERFIL_CLIENTES]
    rabbit = documento['services']['rabbitmq']
    rabbit['environment'].update(
        RABBITMQ_DEFAULT_USER=broker['usuario'], RABBITMQ_DEFAULT_PASS=broker['contrasena'],
        RABBITMQ_DEFAULT_VHOST=broker['virtual_host'], RABBITMQ_NODE_PORT=str(puerto),
    )
    for enlace in rabbit.get('ports', []):
        if enlace['target'] == 5672:
            enlace['target'] = puerto


def generar_clientes(servicios, cantidad):
    """Crea clientes independientes que leen la misma entrada y separan sus resultados."""
    if type(cantidad) is not int or cantidad < 1:
        raise ValueError('La cantidad de clientes debe ser un entero positivo')
    original = deepcopy(servicios['client'])
    for instancia in range(cantidad):
        nombre = 'client' if instancia == 0 else f'client-{instancia}'
        if instancia and nombre in servicios:
            raise ValueError(f'El nombre generado {nombre} ya pertenece a otro servicio')
        servicio = deepcopy(original)
        servicio['environment']['CLIENT_ID'] = str(instancia)
        servicios[nombre] = servicio


def generar(configuracion, plantilla, ruta, clientes):
    """Conserva las opciones de cada servicio y asigna instancias de cero a N menos uno."""
    documento = deepcopy(plantilla)
    servicios = documento['services']
    workers = validar_grupos(configuracion, servicios)
    for grupo, nombre in workers.items():
        original = deepcopy(servicios[nombre])
        for instancia in range(configuracion['grupos'][grupo]):
            destino = nombre if instancia == 0 else f'{nombre}-{instancia}'
            if instancia and destino in servicios:
                raise ValueError(f'El nombre generado {destino} ya pertenece a otro servicio')
            servicio = deepcopy(original)
            servicio.setdefault('deploy', {})['replicas'] = 1
            servicio['environment']['WIKI_MW_PROCESO_INSTANCIA'] = str(instancia)
            servicios[destino] = servicio
    generar_clientes(servicios, clientes)
    configurar_servicios(documento, configuracion, ruta)
    # Las anclas ya fueron expandidas por Compose.
    return {clave: valor for clave, valor in documento.items() if not clave.startswith('x-')}


def guardar(documento, salida):
    """Reemplaza el archivo generado solo después de escribirlo completamente."""
    # Compose convierte su modelo JSON a YAML; no requiere un serializador adicional.
    resultado = subprocess.run(
        ['docker', 'compose', '--project-directory', str(RAIZ), '-f', '-',
         '--profile', PERFIL_CLIENTES, 'config', '--format', 'yaml'],
        input=json.dumps(documento), check=True, capture_output=True, text=True,
    )
    temporal = None
    try:
        with NamedTemporaryFile(mode='w', encoding='utf-8', dir=salida.parent,
                                prefix='.compose-', suffix='.yaml', delete=False) as archivo:
            temporal = Path(archivo.name)
            contenido = resultado.stdout
            if archivo.write(contenido) != len(contenido):
                raise OSError('Escritura incompleta del Compose generado')
        temporal.replace(salida)
    finally:
        if temporal is not None:
            temporal.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=CONFIGURACION,
                        help='Opcional: JSON del sistema (por defecto: %(default)s)')
    parser.add_argument('--salida', type=Path, default=SALIDA,
                        help='Opcional: Compose generado (por defecto: %(default)s)')
    parser.add_argument('--clientes', type=int, default=1,
                        help='Opcional: cantidad de clientes (por defecto: %(default)s)')
    argumentos = parser.parse_args()
    try:
        ruta, salida = argumentos.config.resolve(), argumentos.salida.resolve()
        if salida in (ruta, PLANTILLA):
            raise ValueError('La salida no puede reemplazar la configuración ni la plantilla')
        with ruta.open(encoding='utf-8') as archivo:
            configuracion = json.load(archivo)
        documento = generar(configuracion, leer_plantilla(), ruta, argumentos.clientes)
        guardar(documento, salida)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        detalle = error.stderr if isinstance(error, subprocess.CalledProcessError) else str(error)
        parser.error(f'No se pudo generar el Compose: {detalle}')
    print(f'Compose generado: {salida}')


if __name__ == '__main__':
    main()
