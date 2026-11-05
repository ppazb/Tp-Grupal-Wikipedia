"""Obtiene del compose la entrada de cada cliente, sin depender de rutas absolutas del host."""

from pathlib import Path, PurePosixPath

import yaml


def _entorno(servicio):
    entorno = servicio.get('environment') or {}
    if isinstance(entorno, list):
        entorno = dict(item.split('=', 1) for item in entorno if '=' in item)
    return {clave: str(valor) for clave, valor in entorno.items()}


def _a_local(origen, contexto, raiz):
    """Traduce una ruta del host a una relativa a `raiz`, usando el contexto de build como raíz del proyecto."""
    origen = Path(origen)
    if contexto is not None:
        try:
            return raiz / origen.relative_to(contexto)
        except ValueError:
            pass
    return origen if origen.is_absolute() and origen.exists() else raiz / origen


def entradas_clientes(compose, raiz=None):
    """Devuelve {id_cliente: directorio local con los parquet} para cada servicio con CLIENT_ID e INPUT_DIR."""
    compose = Path(compose)
    raiz = Path(raiz) if raiz is not None else compose.parent
    servicios = (yaml.safe_load(compose.read_text(encoding='utf-8')) or {}).get('services', {})
    entradas = {}
    for nombre, servicio in servicios.items():
        entorno = _entorno(servicio)
        if 'CLIENT_ID' not in entorno or 'INPUT_DIR' not in entorno:
            continue
        build = servicio.get('build')
        contexto = Path(build['context']) if isinstance(build, dict) and 'context' in build else None
        entrada = PurePosixPath(entorno['INPUT_DIR'])
        # Volumen más específico que contiene INPUT_DIR (p. ej. /app/input para /app/input/input_0).
        mejor = None
        for volumen in servicio.get('volumes', []):
            if not isinstance(volumen, dict):
                partes = str(volumen).split(':')
                if len(partes) < 2:
                    continue
                volumen = {'source': partes[0], 'target': partes[1]}
            destino = PurePosixPath(volumen['target'])
            if (entrada == destino or destino in entrada.parents) and (
                    mejor is None or len(destino.parts) > len(PurePosixPath(mejor['target']).parts)):
                mejor = volumen
        if mejor is None:
            raise ValueError(f'{nombre}: INPUT_DIR={entrada} no está montado desde el host')
        local = _a_local(mejor['source'], contexto, raiz) / entrada.relative_to(PurePosixPath(mejor['target']))
        if entorno['CLIENT_ID'] in entradas:
            raise ValueError(f'CLIENT_ID repetido en el compose: {entorno["CLIENT_ID"]}')
        entradas[entorno['CLIENT_ID']] = local
    return entradas
