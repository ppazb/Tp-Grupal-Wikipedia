"""Levanta todas las consultas, verifica actividad y las detiene con docker stop."""

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic, time
from uuid import uuid4

from .configuracion import IMAGEN, crear
from .observador import Observador, nodos

RAIZ = Path(__file__).resolve().parents[3]


def ejecutar(argumentos, *, salida=None, plazo=300):
    resultado = subprocess.run(argumentos, cwd=RAIZ, text=True, stdout=salida or subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=plazo, check=False)
    if resultado.returncode:
        raise RuntimeError(f'Falló {argumentos}: {resultado.stdout or "ver registro del despliegue"}')
    return resultado.stdout


def inspeccionar(identificadores):
    return json.loads(ejecutar(['docker', 'inspect', *identificadores]))


def comprobar_detenidos(estados):
    for estado in estados:
        datos = estado['State']
        nombre = estado['Config']['Labels']['com.docker.compose.service']
        if datos['Running'] or datos['Pid'] != 0 or datos['OOMKilled']:
            raise AssertionError(f'{nombre} no se detuvo limpiamente: {datos}')
        if datos['ExitCode'] not in (0, 143):
            raise AssertionError(f'{nombre} terminó con código {datos["ExitCode"]}')


def tiempos_eventos(proyecto, inicio, fin, estados, plazo):
    texto = ejecutar(['docker', 'events', '--since', str(inicio), '--until', str(fin),
                      '--filter', f'label=com.docker.compose.project={proyecto}',
                      '--filter', 'type=container', '--format', '{{json .}}'])
    eventos = [json.loads(linea) for linea in texto.splitlines() if linea]
    tiempos = {}
    for estado in estados:
        nombre = estado['Config']['Labels']['com.docker.compose.service']
        propios = [e for e in eventos if e['Actor']['ID'] == estado['Id']]
        for evento in propios:
            if evento['Action'] == 'kill' and evento['Actor']['Attributes'].get('signal') == '9':
                raise AssertionError(f'Docker utilizó SIGKILL para {nombre}')
        senales = [e['timeNano'] for e in propios if e['Action'] == 'kill'
                   and e['Actor']['Attributes'].get('signal') == '15']
        salidas = [e['timeNano'] for e in propios if e['Action'] == 'die']
        if not senales or not salidas:
            raise AssertionError(f'Falta evidencia de SIGTERM y salida para {nombre}')
        segundos = (max(salidas) - min(senales)) / 1e9
        if not 0 <= segundos <= plazo:
            raise AssertionError(f'{nombre} tardó {segundos:.3f}s, límite {plazo}s')
        tiempos[nombre] = round(segundos, 3)
    return tiempos


def detener(compose, aplicaciones, observador, opciones, proyecto, registro, informe):
    ids = ejecutar([*compose, 'ps', '-q', *aplicaciones]).split()
    estados = inspeccionar(ids)
    if len(estados) != len(aplicaciones) or not all(e['State']['Running'] for e in estados):
        raise AssertionError('Algún proceso terminó antes de enviar SIGTERM')
    infraestructura = ejecutar([*compose, 'ps', '-q', 'rabbitmq', 'imagenes']).split()
    plazo = opciones['plazo_apagado_segundos']
    margen = opciones['margen_comando_segundos']
    inicio_eventos = int(time()) - 1
    inicio = monotonic()
    print(f'Deteniendo el despliegue con docker compose stop --timeout {plazo}', flush=True)
    ejecutar([*compose, 'stop', '--timeout', str(plazo)], salida=registro,
             plazo=opciones['plazo_despliegue_segundos'] + margen)
    duracion_total = monotonic() - inicio
    estados = inspeccionar([*ids, *infraestructura])
    (informe / 'estados.json').write_text(json.dumps(estados, indent=2))
    comprobar_detenidos(estados)
    observador.esperar(lambda avisos: set(aplicaciones) <= nodos(avisos, 'limpio'), margen)
    fin = datetime.now(timezone.utc).isoformat()
    tiempos = tiempos_eventos(proyecto, inicio_eventos, fin, estados, plazo)
    if duracion_total > opciones['plazo_despliegue_segundos']:
        raise AssertionError(f'El despliegue tardó {duracion_total:.3f}s en detenerse')
    return {'limite_por_proceso_segundos': plazo, 'segundos_por_proceso': tiempos,
            'limite_despliegue_segundos': opciones['plazo_despliegue_segundos'],
            'parada_total_segundos': round(duracion_total, 3)}


def escenario(base, informe, opciones, proyecto):
    archivo, aplicaciones = crear(base, RAIZ, opciones)
    compose = ['docker', 'compose', '-p', proyecto, '-f', str(archivo)]
    observador = Observador(base / 'avisos.sock')
    resultado = {}
    with (informe / 'docker.log').open('w') as registro:
        try:
            ejecutar(['docker', 'build', '-t', IMAGEN, '-f', 'tests/Dockerfile', '.'], salida=registro)
            ejecutar([*compose, 'run', '--rm', '--no-deps', 'preparador'], salida=registro)
            ejecutar([*compose, 'up', '-d', *aplicaciones], salida=registro,
                     plazo=opciones['plazo_arranque_segundos'])
            discos = {'top_n-0', 'q5_comparator-0'}
            observador.esperar(
                lambda avisos: set(aplicaciones) <= nodos(avisos, 'activo')
                and discos <= nodos(avisos, 'disco'), opciones['plazo_arranque_segundos'])
            for nombre in discos:
                if not any(p.is_file() for p in (base / 'temporales' / nombre).rglob('*')):
                    raise AssertionError(f'No hay estado persistido pendiente en {nombre}')
            resultado = detener(compose, aplicaciones, observador, opciones, proyecto, registro, informe)
            sobrantes = [str(p.relative_to(base)) for p in (base / 'temporales').glob('*/*')]
            if sobrantes:
                raise AssertionError(f'Quedaron temporales después del apagado: {sobrantes}')
            resultado['procesos_aplicacion'] = len(aplicaciones)
            resultado['estado'] = 'OK'
        finally:
            try:
                with (informe / 'procesos.log').open('w') as salida:
                    ejecutar([*compose, 'logs', '--no-color', '--tail', '200'], salida=salida, plazo=30)
            finally:
                (informe / 'avisos.json').write_text(json.dumps(observador.avisos, indent=2))
                observador.cerrar()
                ejecutar([*compose, 'down', '--volumes', '--remove-orphans'], salida=registro)
    return resultado


def main():
    opciones = json.loads(Path(__file__).with_name('escenario.json').read_text())
    proyecto = f'wiki-apagado-{uuid4().hex[:10]}'
    informe = RAIZ / 'registros/pruebas-sigterm' / proyecto
    informe.mkdir(parents=True)
    print(f'Registros del escenario: {informe}', flush=True)
    with TemporaryDirectory(prefix='wiki-stop-') as temporal:
        resultado = escenario(Path(temporal), informe, opciones, proyecto)
    (informe / 'tiempos.json').write_text(json.dumps(resultado, indent=2))
    print(json.dumps(resultado, indent=2), flush=True)


if __name__ == '__main__':
    main()
