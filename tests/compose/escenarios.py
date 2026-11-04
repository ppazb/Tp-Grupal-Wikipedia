"""Comprueba la generación real con distintas cantidades y configuraciones inválidas."""

import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


RAIZ = Path(__file__).resolve().parents[2]


class EscenariosCompose(unittest.TestCase):
    def setUp(self):
        temporal = TemporaryDirectory(prefix='prueba-compose-')
        self.addCleanup(temporal.cleanup)
        self.directorio = Path(temporal.name)
        self.config = json.loads((RAIZ / 'config/config.json').read_text())
        self.entrada = self.directorio / 'config.json'
        self.salida = self.directorio / 'compose.yaml'

    def generar(self, clientes=1):
        self.entrada.write_text(json.dumps(self.config))
        return subprocess.run(
            [sys.executable, str(RAIZ / 'scripts/generar_compose.py'),
             '--config', str(self.entrada), '--salida', str(self.salida), '--clientes', str(clientes)],
            capture_output=True, text=True, timeout=30,
        )

    def validar_compose(self):
        resultado = subprocess.run(
            ['docker', 'compose', '-f', str(self.salida), '--profile', 'clientes',
             'config', '--format', 'json'],
            check=True, capture_output=True, text=True, timeout=30,
        )
        return json.loads(resultado.stdout)['services']

    def test_replicas_con_identidades_unicas_y_regeneracion_al_reducir(self):
        self.config['grupos'].update(date_router=2, word_occurrences=3, frecuencias=4)
        resultado = self.generar()
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        servicios = self.validar_compose()
        for grupo, cantidad in self.config['grupos'].items():
            if grupo in ('gateway', 'gateway_resultados'):
                continue
            instancias = [s['environment']['WIKI_MW_PROCESO_INSTANCIA']
                          for s in servicios.values()
                          if s.get('environment', {}).get('WIKI_MW_PROCESO_GRUPO') == grupo]
            self.assertCountEqual(instancias, [str(i) for i in range(cantidad)])
        self.assertEqual(servicios['q4_top_n']['command'], ['--presupuesto-memoria-bytes', '16777216'])
        self.assertIn('inicializador', servicios['q4_frequency-3']['depends_on'])
        self.config['grupos']['frecuencias'] = 1
        self.assertEqual(self.generar().returncode, 0)
        self.assertNotIn('q4_frequency-1', self.validar_compose())

    def test_configuracion_alternativa_conserva_montajes_y_sincroniza_clientes_y_broker(self):
        self.config['limites']['max_bytes_por_mensaje'] = 65536
        self.config['broker'].update(usuario='prueba', contrasena='clave', virtual_host='/prueba')
        resultado = self.generar()
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        servicios = self.validar_compose()
        for nombre in ('gateway', 'inicializador', 'date_router', 'q4_top_n'):
            montajes = {v['target']: v for v in servicios[nombre]['volumes']}
            self.assertEqual(montajes['/app/config/config.json']['source'], str(self.entrada))
            self.assertTrue(montajes['/app/config/config.json']['read_only'])
        for nombre in ('client',):
            self.assertEqual(servicios[nombre]['environment']['MAX_FRAME_BYTES'], '65536')
            self.assertEqual(servicios[nombre]['profiles'], ['clientes'])
        self.assertEqual(servicios['rabbitmq']['environment']['RABBITMQ_DEFAULT_USER'], 'prueba')
        self.assertEqual(servicios['rabbitmq']['environment']['RABBITMQ_DEFAULT_VHOST'], '/prueba')
        self.assertIn('/app/data/temp', [v['target'] for v in servicios['q5_comparator']['volumes']])

    def test_clientes_variables_comparten_entrada_y_tienen_identidades_independientes(self):
        for cantidad in (3, 5, 1):
            with self.subTest(clientes=cantidad):
                resultado = self.generar(clientes=cantidad)
                self.assertEqual(resultado.returncode, 0, resultado.stderr)
                clientes = [s for s in self.validar_compose().values()
                            if 'CLIENT_ID' in s.get('environment', {})]
                self.assertCountEqual([s['environment']['CLIENT_ID'] for s in clientes],
                                      [str(i) for i in range(cantidad)])
                entradas = []
                for servicio in clientes:
                    self.assertEqual(servicio['environment']['INPUT_DIR'], '/app/input')
                    self.assertEqual(servicio['environment']['OUTPUT_DIR'], '/app/output')
                    montaje = next(v for v in servicio['volumes'] if v['target'] == '/app/input')
                    self.assertTrue(montaje['read_only'])
                    entradas.append(montaje['source'])
                self.assertEqual(len(set(entradas)), 1)
        anterior = self.salida.read_bytes()
        for invalido in (0, -1, 'no-es-numero'):
            self.assertNotEqual(self.generar(clientes=invalido).returncode, 0)
            self.assertEqual(self.salida.read_bytes(), anterior)

    def test_configuracion_invalida_no_reemplaza_el_despliegue_anterior(self):
        self.assertEqual(self.generar().returncode, 0)
        anterior = self.salida.read_bytes()
        for grupo, cantidad in [('frecuencias', 0), ('frecuencias', True), ('frecuencias', 1.5),
                                ('desconocido', 1), ('gateway', 2), ('q5_average', 2),
                                ('q5_comparator', 2), ('gateway_resultados', 2)]:
            with self.subTest(grupo=grupo, cantidad=cantidad):
                original = dict(self.config['grupos'])
                self.config['grupos'][grupo] = cantidad
                resultado = self.generar()
                self.assertNotEqual(resultado.returncode, 0)
                self.assertIn('No se pudo generar', resultado.stderr)
                self.assertEqual(self.salida.read_bytes(), anterior)
                self.config['grupos'] = original



class EscenariosEsperaClientes(unittest.TestCase):
    """Verifica la espera y el resumen con una interfaz Docker simulada."""

    def ejecutar(self, codigos, fallo_espera=False):
        import os
        with TemporaryDirectory(prefix='prueba-espera-clientes-') as temporal:
            raiz = Path(temporal)
            docker = raiz / 'docker'
            docker.write_text('''#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['REGISTRO_DOCKER'], 'a') as archivo:
    archivo.write(json.dumps(args) + '\\n')
if args[0] == 'wait':
    if os.environ['FALLO_ESPERA'] == '1':
        sys.exit(1)
    indice = int(args[1].removeprefix('contenedor-'))
    print(json.loads(os.environ['CODIGOS_CLIENTES'])[indice])
elif 'ps' in args:
    servicio = args[-1]
    print('contenedor-' + ('0' if servicio == 'client' else servicio.split('-')[1]))
''')
            docker.chmod(0o755)
            registro = raiz / 'llamadas.jsonl'
            entorno = dict(os.environ, PATH=f'{raiz}:{os.environ["PATH"]}',
                           REGISTRO_DOCKER=str(registro), CODIGOS_CLIENTES=json.dumps(codigos),
                           FALLO_ESPERA=str(int(fallo_espera)))
            resultado = subprocess.run(
                ['bash', str(RAIZ / 'scripts/ejecutar_sistema.sh'), str(len(codigos))],
                env=entorno, capture_output=True, text=True, timeout=10,
            )
            llamadas = [json.loads(linea) for linea in registro.read_text().splitlines()]
            return resultado, llamadas

    def test_espera_todos_los_clientes_y_conserva_el_despliegue(self):
        resultado, llamadas = self.ejecutar([0, 0, 0])
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        self.assertEqual([c for c in llamadas if c[0] == 'wait'],
                         [['wait', f'contenedor-{i}'] for i in range(3)])
        self.assertIn('Todos los clientes terminaron correctamente', resultado.stdout)
        for indice in range(3):
            self.assertIn(f'client_{indice}: resultados completos', resultado.stdout)
        self.assertFalse(any('down' in c or 'stop' in c for c in llamadas))

    def test_un_cliente_fallido_no_oculta_los_demas_y_docker_fallido_no_da_exito(self):
        resultado, llamadas = self.ejecutar([1, 0, 143])
        self.assertNotEqual(resultado.returncode, 0)
        self.assertEqual(len([c for c in llamadas if c[0] == 'wait']), 3)
        self.assertIn('client_0: ERROR', resultado.stderr)
        self.assertIn('client_2: ERROR', resultado.stderr)
        self.assertNotIn('Todos los clientes terminaron correctamente', resultado.stdout)
        resultado, _ = self.ejecutar([0], fallo_espera=True)
        self.assertNotEqual(resultado.returncode, 0)
        self.assertNotIn('Todos los clientes terminaron correctamente', resultado.stdout)


if __name__ == '__main__':
    unittest.main()
