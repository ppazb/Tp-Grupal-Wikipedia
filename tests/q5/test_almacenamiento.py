"""Persistencia real, archivos incompletos y limpieza de ejecuciones de Q5."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.common.io_adapters.archivos import leer_completo, escribir_completo
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Candidato, Esquema, Lote, ResumenHistorico
from src.workers.q5.comparator import storage
from src.workers.q5.comparator.worker import ComparatorQ5Worker


CONFIGURACION = Path(__file__).resolve().parents[2] / 'config/config.json'


class ArchivoParcial:
    """Limita cada operación de un archivo real para reproducir E/S parcial."""

    def __init__(self, archivo):
        self.archivo = archivo

    def read(self, cantidad):
        return self.archivo.read(min(cantidad, 3))

    def write(self, datos):
        return self.archivo.write(datos[:3])


class MiddlewareObservado:
    def __init__(self):
        self.pendientes = {'candidatos', 'resumen_historico'}
        self.resultados = []
        self.cierres = []

    def entradas_pendientes(self, ejecucion):
        return self.pendientes

    def crear_presupuesto_lote(self, entrada, esquema):
        return self

    def admitir(self, registro):
        return True

    def publicar(self, grupo, datos):
        self.resultados.extend(datos.contenido.registros)

    def finalizar_entrada(self, grupo, entrada):
        self.cierres.append(entrada)


class EscenariosAlmacenamientoQ5(unittest.TestCase):
    def setUp(self):
        temporal = TemporaryDirectory(prefix='prueba-q5-')
        self.addCleanup(temporal.cleanup)
        self.directorio = Path(temporal.name)

    def crear_worker(self):
        with patch.dict('os.environ', {'TEMP_DIR': str(self.directorio), 'BATCH_SIZE_Q5': '2',
                                     'WIKI_MW_PROCESO_GRUPO': 'q5_comparator'}):
            worker = ComparatorQ5Worker(str(CONFIGURACION))
        self.addCleanup(worker.cerrar)
        worker.middleware = MiddlewareObservado()
        worker.procesar(Datos(Entrada('A', 'candidatos'),
                             Lote(Esquema.CANDIDATO, (Candidato('forêt', 8), Candidato('été', 2)))))
        worker.procesar(Datos(Entrada('A', 'resumen_historico'),
                             Lote(Esquema.RESUMEN_HISTORICO, (ResumenHistorico(10, 2),))))
        worker.middleware.pendientes.remove('candidatos')
        worker.al_finalizar(Entrada('A', 'candidatos'))
        self.assertFalse(worker.middleware.resultados)
        worker.middleware.pendientes.clear()
        return worker

    def test_ejecuciones_intercaladas_con_unicode_y_lecturas_escrituras_parciales(self):
        almacen = storage.AlmacenamientoCandidatos(str(self.directorio))
        self.addCleanup(almacen.cerrar)
        def escribir(archivo, datos, comprobar):
            escribir_completo(ArchivoParcial(archivo), datos, comprobar)
        def leer(archivo, cantidad, **opciones):
            return leer_completo(ArchivoParcial(archivo), cantidad, **opciones)
        with patch.object(storage, 'escribir_completo', escribir), patch.object(storage, 'leer_completo', leer):
            almacen.guardar_candidatos('A', [Candidato('forêt', 8)])
            almacen.guardar_candidatos('B', [Candidato('été', 0)])
            almacen.guardar_candidatos('A', [Candidato('niño', 3)])
            self.assertEqual(list(almacen.iterar_candidatos('A')), [Candidato('forêt', 8), Candidato('niño', 3)])
            almacen.limpiar_ejecucion('A')
            almacen.limpiar_ejecucion('A')
            self.assertEqual(list(almacen.iterar_candidatos('B')), [Candidato('été', 0)])
            self.assertEqual(list(almacen.iterar_candidatos('sin_aportes')), [])
        almacen.cerrar()
        self.assertEqual(list(self.directorio.iterdir()), [])

    def test_comparacion_y_limpieza_antes_de_emitir_el_cierre(self):
        worker = self.crear_worker()
        cerrar = worker.middleware.finalizar_entrada
        def verificar_cierre(grupo, entrada):
            self.assertEqual(list(worker._storage._directorio.iterdir()), [])
            cerrar(grupo, entrada)
        worker.middleware.finalizar_entrada = verificar_cierre
        worker.al_finalizar(Entrada('A', 'resumen_historico'))
        self.assertEqual(worker.middleware.resultados, [Candidato('forêt', 8)])
        self.assertEqual(worker.middleware.cierres, [Entrada('A', 'q5')])

    def test_archivo_incompleto_invalido_o_perdido_no_publica_cierre_exitoso(self):
        casos = [b'\0\0', (4).to_bytes(4, 'big') + b'ab',
                 (1).to_bytes(4, 'big') + b'a' + b'\0\0',
                 (1).to_bytes(4, 'big') + b'\xff' + (8).to_bytes(8, 'big'), None]
        for cuerpo in casos:
            with self.subTest(cuerpo=cuerpo):
                worker = self.crear_worker()
                ruta = worker._storage._archivos['A']
                if cuerpo is None:
                    ruta.unlink()
                else:
                    ruta.write_bytes(cuerpo)
                with self.assertRaises((ValueError, FileNotFoundError)):
                    worker.al_finalizar(Entrada('A', 'resumen_historico'))
                self.assertEqual(worker.middleware.cierres, [])
                self.assertEqual(worker.middleware.resultados, [])
                self.assertFalse(ruta.exists())
                worker.cerrar()

    def test_error_de_limpieza_se_informa_y_no_reemplaza_un_error_de_lectura(self):
        worker = self.crear_worker()
        with patch.object(Path, 'unlink', side_effect=PermissionError('sin permiso')):
            with self.assertRaises(PermissionError):
                worker.al_finalizar(Entrada('A', 'resumen_historico'))
        self.assertFalse(worker.middleware.cierres)
        worker.cerrar()
        worker = self.crear_worker()
        worker._storage._archivos['A'].write_bytes(b'\0')
        with patch.object(Path, 'unlink', side_effect=PermissionError('sin permiso')):
            with self.assertLogs('src.workers.q5.comparator.worker', level='ERROR'):
                with self.assertRaisesRegex(ValueError, 'truncado'):
                    worker.al_finalizar(Entrada('A', 'resumen_historico'))
        self.assertFalse(worker.middleware.cierres)
        worker.cerrar()
        self.assertEqual(list(self.directorio.iterdir()), [])


if __name__ == '__main__':
    unittest.main()
