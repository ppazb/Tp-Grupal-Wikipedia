"""Compara Q4 distribuida con una referencia serial sobre abstracts controlados."""

from dataclasses import replace
from pathlib import Path

from tests.soporte import EscenarioRabbitMQ
from .soporte import consumir, frecuencia, producir, serial, top_n


class EscenariosQ4(EscenarioRabbitMQ):
    """Ejecuta workers reales, permisos y EOF con productores concurrentes."""

    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'productores'

    def configurar(self, grupo=None, instancia=0):
        configuracion = super().configurar(grupo, instancia)
        replicas = 3 if self._testMethodName.endswith('tres_replicas') else 1
        grupos = tuple(replace(g, instancias=replicas) if g.nombre == 'frecuencias' else g
                       for g in configuracion.grupos)
        return replace(configuracion, grupos=grupos)

    def comprobar_pipeline(self, clientes, exigir_discos=False):
        esperados = {cliente: serial(abstracts) for cliente, abstracts in clientes.items()}
        sumidero = self.lanzar(consumir, 'sumidero', tuple(clientes))
        self.assertEqual(self.esperar(sumidero[1]), ('listo',))
        workers = []
        for grupo, funcion in [('top_n', top_n), ('frecuencias', frecuencia)]:
            for instancia in range(self.configuracion.obtener_grupo(grupo).instancias):
                proceso, eventos = self.lanzar(funcion, grupo, tuple(clientes), instancia=instancia)
                self.assertEqual(self.esperar(eventos), ('listo',))
                workers.append((grupo, proceso, eventos))
        fuentes = [self.lanzar(producir, 'productores', clientes, n, instancia=n) for n in range(2)]
        for proceso, eventos in fuentes:
            self.assertEqual(self.esperar(eventos), ('producido',))
            self.finalizar(proceso)
        _, resultados, lotes, cierres = self.esperar(sumidero[1])
        self.assertEqual(resultados, esperados)
        self.assertEqual(cierres, set(clientes))
        self.finalizar(sumidero[0])
        tramos = 0
        for grupo, proceso, eventos in workers:
            evento = self.esperar(eventos)
            self.assertEqual(evento[0], 'terminado')
            if grupo == 'top_n':
                tramos += evento[1]
            self.finalizar(proceso)
        if exigir_discos:
            self.assertGreater(tramos, 4, 'No se ejercitaron múltiples tramos y fusiones')
            self.assertGreater(lotes['A'], 1, 'No se ejercitó la salida en varios lotes')
        self.comprobar_capacidad_recuperada()

    def muestra_extensa(self):
        vocabulario = [f'mot{n:02}' for n in range(30)] + ['été', 'niño']
        # Todas empatan en A; B usa las mismas palabras con frecuencias distintas.
        return {
            'A': ['the ' + ' '.join(vocabulario + vocabulario) for _ in range(8)],
            'B': ['et été été mot00 mot01'] * 7 + ['mot01 niño niño'] * 3,
            'sin_aportes': ['the and le et'],
        }

    def test_repeticiones_empates_y_parciales_con_una_replica(self):
        """Deduplicación por abstract, corte del puesto veinte, disco y aislamiento."""
        self.comprobar_pipeline(self.muestra_extensa(), exigir_discos=True)

    def test_repeticiones_empates_y_parciales_con_tres_replicas(self):
        """La misma referencia se conserva al repartir Frecuencia entre tres procesos."""
        self.comprobar_pipeline(self.muestra_extensa(), exigir_discos=True)

    def test_cierres_sin_aportes_y_top_incompleto(self):
        """Cierra ejecuciones sin datos y devuelve menos de veinte pares si corresponde."""
        self.comprobar_pipeline({'vacia': [], 'filtrada': ['the et'],
                                 'breve': ['été été niño', 'niño and']})
