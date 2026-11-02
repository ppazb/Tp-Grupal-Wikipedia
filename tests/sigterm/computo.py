"""Barreras para enviar señales dentro de operaciones reales, antes de completarlas."""

import sys
from unittest.mock import patch

from src.common.protocol.primitivas import EscritorBinario, LectorBinario
from src.workers.base_worker import Worker
from src.workers.q4.top_n.consolidador import ConsolidadorFrecuencias
from tests.soporte import PLAZO_PROCESO


class ObservadorComputo:
    """Retiene un paso interno y exige que la implementación original lo cancele."""

    def __init__(self, modo, eventos, continuar):
        self.modo = modo
        self.eventos = eventos
        self.continuar = continuar
        self.pasos = 0

    def _retener(self):
        self.pasos += 1
        if self.pasos != 2:
            return False
        self.eventos.put('computando')
        if not self.continuar.wait(PLAZO_PROCESO):
            raise AssertionError('No se recibió permiso para continuar el cómputo observado')
        return True

    def instalar(self, recursos):
        """Observa comprobaciones internas, ordenación o acceso a campos binarios."""
        origenes = {
            'computo': 'procesar',
            'secciones': '_es_biografico',
            'descarga': '_leer_imagen',
            'fragmentos': '_publicar_imagen',
            'tokenizacion': 'extract_non_stopwords_from_abstract',
            'almacenamiento': 'guardar_candidatos',
        }
        if self.modo in origenes:
            original = Worker.comprobar_detencion

            def comprobar(worker):
                origen = sys._getframe(1).f_code.co_name
                retenido = origen == origenes[self.modo] and self._retener()
                original(worker)
                if retenido:
                    raise AssertionError('El cómputo continuó después de SIGTERM')

            recursos.enter_context(patch.object(Worker, 'comprobar_detencion', comprobar))
        else:
            operaciones = {
                'ordenacion': (ConsolidadorFrecuencias, '_comparar_aportes'),
                'serializacion': (EscritorBinario, 'agregar'),
                'decodificacion': (LectorBinario, 'tomar'),
            }
            if self.modo not in operaciones:
                return
            clase, nombre = operaciones[self.modo]
            original = getattr(clase, nombre)

            def operar(instancia, *argumentos, **opciones):
                retenido = self._retener()
                resultado = original(instancia, *argumentos, **opciones)
                if retenido:
                    raise AssertionError('La operación continuó después de SIGTERM')
                return resultado

            recursos.enter_context(patch.object(clase, nombre, operar))
