"""Worker de Top N: consolidación completa, resultado definitivo y cierre de Q4."""

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote
from src.workers.base_worker import Worker
from .seleccion import seleccionar_top


RECURSO_CONTEOS = 'conteos_q4'


class WorkerTopN(Worker):
    """Selecciona el top de cada ejecución después del cierre consolidado de su entrada.

    Los productores deben usar afinidad por ejecución para que todos sus parciales
    lleguen a la misma réplica. Una réplica sin aportes publica únicamente su EOF.
    El proceso propietario abre y cierra el consolidador y el middleware.
    """

    def __init__(self, middleware, consolidador, *, entrada, grupo_salida, entrada_salida):
        super().__init__(middleware)
        for nombre in (entrada, grupo_salida, entrada_salida):
            if type(nombre) is not str or not nombre.strip():
                raise ValueError('Los nombres de entrada y salida deben ser textos no vacíos')
        self._consolidador = consolidador
        self._entrada = entrada
        self._grupo_salida = grupo_salida
        self._entrada_salida = entrada_salida

    def procesar(self, datos):
        """Consolida los parciales de la ejecución sin esperar su EOF para consumir."""
        self._validar_entrada(datos.entrada)
        if datos.contenido.esquema is not Esquema.CONTEO_PALABRA:
            raise ValueError('Top N requiere lotes con esquema CONTEO_PALABRA')
        for parcial in datos.contenido.registros:
            self.comprobar_detencion()
            self._consolidador.acumular(datos.entrada.id_ejecucion, parcial)

    def finalizar_entrada(self, entrada):
        """Publica el resultado completo y después el cierre de Q4 de esta instancia."""
        self._validar_entrada(entrada)
        ejecucion = entrada.id_ejecucion
        self._consolidador.finalizar_entrada(ejecucion)
        self._middleware.registrar_recurso(
            ejecucion, RECURSO_CONTEOS,
            lambda: self._consolidador.liberar_ejecucion(ejecucion),
        )
        with self._consolidador.iterar_conteos(ejecucion) as totales:
            resultado = seleccionar_top(totales)
        salida = Entrada(ejecucion, self._entrada_salida)
        self._publicar_resultado(salida, resultado)
        self._middleware.finalizar_entrada(self._grupo_salida, salida)

    def _publicar_resultado(self, entrada, resultado):
        if not resultado:
            return
        presupuesto = self._crear_presupuesto(entrada)
        registros = []
        for registro in resultado:
            if not presupuesto.admitir(registro):
                self._publicar_lote(entrada, registros)
                registros.clear()
                presupuesto = self._crear_presupuesto(entrada)
                if not presupuesto.admitir(registro):
                    raise ValueError('El resultado no cabe en un lote vacío')
            registros.append(registro)
        self._publicar_lote(entrada, registros)

    def _crear_presupuesto(self, entrada):
        return self._middleware.crear_presupuesto_lote(entrada, Esquema.CONTEO_PALABRA)

    def _publicar_lote(self, entrada, registros):
        datos = Datos(entrada, Lote(Esquema.CONTEO_PALABRA, tuple(registros)))
        self._middleware.publicar(
            self._grupo_salida, datos, clave_afinidad=entrada.id_ejecucion,
        )

    def _validar_entrada(self, entrada):
        if entrada.nombre != self._entrada:
            raise ValueError('La entrada no corresponde al worker de Top N')
