"""Worker de Top N: consolidación completa, resultado definitivo y cierre de Q4."""

from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.primitivas import BYTES_U32, BYTES_U64
from src.common.protocol.registros import ConteoPalabra, Esquema, Lote
from src.workers.base_worker import Worker
from src.workers.deduplicacion import DedupPorOrigen
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from .seleccion import seleccionar_top


RECURSO_CONTEOS = 'conteos_q4'
TIPO_LOTE = 1  # registro del WAL: identidad del lote + los conteos parciales que aportó


class WorkerTopN(Worker):
    """Selecciona el top de cada ejecución después del cierre consolidado de su entrada.

    Los productores deben usar afinidad por ejecución para que todos sus parciales
    lleguen a la misma réplica. Una réplica sin aportes publica únicamente su EOF.
    El proceso propietario abre y cierra el consolidador y el middleware.

    Cada lote consolidado se agrega al WAL de su ejecución; como el consolidador vive en
    un directorio temporal, al arrancar se reconstruye reaplicando esos registros.
    """

    def __init__(self, middleware, consolidador, *, entrada, grupo_salida, entrada_salida,
                 ventana_lotes, directorio_wal=None):
        super().__init__(middleware)
        for nombre in (entrada, grupo_salida, entrada_salida):
            if type(nombre) is not str or not nombre.strip():
                raise ValueError('Los nombres de entrada y salida deben ser textos no vacíos')
        self._consolidador = consolidador
        self._entrada = entrada
        self._grupo_salida = grupo_salida
        self._entrada_salida = entrada_salida
        self._dedup = DedupPorOrigen(ventana_lotes)
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        self._recuperar()

    def _recuperar(self):
        for id_ejecucion, registros in self._wals.recuperar():
            for datos in registros:
                _, identidad, lector = decodificar(datos)
                for _ in range(lector.entero(BYTES_U32)):
                    palabra, cantidad = lector.texto(), lector.entero(BYTES_U64)
                    self._consolidador.acumular(id_ejecucion, ConteoPalabra(palabra, cantidad))
                lector.finalizar()
                self._dedup.registrar(entrega_de(Entrada(id_ejecucion, self._entrada), identidad))

    def procesar(self, datos):
        """Consolida los parciales de la ejecución sin esperar su EOF para consumir."""
        self._validar_entrada(datos.entrada)
        self.anunciar_cliente("q4_top_n", datos.entrada.id_ejecucion)
        if datos.contenido.esquema is not Esquema.CONTEO_PALABRA:
            raise ValueError('Top N requiere lotes con esquema CONTEO_PALABRA')
        id_ejecucion = datos.entrada.id_ejecucion
        entrega = self._middleware.entrega_actual
        if self._wals.esta_cerrada(id_ejecucion) or self._dedup.es_duplicado(entrega):
            return
        parciales = []
        for parcial in datos.contenido.registros:
            self.comprobar_detencion()
            self._consolidador.acumular(id_ejecucion, parcial)
            parciales.append(parcial)
        self._wals.agregar(id_ejecucion, codificar(TIPO_LOTE, entrega, lambda escritor: (
            escritor.entero(len(parciales), BYTES_U32),
            [(escritor.texto(p.palabra), escritor.entero(p.cantidad, BYTES_U64)) for p in parciales],
        )))
        self._dedup.registrar(entrega)

    def finalizar_entrada(self, entrada):
        """Publica el resultado completo y después el cierre de Q4 de esta instancia."""
        self._validar_entrada(entrada)
        ejecucion = entrada.id_ejecucion
        if self._wals.esta_cerrada(ejecucion):
            return
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
        self._wals.cerrar_ejecucion(ejecucion)
        self._dedup.cerrar_ejecucion(ejecucion)

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
        self._middleware.publicar(self.envio(
            self._grupo_salida, datos, clave_afinidad=entrada.id_ejecucion,
        ))

    def _validar_entrada(self, entrada):
        if entrada.nombre != self._entrada:
            raise ValueError('La entrada no corresponde al worker de Top N')
