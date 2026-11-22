from src.workers.base_worker import WorkerBase
from src.workers.deduplicacion import DedupDesdeCompartida
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.primitivas import BYTES_U8, BYTES_U32
from src.common.protocol.registros import Esquema, Idioma, Lote, Palabra


GRUPO_RESULTADOS = "gateway_resultados"
ENTRADA_RESULTADOS = "q3"
ENTRADA = "palabras_idioma"
TIPO_LOTE = 1  # registro del WAL: identidad del lote + los pares (palabra, idioma) que eran nuevos

# Estado de cada palabra: en qué idiomas apareció y si ya se envió como resultado
_EN, _FR, _ENVIADA = 1, 2, 4
_AMBOS = _EN | _FR
_BIT_IDIOMA = {Idioma.INGLES: _EN, Idioma.FRANCES: _FR}


class IntersectionQ3(WorkerBase):
    """
    Palabras presentes tanto en artículos en inglés como en francés.

    Recibe pares (palabra, idioma). Las ocurrencias de una misma palabra llegan siempre a
    la misma instancia (afinidad por palabra), así que cada instancia solo guarda su porción
    del vocabulario y la unión de sus resultados es la intersección completa.
    Una palabra se publica en cuanto se ve en ambos idiomas y no se vuelve a enviar.

    Los resultados de un lote se publican al terminar de procesarlo, así dependen solo del
    estado persistido y del lote. El WAL guarda de cada lote únicamente los pares que
    cambiaron el estado; al arrancar se reaplican en orden.
    """

    def __init__(self, ruta_configuracion: str, directorio_wal: str = None):
        super().__init__(ruta_configuracion)
        self._estados = {}  # id_ejecucion -> {palabra: combinación de _EN, _FR y _ENVIADA}
        self.registrar_limpieza(self._estados.clear)
        # Recibe de word_occurrences, que usa cola compartida
        self._dedup = DedupDesdeCompartida()
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        self._recuperar()

    def _recuperar(self) -> None:
        for id_ejecucion, registros in self._wals.recuperar():
            for datos in registros:
                _, identidad, lector = decodificar(datos)
                for _ in range(lector.entero(BYTES_U32)):
                    palabra, idioma = lector.texto(), Idioma(lector.entero(BYTES_U8))
                    self._agregar_par(id_ejecucion, palabra, idioma)
                lector.finalizar()
                self._dedup.registrar(entrega_de(Entrada(id_ejecucion, ENTRADA), identidad))

    def _agregar_par(self, id_ejecucion: str, palabra: str, idioma: Idioma):
        """Anota el par en el estado. Devuelve None si no aportaba nada; si no, si completó la palabra."""
        estado_palabras = self._estados.setdefault(id_ejecucion, {})
        estado = estado_palabras.get(palabra, 0)
        bit = _BIT_IDIOMA[idioma]
        if estado & bit:
            return None
        estado |= bit
        completa = estado & _AMBOS == _AMBOS
        if completa:
            estado |= _ENVIADA
        estado_palabras[palabra] = estado
        return completa

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q3_intersection", id_ejecucion)
        entrega = self.middleware.entrega_actual
        if self._wals.esta_cerrada(id_ejecucion) or self._dedup.es_duplicado(entrega):
            return

        pares_nuevos = []   # los que cambiaron el estado: es lo que guarda el WAL
        resultados = []     # palabras que con este lote quedaron en ambos idiomas
        for registro in datos.contenido.registros:
            self.comprobar_detencion()
            completa = self._agregar_par(id_ejecucion, registro.palabra, registro.idioma)
            if completa is not None:
                pares_nuevos.append(registro)
                if completa:
                    resultados.append(Palabra(registro.palabra))

        self._publicar_resultados(id_ejecucion, resultados)
        self._wals.agregar(id_ejecucion, codificar(
            TIPO_LOTE, entrega, lambda escritor: _escribir_pares(escritor, pares_nuevos),
        ))
        self._dedup.registrar(entrega)

    def _publicar_resultados(self, id_ejecucion: str, palabras: list) -> None:
        """Publica las palabras en tantos mensajes como haga falta para respetar el tamaño máximo."""
        entrada = Entrada(id_ejecucion, ENTRADA_RESULTADOS)
        pendientes = []
        presupuesto = self.middleware.crear_presupuesto_lote(entrada, Esquema.PALABRA)
        for palabra in palabras:
            if not presupuesto.admitir(palabra):
                self._publicar(entrada, pendientes)
                pendientes = []
                presupuesto = self.middleware.crear_presupuesto_lote(entrada, Esquema.PALABRA)
                if not presupuesto.admitir(palabra):
                    raise ValueError("Una palabra no cabe en un lote vacío")
            pendientes.append(palabra)
        self._publicar(entrada, pendientes)

    def _publicar(self, entrada: Entrada, palabras: list) -> None:
        if palabras:
            lote = Lote(Esquema.PALABRA, tuple(palabras))
            self.middleware.publicar(self.envio(GRUPO_RESULTADOS, Datos(entrada, lote)))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Cierra la salida hacia el gateway y la ejecución."""
        id_ejecucion = entrada.id_ejecucion
        if self._wals.esta_cerrada(id_ejecucion):
            return
        self.middleware.finalizar_entrada(GRUPO_RESULTADOS, Entrada(id_ejecucion, ENTRADA_RESULTADOS))
        self._wals.cerrar_ejecucion(id_ejecucion)
        self._estados.pop(id_ejecucion, None)
        self._dedup.cerrar_ejecucion(id_ejecucion)


def _escribir_pares(escritor, pares) -> None:
    escritor.entero(len(pares), BYTES_U32)
    for par in pares:
        escritor.texto(par.palabra)
        escritor.entero(par.idioma.value, BYTES_U8)
