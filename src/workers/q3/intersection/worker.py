import logging

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Idioma, Lote, Palabra

logger = logging.getLogger(__name__)

GRUPO_RESULTADOS = "gateway_resultados"
ENTRADA_RESULTADOS = "q3"

# Estado de cada palabra: en qué idiomas apareció y si ya se envió como resultado
_EN, _FR, _ENVIADA = 1, 2, 4
_AMBOS = _EN | _FR
_BIT_IDIOMA = {Idioma.INGLES: _EN, Idioma.FRANCES: _FR}


class _Ejecucion:
    """Lo que recuerda la instancia de una ejecución: palabras vistas y resultados sin enviar."""

    def __init__(self):
        self.estado = {}      # palabra -> combinación de _EN, _FR y _ENVIADA
        self.pendientes = []  # palabras compartidas aún sin publicar
        self.presupuesto = None


class IntersectionQ3(WorkerBase):
    """
    Palabras presentes tanto en artículos en inglés como en francés.

    Recibe pares (palabra, idioma). Las ocurrencias de una misma palabra llegan siempre a
    la misma instancia (afinidad por hash de la palabra), así que cada instancia solo
    guarda su porción del vocabulario y la unión de sus resultados es la intersección completa.
    Una palabra se publica en cuanto se ve en ambos idiomas y no se vuelve a enviar.
    """

    def __init__(self, ruta_configuracion: str):
        super().__init__(ruta_configuracion)
        self._ejecuciones = {}

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q3_intersection", id_ejecucion)
        ejecucion = self._ejecuciones.setdefault(id_ejecucion, _Ejecucion())

        for registro in datos.contenido.registros:
            bit = _BIT_IDIOMA[registro.idioma]
            estado = ejecucion.estado.get(registro.palabra, 0)
            if estado & bit:
                continue
            estado |= bit
            if estado & _AMBOS == _AMBOS:
                estado |= _ENVIADA
                self._agregar_resultado(id_ejecucion, ejecucion, Palabra(registro.palabra))
            ejecucion.estado[registro.palabra] = estado

    def _agregar_resultado(self, id_ejecucion: str, ejecucion: _Ejecucion, palabra: Palabra) -> None:
        """Junta resultados hasta llenar el mensaje; solo entonces los publica."""
        entrada = Entrada(id_ejecucion, ENTRADA_RESULTADOS)
        if ejecucion.presupuesto is None:
            ejecucion.presupuesto = self.middleware.crear_presupuesto_lote(entrada, Esquema.PALABRA)
        if not ejecucion.presupuesto.admitir(palabra):
            self._publicar_pendientes(id_ejecucion, ejecucion)
            ejecucion.presupuesto = self.middleware.crear_presupuesto_lote(entrada, Esquema.PALABRA)
            if not ejecucion.presupuesto.admitir(palabra):
                raise ValueError("Una palabra no cabe en un lote vacío")
        ejecucion.pendientes.append(palabra)

    def _publicar_pendientes(self, id_ejecucion: str, ejecucion: _Ejecucion) -> None:
        if ejecucion.pendientes:
            entrada = Entrada(id_ejecucion, ENTRADA_RESULTADOS)
            lote = Lote(Esquema.PALABRA, tuple(ejecucion.pendientes))
            self.middleware.publicar(GRUPO_RESULTADOS, Datos(entrada, lote))
        ejecucion.pendientes = []
        ejecucion.presupuesto = None

    def al_finalizar(self, entrada: Entrada) -> None:
        """Publica lo que quedó pendiente y cierra la salida hacia el gateway."""
        id_ejecucion = entrada.id_ejecucion
        ejecucion = self._ejecuciones.pop(id_ejecucion, _Ejecucion())
        logger.info(
            "q3_intersection: cierre (EOF) recibido para '%s'. Palabras vistas en esta instancia: %d",
            id_ejecucion, len(ejecucion.estado),
        )
        self._publicar_pendientes(id_ejecucion, ejecucion)
        self.middleware.finalizar_entrada(GRUPO_RESULTADOS, Entrada(id_ejecucion, ENTRADA_RESULTADOS))
