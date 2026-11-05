from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, ResultadoQ1, Idioma


class FilterLangQ1(WorkerBase):
    """
    Filtra articulos conservando únicamente artículos en idioma ingles  y
    emite el  mensaje  con unicamente el resultado necesario(url, fecha).
    """

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q1_filter_lang", datos.entrada.id_ejecucion)
        registros_ingles = []
        for referencia in datos.contenido.registros:
            self.comprobar_detencion()
            if referencia.idioma == Idioma.INGLES:
                # Me quedo solo con URL y fecha
                registros_ingles.append(
                    ResultadoQ1(referencia.url, referencia.fecha)
                )

        id_ejecucion = datos.entrada.id_ejecucion
        if registros_ingles:
            nueva_entrada = Entrada(id_ejecucion, "filtrados_ing_q1")
            nuevo_lote = Lote(Esquema.RESULTADO_Q1, tuple(registros_ingles))
            self.middleware.publicar("q1_formatter", Datos(nueva_entrada, nuevo_lote))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga (EOF) hacia el formateador (su nodo siguiente)."""
        id_ejecucion = entrada.id_ejecucion
        nueva_entrada = Entrada(id_ejecucion, "filtrados_ing_q1")
        self.middleware.finalizar_entrada("q1_formatter", nueva_entrada)
