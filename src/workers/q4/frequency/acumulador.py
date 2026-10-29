"""Acumulación local de aportes de palabras, separada por ejecución."""

from collections.abc import Iterator

from src.common.protocol.registros import ConteoPalabra, Palabra


class AcumuladorFrecuencias:
    """Suma una presencia por cada aporte de Ocurrencia Palabras.

    Los aportes deben llegar sin stopwords y sin repeticiones dentro de un mismo
    abstract. Una palabra repetida en distintos aportes incrementa su frecuencia.
    Cada instancia se utiliza secuencialmente dentro de un proceso y conserva
    únicamente un contador por palabra distinta de cada ejecución.
    """

    def __init__(self):
        self._frecuencias: dict[str, dict[str, int]] = {}

    def acumular(self, id_ejecucion: str, aporte: Palabra) -> None:
        """Incrementa la frecuencia local de una palabra para la ejecución indicada."""
        self._validar_ejecucion(id_ejecucion)
        self._validar_aporte(aporte)
        frecuencias = self._frecuencias.setdefault(id_ejecucion, {})
        frecuencias[aporte.palabra] = frecuencias.get(aporte.palabra, 0) + 1

    def iterar_conteos(self, id_ejecucion: str) -> Iterator[ConteoPalabra]:
        """Recorre los conteos sin copiarlos, ordenarlos ni consumirlos.

        Una ejecución sin aportes produce un recorrido vacío. El llamador no debe
        acumular ni liberar esa ejecución mientras recorre sus conteos.
        """
        self._validar_ejecucion(id_ejecucion)
        frecuencias = self._frecuencias.get(id_ejecucion, {})
        return (
            ConteoPalabra(palabra, cantidad)
            for palabra, cantidad in frecuencias.items()
        )

    def contiene_palabra(self, id_ejecucion: str, aporte: Palabra) -> bool:
        """Consulta si el aporte actualizaría un contador existente, sin crear estado."""
        self._validar_ejecucion(id_ejecucion)
        self._validar_aporte(aporte)
        return aporte.palabra in self._frecuencias.get(id_ejecucion, {})

    def liberar_ejecucion(self, id_ejecucion: str) -> None:
        """Elimina los conteos de una ejecución; repetir la operación no tiene efecto."""
        self._validar_ejecucion(id_ejecucion)
        self._frecuencias.pop(id_ejecucion, None)

    @staticmethod
    def _validar_aporte(aporte):
        if type(aporte) is not Palabra:
            raise TypeError("El aporte debe ser un DTO Palabra")
        if type(aporte.palabra) is not str or not aporte.palabra.strip():
            raise ValueError("La palabra del aporte debe ser texto no vacío")

    @staticmethod
    def _validar_ejecucion(id_ejecucion: str) -> None:
        if type(id_ejecucion) is not str or not id_ejecucion.strip():
            raise ValueError("La ejecución debe tener un identificador no vacío")
