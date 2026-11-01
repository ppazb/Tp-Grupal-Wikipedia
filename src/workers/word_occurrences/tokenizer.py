import logging
import re
from typing import List, Set

logger = logging.getLogger(__name__)

# Carga de stopwords usando NLTK (inglés y francés) con fallback idéntico
try:
    import nltk
    from nltk.corpus import stopwords
    try:
        stop_words = set(stopwords.words("english")) | set(stopwords.words("french"))
    except LookupError:
        try:
            nltk.download("stopwords", quiet=True)
            stop_words = set(stopwords.words("english")) | set(stopwords.words("french"))
        except Exception:
            nltk.download("all", quiet=True)
            stop_words = set(stopwords.words("english")) | set(stopwords.words("french"))
except Exception as error:
    logger.debug("NLTK no disponible o sin conexión (%s)", error)

# Expresiones regulares
WORD_REGEX = r'\b\w+\b'
NUMBER_REGEX = r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)'


def extract_non_stopwords_from_abstract(abstract: str, comprobar_detencion=None) -> List[str]:
    """Extrae palabras del abstract filtrando stopwords"""
    if not isinstance(abstract, str):
        return []
    palabras = []
    for coincidencia in re.finditer(WORD_REGEX, abstract.lower()):
        if comprobar_detencion is not None:
            comprobar_detencion()
        palabra = coincidencia.group()
        if palabra not in stop_words and not re.fullmatch(NUMBER_REGEX, palabra):
            palabras.append(palabra)
    return palabras


def extraer_palabras_unicas(abstract: str, comprobar_detencion=None) -> Set[str]:
    """Devuelve las palabras únicas no triviales de un abstract.
    
    Dentro de un mismo abstract las palabras se cuentan una sola vez para Q4 y Q5.
    """
    unicas = set()
    for palabra in extract_non_stopwords_from_abstract(abstract, comprobar_detencion):
        if comprobar_detencion is not None:
            comprobar_detencion()
        unicas.add(palabra)
    return unicas
