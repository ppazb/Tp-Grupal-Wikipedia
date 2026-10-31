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
NUMBER_REGEX = r'[+-]?((\d+\.?\d*)|(\.\d+))'


def extract_non_stopwords_from_abstract(abstract: str) -> List[str]:
    """Extrae palabras del abstract filtrando stopwords"""
    if not isinstance(abstract, str):
        return []
    words = re.findall(WORD_REGEX, abstract.lower())
    return [word for word in words if word not in stop_words and not re.fullmatch(NUMBER_REGEX, word)]


def extraer_palabras_unicas(abstract: str) -> Set[str]:
    """Devuelve las palabras únicas no triviales de un abstract.
    
    Dentro de un mismo abstract las palabras se cuentan una sola vez para Q4 y Q5.
    """
    return set(extract_non_stopwords_from_abstract(abstract))
