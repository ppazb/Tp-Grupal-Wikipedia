
from ...common.protocol.registros import Articulo, Idioma


IDIOMAS = {"en": Idioma.INGLES, "fr": Idioma.FRANCES}

"""Construcción de artículos a partir de las filas crudas que envía el cliente."""

def _texto_requerido(valor, campo, indice):
    if not isinstance(valor, str) or not valor.strip():
        raise ValueError(f"El campo {campo} de la fila {indice} está vacío")
    return valor


def articulo_desde_fila(fila, indice):
    """Arma un Articulo desde una fila decodificada (todos los campos son texto; vacío = nulo)."""
    idioma = fila["in_language"]
    if idioma not in IDIOMAS:
        raise ValueError(f"Idioma no admitido en la fila {indice}: {idioma!r}")

    return Articulo(
        url=_texto_requerido(fila["url"], "url", indice),
        nombre=_texto_requerido(fila["name"], "name", indice),
        fecha=_texto_requerido(fila["date_modified"], "date_modified", indice),
        idioma=IDIOMAS[idioma],
        abstract=fila["abstract"],
        secciones=fila["sections"],
        url_imagen=fila["image_content_url"],
    )
