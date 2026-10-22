"""Lectura y validación de comunicación desde INI y variables de entorno."""

import configparser
import os
from pathlib import Path
import re
from typing import Union

from .configuracion import (
    ConfiguracionBroker,
    ConfiguracionComunicacion,
    ConfiguracionGrupo,
    ErrorConfiguracion,
    IdentidadProceso,
    LimitesComunicacion,
    Ruta,
)


PREFIJO_ENTORNO = "WIKI_MW_"
PUERTO_MAXIMO = 65535
MAXIMO_ENTERO_CORTO_AMQP = 65535
PATRON_IDENTIFICADOR = re.compile(r"[a-z][a-z0-9_]*")
CAMPOS_POR_SECCION = {
    "broker": (
        "host", "puerto", "usuario", "contrasena", "virtual_host",
        "latido_segundos", "timeout_conexion_segundos", "timeout_bloqueo_segundos",
    ),
    "proceso": ("grupo", "instancia"),
    "limites": (
        "mensajes_sin_confirmar", "max_mensajes_por_cola", "max_bytes_por_mensaje",
        "max_envios_por_publicacion",
    ),
}
CAMPOS_RUTA = ("origen", "destino", "entrada")


def cargar_configuracion(ruta: Union[str, Path]) -> ConfiguracionComunicacion:
    """Carga y valida la configuración de comunicación desde un INI UTF-8.

    Las variables WIKI_MW_ del proceso sobrescriben los campos admitidos.
    Eleva ErrorConfiguracion ante errores de lectura, valores o topología.
    """
    lector = _leer_archivo(ruta)
    _validar_secciones(lector)
    _aplicar_entorno(lector, dict(os.environ))
    _validar_campos(lector)
    configuracion = ConfiguracionComunicacion(
        broker=_leer_broker(lector["broker"]),
        proceso=IdentidadProceso(
            grupo=_identificador(lector["proceso"], "grupo"),
            instancia=_entero(lector["proceso"], "instancia", minimo=0),
        ),
        limites=_leer_limites(lector["limites"]),
        grupos=tuple(
            ConfiguracionGrupo(
                nombre=_validar_identificador(nombre, "grupos"),
                instancias=_entero(lector["grupos"], nombre),
            )
            for nombre in lector["grupos"]
        ),
        rutas=tuple(
            _leer_ruta(lector[seccion])
            for seccion in lector.sections() if seccion.startswith("ruta:")
        ),
    )
    _validar_topologia(configuracion)
    return configuracion


def _leer_archivo(ruta):
    lector = configparser.ConfigParser(interpolation=None, empty_lines_in_values=False)
    lector.optionxform = str
    try:
        with open(ruta, encoding="utf-8") as archivo:
            lector.read_file(archivo)
    except (OSError, UnicodeError) as error:
        raise ErrorConfiguracion("No se pudo leer el archivo de configuración UTF-8") from error
    except configparser.Error:
        # ConfigParser incluye líneas originales en sus errores. Se omite esa
        # causa para que una configuración mal escrita no exponga credenciales.
        raise ErrorConfiguracion(
            "Sintaxis INI inválida o secciones/opciones duplicadas"
        ) from None
    return lector


def _validar_secciones(lector):
    if lector.defaults():
        raise ErrorConfiguracion("No se admiten opciones en DEFAULT")
    obligatorias = set(CAMPOS_POR_SECCION) | {"grupos"}
    faltantes = obligatorias - set(lector.sections())
    if faltantes:
        raise ErrorConfiguracion(f"Faltan secciones: {', '.join(sorted(faltantes))}")
    for seccion in lector.sections():
        if seccion not in obligatorias and not seccion.startswith("ruta:"):
            raise ErrorConfiguracion("Se encontró una sección desconocida")
    if not lector["grupos"]:
        raise ErrorConfiguracion("Debe declararse al menos un grupo")
    if not any(seccion.startswith("ruta:") for seccion in lector.sections()):
        raise ErrorConfiguracion("Debe declararse al menos una ruta")


def _aplicar_entorno(lector, entorno):
    # Cada variable permitida apunta a una sección y un campo del INI.
    destinos = {
        f"{PREFIJO_ENTORNO}{seccion.upper()}_{campo.upper()}": (seccion, campo)
        for seccion, campos in CAMPOS_POR_SECCION.items() for campo in campos
    }
    destinos.update({
        f"{PREFIJO_ENTORNO}GRUPO_{nombre.upper()}_INSTANCIAS": ("grupos", nombre)
        for nombre in lector["grupos"]
    })
    for variable, valor in entorno.items():
        if not variable.startswith(PREFIJO_ENTORNO):
            continue
        if variable not in destinos:
            raise ErrorConfiguracion(f"Variable de entorno desconocida: {variable}")
        seccion, campo = destinos[variable]
        lector[seccion][campo] = valor


def _validar_campos(lector):
    for seccion in lector.sections():
        if seccion == "grupos":
            continue
        esperados = set(CAMPOS_POR_SECCION.get(seccion, CAMPOS_RUTA))
        presentes = set(lector[seccion])
        faltantes = esperados - presentes
        if faltantes:
            raise ErrorConfiguracion(
                f"Faltan campos en [{seccion}]: {', '.join(sorted(faltantes))}"
            )
        if presentes - esperados:
            raise ErrorConfiguracion(f"Hay campos desconocidos en [{seccion}]")


def _texto(seccion, campo):
    valor = seccion[campo]
    if not valor.strip() or any(caracter in valor for caracter in ("\n", "\r", "\x00")):
        raise ErrorConfiguracion(f"[{seccion.name}] {campo} debe contener texto no vacío de una línea")
    return valor


def _entero(seccion, campo, minimo=1, maximo=None):
    try:
        valor = int(seccion[campo])
    except ValueError:
        raise ErrorConfiguracion(f"[{seccion.name}] {campo} debe ser un entero") from None
    if valor < minimo or (maximo is not None and valor > maximo):
        raise ErrorConfiguracion(f"[{seccion.name}] {campo} está fuera del rango permitido")
    return valor


def _validar_identificador(valor, campo):
    if not PATRON_IDENTIFICADOR.fullmatch(valor):
        raise ErrorConfiguracion(
            f"{campo} debe comenzar con una letra minúscula y usar letras ASCII, números o guion bajo"
        )
    return valor


def _identificador(seccion, campo):
    return _validar_identificador(_texto(seccion, campo), f"[{seccion.name}] {campo}")


def _leer_broker(seccion):
    return ConfiguracionBroker(
        host=_texto(seccion, "host"),
        puerto=_entero(seccion, "puerto", maximo=PUERTO_MAXIMO),
        usuario=_texto(seccion, "usuario"),
        contrasena=_texto(seccion, "contrasena"),
        virtual_host=_texto(seccion, "virtual_host"),
        latido_segundos=_entero(
            seccion, "latido_segundos", maximo=MAXIMO_ENTERO_CORTO_AMQP
        ),
        timeout_conexion_segundos=_entero(seccion, "timeout_conexion_segundos"),
        timeout_bloqueo_segundos=_entero(seccion, "timeout_bloqueo_segundos"),
    )


def _leer_limites(seccion):
    return LimitesComunicacion(
        mensajes_sin_confirmar=_entero(
            seccion, "mensajes_sin_confirmar", maximo=MAXIMO_ENTERO_CORTO_AMQP
        ),
        max_mensajes_por_cola=_entero(seccion, "max_mensajes_por_cola"),
        max_bytes_por_mensaje=_entero(seccion, "max_bytes_por_mensaje"),
        max_envios_por_publicacion=_entero(seccion, "max_envios_por_publicacion"),
    )


def _leer_ruta(seccion):
    return Ruta(
        nombre=_validar_identificador(seccion.name.partition(":")[2], "nombre de ruta"),
        origen=_identificador(seccion, "origen"),
        destino=_identificador(seccion, "destino"),
        entrada=_identificador(seccion, "entrada"),
    )


def _validar_topologia(configuracion):
    """Comprueba identidad local, referencias entre grupos y rutas repetidas."""
    grupo_local = configuracion.obtener_grupo(configuracion.proceso.grupo)
    if configuracion.proceso.instancia >= grupo_local.instancias:
        raise ErrorConfiguracion("[proceso] instancia excede las réplicas del grupo")
    conexiones = set()
    for ruta in configuracion.rutas:
        configuracion.obtener_grupo(ruta.origen)
        configuracion.obtener_grupo(ruta.destino)
        conexion = (ruta.origen, ruta.destino, ruta.entrada)
        if conexion in conexiones:
            raise ErrorConfiguracion("Hay rutas duplicadas para el mismo origen, destino y entrada")
        conexiones.add(conexion)
