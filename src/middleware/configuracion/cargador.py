"""Lectura y validación de comunicación desde JSON y variables de entorno."""

import json
import os
from pathlib import Path
import re
from typing import Union

from ...common.protocol.registros import Esquema

from .modelos import (
    ConfiguracionBroker,
    ConfiguracionComunicacion,
    ConfiguracionGrupo,
    ErrorConfiguracion,
    IdentidadProceso,
    LimitesComunicacion,
    Ruta,
)


PREFIJO_ENTORNO = "WIKI_MW_"
PUERTO_MAXIMO = 65535  # Mayor puerto TCP, representado con 16 bits.
MAXIMO_ENTERO_CORTO_AMQP = 65535  # Máximo u16 para heartbeat y prefetch de AMQP.
PATRON_IDENTIFICADOR = re.compile(r"[a-z][a-z0-9_]*")
CAMPOS_POR_SECCION = {
    "broker": (
        "host", "puerto", "usuario", "contrasena", "virtual_host",
        "latido_segundos", "timeout_conexion_segundos", "timeout_bloqueo_segundos",
    ),
    "proceso": ("grupo", "instancia"),
    "limites": (
        "mensajes_sin_confirmar", "max_mensajes_por_cola", "max_bytes_por_mensaje",
    ),
}
CAMPOS_RUTA = ("nombre", "origen", "destino", "entrada", "esquemas")


def cargar_configuracion(ruta: Union[str, Path]) -> ConfiguracionComunicacion:
    """Carga y valida la configuración de comunicación desde un JSON UTF-8.

    Las variables WIKI_MW_ del proceso sobrescriben los campos admitidos.
    Eleva ErrorConfiguracion ante errores de lectura, valores o topología.
    """
    lector = _leer_json(ruta)
    _validar_secciones(lector)
    _validar_campos_y_nombres_de_ruta(lector)
    _aplicar_variables_de_entorno(lector, dict(os.environ))
    configuracion = ConfiguracionComunicacion(
        broker=_leer_broker(lector["broker"]),
        proceso=IdentidadProceso(
            grupo=_leer_identificador(lector["proceso"], "grupo"),
            instancia=_leer_entero_acotado(lector["proceso"], "instancia", minimo=0),
        ),
        limites=_leer_limites(lector["limites"]),
        grupos=tuple(
            ConfiguracionGrupo(
                nombre=_validar_identificador(nombre, "grupos"),
                instancias=_leer_entero_acotado(lector["grupos"], nombre),
            )
            for nombre in lector["grupos"]
        ),
        rutas=tuple(
            _leer_ruta(ruta) for ruta in lector["rutas"]
        ),
    )
    _validar_topologia(configuracion)
    return configuracion


def _objeto_sin_duplicados(pares):
    """Construye un objeto JSON y rechaza claves repetidas."""
    objeto = {}
    for clave, valor in pares:
        if clave in objeto:
            raise ErrorConfiguracion("El JSON contiene claves duplicadas")
        objeto[clave] = valor
    return objeto


def _rechazar_constante_no_finita(_valor):
    """Rechaza las constantes NaN e infinito del lector JSON."""
    raise ErrorConfiguracion("El JSON no admite NaN ni infinitos")


def _leer_json(ruta):
    """Lee el archivo JSON UTF-8 y traduce errores sin exponer credenciales."""
    try:
        with open(ruta, encoding="utf-8") as archivo:
            return json.load(
                archivo, object_pairs_hook=_objeto_sin_duplicados,
                parse_constant=_rechazar_constante_no_finita,
            )
    except (OSError, UnicodeError) as error:
        raise ErrorConfiguracion("No se pudo leer el archivo de configuración UTF-8") from error
    except json.JSONDecodeError:
        # No se incluye el contenido del archivo para evitar exponer credenciales.
        raise ErrorConfiguracion("Sintaxis JSON inválida") from None


def _comprobar_campos(objeto, esperados, contexto):
    """Exige los campos indicados y rechaza los desconocidos."""
    if type(objeto) is not dict:
        raise ErrorConfiguracion(f"{contexto} debe ser un objeto JSON")
    faltantes = set(esperados) - objeto.keys()
    if faltantes:
        raise ErrorConfiguracion(f"Faltan campos en {contexto}: {', '.join(sorted(faltantes))}")
    if objeto.keys() - set(esperados):
        raise ErrorConfiguracion(f"Hay campos desconocidos en {contexto}")


def _validar_secciones(lector):
    """Valida las secciones obligatorias y los nombres de grupos."""
    _comprobar_campos(lector, (*CAMPOS_POR_SECCION, "grupos", "rutas"), "configuración")
    if type(lector["grupos"]) is not dict or not lector["grupos"]:
        raise ErrorConfiguracion("grupos debe ser un objeto no vacío")
    if type(lector["rutas"]) is not list or not lector["rutas"]:
        raise ErrorConfiguracion("rutas debe ser una lista no vacía")
    for nombre in lector["grupos"]:
        _validar_identificador(nombre, "grupos")


def _aplicar_variables_de_entorno(lector, entorno):
    # Cada variable permitida apunta a una sección y un campo de la configuración.
    """Sobrescribe los campos permitidos con las variables WIKI_MW_."""
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
        campos_texto = {"host", "usuario", "contrasena", "virtual_host", "grupo"}
        if seccion == "grupos" or campo not in campos_texto:
            try:
                valor = int(valor)
            except ValueError:
                raise ErrorConfiguracion(f"{variable} debe ser un entero") from None
        lector[seccion][campo] = valor


def _validar_campos_y_nombres_de_ruta(lector):
    """Valida los campos de cada sección y exige nombres de ruta únicos."""
    for seccion, campos in CAMPOS_POR_SECCION.items():
        _comprobar_campos(lector[seccion], campos, seccion)
    nombres = set()
    for ruta in lector["rutas"]:
        _comprobar_campos(ruta, CAMPOS_RUTA, "ruta")
        nombre = _leer_identificador(ruta, "nombre")
        if nombre in nombres:
            raise ErrorConfiguracion("Hay nombres de ruta repetidos")
        nombres.add(nombre)


def _leer_texto_obligatorio(seccion, campo):
    """Obtiene un texto no vacío de una sola línea."""
    valor = seccion[campo]
    if type(valor) is not str or not valor.strip() or any(caracter in valor for caracter in ("\n", "\r", "\x00")):
        raise ErrorConfiguracion(f"{campo} debe contener texto no vacío de una línea")
    return valor


def _leer_entero_acotado(seccion, campo, minimo=1, maximo=None):
    """Obtiene un entero dentro del intervalo permitido."""
    valor = seccion[campo]
    if type(valor) is not int:
        raise ErrorConfiguracion(f"{campo} debe ser un entero JSON")
    if valor < minimo or (maximo is not None and valor > maximo):
        raise ErrorConfiguracion(f"{campo} está fuera del rango permitido")
    return valor


def _validar_identificador(valor, campo):
    """Exige un identificador de letras minúsculas ASCII, números y guion bajo."""
    if type(valor) is not str or not PATRON_IDENTIFICADOR.fullmatch(valor):
        raise ErrorConfiguracion(
            f"{campo} debe comenzar con una letra minúscula y usar letras ASCII, números o guion bajo"
        )
    return valor


def _leer_identificador(seccion, campo):
    """Obtiene y valida un identificador de la sección."""
    return _validar_identificador(_leer_texto_obligatorio(seccion, campo), campo)


def _leer_broker(seccion):
    """Construye los parámetros validados de conexión al broker."""
    return ConfiguracionBroker(
        host=_leer_texto_obligatorio(seccion, "host"),
        puerto=_leer_entero_acotado(seccion, "puerto", maximo=PUERTO_MAXIMO),
        usuario=_leer_texto_obligatorio(seccion, "usuario"),
        contrasena=_leer_texto_obligatorio(seccion, "contrasena"),
        virtual_host=_leer_texto_obligatorio(seccion, "virtual_host"),
        latido_segundos=_leer_entero_acotado(
            seccion, "latido_segundos", maximo=MAXIMO_ENTERO_CORTO_AMQP
        ),
        timeout_conexion_segundos=_leer_entero_acotado(seccion, "timeout_conexion_segundos"),
        timeout_bloqueo_segundos=_leer_entero_acotado(seccion, "timeout_bloqueo_segundos"),
    )


def _leer_limites(seccion):
    """Construye los límites validados de entregas, colas y mensajes."""
    return LimitesComunicacion(
        mensajes_sin_confirmar=_leer_entero_acotado(
            seccion, "mensajes_sin_confirmar", maximo=MAXIMO_ENTERO_CORTO_AMQP
        ),
        max_mensajes_por_cola=_leer_entero_acotado(seccion, "max_mensajes_por_cola"),
        max_bytes_por_mensaje=_leer_entero_acotado(seccion, "max_bytes_por_mensaje"),
    )


def _leer_ruta(seccion):
    """Construye una ruta con sus grupos, entrada y esquemas admitidos."""
    return Ruta(
        nombre=_leer_identificador(seccion, "nombre"),
        origen=_leer_identificador(seccion, "origen"),
        destino=_leer_identificador(seccion, "destino"),
        entrada=_leer_identificador(seccion, "entrada"),
        esquemas=_leer_esquemas(seccion),
    )


def _leer_esquemas(seccion):
    """Lee una lista no vacía de nombres de esquemas sin duplicados."""
    nombres = seccion["esquemas"]
    if type(nombres) is not list or not nombres or any(type(nombre) is not str for nombre in nombres):
        raise ErrorConfiguracion("esquemas debe ser una lista no vacía de textos")
    if len(set(nombres)) != len(nombres):
        raise ErrorConfiguracion("La ruta contiene esquemas repetidos")
    try:
        return tuple(Esquema[nombre] for nombre in nombres)
    except KeyError as error:
        raise ErrorConfiguracion("La ruta contiene un esquema desconocido") from error


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
