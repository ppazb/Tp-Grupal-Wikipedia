"""Lectura y validación de comunicación desde JSON y variables de entorno."""

import json
import os
from pathlib import Path
import re
from typing import Union

from ..common.protocol.registros import Esquema

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
    ),
}
CAMPOS_RUTA = ("nombre", "origen", "destino", "entrada", "esquemas")


def cargar_configuracion(ruta: Union[str, Path]) -> ConfiguracionComunicacion:
    """Carga y valida la configuración de comunicación desde un JSON UTF-8.

    Las variables WIKI_MW_ del proceso sobrescriben los campos admitidos.
    Eleva ErrorConfiguracion ante errores de lectura, valores o topología.
    """
    lector = _leer_archivo(ruta)
    _validar_secciones(lector)
    _validar_campos(lector)
    _aplicar_entorno(lector, dict(os.environ))
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
            _leer_ruta(ruta) for ruta in lector["rutas"]
        ),
    )
    _validar_topologia(configuracion)
    return configuracion


def _objeto_sin_duplicados(pares):
    objeto = {}
    for clave, valor in pares:
        if clave in objeto:
            raise ErrorConfiguracion("El JSON contiene claves duplicadas")
        objeto[clave] = valor
    return objeto


def _rechazar_constante(valor):
    raise ErrorConfiguracion("El JSON no admite NaN ni infinitos")


def _leer_archivo(ruta):
    try:
        with open(ruta, encoding="utf-8") as archivo:
            return json.load(
                archivo, object_pairs_hook=_objeto_sin_duplicados,
                parse_constant=_rechazar_constante,
            )
    except (OSError, UnicodeError) as error:
        raise ErrorConfiguracion("No se pudo leer el archivo de configuración UTF-8") from error
    except json.JSONDecodeError:
        # No se incluye el contenido del archivo para evitar exponer credenciales.
        raise ErrorConfiguracion("Sintaxis JSON inválida") from None


def _comprobar_campos(objeto, esperados, contexto):
    if type(objeto) is not dict:
        raise ErrorConfiguracion(f"{contexto} debe ser un objeto JSON")
    faltantes = set(esperados) - objeto.keys()
    if faltantes:
        raise ErrorConfiguracion(f"Faltan campos en {contexto}: {', '.join(sorted(faltantes))}")
    if objeto.keys() - set(esperados):
        raise ErrorConfiguracion(f"Hay campos desconocidos en {contexto}")


def _validar_secciones(lector):
    _comprobar_campos(lector, (*CAMPOS_POR_SECCION, "grupos", "rutas"), "configuración")
    if type(lector["grupos"]) is not dict or not lector["grupos"]:
        raise ErrorConfiguracion("grupos debe ser un objeto no vacío")
    if type(lector["rutas"]) is not list or not lector["rutas"]:
        raise ErrorConfiguracion("rutas debe ser una lista no vacía")
    for nombre in lector["grupos"]:
        _validar_identificador(nombre, "grupos")


def _aplicar_entorno(lector, entorno):
    # Cada variable permitida apunta a una sección y un campo de la configuración.
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


def _validar_campos(lector):
    for seccion, campos in CAMPOS_POR_SECCION.items():
        _comprobar_campos(lector[seccion], campos, seccion)
    nombres = set()
    for ruta in lector["rutas"]:
        _comprobar_campos(ruta, CAMPOS_RUTA, "ruta")
        nombre = _identificador(ruta, "nombre")
        if nombre in nombres:
            raise ErrorConfiguracion("Hay nombres de ruta repetidos")
        nombres.add(nombre)


def _texto(seccion, campo):
    valor = seccion[campo]
    if type(valor) is not str or not valor.strip() or any(caracter in valor for caracter in ("\n", "\r", "\x00")):
        raise ErrorConfiguracion(f"{campo} debe contener texto no vacío de una línea")
    return valor


def _entero(seccion, campo, minimo=1, maximo=None):
    valor = seccion[campo]
    if type(valor) is not int:
        raise ErrorConfiguracion(f"{campo} debe ser un entero JSON")
    if valor < minimo or (maximo is not None and valor > maximo):
        raise ErrorConfiguracion(f"{campo} está fuera del rango permitido")
    return valor


def _validar_identificador(valor, campo):
    if type(valor) is not str or not PATRON_IDENTIFICADOR.fullmatch(valor):
        raise ErrorConfiguracion(
            f"{campo} debe comenzar con una letra minúscula y usar letras ASCII, números o guion bajo"
        )
    return valor


def _identificador(seccion, campo):
    return _validar_identificador(_texto(seccion, campo), campo)


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
    )


def _leer_ruta(seccion):
    return Ruta(
        nombre=_identificador(seccion, "nombre"),
        origen=_identificador(seccion, "origen"),
        destino=_identificador(seccion, "destino"),
        entrada=_identificador(seccion, "entrada"),
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
