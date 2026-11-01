#!/usr/bin/env python3
"""
Simulador de Gateway para prueba punta a punta de Query 1
  1. mock de 'gateway' para enviar artículos crudos hacia 'date_router'
  2. date_router filtra fechas <= 2020 y envía 'historicos_q1' hacia 'q1_filter_lang'
  3. q1_filter_lang filtra idioma inglés y envía hacia 'q1_formatter'
  4. q1_formatter formatea a (url, fecha) y publica en 'gateway_resultados'
  5. mock de 'gateway_resultados' para recibir los resultados y validar
"""

from dataclasses import replace
import logging
import os
from pathlib import Path
import sys
import time

# Asegurar que la raiz del proyecto este en sys.path
repo_root = str(Path(__file__).resolve().parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

os.environ.setdefault("WIKI_MW_BROKER_HOST", "localhost")

from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.common.protocol.dto import Datos, Entrada, IdentidadProceso
from src.common.protocol.registros import Articulo, Idioma, Esquema, Lote

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulador_q1")


def preparar_articulos():
    """Genera artículos de prueba que cubren los casos de filtrado de Query 1."""
    articulos = (
        # Válidos para Q1 (Inglés y fecha <= 2020)
        Articulo(
            url="https://en.wikipedia.org/wiki/Ada_Lovelace",
            nombre="Ada Lovelace",
            fecha="2019-05-12T10:00:00Z",
            idioma=Idioma.INGLES,
            abstract="Matemática británica...",
            secciones=("Biography",),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Alan_Turing",
            nombre="Alan Turing",
            fecha="2020-12-31 23:59:59",
            idioma=Idioma.INGLES,
            abstract="Científico de la computación...",
            secciones=("Biography",),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Claude_Shannon",
            nombre="Claude Shannon",
            fecha="2005-01-15T08:00:00Z",
            idioma=Idioma.INGLES,
            abstract="Teoría de la información...",
            secciones=(),
            url_imagen="",
        ),
        # Descartado por fecha > 2020 (date_router)
        Articulo(
            url="https://en.wikipedia.org/wiki/Recent_AI",
            nombre="Recent AI",
            fecha="2024-10-31 12:58:40",
            idioma=Idioma.INGLES,
            abstract="Avances recientes...",
            secciones=(),
            url_imagen="",
        ),
        # Descartado por idioma != INGLES (q1_filter_lang)
        Articulo(
            url="https://fr.wikipedia.org/wiki/Marie_Curie",
            nombre="Marie Curie",
            fecha="2018-04-10T14:30:00Z",
            idioma=Idioma.FRANCES,
            abstract="Physicienne...",
            secciones=(),
            url_imagen="",
        ),
        # Descartado por ambos criterios (date_router)
        Articulo(
            url="https://fr.wikipedia.org/wiki/France_Moderne",
            nombre="France Moderne",
            fecha="2023-01-01T00:00:00Z",
            idioma=Idioma.FRANCES,
            abstract="Article moderne...",
            secciones=(),
            url_imagen="",
        ),
    )

    esperados = {
        "https://en.wikipedia.org/wiki/Ada_Lovelace",
        "https://en.wikipedia.org/wiki/Alan_Turing",
        "https://en.wikipedia.org/wiki/Claude_Shannon",
    }

    return Lote(Esquema.ARTICULO, articulos), esperados


def main():
    ruta_config = sys.argv[1] if len(sys.argv) > 1 else "config/config.json"
    logger.info("=== SIMULACIÓN GATEWAY -> QUERY 1 PIPELINE ===")
    logger.info("Cargando configuración desde: %s", ruta_config)

    configuracion_base = cargar_configuracion(ruta_config)
    # 'gateway' produce envia a date_router (ruta: gateway_a_router)
    config_gateway = replace(
        configuracion_base,
        proceso=IdentidadProceso(grupo="gateway", instancia=0),
    )
    # 'gateway_resultados' consume desde q1_formatter (ruta: q1_formatter_a_gateway)
    config_resultados = replace(
        configuracion_base,
        proceso=IdentidadProceso(grupo="gateway_resultados", instancia=0),
    )

    lote_prueba, esperados = preparar_articulos()
    id_ejecucion = f"q1_test_{int(time.time())}"

    def manejar_errores(datos_error):
        logger.error("Notificación de rechazo/error recibida en Gateway: %s", datos_error)

    # Abrir middleware consumidor para declarar topología y recibir resultados
    with MiddlewareRabbitMQ(config_resultados, informar_error=manejar_errores) as mw_resultados:
        # Abrir middleware productor  para publicar datos a date_router
        with MiddlewareRabbitMQ(config_gateway) as mw_gateway:
            total_enviados = len(lote_prueba.registros)
            logger.info("Publicando lote de %d artículos crudos hacia 'date_router' desde grupo 'gateway'...", total_enviados)
            entrada_cruda = Entrada(id_ejecucion, "articulos_crudos")
            mw_gateway.publicar("date_router", Datos(entrada_cruda, lote_prueba))

            logger.info("Emitiendo fin de datos (EOF) hacia 'date_router' para '%s'...", id_ejecucion)
            mw_gateway.finalizar_entrada("date_router", entrada_cruda)

        # Esperar resultados consolidados de q1_formatter en gateway_resultados
        recibidos = set()
        terminado = False

        def procesar_resultado(datos: Datos):
            if datos.entrada.nombre != "resultados_finales_q1":
                return
            for reg in datos.contenido.registros:
                recibidos.add(reg.url)
                logger.info(" [RESULTADO RECIBIDO] URL: %s | Fecha: %s", reg.url, reg.fecha)

        def al_finalizar_resultados(entrada: Entrada):
            nonlocal terminado
            if entrada.nombre != "resultados_finales_q1":
                return
            logger.info(" [BARRERA EOF RECIBIDA] Entrada finalizada: %s", entrada.nombre)
            terminado = True
            mw_resultados.detener_recepcion()

        logger.info("A la espera de resultados desde 'q1_formatter' (entrada 'resultados_finales_q1')...")
        plazo_maximo = time.time() + 45
        while not terminado and time.time() < plazo_maximo:
            mw_resultados.recibir(procesar_resultado, al_finalizar=al_finalizar_resultados)

        if not terminado:
            logger.error("Timeout: no se recibieron todos los resultados ni el EOF dentro del plazo.")
            sys.exit(1)

        # Imprimimos lo recibido
        print("\n" + "=" * 60)
        print("QUERY 1:")
        print(f"Total artículos enviados:  {len(lote_prueba.registros)}")
        print(f"Total artículos esperados: {len(esperados)}")
        print(f"Total artículos recibidos: {len(recibidos)}")
        print(f"Coincidencia exacta:       {recibidos == esperados}")
        print("=" * 60 + "\n")

        assert recibidos == esperados, f"Discrepancia en resultados: {recibidos} != {esperados}"
        logger.info("Fin mock Q1")


if __name__ == "__main__":
    main()
