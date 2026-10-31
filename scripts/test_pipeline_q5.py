#!/usr/bin/env python3
"""
Simulo Gateway para prueba punta a punta de Query 5.
  1. mock de  'gateway' para enviar artículos crudos hacia 'date_router'.
  2. date_router bifurca:
     - Fechas <= 2020 -> textos_historicos hacia word_occurrences.
     - Fechas >= 2021 -> textos_recientes hacia word_occurrences.
  3. word_occurrences extrae palabras únicas (filtrando stopwords EN/FR y numeros):
     - Históricos -> CantidadPalabras hacia q5_average.
     - Recientes -> Candidato(url, cantidad) hacia q5_comparator.
  4. q5_average calcula el promedio histórico (suma / articulos) y al recibir EOF emite ResumenHistorico a q5_comparator.
  5. q5_comparator almacena candidatos en disco temporalmente, espera ambas barreras EOF,
     filtra candidatos con cantidad > promedio_historico, y emite los resultados hacia gateway_resultados.
  6. mock de  'gateway_resultados' para recibir los candidatos y validar el resultado.
"""

from dataclasses import replace
import logging
import os
from pathlib import Path
import sys
import time

# Asegurar que la raíz del proyecto esté en sys.path
repo_root = str(Path(__file__).resolve().parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

os.environ.setdefault("WIKI_MW_BROKER_HOST", "localhost")

from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.common.protocol.dto import Datos, Entrada, IdentidadProceso
from src.common.protocol.registros import Articulo, Candidato, Idioma, Esquema, Lote

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulador_q5")


def preparar_articulos():
    """
    Genera artículos de prueba que cubren todos los casos de la Query 5:
    - Históricos (<= 2020):
      con estos datos H1 (4 palabras), H2 (6 palabras), H3 (5 palabras) tendria que dar Suma=15, Total=3 ,Promedio = 5.0
    - Recientes (>= 2021):
      R1: 9 palabras (> 5.0) deberia Pasar filtro
      R2: 3 palabras (<= 5.0) deberia no pasar filtro
      R3: 8 palabras (> 5.0) deberia pasar filtro
      R4: 5 palabras (== 5.0) no deberia pasa filtro
      R5: 7 palabras (> 5.0) deberia pasar filtro
      R6: 0 palabras (abstract vacío)  no deberia pasar filtro
    """
    articulos = (
        Articulo(
            url="https://en.wikipedia.org/wiki/Quantum_Computing_History",
            nombre="Quantum Computing History",
            fecha="2018-05-12T10:00:00Z",
            idioma=Idioma.INGLES,
            # Palabras únicas no-stopwords: computing, fascinating, quantum, revolutionary (4)
            abstract="Quantum computing is fascinating and revolutionary",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Distributed_Architectures_History",
            nombre="Distributed Architectures History",
            fecha="2019-11-20T15:30:00Z",
            idioma=Idioma.INGLES,
            # Palabras únicas: architectures, distributed, enable, robust, scalable, systems (6)
            abstract="Distributed systems enable scalable robust architectures",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Graph_Algorithms_2020",
            nombre="Graph Algorithms 2020",
            fecha="2020-12-31 23:59:59",
            idioma=Idioma.INGLES,
            # Palabras únicas: algorithm, flow, graph, network, optimizes (5)
            abstract="Algorithm 123 optimizes graph network flow in network flow",
            secciones=(),
            url_imagen="",
        ),
        #candidatos
        Articulo(
            url="https://en.wikipedia.org/wiki/Artificial_Intelligence_2021",
            nombre="Artificial Intelligence 2021",
            fecha="2021-03-15T09:00:00Z",
            idioma=Idioma.INGLES,
            # Palabras: efficiently, generate, learning, machine, media, models, modern, novel, synthetic
            abstract="Modern machine learning models generate novel synthetic media efficiently",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Simple_Concept_2022",
            nombre="Simple Concept 2022",
            fecha="2022-01-10T11:00:00Z",
            idioma=Idioma.INGLES,
            # Palabras: basic, overview, quick
            abstract="Quick basic overview",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Advanced_Robotics_2023",
            nombre="Advanced Robotics 2023",
            fecha="2023-06-20T18:00:00Z",
            idioma=Idioma.INGLES,
            # Palabras: autonomous, enables, environments, exploration, mobile, navigation, robots, unknown
            abstract="Autonomous navigation enables mobile robots exploration unknown environments",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Edge_Case_Average_2024",
            nombre="Edge Case Average 2024",
            fecha="2024-02-01T08:00:00Z",
            idioma=Idioma.INGLES,
            # Palabras: average, edge, exact, test, threshold
            abstract="Exact average threshold edge test",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://fr.wikipedia.org/wiki/Intelligence_Artificielle_2022",
            nombre="Intelligence Artificielle 2022",
            fecha="2022-07-14T10:00:00Z",
            idioma=Idioma.FRANCES,
            # Palabras: accélère, durable, développement, mondiale, numérique, technologique, transformation
            abstract="Le développement technologique accélère la transformation numérique mondiale durable",
            secciones=(),
            url_imagen="",
        ),
        Articulo(
            url="https://en.wikipedia.org/wiki/Empty_Abstract_2023",
            nombre="Empty Abstract 2023",
            fecha="2023-05-01T00:00:00Z",
            idioma=Idioma.INGLES,
            abstract="",
            secciones=(),
            url_imagen="",
        ),
    )

    # Resultados esperados para Query 5: {url: cantidad_palabras}
    esperados = {
        "https://en.wikipedia.org/wiki/Artificial_Intelligence_2021": 9,
        "https://en.wikipedia.org/wiki/Advanced_Robotics_2023": 8,
        "https://fr.wikipedia.org/wiki/Intelligence_Artificielle_2022": 7,
    }

    return Lote(Esquema.ARTICULO, articulos), esperados


def main():
    ruta_config = sys.argv[1] if len(sys.argv) > 1 else "config/config.json"
    logger.info("=== SIMULACIÓN GATEWAY -> QUERY 5 DISTRIBUTED PIPELINE ===")
    logger.info("Cargando configuración desde: %s", ruta_config)

    configuracion_base = cargar_configuracion(ruta_config)
    # 'gateway' produce hacia date_router (ruta: gateway_a_router)
    config_gateway = replace(
        configuracion_base,
        proceso=IdentidadProceso(grupo="gateway", instancia=0),
    )
    # 'gateway_resultados' consume desde q5_comparator (ruta: resultado_q5)
    config_resultados = replace(
        configuracion_base,
        proceso=IdentidadProceso(grupo="gateway_resultados", instancia=0),
    )

    lote_prueba, esperados = preparar_articulos()
    id_ejecucion = f"q5_test_{int(time.time())}"

    def manejar_errores(datos_error):
        logger.error("Notificación de rechazo/error recibida en Gateway: %s", datos_error)

    # Abrir middleware consumidor para declarar topología y recibir resultados
    with MiddlewareRabbitMQ(config_resultados, informar_error=manejar_errores) as mw_resultados:
        # Abrir middleware productor con identidad 'gateway' para publicar datos a date_router
        with MiddlewareRabbitMQ(config_gateway) as mw_gateway:
            total_enviados = len(lote_prueba.registros)
            logger.info("Publicando lote de %d artículos crudos hacia 'date_router' desde grupo 'gateway'...", total_enviados)
            entrada_cruda = Entrada(id_ejecucion, "articulos_crudos")
            mw_gateway.publicar("date_router", Datos(entrada_cruda, lote_prueba))

            logger.info("Emitiendo fin de datos (EOF) hacia 'date_router' para '%s'...", id_ejecucion)
            mw_gateway.finalizar_entrada("date_router", entrada_cruda)

        # Esperar resultados consolidados de q5_comparator en gateway_resultados
        recibidos = {}
        terminado = False

        def procesar_resultado(datos: Datos):
            if datos.entrada.nombre != "q5":
                return
            for reg in datos.contenido.registros:
                if isinstance(reg, Candidato):
                    recibidos[reg.url] = reg.cantidad
                    logger.info(" [RESULTADO RECIBIDO] URL: %s | Palabras: %d", reg.url, reg.cantidad)

        def al_finalizar_resultados(entrada: Entrada):
            nonlocal terminado
            if entrada.nombre != "q5":
                return
            logger.info(" [BARRERA EOF RECIBIDA] Entrada finalizada en Gateway: %s", entrada.nombre)
            terminado = True
            mw_resultados.detener_recepcion()

        logger.info("A la espera de resultados desde 'q5_comparator' (entrada 'q5')...")
        plazo_maximo = time.time() + 45
        while not terminado and time.time() < plazo_maximo:
            mw_resultados.recibir(procesar_resultado, al_finalizar=al_finalizar_resultados)

        if not terminado:
            logger.error("Timeout: no se recibieron todos los resultados ni el EOF dentro del plazo.")
            sys.exit(1)

        # resultado
        print("\n" + "=" * 70)  
        print("REPORTE DE SIMULACIÓN QUERY 5:")
        print(f"Total artículos enviados:            {total_enviados}")
        print("  - Históricos (<= 2020):            3 (H1=4, H2=6, H3=5 -> Promedio=5.0)")
        print("  - Recientes (>= 2021):             6")
        print(f"Total candidatos esperados (> 5.0):  {len(esperados)}")
        print(f"Total candidatos recibidos:          {len(recibidos)}")
        print("-" * 70)
        print("Detalle de resultados recibidos:")
        for url, cant in sorted(recibidos.items(), key=lambda x: x[1], reverse=True):
            print(f"  * {url} -> {cant} palabras")
        print("-" * 70)
        print(f"Coincidencia exacta de conjunto:     {recibidos == esperados}")
        print("=" * 70 + "\n")

        assert recibidos == esperados, f"Discrepancia en resultados Q5: {recibidos} != {esperados}"
        logger.info("fin test Q5")


if __name__ == "__main__":
    main()
