import csv
import dataclasses
import logging
import os
import shutil

from ...common.protocol.registros import Esquema

LOG = logging.getLogger(__name__)

DIRECTORIO_IMAGENES = "imagenes"
PREFIJO_RESULTADOS = "resultados_finales_"


def _nombre_consulta(nombre_entrada):
    """Q1 llega por 'resultados_finales_q1', Q2 por 'resultados_finales_q2', etc."""
    return os.path.basename(nombre_entrada.removeprefix(PREFIJO_RESULTADOS))


class _Transferencia:
    """Imagen en armado: los fragmentos se escriben en su desplazamiento, lleguen en el orden que lleguen."""

    def __init__(self, ruta, total_bytes):
        self.ruta = ruta
        self.total_bytes = total_bytes
        self.archivo = open(ruta, "wb")
        self.desplazamientos = set()  # una reentrega no debe contarse dos veces
        self.recibidos = 0

    @property
    def completa(self):
        return self.recibidos >= self.total_bytes


class EscritorResultados:
    """
    Guarda en un directorio los resultados que llegan por lotes.

    Cada consulta tiene su archivo (q1.csv, q2.csv, q4.csv, ...) con una fila por registro
    y encabezado, y los rechazos van a errores.csv. Las imágenes de Q2 se reconstruyen en
    imagenes/<id_transferencia>.img y se indexan en q2.csv al completarse
    un artículo sin imagen figura con la columna archivo vacía
    Al crearse, borra los CSV y las imágenes que dejó una ejecucion anterior
    """

    def __init__(self, directorio):
        self._directorio = directorio
        self._tablas = {}
        self._transferencias = {}
        self._limpiar_resultados_previos()
        os.makedirs(os.path.join(directorio, DIRECTORIO_IMAGENES), exist_ok=True)

    def _limpiar_resultados_previos(self):
        """Una ejecución no debe mezclarse con los archivos de la anterior: borra imágenes y CSV."""
        shutil.rmtree(os.path.join(self._directorio, DIRECTORIO_IMAGENES), ignore_errors=True)
        if os.path.isdir(self._directorio):
            for nombre in os.listdir(self._directorio):
                if nombre.endswith(".csv"):
                    os.remove(os.path.join(self._directorio, nombre))

    def escribir(self, nombre_entrada, lote):
        if lote.esquema is Esquema.FRAGMENTO_IMAGEN:
            for fragmento in lote.registros:
                self._escribir_fragmento(nombre_entrada, fragmento)
            return
        for registro in lote.registros:
            self._fila(nombre_entrada, dataclasses.astuple(registro), [c.name for c in dataclasses.fields(registro)])

    def _fila(self, nombre_entrada, valores, columnas):
        tabla = self._tablas.get(nombre_entrada)
        if tabla is None:
            archivo = open(
                os.path.join(self._directorio, f"{_nombre_consulta(nombre_entrada)}.csv"),
                "w", encoding="utf-8", newline="",
            )
            tabla = self._tablas[nombre_entrada] = (archivo, csv.writer(archivo))
            tabla[1].writerow(columnas)
        tabla[1].writerow(valores)

    def _escribir_fragmento(self, nombre_entrada, fragmento):
        transferencia = self._transferencias.get(fragmento.id_transferencia)
        if transferencia is None and fragmento.total_bytes == 0:
            # Artículo sin imagen: figura en la respuesta con la columna de archivo vacía
            self._fila(nombre_entrada, (fragmento.nombre, fragmento.url, ""), ("nombre", "url", "archivo"))
            return
        if transferencia is None:
            ruta = os.path.join(self._directorio, DIRECTORIO_IMAGENES, f"{fragmento.id_transferencia}.img")
            transferencia = self._transferencias[fragmento.id_transferencia] = _Transferencia(ruta, fragmento.total_bytes)
        if fragmento.desplazamiento not in transferencia.desplazamientos:
            transferencia.desplazamientos.add(fragmento.desplazamiento)
            transferencia.archivo.seek(fragmento.desplazamiento)
            transferencia.archivo.write(fragmento.fragmento)
            transferencia.recibidos += len(fragmento.fragmento)
        if transferencia.completa:
            transferencia.archivo.close()
            del self._transferencias[fragmento.id_transferencia]
            # Ruta relativa a la carpeta del cliente: la absoluta es la del contenedor y no sirve en el host
            relativa = os.path.relpath(transferencia.ruta, self._directorio)
            self._fila(nombre_entrada, (fragmento.nombre, fragmento.url, relativa), ("nombre", "url", "archivo"))

    def cerrar(self):
        for archivo, _ in self._tablas.values():
            archivo.close()
        for transferencia in self._transferencias.values():
            LOG.warning("Imagen incompleta (%d/%d bytes): %s",
                        transferencia.recibidos, transferencia.total_bytes, transferencia.ruta)
            transferencia.archivo.close()
        self._tablas.clear()
        self._transferencias.clear()
