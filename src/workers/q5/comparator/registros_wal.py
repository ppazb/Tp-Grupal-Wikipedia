"""Campos propios de los registros del WAL de q5_comparator (la identidad la pone registro_wal)."""

from src.common.protocol.primitivas import BYTES_U32, BYTES_U64
from src.common.protocol.registros import Candidato

# Candidatos recibidos antes del resumen: identidad + candidatos (se evalúan al llegar el resumen)
TIPO_CANDIDATOS = 1
# Candidatos recibidos después del resumen: solo identidad (se evalúan y publican al llegar)
TIPO_CANDIDATOS_EVALUADOS = 2
# Resumen histórico: identidad + suma + artículos
TIPO_RESUMEN = 3


def escribir_candidatos(escritor, candidatos) -> None:
    escritor.entero(len(candidatos), BYTES_U32)
    for candidato in candidatos:
        escritor.texto(candidato.url)
        escritor.entero(candidato.cantidad, BYTES_U64)


def leer_candidatos(lector) -> list:
    return [Candidato(lector.texto(), lector.entero(BYTES_U64))
            for _ in range(lector.entero(BYTES_U32))]


def escribir_resumen(escritor, suma: int, articulos: int) -> None:
    escritor.entero(suma, BYTES_U64)
    escritor.entero(articulos, BYTES_U64)


def leer_resumen(lector) -> tuple:
    return lector.entero(BYTES_U64), lector.entero(BYTES_U64)
