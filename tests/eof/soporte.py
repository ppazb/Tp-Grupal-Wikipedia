"""Productores y consumidores mínimos para observar el protocolo real de EOF."""

from collections import Counter

from src.common.protocol.dto import Datos, Entrada, FinEntrada, TipoMensaje
from src.common.protocol.registros import ConteoPalabra, Esquema, Lote, Palabra
from src.middleware.interfaz import ErrorSeguimientoEntrada
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from tests.soporte import PLAZO_PROCESO, observar_reserva


def palabras(ejecucion, *valores, entrada='palabras'):
    return Datos(Entrada(ejecucion, entrada), Lote(
        Esquema.PALABRA, tuple(Palabra(valor) for valor in valores),
    ))


def error_inesperado(datos):
    raise AssertionError(f'Rechazo inesperado: {datos}')


def esperar_permiso(permiso):
    if not permiso.wait(PLAZO_PROCESO):
        raise AssertionError('No se habilitó el avance del consumidor de prueba')


def observar_avances(middleware, eventos):
    """Observa controles confirmados sin reemplazar el transporte ni la barrera."""
    emitir = middleware._coordinacion._emitir

    def observado(tipo, entrada, contenido, instancia=None):
        emitir(tipo, entrada, contenido, instancia)
        if tipo is TipoMensaje.AVANCE_REPLICA:
            eventos.put(('avance', entrada, contenido.lotes_resueltos))

    middleware._coordinacion._emitir = observado


def producir(configuracion, eventos, envios, entradas):
    with MiddlewareRabbitMQ(configuracion) as middleware:
        for envio in envios:
            middleware.publicar_en_ramas((envio,))
        for grupo, entrada in entradas:
            middleware.finalizar_entrada(grupo, entrada)
            middleware.finalizar_entrada(grupo, entrada)
    eventos.put(('producido',))


def emitir_acumulados(middleware, entrada, datos):
    conteos = Counter(
        registro.palabra for lote in datos if lote.entrada == entrada
        for registro in lote.contenido.registros if not registro.palabra.startswith('omitir')
    )
    salida = Entrada(entrada.id_ejecucion, 'conteos')
    for palabra, cantidad in sorted(conteos.items()):
        middleware.publicar('sumidero', Datos(salida, Lote(
            Esquema.CONTEO_PALABRA, (ConteoPalabra(palabra, cantidad),),
        )))
    middleware.finalizar_entrada('sumidero', salida)


def consumir(configuracion, eventos, entradas, inicio=None, trabajo=None,
             observar=False, acumular=False, usar_evento_fin=False):
    """Retiene aportes hasta finalizar; comprueba cierres únicos y limpieza por cliente."""
    datos, fines, liberados = [], set(), Counter()
    with MiddlewareRabbitMQ(configuracion, informar_error=error_inesperado) as middleware:
        for ejecucion in {entrada.id_ejecucion for entrada in entradas}:
            middleware.registrar_recurso(
                ejecucion, 'acumulador', lambda clave=ejecucion: liberados.update((clave,)),
            )
        if observar:
            observar_avances(middleware, eventos)
        if acumular:
            observar_reserva(middleware, eventos, numero=2)
        eventos.put(('listo',))
        if inicio is not None:
            esperar_permiso(inicio)

        def procesar(lote):
            if isinstance(lote, FinEntrada):
                finalizar(lote.entrada)
                return
            if lote.entrada in fines:
                raise AssertionError('Se entregaron datos después del cierre')
            if trabajo is not None:
                eventos.put(('procesando', lote.entrada))
                esperar_permiso(trabajo)
            datos.append(lote)

        def finalizar(entrada):
            if entrada in fines or entrada not in entradas:
                raise AssertionError(f'Finalización duplicada o desconocida: {entrada}')
            if acumular:
                emitir_acumulados(middleware, entrada, datos)
            fines.add(entrada)
            eventos.put(('fin', entrada, middleware.entradas_pendientes(entrada.id_ejecucion)))

        while fines != set(entradas):
            middleware.recibir(procesar, al_finalizar=None if usar_evento_fin else finalizar)
            for ejecucion in {entrada.id_ejecucion for entrada in fines}:
                if middleware.entradas_pendientes(ejecucion):
                    try:
                        middleware.liberar_ejecucion(ejecucion)
                    except ErrorSeguimientoEntrada:
                        eventos.put(('limpieza_pendiente', ejecucion))
                    else:
                        raise AssertionError('Se liberó una ejecución con entradas pendientes')
        for ejecucion in {entrada.id_ejecucion for entrada in entradas}:
            middleware.liberar_ejecucion(ejecucion)
            middleware.liberar_ejecucion(ejecucion)
            if liberados[ejecucion] != 1:
                raise AssertionError('La limpieza por ejecución no ocurrió una sola vez')
    eventos.put(('resultado', datos, fines))
