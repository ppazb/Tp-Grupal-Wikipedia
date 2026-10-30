"""Argumentos y validación de rutas para workers con una entrada y una salida."""

from src.middleware.configuracion import ErrorConfiguracion


CONFIGURACION_PREDETERMINADA = 'config/config.json'


def agregar_argumentos_comunicacion(parser, *, entrada, grupo_salida, entrada_salida):
    """Declara opciones de comunicación indicando sus valores predeterminados."""
    parser.add_argument('--config', default=CONFIGURACION_PREDETERMINADA,
                        help='Opcional: archivo JSON de comunicación (por defecto: %(default)s)')
    parser.add_argument('--entrada', default=entrada,
                        help='Opcional: entrada del worker (por defecto: %(default)s)')
    parser.add_argument('--grupo-salida', default=grupo_salida,
                        help='Opcional: grupo destinatario (por defecto: %(default)s)')
    parser.add_argument('--entrada-salida', default=entrada_salida,
                        help='Opcional: entrada del grupo destino (por defecto: %(default)s)')


def validar_rutas(configuracion, entrada, grupo_salida, entrada_salida,
                  *, esquema_entrada, esquema_salida):
    """Exige una entrada lógica del esquema indicado y una salida compatible."""
    grupo = configuracion.proceso.grupo
    entrantes = [ruta for ruta in configuracion.rutas if ruta.destino == grupo]
    if not entrantes or any(
        ruta.entrada != entrada or ruta.esquemas != (esquema_entrada,)
        for ruta in entrantes
    ):
        raise ErrorConfiguracion(
            f'{grupo} requiere una única entrada lógica {entrada!r} de {esquema_entrada.name}',
        )
    salida = configuracion.obtener_ruta(grupo, grupo_salida, entrada_salida)
    if esquema_salida not in salida.esquemas:
        raise ErrorConfiguracion(f'La salida de {grupo} debe admitir {esquema_salida.name}')
