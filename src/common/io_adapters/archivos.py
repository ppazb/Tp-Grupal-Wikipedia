"""Lecturas y escrituras completas sobre archivos binarios."""


BYTES_POR_LECTURA = 64 * 1024


def escribir_completo(archivo, datos, comprobar_detencion=None):
    """Completa escrituras parciales y rechaza operaciones sin avance."""
    vista = memoryview(datos)
    while vista:
        if comprobar_detencion is not None:
            comprobar_detencion()
        escritos = archivo.write(vista)
        if escritos is None or escritos <= 0 or escritos > len(vista):
            raise OSError('No se pudo completar la escritura del archivo')
        vista = vista[escritos:]


def leer_completo(archivo, cantidad, *, admitir_fin=False, comprobar_detencion=None):
    """Completa lecturas parciales; admite EOF solo antes de comenzar un campo."""
    datos = bytearray()
    while len(datos) < cantidad:
        if comprobar_detencion is not None:
            comprobar_detencion()
        parte = archivo.read(min(cantidad - len(datos), BYTES_POR_LECTURA))
        if not parte:
            if admitir_fin and not datos:
                return None
            raise ValueError('Archivo binario truncado')
        datos.extend(parte)
    return datos
