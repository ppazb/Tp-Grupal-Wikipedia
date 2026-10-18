"""Ejemplo ejecutable de negocio desacoplado usando un transporte simulado."""

import unittest
from unittest.mock import call, create_autospec

from src.middleware import Datos, Entrada, FinEntrada, Middleware


def reenviar_evento(middleware: Middleware, evento) -> None:
    """Representa una etapa que proyecta un registro y propaga el cierre."""
    if isinstance(evento, Datos):
        middleware.publicar(
            "frecuencias",
            Datos(evento.entrada, evento.contenido["palabras"]),
        )
    else:
        middleware.finalizar_entrada("frecuencias", evento.entrada)


class PruebasInterfazMiddleware(unittest.TestCase):
    def test_negocio_con_ejecuciones_intercaladas_y_entrada_vacia(self):
        """El negocio proyecta datos y cierra cada ejecución independientemente."""
        primera = Entrada("ejecucion-1", "palabras")
        segunda = Entrada("ejecucion-2", "palabras")
        vacia = Entrada("ejecucion-3", "palabras")
        eventos = [
            Datos(primera, {"palabras": ("sol",), "url": "articulo-1"}),
            Datos(segunda, {"palabras": ("luna",), "url": "articulo-2"}),
            FinEntrada(segunda),
            FinEntrada(vacia),
            FinEntrada(primera),
        ]
        pendientes = iter(eventos)
        simulado = create_autospec(Middleware, instance=True, spec_set=True)
        simulado.recibir.side_effect = lambda procesar: procesar(next(pendientes))

        try:
            for _ in eventos:
                simulado.recibir(lambda evento: reenviar_evento(simulado, evento))
        finally:
            simulado.cerrar()

        self.assertEqual(
            simulado.publicar.call_args_list,
            [
                call("frecuencias", Datos(primera, ("sol",))),
                call("frecuencias", Datos(segunda, ("luna",))),
            ],
        )
        self.assertEqual(
            simulado.finalizar_entrada.call_args_list,
            [call("frecuencias", entrada) for entrada in (segunda, vacia, primera)],
        )
        simulado.cerrar.assert_called_once_with()

    def test_error_del_negocio_llega_al_invocador_y_se_cierran_recursos(self):
        """Una falla del procesador no se convierte en un cierre de entrada."""
        simulado = create_autospec(Middleware, instance=True, spec_set=True)
        evento = Datos(Entrada("ejecucion-1", "palabras"), {})
        simulado.recibir.side_effect = lambda procesar: procesar(evento)

        with self.assertRaises(KeyError):
            try:
                simulado.recibir(lambda recibido: reenviar_evento(simulado, recibido))
            finally:
                simulado.cerrar()

        simulado.publicar.assert_not_called()
        simulado.finalizar_entrada.assert_not_called()
        simulado.cerrar.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
