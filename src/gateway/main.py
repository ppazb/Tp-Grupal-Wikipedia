import logging
import os
import sys

from src.middleware.configuracion import ErrorConfiguracion
from src.middleware.interfaz import ErrorMiddleware
from src.gateway.src.gateway import ejecutar_gateway
from src.common.runtime.apagado import Apagado
from src.gateway.src.apagado import DetencionGateway

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def main() -> int:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s",)
    
    try:
        with Apagado() as apagado:
            detener = DetencionGateway()
            apagado.registrar_limpieza(detener.set)
            return ejecutar_gateway(detener, apagado)

    except (ErrorMiddleware, ErrorConfiguracion) as error:
        logging.getLogger('gateway').error("No se pudo iniciar o ejecutar el gateway: %s", error)
        return 1

    except (OSError, ValueError) as error:
        logging.getLogger('gateway').error("Error de sistema o de configuración: %s", error)
        return 1
        
    except Exception:
        logging.getLogger('gateway').exception("Falló el proceso gateway")
        return 2


if __name__ == "__main__":
    sys.exit(main())
