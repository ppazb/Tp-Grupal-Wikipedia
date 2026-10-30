import logging
import os
import signal
import sys
import threading

from src.middleware.configuracion import ErrorConfiguracion
from src.middleware.interfaz import ErrorMiddleware
from src.gateway.src.gateway import ejecutar_gateway

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def main() -> int:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s",)
    
    stop = threading.Event()
    
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())
    signal.signal(signal.SIGINT, lambda signum, frame: stop.set())
    
    try:
        return ejecutar_gateway(stop)
    
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
