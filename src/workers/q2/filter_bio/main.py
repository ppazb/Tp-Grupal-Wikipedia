import time
import logging

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info("Proceso iniciado. Bucle de espera activo.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("Interrupción manual. Finalizando.")

if __name__ == "__main__":
    main()