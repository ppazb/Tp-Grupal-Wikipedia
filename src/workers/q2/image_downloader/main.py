import logging
import sys
from src.workers.q2.image_downloader.worker import ImageDownloaderQ2

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    ruta_configuracion = sys.argv[1] if len(sys.argv) > 1 else "config/config.json"

    worker = ImageDownloaderQ2(ruta_configuracion)
    worker.iniciar()

if __name__ == "__main__":
    main()
