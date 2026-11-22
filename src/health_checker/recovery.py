"""Módulo de recuperación de contenedores caídos vía Unix Socket de Docker."""

import logging
import socket

logger = logging.getLogger(__name__)

DOCKER_SOCKET_PATH = "/var/run/docker.sock"
SOCKET_TIMEOUT_SECONDS = 10.0
BUFFER_LECTURA = 4096


class DockerRestarter:
    """
    Cliente minimalista sobre socket Unix (/var/run/docker.sock) que envía
    la orden de reinicio al demonio de Docker usando únicamente la stdlib.

    Cumple con la restricción de la cátedra: NO consulta ni inspecciona el
    estado de los contenedores en Docker; únicamente dispara el reinicio cuando
    el monitor UDP y el consenso Bully ya determinaron la caída.
    """

    def __init__(
        self,
        socket_path: str = DOCKER_SOCKET_PATH,
        timeout: float = SOCKET_TIMEOUT_SECONDS,
    ):
        if not socket_path or not socket_path.strip():
            raise ValueError("La ruta del socket de Docker no puede estar vacía")
        if timeout <= 0:
            raise ValueError("El timeout del socket debe ser mayor que cero")

        self.socket_path = socket_path.strip()
        self.timeout = timeout

    def restart_container(self, container_name: str) -> bool:
        """
        Solicita al demonio de Docker iniciar el contenedor caído indicado.
        Devuelve True si Docker aceptó la orden (HTTP 204 o 304), False si falló.
        """
        if not container_name or not container_name.strip():
            raise ValueError("El nombre del contenedor a reiniciar no puede estar vacío")

        nombre = container_name.strip()
        peticion = (
            f"POST /containers/{nombre}/start HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Connection: close\r\n"
            f"Content-Length: 0\r\n"
            f"\r\n"
        ).encode("utf-8")

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(self.timeout)
                sock.connect(self.socket_path)
                self._enviar_todo(sock, peticion)
                respuesta = self._recibir_respuesta(sock)
        except OSError as error:
            logger.error(
                "Error de socket al intentar reiniciar '%s' vía %s: %s",
                nombre,
                self.socket_path,
                error,
            )
            return False

        codigo = self._extraer_codigo_http(respuesta)
        if codigo == 204:
            logger.info("Contenedor '%s' reiniciado exitosamente (HTTP 204)", nombre)
            return True
        if codigo == 304:
            logger.info("Contenedor '%s' ya se encontraba en ejecución (HTTP 304)", nombre)
            return True

        logger.error(
            "Falló el reinicio del contenedor '%s' (HTTP %s). Respuesta: %s",
            nombre,
            codigo,
            respuesta.splitlines()[0] if respuesta else "vacía",
        )
        return False

    def _enviar_todo(self, sock: socket.socket, datos: bytes) -> None:
        """Garantiza el envío completo verificando la cantidad de bytes escritos en el FD."""
        total_enviado = 0
        esperados = len(datos)
        while total_enviado < esperados:
            enviados = sock.send(datos[total_enviado:])
            if enviados <= 0:
                raise OSError("El socket Unix de Docker se cerró durante el envío")
            total_enviado += enviados

    def _recibir_respuesta(self, sock: socket.socket) -> str:
        """Lee la respuesta HTTP hasta que el servidor cierre la conexión."""
        partes = []
        while True:
            bloque = sock.recv(BUFFER_LECTURA)
            if not bloque:
                break
            partes.append(bloque)
        return b"".join(partes).decode("utf-8", errors="replace")

    @staticmethod
    def _extraer_codigo_http(respuesta: str) -> int:
        """Extrae el código de estado de la primera línea 'HTTP/1.1 <codigo> <texto>'."""
        if not respuesta:
            return 0
        primera_linea = respuesta.splitlines()[0].strip()
        partes = primera_linea.split()
        if len(partes) >= 2 and partes[1].isdigit():
            return int(partes[1])
        return 0
