# Tp-Grupal-Wikipedia

Sistema distribuido de análisis de artículos de Wikipedia, en Python, con cinco
consultas y comunicación interna mediante RabbitMQ.

## Requisitos y datos de entrada

Ejecutar los comandos desde la raíz del proyecto. Se necesitan Docker con el
plugin Compose, `make` y Python 3 en el host. Las dependencias de la aplicación
se instalan dentro de las imágenes; la primera construcción requiere acceso a
Internet para descargarlas, junto con las stopwords de NLTK.

Colocar los archivos `.parquet` del dataset directamente en `input/`. Todos los
clientes leen esos mismos archivos, en orden de nombre y con acceso de solo lectura.
Cada cliente procesa el conjunto completo como una ejecución independiente; los
archivos no se reparten entre clientes.

## Configurar y ejecutar

Editar las cantidades de réplicas en `grupos` de `config/config.json`.
`gateway`, `gateway_resultados`, `q5_average` y `q5_comparator` deben conservar
una instancia.

```bash
make generar-compose CLIENTES=3  # Genera docker-compose.yaml con tres clientes
make up CLIENTES=3               # Regenera, construye y levanta el sistema
make up-quiet         # Ejecuta con el cliente 0 y limpia el despliegue al terminar
make down             # Elimina contenedores, red y volumen del broker
```

`CLIENTES` puede ser cualquier entero positivo; si se omite, se levanta un cliente.
`make up-quiet` ejecuta solamente el cliente 0 sobre la misma carpeta `input/`.
Los clientes comienzan a enviar sus datos al arrancar. El servicio `inicializador` prepara automáticamente
los permisos del middleware antes de iniciar los workers; no hay que ejecutarlo
manualmente con este despliegue.

El generador crea un servicio por instancia con identidad propia: la instancia 0
conserva el nombre original y las adicionales usan sufijos `-1`, `-2`, etc.
`docker-compose.yaml` es un archivo local generado: no editarlo ni copiarlo
a otra computadora; regenerarlo allí. Para comandos directos, usar
`docker compose -f docker-compose.yaml --profile clientes`.

Para cambiar réplicas entre ejecuciones, ejecutar `make down`, editar el JSON y
volver a ejecutar `make up CLIENTES=N` con la cantidad elegida. No cambiar los participantes durante una ejecución.
La limpieza del broker permite reinicializar la capacidad con la nueva topología.

## Resultados y apagado

Los resultados quedan en `output/client_<id>` (identidades de 0 a N−1): CSV por
consulta e imágenes de Q2 en el subdirectorio `imagenes`. `output/` se reserva
para estos resultados, que permiten compararlos con la ejecución serial. Cada nueva ejecución de un cliente
borra sus resultados anteriores. `make down` conserva los directorios de `output`.

Para detener el despliegue mediante SIGTERM, sin eliminar todavía sus recursos:

```bash
docker compose -f docker-compose.yaml --profile clientes stop --timeout 30
```

Después, ejecutar `make down` para limpiar antes de una nueva ejecución.

## Pruebas

```bash
make test                    # Todos los paquetes y escenarios de despliegue
make test-middleware         # Comunicación con RabbitMQ real
make test-eof                # Protocolo EOF
make test-q4                 # Q4 contra cálculo serial con datos sintéticos
make test-sigterm            # Apagado por componente
make test-sigterm-despliegue  # Parada del despliegue con clientes concurrentes y réplicas
make test-compose            # Generación de Compose, sin levantar contenedores
```

Los escenarios de comunicación usan proyectos Compose aislados, sin puertos
publicados, y eliminan sus contenedores y volúmenes al terminar. Usan datos
sintéticos y no necesitan los Parquet de `input`. Un fallo devuelve un código de
salida distinto de cero.
