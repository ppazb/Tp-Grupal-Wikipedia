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
make up CLIENTES=3               # Levanta el sistema y espera a los tres clientes
make up-quiet         # Ejecuta con el cliente 0 y limpia el despliegue al terminar
make down             # Elimina contenedores, red y volumen del broker
```

`CLIENTES` puede ser cualquier entero positivo; si se omite, se levanta un cliente.
`make up-quiet` ejecuta solamente el cliente 0 sobre la misma carpeta `input/`.
`make up` termina cuando todos los clientes salieron y muestra un resumen por cliente.
Solo informa éxito si todos terminaron con código 0, después de recibir el FIN del
gateway y cerrar los archivos. Si alguno falla, devuelve error.
Workers y RabbitMQ quedan levantados para inspeccionarlos; ejecutar `make down`
para eliminarlos. Si se interrumpe la espera con Ctrl+C, el despliegue también
permanece levantado y no se informa una ejecución exitosa.
Para seguir los logs desde otra terminal:
`docker compose -f docker-compose.yaml --profile clientes logs -f`.

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
make test-q5                 # Persistencia, lectura y limpieza de candidatos
make test-sigterm            # Apagado por componente
make test-sigterm-despliegue  # Parada del despliegue con clientes concurrentes y réplicas
make test-compose            # Generación de Compose, sin levantar contenedores
```

Los escenarios de comunicación usan proyectos Compose aislados, sin puertos
publicados, y eliminan sus contenedores y volúmenes al terminar. Usan datos
sintéticos y no necesitan los Parquet de `input`. Un fallo devuelve un código de
salida distinto de cero.

## Comparación con el notebook serial

Secuencia completa (ajustar `CLIENTES` según la ejecución):

```bash
make up CLIENTES=3  # Esperar el resumen de finalización exitosa de todos los clientes
make down          # Conserva output/
make comparar-serial
```

Lee todos los Parquet de `input/`, ordenados por nombre, y compara la referencia
con cada directorio `output/client_*`. No cambiar la entrada entre la ejecución
distribuida y la serial, ni dejar directorios de clientes de ejecuciones anteriores.
El comando no levanta el sistema: compara sus resultados ya guardados.

Usa `version-secuencial.ipynb` de la raíz sin modificarlo: reemplaza su carga de
Kaggle por los archivos locales y ejecuta sus celdas de cálculo. La referencia
conserva el uso de memoria de pandas del notebook: necesita memoria para el conjunto
completo de entrada. Sus dependencias y las stopwords se instalan en una imagen
Docker separada. No es necesario instalar Jupyter en el host.

Los CSV seriales, las imágenes descargadas para Q2, el informe y las diferencias
quedan en `registros/comparacion-serial/<ejecución>/`. Las imágenes se comparan por
contenido. Solo se omiten las imágenes inexistentes (HTTP 404/410), como en el worker de Q2,
y se registra el motivo en `referencia/descargas_fallidas_q2.csv`.
El informe incluye la cantidad de descartes. Las demás consultas se comparan igualmente.
Si solo una ejecución consiguió una imagen, o sus bytes cambiaron entre descargas,
se informa una diferencia; no se elimina esa fila de la comparación.
Los errores locales de archivos siguen impidiendo completar la verificación.
Se ignora el orden de las filas pero se conservan sus repeticiones. Q4 se compara
estrictamente, incluyendo las palabras elegidas en empates del puesto veinte.

El comando devuelve 0 si todos coinciden, 1 si hay diferencias y 2 si no pudo
completar la verificación. La ausencia de un CSV representa una consulta vacía,
como en el cliente; ejecutar únicamente después de recibir todos los cierres.
`make test-serial` verifica el adaptador y comparador con datos sintéticos, sin
acceder a Wikimedia. Para otras rutas u opciones: `bash scripts/comparar_serial.sh --help`.
