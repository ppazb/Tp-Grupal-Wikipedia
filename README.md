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
una instancia. `q2_image_downloader` admite una o dos réplicas.

```bash
make generar-compose CLIENTES=3  # Genera docker-compose.yaml con tres clientes
make up CLIENTES=3              # Limpia el despliegue anterior, levanta y espera a los clientes
make down                      # Elimina contenedores, red y volúmenes, incluidos los WAL
```

`CLIENTES` puede ser cualquier entero positivo; su valor predeterminado es 1.
Actualmente `make up` no genera el Compose: primero ejecutar `make generar-compose`
y pasar la misma cantidad de clientes a ambos comandos. Cambiar `CLIENTES` solo en
`make up` no agrega ni elimina servicios del archivo generado.
`make up` ejecuta primero `make down`, por lo que inicia una ejecución nueva y borra
la persistencia del despliegue anterior; no usarlo para recuperar un nodo caído.
`make up` termina cuando todos los clientes salieron y muestra un resumen por cliente.
Solo informa éxito si todos terminaron con código 0, después de recibir el FIN del
gateway y cerrar los archivos. Si alguno falla, devuelve error.
Workers y RabbitMQ quedan levantados para inspeccionarlos; ejecutar `make down`
para eliminarlos. Si se interrumpe la espera con Ctrl+C, el despliegue también
permanece levantado y no se informa una ejecución exitosa.
Para seguir los logs desde otra terminal:

```bash
docker compose -f docker-compose.yaml --profile clientes logs -f
```

Los clientes comienzan a enviar sus datos al arrancar. El servicio `inicializador` prepara automáticamente
los permisos del middleware antes de iniciar los workers; no hay que ejecutarlo
manualmente con este despliegue.

El generador crea un servicio por instancia con identidad propia: la instancia 0
conserva el nombre original y las adicionales usan sufijos `-1`, `-2`, etc.
`docker-compose.yaml` es un archivo local generado: no editarlo ni copiarlo
a otra computadora; regenerarlo allí. Para comandos directos, usar:

```bash
docker compose -f docker-compose.yaml --profile clientes
```

Para cambiar réplicas entre ejecuciones, ejecutar `make down`, editar el JSON y
regenerar con `make generar-compose CLIENTES=N` y ejecutar `make up CLIENTES=N`.
No cambiar los participantes durante una ejecución.
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

## Reiniciar un cliente caído

Los healthcheckers supervisan los workers, el gateway y los otros healthcheckers,
pero no los clientes. Si un proceso cliente cae, hay que iniciarlo manualmente;
el resto del despliegue puede permanecer levantado.

Los servicios se llaman `client` (ID 0), `client-1` (ID 1), `client-2` (ID 2), etc.
Para iniciar un contenedor cliente que ya existe y está detenido:

```bash
docker compose -f docker-compose.yaml --profile clientes ps -a
docker compose -f docker-compose.yaml --profile clientes start client-1
```

Reemplazar `client-1` por el servicio correspondiente. `start` inicia el contenedor
detenido; `restart` también detiene el proceso si aún está activo, por lo que no
hace falta para este caso. No ejecutar `make down` ni `make up` para reiniciar solo
un cliente: borrarían el estado persistido del despliegue.

El cliente reiniciado comienza una ejecución nueva, vuelve a leer toda su entrada
y reemplaza sus resultados anteriores en `output/client_<id>`. No recupera la sesión
que perdió al morir. Esto es diferente de una desconexión del gateway con el cliente
vivo: en ese caso el cliente intenta reconectarse y reanudar su sesión automáticamente,
dentro del plazo configurado en `RECONEXION_MAX_SEGUNDOS`.

La espera de `make up` registra la salida de cada contenedor; no sigue automáticamente
una nueva ejecución iniciada manualmente después de una caída. Para esperar ese
cliente y obtener su nuevo código de salida, ejecutar después de `start`:

```bash
docker wait client-1
```

Un código 0 indica que esa ejecución terminó correctamente. Antes de comparar,
comprobar que también terminaron los demás clientes.

## Pruebas

```bash
make test                    # Todos los paquetes de pruebas disponibles
make test-q5                 # Persistencia, lectura y limpieza de candidatos
```

Los escenarios de comunicación usan proyectos Compose aislados, sin puertos
publicados, y eliminan sus contenedores y volúmenes al terminar. Usan datos
sintéticos y no necesitan los Parquet de `input`. Un fallo devuelve un código de
salida distinto de cero.

## Comparación con el notebook serial

Secuencia completa (ajustar `CLIENTES` según la ejecución):

```bash
make generar-compose CLIENTES=3
make up CLIENTES=3  # Esperar el resumen de finalización exitosa de todos los clientes
make down          # Conserva output/
make comparar-serial
```

Lee todos los Parquet de `input/`, ordenados por nombre, y compara la referencia
con los clientes definidos en el Compose generado. Los directorios de clientes
adicionales de ejecuciones anteriores no se incluyen en este modo. No cambiar la
entrada ni regenerar el Compose entre la ejecución distribuida y la comparación.
El comando no levanta el sistema: compara sus resultados ya guardados.

Usa `version-secuencial.ipynb` de la raíz sin modificarlo: reemplaza su carga de
Kaggle por los archivos locales y ejecuta sus celdas de cálculo. La ejecución sigue
siendo serial, pero lee los Parquet en lotes de 100 000 filas y
corre las celdas originales sobre cada lote, combinando los aportes de forma exacta
(concatenación, suma de contadores, unión de conjuntos, suma y cantidad para el promedio
de Q5). No carga todos los Parquet juntos, aunque conserva resultados intermedios,
contadores y conjuntos en memoria; su consumo puede crecer con la entrada.
Sus dependencias y las stopwords se instalan en una imagen
Docker separada. No es necesario instalar Jupyter en el host.

La descarga de imágenes sigue la misma política que el worker de Q2 (definida en
`src/common/io_adapters/descargas_http.py`): hasta 3 reintentos con esperas de 10, 20 y 30 s,
pausa mínima de 0,2 s entre pedidos con una réplica, 2,8 MB/s de ancho de banda total
y 15 s de timeout de conexión y lectura. Con dos réplicas se reparte la tasa y se
ajusta el intervalo por réplica. Se respeta `Retry-After` si exige una espera mayor. Si tras los
reintentos no se obtiene una imagen, el artículo queda sin ella, igual que en el worker. Se
puede ajustar con las variables `IMAGE_*` (por ejemplo `IMAGE_MAX_RETRIES`).

El comparador lee de `docker-compose.yaml` el `INPUT_DIR` de cada cliente (servicios con
`CLIENT_ID`), calcula una referencia por cada directorio de entrada distinto y compara cada
`output/client_<id>` con la suya (con varios directorios, `referencia_<n>/` y `diferencias_<n>.csv`).
`--compose` elige otro archivo; `--entrada` ignora el compose y usa un único directorio para todos.

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
Para otras rutas u opciones: `bash scripts/comparar_serial.sh --help`.

## Simulación de fallas (Chaos Monkey)

Para probar la tolerancia a fallos se incluye `scripts/chaos_monkey.py`, que envía `SIGKILL`
a contenedores del despliegue (workers, `gateway`, `health_checker-*` y clientes). Lee los
servicios directamente desde `docker-compose.yaml`, nunca toca `rabbitmq` ni `inicializador`,
mantiene siempre al menos un `health_checker` vivo para que el sistema pueda recuperarse y
aplica un cooldown de 20 s por nodo para no matar siempre el mismo contenedor seguido.
En modo aleatorio también conserva al menos un cliente vivo. Si los objetivos son
exclusivamente clientes, el mínimo predeterminado pasa a cero.

Hay dos formas de usarlo desde el `Makefile`:

- **`make up-chaos`**: genera el compose, limpia volúmenes previos, levanta todo el sistema y
  corre Chaos Monkey en paralelo hasta que terminan los clientes. Al finalizar detiene el script
  y valida que los clientes sobrevivientes hayan terminado con código 0 (si derribó algún cliente,
  descarta su carpeta parcial de `output/`). Requiere que al menos un cliente complete
  la ejecución. Usa `CLIENTES_CHAOS`, no `CLIENTES`, para determinar cuántos levantar.
- **`make chaos`**: se ejecuta en una segunda terminal sobre un despliegue **ya levantado**
  (por ejemplo con `make up`). Se puede frenar en cualquier momento con `Ctrl+C` sin afectar al
  sistema y volver a lanzar con otros parámetros.

Algunos ejemplos útiles de ejecución:

```bash
# Modo automático punta a punta (por defecto: 2 clientes, 1 caída cada 12 s)
make up-chaos
make up-chaos CLIENTES_CHAOS=3 INTERVALO=15 CANTIDAD=2

# Con el sistema ya levantado, matar 1 nodo random cada 25 s (Ctrl+C para frenar)
make chaos INTERVALO=25

# Subir la carga de fallas: 2 nodos random cada 10 s
make chaos INTERVALO=10 CANTIDAD=2

# Fallas puntuales de una sola ronda (--rondas 1)
make chaos CHAOS_ARGS="--objetivos gateway --rondas 1"          # caída del gateway y reconexión del cliente
make chaos CHAOS_ARGS="--objetivos health_checker-3 --rondas 1" # caída de un healthchecker; reelección si era el líder
make chaos CHAOS_ARGS="--objetivos q4_top_n --rondas 1"         # recuperación de worker con WAL
make chaos CHAOS_ARGS="--objetivos client-1 --rondas 1"         # caída del cliente 1; no se reinicia automáticamente

# Matar varias réplicas a la vez usando comodines o categorías (@workers, @health_checkers, @clientes)
make chaos CANTIDAD=2 CHAOS_ARGS="--objetivos date_router* --rondas 1"
make chaos CANTIDAD=2 CHAOS_ARGS="--objetivos @health_checkers --rondas 1"

# Fijar la semilla (la selección también depende de los nodos vivos), o simular
make chaos INTERVALO=10 CHAOS_ARGS="--semilla 42"
make chaos CANTIDAD=2 CHAOS_ARGS="--simulacion --rondas 3"
```

Durante `make up-chaos`, dejar que el script gestione las víctimas: no reiniciar
manualmente sus clientes mientras está validando y limpiando sus salidas.

En el modo manual `make chaos`, el script no elimina los resultados parciales del
cliente caído. El comparador tampoco verifica que el cliente haya finalizado:
puede incluir esa carpeta incompleta y una coincidencia de archivos no demuestra
que la ejecución haya terminado correctamente. Antes de comparar, detener Chaos
Monkey con Ctrl+C y comprobar la finalización de los clientes (código de salida 0,
además de sus logs).

Si un cliente cayó, se puede reiniciarlo como se explicó antes y esperar a que
complete su nueva ejecución. Si se decide abandonar esa ejecución, eliminar
manualmente solo su carpeta `output/client_<id>`. Después, con al menos un cliente
completado y sin carpetas parciales ni resultados de ejecuciones anteriores,
ejecutar `bash scripts/comparar_serial.sh --entrada input` para comparar los
clientes restantes. No usar `make comparar-serial` en ese caso: toma los clientes
del Compose y fallará por la carpeta eliminada. Si todos los clientes definidos
en el Compose completaron correctamente, sí se puede usar `make comparar-serial`.

Si después de correr `make up-chaos` se quieren comparar los resultados de los clientes que
completaron contra la versión serial, ejecutar `make down` y luego
`bash scripts/comparar_serial.sh --entrada input`. En este modo se comparan todos los
directorios `output/client_*` que queden; el script automático ya eliminó los de sus
clientes derribados. Para ver todas las opciones disponibles:

```bash
python3 scripts/chaos_monkey.py --help
```
