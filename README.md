# Tp-Grupal-Wikipedia

Sistema distribuido de análisis de artículos de Wikipedia, en Python.

Para instalar las dependencias, desde la raíz del proyecto:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

La configuración está en `config/config.json`. Las variables `WIKI_MW_` permiten
sobrescribir sus valores; por ejemplo, `WIKI_MW_BROKER_HOST`,
`WIKI_MW_PROCESO_GRUPO` y `WIKI_MW_PROCESO_INSTANCIA`.
El archivo actual contiene un ejemplo del tramo Q4, no la topología completa.
El broker debe tener creados el usuario y el virtual host indicados en el archivo.

Antes de iniciar los procesos, inicializar una sola vez la capacidad del despliegue,
con las colas de datos vacías y sin consumidores:

```bash
.venv/bin/python -m src.middleware.inicializar config/config.json
```

La inicialización rechaza repeticiones para evitar duplicar permisos. Una preparación
incompleta requiere un despliegue limpio; no se purgan colas automáticamente.

Para iniciar una instancia de Frecuencia, con el broker y la capacidad preparados:

```bash
.venv/bin/python -m src.workers.q4.frequency.main
```

Los parciales respetan `limites.max_bytes_por_mensaje`, incluido el sobre.
`--config` selecciona el JSON de comunicación; `WIKI_MW_PROCESO_INSTANCIA`
identifica cada réplica.
Para construir su imagen desde la raíz del proyecto:

```bash
docker build -f src/workers/q4/frequency/Dockerfile -t wikipedia-frecuencia .
```

Para iniciar Top N con el mismo despliegue y un presupuesto de buffer de 16 MiB:

```bash
WIKI_MW_PROCESO_GRUPO=top_n .venv/bin/python -m src.workers.q4.top_n.main --presupuesto-memoria-bytes 16777216
```

El presupuesto es obligatorio y compartido entre las ejecuciones de esa instancia.
`--directorio-temporal` permite elegir un directorio existente; por defecto se usa
el temporal del sistema. La ruta `q4` entrega pares `CONTEO_PALABRA` y su cierre
al grupo `gateway_resultados`, que debe estar consumiendo. El Compose general
configura las réplicas y su conectividad. Para construir la imagen:

```bash
docker build -f src/workers/q4/top_n/Dockerfile -t wikipedia-top-n .
```

Para ejecutar los escenarios con RabbitMQ real (requiere Docker
con Compose y `make`):

```bash
make test             # Todos los paquetes
make test-middleware  # Comunicación
make test-eof         # Protocolo EOF
```

El comando construye el contenedor de pruebas, levanta un broker aislado sin
publicar puertos y elimina sus contenedores y volúmenes al terminar. Devuelve un
código distinto de cero si falla una prueba. La primera ejecución descarga las
imágenes y dependencias. Los escenarios están en `tests/middleware/` y `tests/eof/`.
