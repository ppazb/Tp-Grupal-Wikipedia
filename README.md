# Tp-Grupal-Wikipedia

Sistema distribuido de análisis de artículos de Wikipedia, en Python.

La configuración se carga desde un archivo INI y las variables del proceso:

```python
from src.middleware import cargar_configuracion

configuracion = cargar_configuracion("config/config.ini")
```

`config/config.ini` contiene un ejemplo del tramo Q4. `configuracion.py` define
las estructuras resultantes y `cargador_configuracion.py` lee y valida los valores.
Las variables `WIKI_MW_BROKER_<CAMPO>`, `WIKI_MW_PROCESO_<CAMPO>` y
`WIKI_MW_LIMITES_<CAMPO>` reemplazan los campos correspondientes del INI.
`WIKI_MW_GRUPO_<NOMBRE>_INSTANCIAS` reemplaza la cantidad de réplicas de un grupo
declarado. Los campos y nombres se escriben en mayúsculas en estas variables.
Cada proceso debe tener una identidad distinta y conocer las mismas rutas y
cantidades de réplicas. La carga no abre conexiones ni aplica límites al broker.

La codificación interna usa un sobre JSON UTF-8 con este formato:

```json
{
  "id_ejecucion": "ejecucion-1",
  "entrada": "conteos",
  "tipo": "datos",
  "origen": {"grupo": "frecuencias", "instancia": 0},
  "contenido": {"palabra": "arbol", "cantidad": 2}
}
```

`tipo` admite `datos` y `eof`. Un EOF exige `contenido: null` y declara el fin de
los aportes del productor de `origen`. No equivale a `FinEntrada`, que representa
el cierre consolidado de una entrada.

Los DTO comunes están en `src/common/protocol/dto.py`. Los imports anteriores
desde `middleware` siguen disponibles y apuntan a las mismas clases.

```python
from src.common.protocol.dto import Entrada, SobreMensaje, TipoMensaje
from src.middleware import CodificadorMensajes

codificador = CodificadorMensajes(configuracion.limites.max_bytes_por_mensaje)
sobre = SobreMensaje(
    entrada=Entrada("ejecucion-1", "conteos"),
    tipo=TipoMensaje.DATOS,
    origen=configuracion.proceso,
    contenido={"palabra": "arbol", "cantidad": 2},
)
cuerpo = codificador.codificar(sobre)
recibido = codificador.decodificar(cuerpo)
```

`common/protocol/serializer.py` convierte valores JSON y bytes; el codificador
del middleware valida el sobre y traduce los errores a `ErrorMensaje`.
Se admiten diccionarios con claves de texto, listas, tuplas, strings, números
finitos, booleanos y bytes. Las tuplas se reciben como listas. Un mensaje de datos
no puede tener contenido nulo, aunque sus campos internos sí pueden contenerlo.

Los bytes se representan mediante un objeto exclusivo
`{"__bytes_base64__": "..."}` y se recuperan como bytes. Esa clave queda reservada
en todos los diccionarios del protocolo. El límite configurado incluye el sobre,
la codificación UTF-8 y la expansión base64; se verifica antes de publicar y antes
de interpretar cuerpos recibidos. Se codifica un registro o lote acotado por vez.

El ciclo de vida del transporte está en `src/middleware/broker_connection.py`.
Requiere Pika, cuya versión está fijada en `requirements.txt`. Para instalar las
dependencias en un entorno virtual:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

`ConexionBroker` es un componente interno del middleware: administra una conexión
y un canal por instancia. El negocio seguirá usando la interfaz `Middleware`.
Su constructor guarda la configuración; `abrir()` crea los recursos y `cerrar()`
los libera. También admite `with`, que cierra los recursos al salir del bloque:

```python
from src.middleware.broker_connection import ConexionBroker

with ConexionBroker(configuracion.broker) as conexion:
    canal = conexion.canal  # Para los futuros manejadores de colas y exchanges.
```

La apertura usa host, puerto, credenciales, virtual host, latido y tiempos del INI.
`timeout_conexion_segundos` se aplica tanto a la conexión TCP como al establecimiento
del protocolo; `timeout_bloqueo_segundos` se aplica al bloqueo informado por el
broker. Se hace un solo intento, sin reconexión automática. El broker, el usuario
y el virtual host deben existir; esta clase no los crea.

Si falla la apertura del canal, se intenta cerrar la conexión ya adquirida y se
informa `ErrorConexion` conservando su causa. El cierre intenta liberar canal y
conexión aunque uno falle. Repetir un cierre exitoso no tiene efecto; si falla,
se informa `ErrorCierre` y se conservan los recursos pendientes para volver a
intentar cerrarlos. Si ya había otro error en curso, se conserva ese error y se
registran los fallos de limpieza. Una instancia cerrada o con apertura fallida
no se puede volver a abrir.

La instancia debe crearse dentro del proceso que la utiliza; se rechaza su uso
desde un proceso hijo que la haya heredado. Este componente todavía no declara
colas, configura confirmaciones, publica mensajes ni integra SIGTERM. Se verificó
con conexiones simuladas y con un rechazo real de conexión local usando Pika;
queda pendiente comprobar la apertura exitosa contra un broker RabbitMQ.
