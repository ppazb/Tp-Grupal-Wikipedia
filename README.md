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
