# Tp-Grupal-Wikipedia

Sistema distribuido de análisis de artículos de Wikipedia, en Python.

Para instalar las dependencias, desde la raíz del proyecto:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

La configuración está en `config/config.ini`. Las variables `WIKI_MW_` permiten
sobrescribir sus valores; por ejemplo, `WIKI_MW_BROKER_HOST`,
`WIKI_MW_PROCESO_GRUPO` y `WIKI_MW_PROCESO_INSTANCIA`.
El archivo actual contiene un ejemplo del tramo Q4, no la topología completa.
El broker debe tener creados el usuario y el virtual host indicados en el archivo.
