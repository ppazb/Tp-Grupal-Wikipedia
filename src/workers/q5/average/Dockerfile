FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY src/common ./src/common
COPY src/middleware ./src/middleware
COPY src/workers/__init__.py ./src/workers/__init__.py
COPY src/workers/base_worker.py ./src/workers/base_worker.py
COPY src/workers/deduplicacion.py ./src/workers/deduplicacion.py
COPY src/workers/registro_wal.py ./src/workers/registro_wal.py
COPY src/workers/wal_ejecuciones.py ./src/workers/wal_ejecuciones.py
COPY src/workers/q5/average ./src/workers/q5/average

ENV PYTHONPATH="/app"

ENTRYPOINT ["python3", "src/workers/q5/average/main.py"]