FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY src/common ./src/common
COPY src/middleware ./src/middleware
COPY src/workers/__init__.py ./src/workers/__init__.py
COPY src/workers/base_worker.py ./src/workers/base_worker.py
COPY src/workers/word_occurrences ./src/workers/word_occurrences

ENV PYTHONPATH="/app"

ENTRYPOINT ["python3", "src/workers/word_occurrences/main.py"]