FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY src/__init__.py ./src/__init__.py
COPY src/common/__init__.py ./src/common/__init__.py
COPY src/common/runtime ./src/common/runtime
COPY src/health_checker ./src/health_checker

ENV PYTHONPATH="/app"

ENTRYPOINT ["python3", "-u", "src/health_checker/main.py"]
