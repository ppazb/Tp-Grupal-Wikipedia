FROM python:3.12-slim
WORKDIR /proyecto
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
COPY config ./config
COPY docker-compose.yml .
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"]
