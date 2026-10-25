FROM python:3.12-slim
WORKDIR /proyecto
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
CMD ["python", "-m", "unittest", "-v", "tests.middleware.test_configuracion", "tests.middleware.test_escenarios"]
