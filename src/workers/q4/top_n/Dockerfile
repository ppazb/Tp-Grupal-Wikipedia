FROM python:3.12-slim
WORKDIR /proyecto
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY config ./config
ENTRYPOINT ["python", "-m", "src.workers.q4.top_n.main"]
