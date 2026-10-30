FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt pyarrow

COPY src/common ./src/common
COPY src/client ./src/client
COPY src ./src
COPY config ./config

ENV PYTHONPATH="/app"

ENTRYPOINT ["python3", "src/client/main.py"]
