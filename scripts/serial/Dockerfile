FROM python:3.11-slim
WORKDIR /proyecto
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 NLTK_DATA=/opt/nltk_data
COPY scripts/serial/requirements.txt /tmp/requirements-serial.txt
RUN pip install --no-cache-dir -r /tmp/requirements-serial.txt && python -m nltk.downloader -d /opt/nltk_data stopwords
ENTRYPOINT ["python", "scripts/comparar_serial.py"]
