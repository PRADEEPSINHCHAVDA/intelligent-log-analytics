FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir kafka-python==2.0.2
COPY ../base_service.py /app/base_service.py
COPY app.py /app/app.py
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python3 -c "import kafka; print('ok')" || exit 1
CMD ["python3", "app.py"]
