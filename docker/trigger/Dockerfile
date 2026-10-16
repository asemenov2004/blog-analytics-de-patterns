FROM python:3.12-slim
RUN pip install --no-cache-dir "confluent-kafka>=2.5" "requests>=2.32" && useradd -m trigger
COPY pipelines/trigger_listener/listener.py /app/listener.py
USER trigger
WORKDIR /app
ENTRYPOINT ["python", "listener.py"]
