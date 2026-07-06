FROM python:3.11-slim
RUN pip install --no-cache-dir boto3 && \
    apt-get update && apt-get install -y --no-install-recommends cron && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY s3_project_exporter.py collector.py bootstrap_counter.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh
ENV OUT_FILE=/data/s3_metrics.prom OWNER_TSV=/data/project_owner.tsv
EXPOSE 9104
ENTRYPOINT ["/app/entrypoint.sh"]
