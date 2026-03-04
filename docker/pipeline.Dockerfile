# Pipeline container — Python 3.12 with ODBC Driver 18 for SQL Server.
# Runs the 16-step ingestion pipeline via ddtrace-run for Datadog APM.

FROM python:3.12-slim

# Install ODBC Driver 18 for SQL Server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl gnupg2 unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY utils/ utils/
COPY ingestion/ ingestion/
COPY db/ db/
COPY data/definitions/ data/definitions/
COPY docker/pipeline-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Run as non-root (Cloud Run; overridden in docker-compose for local dev)
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER appuser

# Data directories (created as appuser so volume mounts inherit ownership)
RUN mkdir -p data/dimensions data/ohlcv data/signals data/pulse data/tickers logs

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["ddtrace-run", "python", "utils/run_pipeline.py"]
