#!/bin/bash
# Wait for SQL Server to start, then run DDL schemas.
# Mounted as entrypoint supplement via docker-compose.

set -e

echo "Waiting for SQL Server to accept connections..."
for i in $(seq 1 30); do
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" > /dev/null 2>&1 && break
    echo "  attempt $i/30..."
    sleep 2
done

echo "Running bronze schema..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /docker-entrypoint-initdb.d/bronze_schema.sql

echo "Running silver schema..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /docker-entrypoint-initdb.d/silver_schema.sql

echo "Running gold schema..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /docker-entrypoint-initdb.d/gold_schema.sql

echo "Database initialization complete."
