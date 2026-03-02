#!/bin/bash
# Custom entrypoint: start SQL Server, run init scripts, keep running.

# Start SQL Server in background
/opt/mssql/bin/sqlservr &
MSSQL_PID=$!

# Run init script
/docker-entrypoint-initdb.d/db-init.sh

# Keep SQL Server in foreground
wait $MSSQL_PID
