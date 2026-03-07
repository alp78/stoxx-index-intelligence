#!/bin/bash
set -euo pipefail

# ==========================================================================
# Verify migration: compare row counts between source and destination
#
# Usage:
#   export SOURCE_IP="<cloud-sql-private-ip>"
#   export SOURCE_PWD="<cloud-sql-sa-password>"
#   export DEST_PWD="<vm-sa-password>"
#   bash verify_migration.sh
# ==========================================================================

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

SOURCE_IP="${SOURCE_IP:?Set SOURCE_IP to Cloud SQL private IP}"
SOURCE_USER="${SOURCE_USER:-sqlserver}"
SOURCE_PWD="${SOURCE_PWD:?Set SOURCE_PWD to Cloud SQL password}"

DEST_IP="127.0.0.1"
DEST_USER="sa"
DEST_PWD="${DEST_PWD:?Set DEST_PWD to VM SA password}"

TRUST="-C"

TABLES=(
    "bronze.dim_country"
    "bronze.dim_index"
    "bronze.index_dim"
    "bronze.trading_calendar"
    "bronze.signals_daily"
    "bronze.signals_quarterly"
    "bronze.pulse"
    "bronze.pulse_tickers"
    "bronze.eurostoxx50_ohlcv"
    "bronze.stoxxasia50_ohlcv"
    "bronze.stoxxusa50_ohlcv"
    "silver.index_dim"
    "silver.signals_daily"
    "silver.signals_quarterly"
    "silver.eurostoxx50_ohlcv"
    "silver.stoxxasia50_ohlcv"
    "silver.stoxxusa50_ohlcv"
    "gold.scores_daily"
    "gold.scores_quarterly"
    "gold.index_performance"
)

count_rows() {
    local server=$1 user=$2 pwd=$3 table=$4
    $SQLCMD -S "$server" -U "$user" -P "$pwd" -d stoxx $TRUST \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM ${table}" -h -1 -W 2>/dev/null | head -1 | tr -d ' '
}

printf "%-35s %12s %12s %s\n" "TABLE" "SOURCE" "DEST" "STATUS"
printf "%-35s %12s %12s %s\n" "-----" "------" "----" "------"

MISMATCHES=0
for TABLE in "${TABLES[@]}"; do
    SRC_COUNT=$(count_rows "$SOURCE_IP" "$SOURCE_USER" "$SOURCE_PWD" "$TABLE" || echo "ERR")
    DST_COUNT=$(count_rows "$DEST_IP" "$DEST_USER" "$DEST_PWD" "$TABLE" || echo "ERR")

    if [ "$SRC_COUNT" = "$DST_COUNT" ]; then
        STATUS="OK"
    else
        STATUS="MISMATCH"
        MISMATCHES=$((MISMATCHES + 1))
    fi

    printf "%-35s %12s %12s %s\n" "$TABLE" "$SRC_COUNT" "$DST_COUNT" "$STATUS"
done

echo ""
if [ "$MISMATCHES" -eq 0 ]; then
    echo "All tables match. Migration verified."
else
    echo "WARNING: ${MISMATCHES} table(s) have mismatched counts."
fi
