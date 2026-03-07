#!/bin/bash
set -euo pipefail

# ==========================================================================
# Step 4: Migrate data from Cloud SQL to VM SQL Server
#
# Uses bcp (bulk copy program) to export from source → flat files → import
# to destination. Both instances are on the same VPC.
#
# Prerequisites:
#   - Run on the VM (stoxx-sql) via IAP SSH
#   - mssql-tools18 installed (provides bcp at /opt/mssql-tools18/bin/bcp)
#   - Source (Cloud SQL) and destination (VM) both have the stoxx database
#   - DDL already applied on VM (Step 3)
#
# Usage:
#   export SOURCE_IP="<cloud-sql-private-ip>"
#   export SOURCE_PWD="<cloud-sql-sa-password>"
#   export DEST_PWD="<vm-sa-password>"
#   bash migrate_data.sh
# ==========================================================================

BCP="/opt/mssql-tools18/bin/bcp"
DUMP_DIR="/tmp/bcp_export"
mkdir -p "$DUMP_DIR"

# --- Connection settings ---
SOURCE_IP="${SOURCE_IP:?Set SOURCE_IP to Cloud SQL private IP}"
SOURCE_USER="${SOURCE_USER:-sqlserver}"
SOURCE_PWD="${SOURCE_PWD:?Set SOURCE_PWD to Cloud SQL password}"

DEST_IP="127.0.0.1"
DEST_USER="sa"
DEST_PWD="${DEST_PWD:?Set DEST_PWD to VM SA password}"

# Trust server certificate (self-signed on both instances)
# NOTE: bcp uses -Yu (not -C like sqlcmd) to trust server certificates
TRUST="-Yu"

# --- Tables to migrate ---
# Order matters: reference/dimension tables first, then facts
TABLES=(
    # Bronze - reference
    "bronze.dim_country"
    "bronze.dim_index"
    "bronze.index_dim"
    "bronze.trading_calendar"

    # Bronze - signals
    "bronze.signals_daily"
    "bronze.signals_quarterly"

    # Bronze - pulse
    "bronze.pulse"
    "bronze.pulse_tickers"

    # Bronze - OHLCV (dynamic, per-index)
    "bronze.eurostoxx50_ohlcv"
    "bronze.stoxxasia50_ohlcv"
    "bronze.stoxxusa50_ohlcv"

    # Silver - dimensions
    "silver.index_dim"

    # Silver - signals
    "silver.signals_daily"
    "silver.signals_quarterly"

    # Silver - OHLCV (dynamic, per-index)
    "silver.eurostoxx50_ohlcv"
    "silver.stoxxasia50_ohlcv"
    "silver.stoxxusa50_ohlcv"

    # Gold - scores
    "gold.scores_daily"
    "gold.scores_quarterly"
    "gold.index_performance"
)

echo "=== BCP Data Migration ==="
echo "Source: ${SOURCE_IP} (Cloud SQL)"
echo "Dest:   ${DEST_IP} (VM local)"
echo "Tables: ${#TABLES[@]}"
echo ""

FAILED=()

for TABLE in "${TABLES[@]}"; do
    FILE="${DUMP_DIR}/${TABLE}.dat"

    echo "--- ${TABLE} ---"

    # Export from source
    echo "  Exporting..."
    EXPORT_OUT=$($BCP "${TABLE}" out "$FILE" \
        -S "$SOURCE_IP" -U "$SOURCE_USER" -P "$SOURCE_PWD" \
        -d stoxx -n -Yu 2>&1) || true
    EXPORT_LAST=$(echo "$EXPORT_OUT" | tail -1)
    echo "  $EXPORT_LAST"

    if echo "$EXPORT_OUT" | grep -qi "error"; then
        echo "  EXPORT FAILED for ${TABLE}"
        FAILED+=("${TABLE} (export)")
        continue
    fi

    # Check file size
    SIZE=$(stat -c%s "$FILE" 2>/dev/null || echo "0")
    if [ "$SIZE" -eq 0 ]; then
        echo "  Empty export (0 bytes), skipping import. Table may be empty."
        continue
    fi
    echo "  Exported: $(numfmt --to=iec $SIZE)"

    # Import to destination with -E to keep identity values
    echo "  Importing..."
    IMPORT_OUT=$($BCP "${TABLE}" in "$FILE" \
        -S "$DEST_IP" -U "$DEST_USER" -P "$DEST_PWD" \
        -d stoxx -n -E -Yu 2>&1) || true
    IMPORT_LAST=$(echo "$IMPORT_OUT" | tail -1)
    echo "  $IMPORT_LAST"

    if echo "$IMPORT_OUT" | grep -qi "error"; then
        echo "  IMPORT FAILED for ${TABLE}"
        FAILED+=("${TABLE} (import)")
        continue
    fi

    echo "  Done."
    echo ""
done

echo "=== Migration Summary ==="
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "All ${#TABLES[@]} tables migrated successfully."
else
    echo "FAILURES (${#FAILED[@]}):"
    for F in "${FAILED[@]}"; do
        echo "  - $F"
    done
fi

# Cleanup
echo ""
echo "Export files in ${DUMP_DIR} — delete when verified:"
echo "  rm -rf ${DUMP_DIR}"
