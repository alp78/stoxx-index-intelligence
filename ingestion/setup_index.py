"""Creates per-index OHLCV tables (bronze + silver) for every index in indices.json.
Idempotent: skips tables that already exist. Run after adding a new index to the config."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_connection
from config import INDICES
from logger import get_logger, log_info

logger = get_logger(__name__)


BRONZE_OHLCV_DDL = """
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'bronze' AND t.name = '{table}')
BEGIN
    CREATE TABLE bronze.{table} (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        _ingested_at            DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        symbol                  VARCHAR(20)     NOT NULL,
        date                    DATE            NOT NULL,
        [open]                  FLOAT,
        high                    FLOAT,
        low                     FLOAT,
        [close]                 FLOAT,
        adj_close               FLOAT,
        volume                  BIGINT,
        dividends               FLOAT,
        stock_splits            FLOAT
    );

    CREATE INDEX IX_bronze_{table}_symbol_date
        ON bronze.{table} (symbol, date);

    PRINT 'Created bronze.{table}';
END
ELSE
    PRINT 'bronze.{table} already exists';
"""

SILVER_OHLCV_DDL = """
IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'silver' AND t.name = '{table}')
BEGIN
    CREATE TABLE silver.{table} (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        symbol                  VARCHAR(20)     NOT NULL,
        date                    DATE            NOT NULL,
        [open]                  FLOAT,
        high                    FLOAT,
        low                     FLOAT,
        [close]                 FLOAT,
        adj_close               FLOAT,
        volume                  BIGINT,
        dividends               FLOAT,
        stock_splits            FLOAT,
        is_filled               BIT             NOT NULL DEFAULT 0
    );

    CREATE UNIQUE INDEX IX_silver_{table}_symbol_date
        ON silver.{table} (symbol, date);

    PRINT 'Created silver.{table}';
END
ELSE
    PRINT 'silver.{table} already exists';
"""


def setup():
    conn = get_connection()
    cursor = conn.cursor()

    for idx in INDICES:
        table = idx["ohlcv_table"]

        cursor.execute(BRONZE_OHLCV_DDL.format(table=table))
        cursor.execute(SILVER_OHLCV_DDL.format(table=table))
        conn.commit()

        log_info(logger, "Index tables checked", step="setup",
                 index=idx["key"], table=table)

    cursor.close()
    conn.close()
    log_info(logger, "Setup complete", step="setup",
             indices=len(INDICES))


if __name__ == "__main__":
    setup()
