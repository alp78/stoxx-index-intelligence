"""Database connection helper — creates pyodbc connections from .env config."""

import pyodbc
from pathlib import Path
from dotenv import load_dotenv
import os

def get_connection(autocommit=False, database=None):
    """Returns a pyodbc connection using .env config.

    Args:
        autocommit: Enable autocommit mode.
        database: Override the target database (e.g. 'master' for initial setup).
    """
    # Load .env from project root
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(env_path)

    host = os.getenv("SQL_HOST", "localhost")
    port = os.getenv("SQL_PORT", "1434")
    db = database or os.getenv("SQL_DATABASE", "stoxx")
    user = os.getenv("SQL_USER", "sa")
    password = os.getenv("SA_PASSWORD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_cert = os.getenv("SQL_TRUST_CERT", "yes")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate={trust_cert}"
    )
    return pyodbc.connect(conn_str, autocommit=autocommit)


def get_index_symbols(index_key):
    """Read stock symbols and price_data_start for an index from bronze.index_dim."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT symbol, price_data_start FROM bronze.index_dim WHERE _index = ?", index_key)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
