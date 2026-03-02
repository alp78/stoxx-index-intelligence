import pyodbc
from pathlib import Path
from dotenv import load_dotenv
import os

def get_connection(autocommit=False):
    """Returns a pyodbc connection using .env config."""
    # Load .env from project root
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(env_path)

    host = os.getenv("SQL_HOST", "localhost")
    port = os.getenv("SQL_PORT", "1434")
    database = os.getenv("SQL_DATABASE", "ESG")
    user = os.getenv("SQL_USER", "sa")
    password = os.getenv("SA_PASSWORD")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str, autocommit=autocommit)
