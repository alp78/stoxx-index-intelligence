"""Run DDL scripts against SQL Server using pyodbc.

Splits on GO batches and executes each batch separately,
since pyodbc doesn't support GO as a SQL command.

Usage: python db/run_ddl.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import get_connection

DDL_DIR = Path(__file__).resolve().parent / "ddl"
SCRIPTS = ["bronze_schema.sql", "silver_schema.sql", "gold_schema.sql"]


def _run_script(cursor, script_path):
    """Execute a single DDL script, splitting on GO batch separators."""
    print(f"Running {script_path.name}...")
    sql = script_path.read_text(encoding="utf-8")

    batches = [b.strip() for b in sql.split("\nGO") if b.strip()]
    for batch in batches:
        lines = [l for l in batch.splitlines() if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue
        # Skip USE statements — connection already targets the right database
        if len(lines) == 1 and lines[0].strip().upper().startswith("USE "):
            continue
        try:
            cursor.execute(batch)
        except Exception as e:
            print(f"  Error in {script_path.name}: {e}")
            print(f"  Batch: {batch[:200]}...")
            raise

    print(f"  {script_path.name} complete.")


def run_ddl():
    # First connect to master to create the database if needed
    master_conn = get_connection(autocommit=True, database="master")
    master_cursor = master_conn.cursor()
    master_cursor.execute(
        "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'stoxx') CREATE DATABASE stoxx"
    )
    master_cursor.close()
    master_conn.close()
    print("Database 'stoxx' ensured.")

    # Now connect to stoxx and run schema scripts
    conn = get_connection(autocommit=True)
    cursor = conn.cursor()

    for script_name in SCRIPTS:
        _run_script(cursor, DDL_DIR / script_name)

    cursor.close()
    conn.close()
    print("All DDL scripts executed successfully.")


if __name__ == "__main__":
    run_ddl()
