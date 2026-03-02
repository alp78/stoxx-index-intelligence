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


def run_ddl():
    conn = get_connection(autocommit=True)
    cursor = conn.cursor()

    for script_name in SCRIPTS:
        script_path = DDL_DIR / script_name
        print(f"Running {script_name}...")

        sql = script_path.read_text(encoding="utf-8")

        # Split on GO statements (batch separator)
        batches = [b.strip() for b in sql.split("\nGO") if b.strip()]

        for batch in batches:
            # Skip empty or comment-only batches
            lines = [l for l in batch.splitlines() if l.strip() and not l.strip().startswith("--")]
            if not lines:
                continue
            try:
                cursor.execute(batch)
            except Exception as e:
                print(f"  Error in {script_name}: {e}")
                print(f"  Batch: {batch[:200]}...")
                raise

        print(f"  {script_name} complete.")

    cursor.close()
    conn.close()
    print("All DDL scripts executed successfully.")


if __name__ == "__main__":
    run_ddl()
