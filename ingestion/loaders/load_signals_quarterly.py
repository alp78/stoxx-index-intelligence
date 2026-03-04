"""Loads quarterly signals JSON into bronze.signals_quarterly. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from utils.db import get_connection
from utils.config import data_path, get_all_keys
from utils.logger import get_logger, log_info, log_error, StepTimer

logger = get_logger(__name__)


def load(json_file, index_name):
    log_info(logger, "Loading quarterly signals into bronze (truncate & reload for this index)",
             step="load", index=index_name, table="bronze.signals_quarterly")

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log_error(logger, "Cannot load quarterly signals — JSON file missing or corrupt",
                  step="load", index=index_name, file=str(json_file), error=str(e))
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        with StepTimer() as timer:
            cursor.execute("DELETE FROM bronze.signals_quarterly WHERE _index = ?", index_name)

            rows = []
            for rec in records:
                symbol = rec.get("symbol")
                if not symbol:
                    continue

                as_of = rec.get("as_of_date")
                qm = rec.get("quality_metrics", {})
                cs = rec.get("capital_structure", {})
                fc = rec.get("fiscal_calendar", {})
                gov = rec.get("governance", {})

                rows.append((
                    index_name, symbol, as_of,
                    qm.get("grossMargins"), qm.get("operatingMargins"),
                    qm.get("returnOnEquity"), qm.get("revenueGrowth"),
                    qm.get("earningsGrowth"),
                    cs.get("sharesOutstanding"), cs.get("floatShares"),
                    cs.get("debtToEquity"), cs.get("currentRatio"),
                    cs.get("freeCashflow"),
                    fc.get("_last_fiscal_year_end"), fc.get("_most_recent_quarter"),
                    gov.get("overallRisk"), gov.get("auditRisk"),
                    gov.get("boardRisk"), gov.get("compensationRisk"),
                    gov.get("shareHolderRightsRisk"),
                    1 if gov.get("esgPopulated") else 0
                ))

            cursor.fast_executemany = True
            cursor.executemany("""
                INSERT INTO bronze.signals_quarterly (
                    _index, symbol, as_of_date,
                    gross_margins, operating_margins, return_on_equity,
                    revenue_growth, earnings_growth,
                    shares_outstanding, float_shares, debt_to_equity,
                    current_ratio, free_cashflow,
                    last_fiscal_year_end, most_recent_quarter,
                    overall_risk, audit_risk, board_risk,
                    compensation_risk, shareholder_rights_risk, esg_populated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            inserted = len(rows)
            conn.commit()

        log_info(logger, "Quarterly signals load complete — bronze refreshed with latest snapshot",
                 step="load", index=index_name, records_inserted=inserted,
                 duration_ms=timer.duration_ms)
    except Exception:
        conn.rollback()
        log_error(logger, "Quarterly signals load failed — rolling back", exc_info=True,
                  step="load", index=index_name, table="bronze.signals_quarterly")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "signals_quarterly"), key)
