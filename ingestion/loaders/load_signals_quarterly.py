"""Loads quarterly signals JSON into bronze.signals_quarterly. Strategy: append."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection


def load(json_file, index_name):
    print(f"\n--- Loading quarterly signals: {index_name} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    for rec in records:
        symbol = rec.get("symbol")
        if not symbol:
            continue

        qm = rec.get("quality_metrics", {})
        cs = rec.get("capital_structure", {})
        fc = rec.get("fiscal_calendar", {})
        gov = rec.get("governance", {})

        cursor.execute("""
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
        """,
            index_name, symbol, rec.get("as_of_date"),
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
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {len(records)} records appended for {index_name}")


if __name__ == "__main__":
    signals_dir = Path(__file__).resolve().parent.parent.parent / "data" / "stage"

    load(signals_dir / "eurostoxx50_signals_quarterly.json", "euro_stoxx")
    load(signals_dir / "stoxxusa50_signals_quarterly.json", "stoxx_usa")
