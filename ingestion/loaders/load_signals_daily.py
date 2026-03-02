"""Loads daily signals JSON into bronze.signals_daily. Strategy: append."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection


def load(json_file, index_name):
    print(f"\n--- Loading daily signals: {index_name} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    for rec in records:
        symbol = rec.get("symbol")
        if not symbol:
            continue

        pm = rec.get("price_metrics", {})
        mc = rec.get("market_context", {})
        ms = rec.get("momentum_signals", {})
        ss = rec.get("sentiment_signals", {})

        cursor.execute("""
            INSERT INTO bronze.signals_daily (
                _index, symbol, timestamp,
                current_price, forward_pe, price_to_book, ev_to_ebitda,
                dividend_yield, market_cap, beta,
                fifty_two_week_change, sandp_52_week_change,
                fifty_day_average, two_hundred_day_average, dist_from_52_week_high,
                target_median_price, recommendation_mean, upside_potential
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            index_name, symbol, rec.get("timestamp"),
            pm.get("currentPrice"), pm.get("forwardPE"),
            pm.get("priceToBook"), pm.get("evToEbitda"),
            pm.get("dividendYield"), pm.get("marketCap"), pm.get("beta"),
            mc.get("fiftyTwoWeekChange"), mc.get("SandP52WeekChange"),
            ms.get("fiftyDayAverage"), ms.get("twoHundredDayAverage"),
            ms.get("distFrom52WeekHigh"),
            ss.get("targetMedianPrice"), ss.get("recommendationMean"),
            ss.get("upsidePotential")
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {len(records)} records appended for {index_name}")


if __name__ == "__main__":
    signals_dir = Path(__file__).resolve().parent.parent.parent / "data" / "stage"

    load(signals_dir / "eurostoxx50_signals_daily.json", "euro_stoxx")
    load(signals_dir / "stoxxusa50_signals_daily.json", "stoxx_usa")
