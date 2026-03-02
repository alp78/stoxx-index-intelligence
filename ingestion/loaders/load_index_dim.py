"""Loads dimension JSON into bronze.index_dim. Strategy: truncate & reload per index."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_connection
from config import INDICES, data_path, get_all_keys


def load(json_file, index_name):
    print(f"\n--- Loading dimensions: {index_name} ---")

    with open(json_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM bronze.index_dim WHERE _index = ?", index_name)

    for rec in records:
        symbol = rec.get("symbol")
        if not symbol:
            continue

        cursor.execute("""
            INSERT INTO bronze.index_dim (
                _index, symbol, long_name, short_name, sector, sector_key,
                industry, industry_key, country, city, website,
                long_business_summary, exchange, full_exchange_name,
                exchange_timezone_name, exchange_timezone_short, currency,
                financial_currency, quote_type, market, range_start, price_data_start
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            index_name, symbol,
            rec.get("longName"), rec.get("shortName"),
            rec.get("sector"), rec.get("sectorKey"),
            rec.get("industry"), rec.get("industryKey"),
            rec.get("country"), rec.get("city"),
            rec.get("website"), rec.get("longBusinessSummary"),
            rec.get("exchange"), rec.get("fullExchangeName"),
            rec.get("exchangeTimezoneName"), rec.get("exchangeTimezoneShortName"),
            rec.get("currency"), rec.get("financialCurrency"),
            rec.get("quoteType"), rec.get("market"),
            rec.get("_range_start"), rec.get("_price_data_start")
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done: {len(records)} records loaded for {index_name}")


if __name__ == "__main__":
    for key in get_all_keys():
        load(data_path(key, "dim"), key)
