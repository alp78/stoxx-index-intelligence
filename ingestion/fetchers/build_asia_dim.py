"""One-time utility: builds stoxxasia50_dim.json from known STOXX Asia/Pacific 50 tickers.
Source: STOXX official components PDF (SX5PV.pdf), last periodic review."""

import sys
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import data_path, get_index
from fetchers.fetch_index_dim import extract_full_identity

# STOXX Asia/Pacific 50 components mapped to yfinance tickers
# Japan (.T), Australia (.AX), Hong Kong (.HK), Singapore (.SI)
TICKERS = [
    "7203.T",   # Toyota Motor Corp.
    "BHP.AX",   # BHP Group Ltd.
    "6758.T",   # Sony Group Corp.
    "1299.HK",  # AIA Group
    "CBA.AX",   # Commonwealth Bank of Australia
    "6861.T",   # Keyence Corp.
    "CSL.AX",   # CSL Ltd.
    "8306.T",   # Mitsubishi UFJ Financial Group
    "8058.T",   # Mitsubishi Corp.
    "9432.T",   # Nippon Telegraph & Telephone
    "4063.T",   # Shin-Etsu Chemical Co.
    "4568.T",   # Daiichi Sankyo Co.
    "9983.T",   # Fast Retailing Co.
    "8031.T",   # Mitsui & Co.
    "6367.T",   # Daikin Industries Ltd.
    "6501.T",   # Hitachi Ltd.
    "8001.T",   # Itochu Corp.
    "8316.T",   # Sumitomo Mitsui Financial Group
    "NAB.AX",   # National Australia Bank
    "6098.T",   # Recruit Holdings
    "7974.T",   # Nintendo Co.
    "4502.T",   # Takeda Pharmaceutical Co.
    "WBC.AX",   # Westpac Banking Corp.
    "8035.T",   # Tokyo Electron Ltd.
    "7267.T",   # Honda Motor Co.
    "9984.T",   # SoftBank Group Corp.
    "8766.T",   # Tokio Marine Holdings
    "0388.HK",  # Hong Kong Exchanges & Clearing
    "ANZ.AX",   # ANZ Group
    "9433.T",   # KDDI Corp.
    "WDS.AX",   # Woodside Energy Group
    "MQG.AX",   # Macquarie Group Ltd.
    "4661.T",   # Oriental Land Co.
    "7741.T",   # Hoya Corp.
    "D05.SI",   # DBS Group Holdings
    "WES.AX",   # Wesfarmers Ltd.
    "8411.T",   # Mizuho Financial Group
    "6981.T",   # Murata Manufacturing Co.
    "6954.T",   # Fanuc Ltd.
    "3382.T",   # Seven & i Holdings Co.
    "6273.T",   # SMC Corp.
    "TLS.AX",   # Telstra Group
    "WOW.AX",   # Woolworths Group
    "6594.T",   # Nidec Corp.
    "RIO.AX",   # Rio Tinto Ltd.
    "4503.T",   # Astellas Pharma Inc.
    "9022.T",   # Central Japan Railway Co.
    "6702.T",   # Fujitsu Ltd.
    "2269.HK",  # WuXi Biologics
    "1810.HK",  # Xiaomi Corp.
]


def build():
    idx = get_index("stoxx_asia")
    history_start = idx["history_start"]
    output_file = data_path("stoxx_asia", "dim")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    records = []
    for symbol in TICKERS:
        record = extract_full_identity(symbol, history_start)
        if record:
            records.append(record)
            print(f"  OK: {symbol:<10} -> {record.get('exchange')}")
        else:
            print(f"  SKIP: {symbol} (no data or too recent)")
        time.sleep(0.15)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=4, ensure_ascii=False)
    print(f"\nSaved {len(records)}/{len(TICKERS)} records to {output_file}")


if __name__ == "__main__":
    build()
