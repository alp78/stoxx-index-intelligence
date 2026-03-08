# Financial Metrics & Scoring Guide

Reference for all metrics, scores, and thresholds used in the STOXX Index Intelligence dashboard.
Covers index-level aggregates, daily cross-sectional signals, quarterly fundamental scores,
and standard exchange data analysis indicators.

---

## 1. Index Snapshot

Cap-weighted aggregates computed daily from constituent data.

| Metric | Formula | Thresholds |
|--------|---------|------------|
| **YTD Return** | Cumulative equal-weight return from Jan 1 | > +10% strong · < −10% drawdown |
| **30d Return** | Rolling 30 trading-day return | Compare with 90d (see below) |
| **90d Return** | Rolling 90 trading-day return | Compare with 30d (see below) |
| **30d Volatility** | Annualized std dev of daily returns (×√252) | < 15% calm · 15–25% elevated · > 25% high risk |
| **P/E** | Cap-weighted avg forward price-to-earnings | < 15 cheap · > 25 expensive |
| **P/B** | Cap-weighted avg price-to-book | < 1.5 value · > 3 growth premium |
| **Dividend Yield** | Cap-weighted avg yield across constituents | > 3% attractive for income |

### 30d vs 90d Return Interpretation

| Scenario | Meaning |
|----------|---------|
| 30d > 90d, both positive | Strong rally gaining steam |
| 30d < 90d, both positive | Rally decelerating |
| 30d > 90d, both negative | Selloff moderating |
| 30d < 90d, both negative | Deepening selloff |
| Mixed signs | Potential trend reversal |

---

## 2. Chart Metrics

Five time-series charts stacked vertically with synchronized zoom/pan.

### Synthetic Portfolio Return (%)

Equal-weight portfolio holding all index constituents, rebased to 0% at period start.

$$\text{Return}_t = \left(\frac{\text{CumulativeFactor}_t}{\text{CumulativeFactor}_{\text{start}}} - 1\right) \times 100$$

**Interpretation**: Positive slope = index gaining value. A flattening curve signals exhaustion.

### Rolling 30d Return (%)

Trailing 30-day cumulative return, plotted daily. Uses a baseline series (green above zero, red below).

**Interpretation**: Positive = recent momentum is bullish. Sustained values > +5% indicate a strong trend. Zero crossings mark regime changes.

### Drawdown from Peak (%)

Distance from the running all-time high of the cumulative factor.

$$\text{Drawdown}_t = \left(\frac{\text{CumulativeFactor}_t}{\max_{s \le t}(\text{CumulativeFactor}_s)} - 1\right) \times 100$$

**Interpretation**: Always ≤ 0. Depth shows tail risk. Recovery time (from trough back to 0%) measures market resilience.

| Drawdown | Severity |
|----------|----------|
| 0% to −5% | Normal fluctuation |
| −5% to −10% | Correction |
| −10% to −20% | Bear territory |
| > −20% | Severe bear market |

### 30d Annualized Volatility (%)

Rolling 30-day standard deviation of daily returns, annualized.

$$\sigma_{30d} = \text{std}(r_{t-29}, \ldots, r_t) \times \sqrt{252} \times 100$$

A horizontal dashed line marks the historical average for the selected index.

| Volatility | Regime |
|------------|--------|
| < 15% | Low / calm |
| 15–25% | Elevated |
| > 25% | High risk |
| > 40% | Crisis-level (e.g. 2020 COVID, 2022 rate shock) |

### Rolling 30d Sharpe Ratio

Risk-adjusted return: rolling return divided by rolling volatility.

$$\text{Sharpe}_{30d} = \frac{\text{Rolling30dReturn}}{\text{Rolling30dVolatility}}$$

| Sharpe | Interpretation |
|--------|----------------|
| > 2.0 | Exceptional (rarely sustained) |
| 1.0–2.0 | Strong risk-adjusted returns |
| 0–1.0 | Modest positive returns relative to risk |
| < 0 | Losing money |

---

## 3. Daily Signals

Cross-sectional z-scores computed daily across all index constituents. Each composite score averages
its component z-scores, ranked with dense rank (no gaps, ties allowed).

### Momentum Score

Measures sustained price trend strength.

| Component | Source | Method |
|-----------|--------|--------|
| Relative Strength | 52-week return − index 52-week return | z-score within index |
| SMA-50 Ratio | Price / 50-day moving average | z-score within index |
| SMA-200 Ratio | Price / 200-day moving average | z-score within index |
| 52-Week High Proximity | (Price − 52w high) / 52w high | Inverted z-score (closer to peak = higher) |

$$\text{MomentumScore} = \text{mean}(z_{\text{RS}},\ z_{\text{SMA50}},\ z_{\text{SMA200}},\ z_{\text{52wHigh}})$$

**Interpretation**: Rank #1 has the strongest uptrend across all four dimensions. Scores > 1.5 indicate exceptionally strong momentum.

### Divergence Alerts

Contrarian signal: price is falling but analysts still rate the stock a buy.

| Condition | Criteria |
|-----------|----------|
| Price falling | 52-week change < −10% |
| Analysts bullish | Recommendation mean ≤ 2.5 and implied upside > 0 |

Both must be true simultaneously. The alert is either an opportunity (market overreaction) or a value trap (analysts lagging reality). Always cross-reference with fundamentals.

**Columns**:
- **Upside** — (analyst target price / current price) − 1
- **Rec** — consensus recommendation (1.0 = Strong Buy → 5.0 = Strong Sell)

### Relative Value Score

Measures how cheaply a stock trades relative to its index peers. All valuation multiples are inverted (lower = better).

| Component | Source | Method |
|-----------|--------|--------|
| Forward P/E | yfinance `forwardPE` | Inverted z-score within index |
| Price/Book | yfinance `priceToBook` | Inverted z-score within index |
| EV/EBITDA | yfinance `enterpriseToEbitda` | Inverted z-score within index |
| Dividend Yield | yfinance `dividendYield` | z-score within index (higher yield = higher score) |

$$\text{ValueScore} = \text{mean}(-z_{\text{PE}},\ -z_{\text{PB}},\ -z_{\text{EV}},\ z_{\text{Yield}})$$

**Interpretation**: Top-ranked stocks trade at the deepest discount to index median. Scores > 1.0 indicate significant undervaluation. Always verify — cheap can mean value trap.

### Sentiment Score

Combines analyst upside with consensus recommendation strength.

| Component | Source | Method |
|-----------|--------|--------|
| Implied Upside | (Target price / current) − 1 | z-score within index |
| Recommendation | Consensus analyst rating | Inverted z-score (lower mean = more bullish) |

$$\text{SentimentScore} = \text{mean}(z_{\text{Upside}},\ -z_{\text{Rec}})$$

**Recommendation scale**: 1.0 (Strong Buy) → 2.0 (Buy) → 3.0 (Hold) → 4.0 (Sell) → 5.0 (Strong Sell)

---

## 4. Quarterly Signals

Updated with earnings reports. Scores use the most recent quarterly financial data.

### Quality / Moat Score

Measures fundamental strength — profitability, efficiency, and cash generation.

| Component | Source | Method |
|-----------|--------|--------|
| Gross Margin | Quarterly financials | z-score within sector |
| ROE | Return on equity | z-score within sector |
| Operating Margin | Quarterly financials | z-score within sector |
| Leverage | Debt-to-equity | Inverted z-score (lower debt = higher) |
| FCF Yield | Free cash flow / market cap | z-score within sector |

$$\text{QualityScore} = \text{mean}(z_{\text{GM}},\ z_{\text{ROE}},\ z_{\text{OpM}},\ -z_{\text{Lev}},\ z_{\text{FCF}})$$

**Interpretation**: High score = superior profitability, low debt, strong cash generation. Scores > 1.0 indicate clear fundamental superiority.

### Health Warnings

Binary flags from quarterly balance sheet data. Stocks with 2+ flags appear in the table.

| Flag | Condition | Color | Risk |
|------|-----------|-------|------|
| **Liq** (Liquidity) | Current ratio < 1.0 | Blue | Cannot cover short-term obligations |
| **Lev** (Leverage) | Debt/Equity > 200% | Orange | Excessive debt burden |
| **Cash** (Cash Burn) | Free cash flow < 0 | Red | Consuming cash, not generating it |
| **Decl** (Decline) | Revenue AND margin both falling QoQ | Purple | Deteriorating business fundamentals |

| Flags | Risk Level |
|-------|------------|
| 0 | Healthy |
| 1 | Watch |
| 2 | Warning |
| 3+ | Critical |

**Interpretation**: Low quality score + many flags = strong caution signal. Critical-level stocks may face dividend cuts, credit downgrades, or restructuring.

### Governance Risk Score

Composite of ISS-style risk dimensions (sourced from yfinance quarterly data).

| Sub-Score | What it measures | Scale |
|-----------|------------------|-------|
| Audit Risk | Financial reporting integrity | 1 (low) – 10 (high risk) |
| Board Risk | Board independence and effectiveness | 1 – 10 |
| Compensation Risk | Executive pay alignment with shareholders | 1 – 10 |
| Shareholder Rights Risk | Minority shareholder protections | 1 – 10 |

$$\text{GovernanceScore} = 10 - \text{mean}(\text{AuditRisk}, \text{BoardRisk}, \text{CompRisk}, \text{ShareholderRisk})$$

**Scale**: 0 (worst) – 10 (best governance). Scores below 5 warrant attention.

---

## 5. Factor Profile (Radar Chart)

Five-axis radar showing the average factor tilt of the selected index. Raw z-scores are normalized to a 0–100 scale for visualization.

| Axis | Source |
|------|--------|
| Value | Daily relative value z-scores |
| Momentum | Daily momentum z-scores |
| Sentiment | Daily sentiment z-scores |
| Quality | Quarterly quality z-scores |
| Governance | Quarterly governance scores |

**Interpretation**: Larger filled area = stronger overall index profile. Compare across indices to spot factor tilts (e.g., one index may skew value while another skews momentum).

---

## 6. Index Composition (Donut Chart)

Dual-ring doughnut showing how the index is constructed.

| Ring | What it shows |
|------|---------------|
| **Outer ring** | Individual stock weights (cap-weighted) |
| **Inner ring** | Sector allocation (aggregated stock weights) |

Weights are cube-root scaled for display so small-cap constituents remain visible.

---

## 7. Common Exchange Analysis Metrics (Reference)

Standard indicators used in equity index analysis, beyond what the dashboard currently displays.

### Valuation

| Metric | Formula | Use |
|--------|---------|-----|
| **PEG Ratio** | P/E ÷ EPS Growth Rate | Adjusts P/E for growth; < 1 = undervalued relative to growth |
| **CAPE / Shiller P/E** | Price ÷ 10-year inflation-adjusted avg EPS | Long-term valuation; > 25 historically expensive |
| **EV/Sales** | Enterprise Value ÷ Revenue | Useful for unprofitable growth companies; < 1 = cheap |
| **Free Cash Flow Yield** | FCF ÷ Market Cap | Cash return to investors; > 5% is attractive |
| **Earnings Yield** | EPS ÷ Price (inverse of P/E) | Compare directly to bond yields; > 10yr Treasury = equities attractive |

### Momentum & Trend

| Metric | Formula | Use |
|--------|---------|-----|
| **RSI (14-day)** | 100 − 100/(1 + avg gain/avg loss) | > 70 overbought · < 30 oversold |
| **MACD** | EMA(12) − EMA(26), signal = EMA(9) of MACD | Crossover signals trend changes |
| **Bollinger Bands** | SMA(20) ± 2×std(20) | Price outside bands = potential reversal |
| **ADX** | Average Directional Index | > 25 trending · < 20 range-bound |
| **Golden/Death Cross** | SMA(50) crossing SMA(200) | Golden (up) = bullish · Death (down) = bearish |

### Risk & Volatility

| Metric | Formula | Use |
|--------|---------|-----|
| **Beta** | Cov(stock, market) ÷ Var(market) | > 1 = more volatile than market, < 1 = defensive |
| **VIX** | Implied volatility from S&P 500 options | < 15 complacent · 15–25 normal · > 30 fear |
| **Maximum Drawdown** | Worst peak-to-trough decline | Historical tail risk measure |
| **Sortino Ratio** | Return ÷ Downside Deviation | Like Sharpe but only penalizes downside volatility |
| **Calmar Ratio** | Annualized Return ÷ Max Drawdown | Return per unit of tail risk |
| **Value at Risk (95%)** | 5th percentile of daily return distribution | "Worst day in 20" under normal conditions |

### Breadth & Sentiment

| Metric | Formula | Use |
|--------|---------|-----|
| **Advance/Decline Ratio** | Rising stocks ÷ Falling stocks | > 1 = broad participation in rally |
| **% Above 200-day MA** | Count above SMA(200) ÷ Total | > 70% bullish breadth · < 30% bearish |
| **New Highs − New Lows** | 52-week new highs minus new lows | Positive = healthy market · Divergence from index = warning |
| **Put/Call Ratio** | Put volume ÷ Call volume | > 1.0 = bearish sentiment (contrarian bullish) |
| **Short Interest Ratio** | Short shares ÷ Avg daily volume | > 5 days = high short interest, potential squeeze |

### Liquidity & Flow

| Metric | Formula | Use |
|--------|---------|-----|
| **Bid-Ask Spread** | (Ask − Bid) ÷ Mid | Tight = liquid · Wide = illiquid or stressed |
| **Turnover Ratio** | Volume ÷ Shares Outstanding | High = active trading interest |
| **Money Flow Index** | Volume-weighted RSI | > 80 overbought · < 20 oversold |
| **On-Balance Volume** | Cumulative volume on up vs down days | Divergence from price = early signal |

---

## 8. Scoring Methodology

### Z-Score Calculation

All daily and quarterly scores use cross-sectional z-scores within a grouping (index or sector):

$$z = \frac{x - \mu_{\text{group}}}{\sigma_{\text{group}}}$$

Where μ and σ are computed across all constituents in the same group on the same date.
Sector-level grouping is used when sufficient peers exist (≥ 3); otherwise falls back to index-level.

### Composite Scores

Simple average of component z-scores:

$$\text{Composite} = \frac{1}{n}\sum_{i=1}^{n} z_i$$

Some components are sign-inverted before averaging (e.g., P/E: lower is better, so −z is used).

### Ranking

Dense rank within each index, descending by score:

- Rank 1 = highest score (best)
- Ties receive the same rank
- No gaps in ranking sequence

### Index Weights

Cap-weighted using daily market capitalization:

$$w_i = \frac{\text{MarketCap}_i}{\sum_{j \in \text{index}} \text{MarketCap}_j}$$

Used for P/E, P/B, dividend yield, and other index-level aggregates.

---

## 9. Data Sources & Refresh

| Data | Source | Refresh |
|------|--------|---------|
| Price, volume, market cap | yfinance (via daily pipeline) | 3×/day (09:00, 17:00, 22:00 UTC) |
| Forward P/E, P/B, EV/EBITDA | yfinance `info` dict | Daily with signals |
| Analyst target, recommendation | yfinance `info` dict | Daily with signals |
| Dividend yield, beta | yfinance `info` dict | Daily with signals |
| Quarterly financials | yfinance quarterly data | When new quarter reported |
| Governance risk scores | yfinance quarterly data | When new quarter reported |
| Ticker membership | STOXX index composition | Hourly refresh |
| Pulse (intraday price) | yfinance | Every 5 minutes |
