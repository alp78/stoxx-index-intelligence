// ── StockExplorer.razor.js ─────────────────────────────────────────
// JS interop for the stock detail page chart and navigation.

function _resolve(ref) {
    return window.lightweightChartsBlazor.getStoredReference(ref);
}

// Pushes data to all 5 series + sets visible range in a single JS call.
// Replaces 6 sequential SignalR round-trips (5× SetData + SetVisibleRange)
// with 1 call on every stock switch.
// `seriesData`: [{ series: IJSObjectReference, data: [] }]
// `timeScale`: IJSObjectReference
// `from`, `to`: nullable long — if null, fitContent instead of setVisibleRange
export function batchUpdateStockChart(seriesData, timeScale, from, to) {
    for (const sd of seriesData) {
        _resolve(sd.series).setData(sd.data);
    }
    const ts = _resolve(timeScale);
    if (from != null && to != null) ts.setVisibleRange({ from, to });
    else ts.fitContent();
}

// Initializes chart options + all 5 series + pushes data + sets range in ONE call.
// Replaces ~13 sequential wrapper round-trips on first chart load.
// Returns: [{ series, timeScale }] — series refs indexed by order of seriesDefs.
export function batchInitStockChart(chartRef, chartOptions, seriesDefs, seriesData, timeScale_from, timeScale_to) {
    const chart = _resolve(chartRef);
    chart.applyOptions(chartOptions);

    // Map LW series type strings to descriptors
    const typeMap = {
        Area: LightweightCharts.AreaSeries,
        Candlestick: LightweightCharts.CandlestickSeries,
        Line: LightweightCharts.LineSeries,
        Histogram: LightweightCharts.HistogramSeries
    };

    const results = [];
    for (let i = 0; i < seriesDefs.length; i++) {
        const def = seriesDefs[i];
        const descriptor = typeMap[def.type];
        const series = chart.addSeries(descriptor, def.options);
        // Set data if provided
        if (seriesData && seriesData[i]) {
            series.setData(seriesData[i]);
        }
        // Generate ID and store in library's reference map
        if (!series.uniqueJavascriptId) series.uniqueJavascriptId = crypto.randomUUID();
        lightweightChartsBlazor.storedReferencesMap = lightweightChartsBlazor.storedReferencesMap || new Map();
        lightweightChartsBlazor.storedReferencesMap.set(series.uniqueJavascriptId, series);
        results.push({ __uniqueJavascriptId: series.uniqueJavascriptId });
    }

    const ts = chart.timeScale();
    if (!ts.uniqueJavascriptId) ts.uniqueJavascriptId = crypto.randomUUID();
    lightweightChartsBlazor.storedReferencesMap.set(ts.uniqueJavascriptId, ts);

    if (timeScale_from != null && timeScale_to != null) {
        ts.setVisibleRange({ from: timeScale_from, to: timeScale_to });
    } else {
        ts.fitContent();
    }

    return {
        seriesRefs: results,
        timeScaleRef: { __uniqueJavascriptId: ts.uniqueJavascriptId }
    };
}

// ── Chart.js: Stock Radar (per-stock factor profile) ──────────────

const _chartJsTooltip = {
    backgroundColor: '#1A1A2E',
    borderColor: 'rgba(255,255,255,0.08)',
    borderWidth: 1,
    titleColor: '#fff',
    bodyColor: '#A0AEC0',
    titleFont: { family: 'Inter', weight: '600' },
    bodyFont: { family: 'Inter' }
};

let _stockRadarChart = null;

export function initStockRadar(canvasId, labels, data, stockName) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return false;
    const existing = Chart.getChart(ctx);
    if (existing) existing.destroy();
    if (_stockRadarChart) _stockRadarChart.destroy();

    _stockRadarChart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels,
            datasets: [{
                label: stockName,
                data,
                backgroundColor: 'rgba(66, 165, 245, 0.15)',
                borderColor: '#42A5F5',
                borderWidth: 2,
                pointBackgroundColor: '#42A5F5',
                pointBorderColor: '#1E1E2D',
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: { display: false, stepSize: 25 },
                    grid: { color: 'rgba(255,255,255,0.06)' },
                    angleLines: { color: 'rgba(255,255,255,0.06)' },
                    pointLabels: { color: '#A0AEC0', font: { family: 'Inter', size: 11 } }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    ..._chartJsTooltip,
                    callbacks: { label: ctx => `${ctx.label}: ${ctx.raw.toFixed(0)}` }
                }
            }
        }
    });
    return true;
}

export function updateStockRadar(data, stockName) {
    if (!_stockRadarChart) return;
    _stockRadarChart.data.datasets[0].data = data;
    _stockRadarChart.data.datasets[0].label = stockName;
    _stockRadarChart.update('none');
}

export function destroyStockRadar() {
    if (_stockRadarChart) { _stockRadarChart.destroy(); _stockRadarChart = null; }
}

// ── Chart.js: Governance Risk Breakdown (horizontal bar) ──────────

let _govChart = null;

function _govColor(val) {
    if (val <= 3) return '#26A69A';
    if (val <= 6) return '#FFA726';
    return '#EF5350';
}

export function initGovChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return false;
    const existing = Chart.getChart(ctx);
    if (existing) existing.destroy();
    if (_govChart) _govChart.destroy();

    _govChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: data.map(_govColor),
                borderRadius: 4,
                barPercentage: 0.6,
                categoryPercentage: 0.8
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    min: 0,
                    max: 10,
                    ticks: { stepSize: 2, color: '#A0AEC0', font: { family: 'Inter', size: 10 } },
                    grid: { color: 'rgba(255,255,255,0.04)' }
                },
                y: {
                    ticks: { color: '#A0AEC0', font: { family: 'Inter', size: 11 } },
                    grid: { display: false }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    ..._chartJsTooltip,
                    callbacks: {
                        label: ctx => {
                            const v = ctx.raw;
                            const level = v <= 3 ? 'Low' : v <= 6 ? 'Moderate' : 'High';
                            return `${ctx.label}: ${v.toFixed(1)} (${level})`;
                        }
                    }
                }
            }
        }
    });
    return true;
}

export function updateGovChart(data) {
    if (!_govChart) return;
    _govChart.data.datasets[0].data = data;
    _govChart.data.datasets[0].backgroundColor = data.map(_govColor);
    _govChart.update('none');
}

export function destroyGovChart() {
    if (_govChart) { _govChart.destroy(); _govChart = null; }
}

// Scrolls the nav table so the selected row is vertically centered.
export function scrollToSelectedRow(container) {
    if (!container) return;
    const row = container.querySelector('tr[style*="rgba(66,165,245"]');
    if (row) {
        const containerRect = container.getBoundingClientRect();
        const rowRect = row.getBoundingClientRect();
        const offset = rowRect.top - containerRect.top - (containerRect.height / 2) + (rowRect.height / 2);
        container.scrollTop += offset;
    }
}
