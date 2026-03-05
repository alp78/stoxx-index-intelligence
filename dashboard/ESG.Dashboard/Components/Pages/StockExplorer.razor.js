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
