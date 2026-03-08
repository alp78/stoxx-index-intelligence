// ── Home.razor.js ──────────────────────────────────────────────────────
// JS interop for Home page charts (LightweightCharts tooltip, Chart.js
// radar + donut, scrollbar sync).  Module-level variables (_radarChart,
// _donutChart, etc.) do NOT survive across Blazor JS module re-imports —
// Chart.getChart(ctx) is used to detect and destroy orphaned instances.

// ── HTML escape helper (prevents XSS in innerHTML) ──

const _esc = s => typeof s === 'string'
    ? s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
    : s;

// ── Batch data push for LightweightCharts ────────────────────────────
// Each SetData / SetVisibleRange through the Blazor wrapper is a separate
// SignalR round-trip (~10-20ms).  These functions collapse N interop calls
// into 1 by accepting IJSObjectReferences (resolved via the library's
// stored-reference map) and driving the chart API directly in JS.

function _resolve(ref) {
    return window.lightweightChartsBlazor.getStoredReference(ref);
}

let _syncing = false;  // shared guard: prevents zoom/pan sync loops during batch updates

// Initializes multiple line charts in a single JS call — replaces N×(ApplyOptions +
// AddSeries + TimeScale + SetData + SetVisibleRange) wrapper round-trips with 1 call.
// Each entry: { chartRef, data, from?, to? }
// Returns: [{ series: IJSObjectReference, timeScale: IJSObjectReference }] for each chart.
export function batchInitLineCharts(entries, tooltips, chartOptions, seriesOptions) {
    const results = [];
    for (const e of entries) {
        const chart = _resolve(e.chartRef);
        // Apply chart-wide options (layout, grid, timescale, crosshair)
        chart.applyOptions(chartOptions);
        // Remove any pre-existing series (e.g. orphaned from a prior init)
        for (const pane of chart.panes()) {
            for (const s of pane.getSeries()) chart.removeSeries(s);
        }
        // Per-entry series type/options override the defaults
        const sType = e.seriesType === 'Baseline'
            ? LightweightCharts.BaselineSeries
            : e.seriesType === 'Area'
                ? LightweightCharts.AreaSeries
                : LightweightCharts.LineSeries;
        const sOpts = e.seriesOptions ? { ...seriesOptions, ...e.seriesOptions } : seriesOptions;
        const series = chart.addSeries(sType, sOpts);
        // Push data and set visible range
        series.setData(e.data);
        const ts = chart.timeScale();
        if (e.from != null && e.to != null) ts.setVisibleRange({ from: e.from, to: e.to });
        else ts.fitContent();
        // Price lines (e.g. zero line, average line)
        if (e.priceLines) {
            for (const pl of e.priceLines) {
                series.createPriceLine(pl);
            }
        }
        // Generate unique IDs so the stored-reference map can track them
        if (!series.uniqueJavascriptId) series.uniqueJavascriptId = crypto.randomUUID();
        if (!ts.uniqueJavascriptId) ts.uniqueJavascriptId = crypto.randomUUID();
        // Store in the library's map so _resolve() works in later batch calls
        lightweightChartsBlazor.storedReferencesMap = lightweightChartsBlazor.storedReferencesMap || new Map();
        lightweightChartsBlazor.storedReferencesMap.set(series.uniqueJavascriptId, series);
        lightweightChartsBlazor.storedReferencesMap.set(ts.uniqueJavascriptId, ts);
        // Return DotNet-compatible references (with __uniqueJavascriptId)
        results.push({
            series: { __uniqueJavascriptId: series.uniqueJavascriptId },
            timeScale: { __uniqueJavascriptId: ts.uniqueJavascriptId }
        });
    }
    for (const t of tooltips) {
        t.container._ttLookup = t.lookup;
        t.container._ttIndices = t.indices;
        t.container._ttOptions = t.options || {};
    }

    // ── Sync zoom/pan across all charts ──
    // Use time-based range sync so charts with different data lengths stay
    // aligned on the same calendar window (logical range would drift when
    // bar counts differ, e.g. rolling-30d starts later than synthetic).
    const timeScales = [];
    for (const e of entries) {
        const chart = _resolve(e.chartRef);
        timeScales.push(chart.timeScale());
    }
    for (const ts of timeScales) {
        ts.subscribeVisibleTimeRangeChange(range => {
            if (_syncing || !range) return;
            _syncing = true;
            for (const other of timeScales) {
                if (other !== ts) {
                    try { other.setVisibleRange(range); } catch (_) {}
                }
            }
            _syncing = false;
        });
    }

    return results;
}

// Full update: sets data + visible range + tooltip for multiple charts in ONE call.
// `charts`: [{ series, timeScale, data, from?, to? }]
// `tooltips`: [{ container, lookup, indices, options }]
export function batchUpdateCharts(charts, tooltips) {
    _syncing = true;  // suppress zoom/pan sync while updating all charts
    for (const c of charts) {
        const series = _resolve(c.series);
        // Guard: if the chart somehow accumulated extra series, remove them so
        // only our tracked series survives (prevents the "double line" ghost).
        if (c.chartRef) {
            const chart = _resolve(c.chartRef);
            for (const pane of chart.panes()) {
                for (const s of pane.getSeries()) {
                    if (s !== series) chart.removeSeries(s);
                }
            }
        }
        series.setData(c.data);
        // Re-create price lines on update (remove old ones first)
        if (c.priceLines) {
            try {
                const existing = series.priceLines ? series.priceLines() : [];
                for (const pl of existing) series.removePriceLine(pl);
            } catch (_) { /* older versions may not support priceLines() */ }
            for (const pl of c.priceLines) {
                series.createPriceLine(pl);
            }
        }
        const ts = _resolve(c.timeScale);
        if (c.from != null && c.to != null) ts.setVisibleRange({ from: c.from, to: c.to });
        else ts.fitContent();
    }
    _syncing = false;
    for (const t of tooltips) {
        t.container._ttLookup = t.lookup;
        t.container._ttIndices = t.indices;
        t.container._ttOptions = t.options || {};
    }
}

// Range-only update: moves visible window without replacing data or tooltips.
// `entries`: [{ timeScale, from?, to? }]
export function batchSetVisibleRange(entries) {
    _syncing = true;
    for (const e of entries) {
        const ts = _resolve(e.timeScale);
        if (e.from != null && e.to != null) ts.setVisibleRange({ from: e.from, to: e.to });
        else ts.fitContent();
    }
    _syncing = false;
}

// ── Batch momentum chart update (all series + range in ONE call) ─────
// Prevents flash of stale data from sequential SetData round-trips.
// `seriesRefs`: [IJSObjectReference × 5], `dataArrays`: [data × 5]
// `timeScaleRef`: IJSObjectReference, `from`/`to`: visible range timestamps
export function batchUpdateMomSeries(seriesRefs, dataArrays, timeScaleRef, from, to) {
    for (let i = 0; i < seriesRefs.length; i++) {
        const series = _resolve(seriesRefs[i]);
        series.setData(dataArrays[i] || []);
    }
    const ts = _resolve(timeScaleRef);
    if (from != null && to != null) ts.setVisibleRange({ from, to });
    else ts.fitContent();
}

// ── Tooltip (managed entirely in JS to avoid Blazor re-renders) ──────
// Tooltip DOM is manipulated directly — calling back into Blazor would
// trigger StateHasChanged on every crosshair move and kill performance.

// Stores lookup data on the container element for later use by show/hide.
export function setTooltipData(container, lookup, indices, options) {
    container._ttLookup = lookup;
    container._ttIndices = indices;
    container._ttOptions = options || {};
}

// Positions and populates the tooltip div at (x,y) for the given timestamp key.
export function showTooltip(container, x, y, timestamp) {
    const data = container._ttLookup;
    const indices = container._ttIndices;
    const options = container._ttOptions || {};
    if (!data || !indices) return;

    const entry = data[timestamp.toString()];
    if (!entry) return;

    const tooltip = container.querySelector('.chart-tooltip');
    if (!tooltip) return;

    const colorBySign = options.colorBySign !== false;

    // Build content
    let html = `<div style="opacity: 0.5; margin-bottom: 2px;">${_esc(entry.date)}</div>`;
    for (const idx of indices) {
        const val = entry.values[idx.key];
        const hasVal = val !== undefined && val !== null;
        const valColor = colorBySign
            ? (hasVal ? (val >= 0 ? '#26A69A' : '#EF5350') : 'inherit')
            : idx.color;
        const suffix = options.suffix !== undefined ? options.suffix : '%';
        const prefix = (colorBySign && hasVal && val >= 0) ? '+' : '';
        const valText = hasVal ? prefix + val.toFixed(2) + suffix : '--';
        html += `<div style="display: flex; align-items: center; gap: 6px;">` +
            `<span style="display:inline-block;width:8px;height:2px;background:${idx.color};border-radius:1px;flex-shrink:0;"></span>` +
            `<span style="opacity:0.5;flex:1;">${_esc(idx.name)}</span>` +
            `<span style="font-weight:600;color:${valColor};">${valText}</span>` +
            `</div>`;
    }
    tooltip.innerHTML = html;

    // Position with edge detection
    const cw = container.clientWidth;
    const ch = container.clientHeight;
    const ttW = tooltip.offsetWidth || 180;
    const ttH = tooltip.offsetHeight || 80;

    let tx = x + 16;
    let ty = y - 20;
    if (tx + ttW > cw - 10) tx = x - ttW - 12;
    if (tx < 4) tx = 4;
    if (ty + ttH > ch - 4) ty = ch - ttH - 4;
    if (ty < 4) ty = 4;

    tooltip.style.left = tx + 'px';
    tooltip.style.top = ty + 'px';
    tooltip.style.display = 'block';
}

// Hides the tooltip when the crosshair leaves the chart area.
export function hideTooltip(container) {
    const tooltip = container.querySelector('.chart-tooltip');
    if (tooltip) tooltip.style.display = 'none';
}

// ── Chart.js: Radar ─────────────────────────────────────────────────

// Module-scoped ref; set on init, nulled on destroy.
let _radarChart = null;

const _chartJsTooltip = {
    backgroundColor: '#1A1A2E',
    borderColor: 'rgba(255,255,255,0.08)',
    borderWidth: 1,
    titleColor: '#fff',
    bodyColor: '#A0AEC0',
    titleFont: { family: 'Inter', weight: '600' },
    bodyFont: { family: 'Inter' }
};

// Creates a radar chart with score dimensions as spokes (0–100 scale).
export function initRadarChart(canvasId, labels, data, indexName) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return false;
    // Chart.getChart(ctx) finds orphaned instances that survived a JS module re-import
    const existing = Chart.getChart(ctx);
    if (existing) existing.destroy();
    if (_radarChart) _radarChart.destroy();

    _radarChart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels,
            datasets: [{
                label: indexName,
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
                    callbacks: { label: ctx => `${ctx.label}: ${ctx.raw.toFixed(1)}` }
                }
            }
        }
    });
    return true;
}

// Hot-swaps data without destroying/recreating the chart (smooth transition).
export function updateRadarChart(data, indexName) {
    if (!_radarChart) return;
    _radarChart.data.datasets[0].data = data;
    _radarChart.data.datasets[0].label = indexName;
    _radarChart.update('none');
}

export function destroyRadarChart() {
    if (_radarChart) { _radarChart.destroy(); _radarChart = null; }
}

// ── ECharts: Donut (dual-ring doughnut) ──────────────────────────────
// Inner ring = individual stocks with labels + elbow connectors.
// Outer ring = sectors with labels inside arcs.
// Hover sector → highlights member stocks, shows sector name in center.
// Hover stock → highlights parent sector, shows tooltip.
// Click stock → navigates to detail page.

let _donutChart = null;
let _donutStockLabels = [];
let _donutStockNames = [];
let _donutSectorLabels = [];
let _donutStockRealWeights = [];
let _donutSectorRealWeights = [];
let _donutDayChangePcts = [];
let _donutCountryCodes = [];
let _donutLogoPaths = [];
let _donutStockSectorIndices = [];
let _donutIndex = '';
let _donutStockColors = [];
let _donutSectorColors = [];

// Sector label abbreviations for display inside outer ring arcs
const _sectorAbbrev = {
    'Technology': 'Tech', 'Health Care': 'Health', 'Financials': 'Fin',
    'Consumer Discretionary': 'Disc', 'Consumer Staples': 'Stpl',
    'Communication Services': 'Comm', 'Industrials': 'Ind', 'Energy': 'Enrg',
    'Utilities': 'Util', 'Real Estate': 'RE', 'Materials': 'Mat',
    'Basic Materials': 'Mat', 'Financial Services': 'Fin', 'Healthcare': 'Health',
    'Consumer Cyclical': 'Cycl', 'Consumer Defensive': 'Def',
    'Communication': 'Comm', 'Other': 'Oth'
};
function _abbrevSector(name) {
    return _sectorAbbrev[name] || (name && name.length > 5 ? name.substring(0, 4) : name || '');
}

function _dimColor(color, opacity) {
    if (!color) return 'rgba(128,128,128,0.1)';
    if (color.startsWith('#')) {
        const r = parseInt(color.slice(1, 3), 16);
        const g = parseInt(color.slice(3, 5), 16);
        const b = parseInt(color.slice(5, 7), 16);
        return `rgba(${r},${g},${b},${opacity})`;
    }
    const m = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (m) return `rgba(${m[1]},${m[2]},${m[3]},${opacity})`;
    return color;
}

function _buildDonutOption() {
    // Cube root normalization so small stocks still appear visible
    const stockData = _donutStockLabels.map((sym, i) => ({
        name: sym,
        value: Math.cbrt(_donutStockRealWeights[i] || 0),
        itemStyle: { color: _donutStockColors[i] || '#666', borderColor: '#1E1E2D', borderWidth: 1 },
        label: { show: false },
        labelLine: { show: false },
        selected: false,
        _idx: i
    }));

    // Outer ring: sectors with labels inside
    const sectorData = _donutSectorLabels.map((name, i) => ({
        name: name,
        value: _donutSectorRealWeights[i] || 0,
        itemStyle: { color: _donutSectorColors[i] || '#444', borderColor: '#1E1E2D', borderWidth: 1 },
        _idx: i
    }));

    return {
        backgroundColor: 'transparent',
        tooltip: { show: false },
        series: [
            {
                name: 'Sectors',
                type: 'pie',
                radius: ['30%', '50%'],
                center: ['50%', '50%'],
                data: sectorData,
                label: { show: false },
                labelLine: { show: false },
                emphasis: {
                    scale: false,
                    itemStyle: { shadowBlur: 0, shadowColor: 'transparent' }
                },
                z: 1
            },
            {
                name: 'Stocks',
                type: 'pie',
                radius: ['52%', '85%'],
                center: ['50%', '50%'],
                data: stockData,
                label: { show: false },
                labelLine: { show: false },
                selectedMode: 'multiple',
                selectedOffset: 10,
                emphasis: {
                    scale: false,
                    itemStyle: { shadowBlur: 0, shadowColor: 'transparent' }
                },
                animationType: 'scale',
                animationEasing: 'cubicOut',
                z: 2
            },
            // Invisible center text graphic (updated on hover via graphic component)
        ],
        graphic: []
    };
}

function _donutShowStockLabels(sectorIdx) {
    if (!_donutChart) return;
    const sectorColor = _donutSectorColors[sectorIdx] || '#fff';

    // Build label data for stocks in this sector
    const labelData = [];
    for (let i = 0; i < _donutStockLabels.length; i++) {
        if (_donutStockSectorIndices[i] !== sectorIdx) continue;
        labelData.push({
            name: _donutStockLabels[i],
            value: _donutStockRealWeights[i] || 0,
            itemStyle: { color: sectorColor, borderColor: '#1E1E2D', borderWidth: 1 },
            _idx: i
        });
    }

    // Update stock series: highlight members, dim others, show labels for members
    const stockData = _donutStockLabels.map((sym, i) => {
        const belongs = _donutStockSectorIndices[i] === sectorIdx;
        const realW = _donutStockRealWeights[i];
        return {
            name: sym,
            value: Math.cbrt(realW || 0),
            itemStyle: {
                color: belongs ? sectorColor : _dimColor(_donutStockColors[i], 0.2),
                borderColor: '#1E1E2D', borderWidth: 1
            },
            label: belongs ? (function () {
                const logoPath = _donutLogoPaths[i] || '';
                const compName = _donutStockNames[i] || '';
                const wPct = realW !== undefined ? realW.toFixed(1) + '%' : '';
                const line1 = (logoPath ? '{logo|} ' : '') + '{sym|' + sym + '} {wt|' + wPct + '}';
                const line2 = compName ? '\n{name|' + compName + '}' : '';
                return {
                    show: true,
                    formatter: function () { return line1 + line2; },
                    rich: {
                        logo: {
                            height: 14,
                            width: 14,
                            borderRadius: 7,
                            backgroundColor: { image: logoPath },
                            verticalAlign: 'middle'
                        },
                        sym: {
                            fontSize: 10,
                            fontWeight: 600,
                            fontFamily: 'Inter, sans-serif',
                            color: '#E0E0E0',
                            verticalAlign: 'middle',
                            lineHeight: 18
                        },
                        wt: {
                            fontSize: 9,
                            fontFamily: 'Inter, sans-serif',
                            color: '#A0AEC0',
                            verticalAlign: 'middle',
                            lineHeight: 18
                        },
                        name: {
                            fontSize: 8,
                            fontFamily: 'Inter, sans-serif',
                            color: '#A0AEC0',
                            lineHeight: 12
                        }
                    },
                    backgroundColor: 'rgba(26,26,46,0.92)',
                    borderColor: 'rgba(255,255,255,0.1)',
                    borderWidth: 1,
                    borderRadius: 4,
                    padding: [4, 8, 4, 8]
                };
            })() : { show: false },
            labelLine: belongs ? {
                show: true,
                length: 20,
                length2: 30,
                lineStyle: { color: 'rgba(255,255,255,0.25)', width: 1 }
            } : { show: false },
            selected: belongs,
            _idx: i
        };
    });

    // Dim non-highlighted sectors
    const sectorData = _donutSectorLabels.map((name, i) => ({
        name: name,
        value: _donutSectorRealWeights[i] || 0,
        itemStyle: {
            color: i === sectorIdx ? _donutSectorColors[i] : _dimColor(_donutSectorColors[i], 0.3),
            borderColor: '#1E1E2D', borderWidth: 1
        },
        _idx: i
    }));

    // Center text
    const sectorName = _donutSectorLabels[sectorIdx] || '';
    const weightPct = _donutSectorRealWeights[sectorIdx];
    const weightStr = weightPct !== undefined ? weightPct.toFixed(2) + '%' : '';
    // Estimate lines: rough check if name needs wrapping (width ~100px, ~8px/char)
    const nameLines = sectorName.length > 12 ? 2 : 1;
    const nameBlockH = nameLines * 16;
    const nameY = weightStr ? -(nameBlockH / 2 + 2) : 0;
    const weightY = nameBlockH / 2 + 4;

    _donutChart.setOption({
        series: [
            { data: sectorData },
            { data: stockData }
        ],
        graphic: [{
            type: 'group',
            left: 'center',
            top: 'center',
            children: [
                {
                    type: 'text',
                    style: {
                        text: sectorName,
                        fill: sectorColor,
                        fontSize: 13,
                        fontWeight: 600,
                        fontFamily: 'Inter, sans-serif',
                        textAlign: 'center',
                        y: nameY,
                        width: 100,
                        overflow: 'break'
                    }
                },
                {
                    type: 'text',
                    style: {
                        text: weightStr,
                        fill: '#A0AEC0',
                        fontSize: 11,
                        fontFamily: 'Inter, sans-serif',
                        textAlign: 'center',
                        y: weightY
                    }
                }
            ]
        }]
    }, { lazyUpdate: true });
}

function _donutHighlightStock(stockIdx) {
    if (!_donutChart) return;
    const sectorIdx = _donutStockSectorIndices[stockIdx];

    const sectorColor = (sectorIdx >= 0 && sectorIdx < _donutSectorColors.length) ? _donutSectorColors[sectorIdx] : null;

    const stockData = _donutStockLabels.map((sym, i) => ({
        name: sym,
        value: Math.cbrt(_donutStockRealWeights[i] || 0),
        itemStyle: {
            color: i === stockIdx && sectorColor ? sectorColor : _donutStockColors[i],
            borderColor: '#1E1E2D', borderWidth: 1
        },
        label: { show: false },
        labelLine: { show: false },
        selected: false,
        _idx: i
    }));

    const sectorData = _donutSectorLabels.map((name, i) => ({
        name: name,
        value: _donutSectorRealWeights[i] || 0,
        itemStyle: {
            color: (sectorIdx >= 0 && i !== sectorIdx) ? _dimColor(_donutSectorColors[i], 0.3) : _donutSectorColors[i],
            borderColor: '#1E1E2D', borderWidth: 1
        },
        _idx: i
    }));

    // Center text: sector name + weight
    const sectorName = (sectorIdx >= 0 ? _donutSectorLabels[sectorIdx] : '') || '';
    const weightPct = sectorIdx >= 0 ? _donutSectorRealWeights[sectorIdx] : undefined;
    const weightStr = weightPct !== undefined ? weightPct.toFixed(2) + '%' : '';
    const nameLines = sectorName.length > 12 ? 2 : 1;
    const nameBlockH = nameLines * 16;
    const nameY = weightStr ? -(nameBlockH / 2 + 2) : 0;
    const weightY = nameBlockH / 2 + 4;

    _donutChart.setOption({
        series: [{ data: sectorData }, { data: stockData }],
        graphic: sectorName ? [{
            type: 'group',
            left: 'center',
            top: 'center',
            children: [
                {
                    type: 'text',
                    style: {
                        text: sectorName,
                        fill: sectorColor || '#fff',
                        fontSize: 13,
                        fontWeight: 600,
                        fontFamily: 'Inter, sans-serif',
                        textAlign: 'center',
                        y: nameY,
                        width: 100,
                        overflow: 'break'
                    }
                },
                {
                    type: 'text',
                    style: {
                        text: weightStr,
                        fill: '#A0AEC0',
                        fontSize: 11,
                        fontFamily: 'Inter, sans-serif',
                        textAlign: 'center',
                        y: weightY
                    }
                }
            ]
        }] : []
    }, { lazyUpdate: true });
}

function _donutResetHighlight() {
    if (!_donutChart) return;
    _donutChart.setOption(_buildDonutOption(), { notMerge: true });
}

function _donutSetupEvents(container) {
    let highlightedSector = -1;
    let highlightedStock = -1;
    let lockedSector = -1; // permanently selected sector

    _donutChart.on('mouseover', function (params) {
        if (lockedSector >= 0) return; // locked, ignore hover
        if (params.seriesName === 'Sectors') {
            const idx = params.dataIndex;
            if (highlightedSector !== idx) {
                highlightedSector = idx;
                highlightedStock = -1;
                _donutShowStockLabels(idx);
            }
            container.style.cursor = 'pointer';
        } else if (params.seriesName === 'Stocks') {
            const idx = params.dataIndex;
            if (highlightedStock !== idx) {
                highlightedStock = idx;
                highlightedSector = -1;
                _donutHighlightStock(idx);
                _donutShowTooltip(params, container);
            }
            container.style.cursor = 'pointer';
        }
    });

    _donutChart.on('mouseout', function (params) {
        if (lockedSector >= 0) return; // locked, ignore hover out
        highlightedSector = -1;
        highlightedStock = -1;
        _donutResetHighlight();
        _donutHideTooltip(container);
        container.style.cursor = 'default';
    });

    _donutChart.on('click', function (params) {
        if (params.seriesName === 'Sectors') {
            const idx = params.dataIndex;
            if (lockedSector === idx) {
                // Toggle off
                lockedSector = -1;
                highlightedSector = -1;
                _donutResetHighlight();
            } else {
                // Lock this sector (reset first to clear previous offset)
                _donutResetHighlight();
                lockedSector = idx;
                highlightedSector = idx;
                highlightedStock = -1;
                _donutHideTooltip(container);
                _donutShowStockLabels(idx);
            }
        } else if (params.seriesName === 'Stocks') {
            const symbol = _donutStockLabels[params.dataIndex];
            if (symbol && _donutIndex) {
                Blazor.navigateTo(`/stocks/${_donutIndex}/${symbol}`);
            }
        }
    });

    // Click on empty area (background) to unlock
    _donutChart.getZr().on('click', function (e) {
        if (!e.target) {
            // Clicked on empty space
            lockedSector = -1;
            highlightedSector = -1;
            highlightedStock = -1;
            _donutResetHighlight();
            _donutHideTooltip(container);
            container.style.cursor = 'default';
        }
    });
}

function _donutShowTooltip(params, container) {
    let tooltip = container.querySelector('.donut-ext-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'donut-ext-tooltip';
        tooltip.style.cssText = 'position:absolute;pointer-events:none;z-index:20;' +
            'background:rgba(26,26,46,0.95);border:1px solid rgba(255,255,255,0.08);' +
            'border-radius:6px;padding:8px 12px;font-family:Inter,sans-serif;' +
            'font-size:11px;color:#A0AEC0;white-space:nowrap;transition:opacity 0.12s ease;opacity:0;' +
            'text-align:left;';
        container.style.position = 'relative';
        container.appendChild(tooltip);
    }

    const i = params.dataIndex;
    const symbol = _donutStockLabels[i];
    const name = _donutStockNames[i] || '';
    const realW = _donutStockRealWeights[i];
    const value = (realW !== undefined ? realW.toFixed(2) : '0.00') + '%';
    const logoPath = _donutLogoPaths[i] || '';
    const countryCode = _donutCountryCodes[i] || '';
    const changeRaw = _donutDayChangePcts[i] || 0;
    const change = changeRaw * 100;
    const changeTxt = (change >= 0 ? '+' : '') + change.toFixed(2) + '%';
    const changeColor = change >= 0 ? '#66BB6A' : '#EF5350';
    const logoImg = logoPath
        ? `<img src="${logoPath}" style="width:20px;height:20px;border-radius:50%;object-fit:cover;margin-right:6px;" onerror="this.style.display='none'" />`
        : '';
    const flag = countryCode
        ? `<span class="fi fi-${countryCode}" style="margin-right:4px;font-size:12px;"></span>`
        : '';

    tooltip.innerHTML = `<div style="display:flex;align-items:center;margin-bottom:3px;">` +
        `${logoImg}<div><div style="font-weight:600;color:#E0E0E0;">${flag}${_esc(symbol)}</div>` +
        (name ? `<div style="font-size:10px;opacity:0.6;">${_esc(name)}</div>` : '') +
        `</div></div>` +
        `<div>Weight: ${value}</div>` +
        `<div>Day: <span style="color:${changeColor};font-weight:600;">${changeTxt}</span></div>`;

    // Position radially outside the outer ring
    const cw = container.clientWidth;
    const ch = container.clientHeight;
    const centerX = cw / 2;
    const centerY = ch / 2;
    const outerR = Math.min(cw, ch) * 0.85 / 2; // matches outer radius percentage
    const ex = (params.event?.offsetX ?? centerX);
    const ey = (params.event?.offsetY ?? centerY);
    const dx = ex - centerX;
    const dy = ey - centerY;
    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
    const push = outerR + 14;
    let tx = centerX + (dx / dist) * push;
    let ty = centerY + (dy / dist) * push;

    const tw = tooltip.offsetWidth || 120;
    const th = tooltip.offsetHeight || 40;
    // Offset so tooltip doesn't overlap the anchor point
    if (dx > 0) tx += 4; else tx -= tw + 4;
    if (dy > 0) ty += 4; else ty -= th + 4;
    // Clamp within container
    tx = Math.max(0, Math.min(tx, cw - tw));
    ty = Math.max(0, Math.min(ty, ch - th));

    tooltip.style.left = tx + 'px';
    tooltip.style.top = ty + 'px';
    tooltip.style.opacity = '1';
}

function _donutHideTooltip(container) {
    const tooltip = container.querySelector('.donut-ext-tooltip');
    if (tooltip) tooltip.style.opacity = '0';
}

export function initDonutChart(canvasId, stockLabels, stockNames, stockDisplayWeights, stockRealWeights, stockColors,
                                sectorLabels, sectorDisplayWeights, sectorRealWeights, sectorColors, indexKey,
                                dayChangePcts, countryCodes, logoPaths, stockSectorIndices) {
    const container = document.getElementById(canvasId);
    if (!container) return false;
    if (_donutChart) { _donutChart.dispose(); _donutChart = null; }

    _donutStockLabels = stockLabels;
    _donutStockNames = stockNames;
    _donutSectorLabels = sectorLabels;
    _donutStockRealWeights = stockRealWeights;
    _donutSectorRealWeights = sectorRealWeights;
    _donutDayChangePcts = dayChangePcts || [];
    _donutCountryCodes = countryCodes || [];
    _donutLogoPaths = logoPaths || [];
    _donutStockSectorIndices = stockSectorIndices || [];
    _donutStockColors = stockColors || [];
    _donutSectorColors = sectorColors || [];
    _donutIndex = indexKey || '';

    _donutChart = echarts.init(container, null, { renderer: 'canvas' });
    _donutChart.setOption(_buildDonutOption());
    _donutSetupEvents(container);

    // Resize observer for responsiveness
    const ro = new ResizeObserver(() => { if (_donutChart) _donutChart.resize(); });
    ro.observe(container);
    container._donutRO = ro;

    return true;
}

export function updateDonutChart(stockLabels, stockNames, stockDisplayWeights, stockRealWeights, stockColors,
                                  sectorLabels, sectorDisplayWeights, sectorRealWeights, sectorColors, indexKey,
                                  dayChangePcts, countryCodes, logoPaths, stockSectorIndices) {
    if (!_donutChart) return;
    _donutStockLabels = stockLabels;
    _donutStockNames = stockNames;
    _donutSectorLabels = sectorLabels;
    _donutStockRealWeights = stockRealWeights;
    _donutSectorRealWeights = sectorRealWeights;
    _donutDayChangePcts = dayChangePcts || [];
    _donutCountryCodes = countryCodes || [];
    _donutLogoPaths = logoPaths || [];
    _donutStockSectorIndices = stockSectorIndices || [];
    _donutStockColors = stockColors || [];
    _donutSectorColors = sectorColors || [];
    if (indexKey) _donutIndex = indexKey;

    _donutChart.setOption(_buildDonutOption(), { notMerge: true });
}

export function destroyDonutChart() {
    if (_donutChart) {
        const dom = _donutChart.getDom();
        if (dom._donutRO) { dom._donutRO.disconnect(); delete dom._donutRO; }
        _donutChart.dispose();
        _donutChart = null;
    }
}

// ── Scrollbar sync ──────────────────────────────────────────────────
// Mirrors horizontal scroll between a hidden top scrollbar div (.top-scroll-mirror)
// and the MudTable's scroll container below it. This gives users a top scrollbar
// for wide tables. Clone-and-replace removes stale event listeners on re-init.
export function syncTopScrollbars() {
    document.querySelectorAll('.top-scroll-mirror').forEach(mirror => {
        const wrapper = mirror.nextElementSibling;
        if (!wrapper) return;
        const table = wrapper.querySelector('.mud-table-container');
        if (!table) return;

        // Set spacer width to match table scroll width
        const spacer = mirror.querySelector('.top-scroll-spacer');
        if (spacer) spacer.style.width = table.scrollWidth + 'px';

        // Remove old listeners by cloning
        const newMirror = mirror.cloneNode(true);
        mirror.parentNode.replaceChild(newMirror, mirror);
        const newSpacer = newMirror.querySelector('.top-scroll-spacer');
        if (newSpacer) newSpacer.style.width = table.scrollWidth + 'px';

        // Guard flag prevents infinite scroll event ping-pong between the two elements.
        let syncing = false;
        newMirror.addEventListener('scroll', () => {
            if (syncing) return;
            syncing = true;
            table.scrollLeft = newMirror.scrollLeft;
            syncing = false;
        });
        table.addEventListener('scroll', () => {
            if (syncing) return;
            syncing = true;
            newMirror.scrollLeft = table.scrollLeft;
            syncing = false;
        });
    });
}
