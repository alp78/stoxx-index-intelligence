// ── Tooltip (managed entirely in JS to avoid Blazor re-renders) ──

export function setTooltipData(container, lookup, indices, options) {
    container._ttLookup = lookup;
    container._ttIndices = indices;
    container._ttOptions = options || {};
}

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
    let html = `<div style="opacity: 0.5; margin-bottom: 2px;">${entry.date}</div>`;
    for (const idx of indices) {
        const val = entry.values[idx.key];
        const hasVal = val !== undefined && val !== null;
        const valColor = colorBySign
            ? (hasVal ? (val >= 0 ? '#26A69A' : '#EF5350') : 'inherit')
            : idx.color;
        const prefix = (colorBySign && hasVal && val >= 0) ? '+' : '';
        const valText = hasVal ? prefix + val.toFixed(2) + '%' : '--';
        html += `<div style="display: flex; align-items: center; gap: 6px;">` +
            `<span style="display:inline-block;width:8px;height:2px;background:${idx.color};border-radius:1px;flex-shrink:0;"></span>` +
            `<span style="opacity:0.5;flex:1;">${idx.name}</span>` +
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

export function hideTooltip(container) {
    const tooltip = container.querySelector('.chart-tooltip');
    if (tooltip) tooltip.style.display = 'none';
}

// ── Chart.js: Radar ──

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

export function initRadarChart(canvasId, labels, data, indexName) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
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
}

export function updateRadarChart(data, indexName) {
    if (!_radarChart) return;
    _radarChart.data.datasets[0].data = data;
    _radarChart.data.datasets[0].label = indexName;
    _radarChart.update('none');
}

export function destroyRadarChart() {
    if (_radarChart) { _radarChart.destroy(); _radarChart = null; }
}

// ── Chart.js: Donut ──

let _donutChart = null;
let _donutStockLabels = [];
let _donutStockNames = [];
let _donutSectorLabels = [];
let _donutStockRealWeights = [];
let _donutSectorRealWeights = [];

let _donutIndex = '';

function _getOrCreateDonutTooltip(canvas) {
    const wrap = canvas.parentElement;
    let el = wrap.querySelector('.donut-ext-tooltip');
    if (!el) {
        el = document.createElement('div');
        el.className = 'donut-ext-tooltip';
        el.style.cssText = 'position:absolute;pointer-events:none;z-index:20;' +
            'background:rgba(26,26,46,0.95);border:1px solid rgba(255,255,255,0.08);' +
            'border-radius:4px;padding:6px 10px;font-family:Inter,sans-serif;' +
            'font-size:11px;color:#A0AEC0;white-space:nowrap;transition:opacity 0.12s ease;opacity:0;';
        wrap.appendChild(el);
    }
    return el;
}

export function initDonutChart(canvasId, stockLabels, stockNames, stockDisplayWeights, stockRealWeights, stockColors,
                                sectorLabels, sectorDisplayWeights, sectorRealWeights, sectorColors, indexKey) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    if (_donutChart) _donutChart.destroy();

    _donutStockLabels = stockLabels;
    _donutStockNames = stockNames;
    _donutSectorLabels = sectorLabels;
    _donutStockRealWeights = stockRealWeights;
    _donutSectorRealWeights = sectorRealWeights;
    _donutIndex = indexKey || '';

    // Click handler — navigate to stock page
    ctx.onclick = function (evt) {
        if (!_donutChart) return;
        const pts = _donutChart.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, false);
        if (pts.length === 0 || pts[0].datasetIndex !== 0) return;
        const symbol = _donutStockLabels[pts[0].index];
        if (symbol && _donutIndex) {
            window.location.href = `/stocks/${_donutIndex}/${symbol}`;
        }
    };
    ctx.style.cursor = 'default';
    ctx.onmousemove = function (evt) {
        if (!_donutChart) return;
        const pts = _donutChart.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, false);
        ctx.style.cursor = (pts.length > 0 && pts[0].datasetIndex === 0) ? 'pointer' : 'default';
    };

    _donutChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            datasets: [
                { data: stockDisplayWeights, backgroundColor: stockColors, borderColor: '#1E1E2D', borderWidth: 1, weight: 1 },
                { data: sectorDisplayWeights, backgroundColor: sectorColors, borderColor: '#1E1E2D', borderWidth: 1, weight: 0.6 }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '30%',
            plugins: {
                legend: { display: false },
                tooltip: {
                    enabled: false,
                    external: function (context) {
                        const tooltip = _getOrCreateDonutTooltip(context.chart.canvas);
                        if (context.tooltip.opacity === 0) {
                            tooltip.style.opacity = '0';
                            return;
                        }
                        const tt = context.tooltip;
                        const el = tt.dataPoints[0];
                        const isSector = el.datasetIndex === 1;
                        const symbol = isSector ? _donutSectorLabels[el.dataIndex] : _donutStockLabels[el.dataIndex];
                        const name = (!isSector && _donutStockNames[el.dataIndex]) ? _donutStockNames[el.dataIndex] : '';
                        const realW = isSector ? _donutSectorRealWeights[el.dataIndex] : _donutStockRealWeights[el.dataIndex];
                        const value = (realW !== undefined ? realW.toFixed(2) : el.raw.toFixed(2)) + '%';
                        const color = el.element?.options?.backgroundColor || '';
                        const swatch = isSector && color
                            ? `<span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${color};margin-right:5px;vertical-align:middle;"></span>`
                            : '';
                        tooltip.innerHTML =
                            `<div style="font-weight:600;color:#E0E0E0;display:flex;align-items:center;">${swatch}${symbol}</div>` +
                            (name ? `<div style="font-size:10px;opacity:0.6;">${name}</div>` : '') +
                            `<div>Weight: ${value}</div>`;

                        // Position outside the donut: push outward from center
                        const chart = context.chart;
                        const cx = (chart.chartArea.left + chart.chartArea.right) / 2;
                        const cy = (chart.chartArea.top + chart.chartArea.bottom) / 2;
                        const dx = tt.caretX - cx;
                        const dy = tt.caretY - cy;
                        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
                        const outerR = chart.getDatasetMeta(0).data[0]?.outerRadius || 100;
                        const push = outerR + 14;
                        let tx = cx + (dx / dist) * push;
                        let ty = cy + (dy / dist) * push;

                        // Offset so tooltip doesn't overlap the edge
                        const tw = tooltip.offsetWidth || 120;
                        const th = tooltip.offsetHeight || 40;
                        if (dx > 0) tx += 4; else tx -= tw + 4;
                        if (dy > 0) ty += 4; else ty -= th + 4;

                        // Clamp within container
                        const cw = chart.canvas.parentElement.clientWidth;
                        const ch = chart.canvas.parentElement.clientHeight;
                        tx = Math.max(0, Math.min(tx, cw - tw));
                        ty = Math.max(0, Math.min(ty, ch - th));

                        tooltip.style.left = tx + 'px';
                        tooltip.style.top = ty + 'px';
                        tooltip.style.opacity = '1';
                    }
                }
            }
        }
    });
}

export function updateDonutChart(stockLabels, stockNames, stockDisplayWeights, stockRealWeights, stockColors,
                                  sectorLabels, sectorDisplayWeights, sectorRealWeights, sectorColors, indexKey) {
    if (!_donutChart) return;
    _donutStockLabels = stockLabels;
    _donutStockNames = stockNames;
    _donutSectorLabels = sectorLabels;
    _donutStockRealWeights = stockRealWeights;
    _donutSectorRealWeights = sectorRealWeights;
    if (indexKey) _donutIndex = indexKey;
    _donutChart.data.datasets[0].data = stockDisplayWeights;
    _donutChart.data.datasets[0].backgroundColor = stockColors;
    _donutChart.data.datasets[1].data = sectorDisplayWeights;
    _donutChart.data.datasets[1].backgroundColor = sectorColors;
    _donutChart.update('none');
}

export function destroyDonutChart() {
    if (_donutChart) { _donutChart.destroy(); _donutChart = null; }
}

// ── Scrollbar sync ──

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
