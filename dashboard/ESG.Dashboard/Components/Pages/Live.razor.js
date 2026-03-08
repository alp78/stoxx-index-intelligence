// Live.razor.js — Dual synchronized multi-line charts for Pulse page
// Volume Surge + Range Intensity, per-stock lines + portfolio average

let _charts = [];  // { chart, seriesMap, avgSeries }

const PALETTE = [
    '#42A5F5', '#26A69A', '#AB47BC', '#FFA726', '#EF5350',
    '#66BB6A', '#EC407A', '#29B6F6', '#D4E157', '#8D6E63'
];

function _createChart(containerId, seriesData, legendId, avgData, suffix, baseline) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    const sfx = suffix || 'x';
    const chart = LightweightCharts.createChart(container, {
        autoSize: true,
        layout: {
            background: { type: 'solid', color: 'transparent' },
            textColor: '#A0AEC0',
            fontFamily: "'Inter', sans-serif",
            fontSize: 11,
        },
        grid: {
            vertLines: { color: 'rgba(255,255,255,0.04)' },
            horzLines: { color: 'rgba(255,255,255,0.04)' },
        },
        timeScale: {
            borderVisible: false,
            fixLeftEdge: true,
            fixRightEdge: true,
            rightOffset: 0,
            lockVisibleTimeRangeOnResize: true,
        },
        rightPriceScale: {
            borderVisible: false,
            scaleMargins: { top: 0.08, bottom: 0.08 },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
            vertLine: { color: 'rgba(255,255,255,0.15)', width: 1, style: 2, labelVisible: false },
            horzLine: { color: 'rgba(255,255,255,0.15)', width: 1, style: 2 },
        },
        handleScroll: { vertTouchDrag: false },
        localization: {
            priceFormatter: v => v.toFixed(2) + sfx,
        },
    });

    // Optional baseline (e.g. 1.0x for volume surge)
    let baselineSeries = null;
    if (baseline != null) {
        baselineSeries = chart.addSeries(LightweightCharts.LineSeries, {
            color: 'rgba(255,255,255,0.12)',
            lineWidth: 1,
            lineStyle: 2,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
            lastValueVisible: false,
        });
    }

    const seriesMap = {};
    let i = 0;
    const legendItems = [];

    for (const sd of seriesData) {
        const color = PALETTE[i % PALETTE.length];
        const series = chart.addSeries(LightweightCharts.LineSeries, {
            color: color,
            lineWidth: 0.5,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 3,
        });
        series.setData(sd.data);
        seriesMap[sd.symbol] = series;
        legendItems.push({ symbol: sd.symbol, color: color });
        i++;
    }

    // AVG line (white, thicker)
    let avgSeries = null;
    if (avgData && avgData.length > 0) {
        avgSeries = chart.addSeries(LightweightCharts.LineSeries, {
            color: 'rgba(255,255,255,0.7)',
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 3,
        });
        avgSeries.setData(avgData);
        legendItems.push({ symbol: 'AVG', color: 'rgba(255,255,255,0.7)' });
    }

    // Baseline spanning data range
    let minDate = null, maxDate = null;
    if (seriesData.length > 0 && seriesData[0].data.length > 0) {
        const allDates = seriesData.flatMap(s => s.data.map(d => d.time));
        if (avgData) allDates.push(...avgData.map(d => d.time));
        minDate = allDates.reduce((a, b) => a < b ? a : b);
        maxDate = allDates.reduce((a, b) => a > b ? a : b);
        if (baselineSeries && baseline != null) {
            baselineSeries.setData([{ time: minDate, value: baseline }, { time: maxDate, value: baseline }]);
        }
    }

    chart.timeScale().fitContent();

    // Legend
    const legendEl = document.getElementById(legendId);
    if (legendEl) {
        legendEl.innerHTML = '';
        for (const item of legendItems) {
            const span = document.createElement('span');
            span.style.cssText = 'display:inline-flex;align-items:center;gap:3px;margin-right:10px;font-size:0.6rem;opacity:0.7;cursor:pointer;';
            span.innerHTML = `<span style="display:inline-block;width:8px;height:3px;border-radius:1px;background:${item.color};"></span>${item.symbol}`;
            span.addEventListener('click', () => {
                const s = item.symbol === 'AVG' ? avgSeries : seriesMap[item.symbol];
                if (!s) return;
                const visible = s.options().visible !== false;
                s.applyOptions({ visible: !visible });
                span.style.opacity = visible ? '0.25' : '0.7';
            });
            legendEl.appendChild(span);
        }
    }

    // Tooltip
    let tooltip = container.querySelector('.perf-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'perf-tooltip';
        tooltip.style.cssText = 'display:none;position:absolute;z-index:10;background:rgba(21,21,33,0.94);padding:6px 10px;border-radius:4px;font-size:10px;pointer-events:none;font-family:"Inter",monospace;line-height:1.6;white-space:nowrap;color:#e0e0e0;border:1px solid rgba(255,255,255,0.08);';
        container.style.position = 'relative';
        container.appendChild(tooltip);
    }

    chart.subscribeCrosshairMove(param => {
        if (!param.time || param.seriesData.size === 0) {
            tooltip.style.display = 'none';
            return;
        }
        let html = `<div style="opacity:0.5;margin-bottom:2px;">${param.time}</div>`;
        const entries = [];
        for (const sd of seriesData) {
            const s = seriesMap[sd.symbol];
            if (!s) continue;
            const d = param.seriesData.get(s);
            if (d && d.value !== undefined) {
                entries.push({ symbol: sd.symbol, value: d.value, color: s.options().color });
            }
        }
        entries.sort((a, b) => b.value - a.value);
        if (avgSeries) {
            const ad = param.seriesData.get(avgSeries);
            if (ad && ad.value !== undefined) {
                const valColor = ad.value > 1.5 ? '#FFA726' : ad.value > 1.0 ? '#42A5F5' : '#A0A0B0';
                html += `<div style="border-bottom:1px solid rgba(255,255,255,0.1);padding-bottom:3px;margin-bottom:3px;"><span style="color:rgba(255,255,255,0.7);">&#9679;</span> <span style="font-weight:600;">INDEX AVG</span> <span style="color:${valColor};">${ad.value.toFixed(2)}${sfx}</span></div>`;
            }
        }
        for (const e of entries) {
            const valColor = e.value > 2.0 ? '#EF5350' : e.value > 1.5 ? '#FFA726' : e.value > 1.0 ? '#42A5F5' : '#A0A0B0';
            html += `<div><span style="color:${e.color};">&#9679;</span> <span style="font-weight:600;">${e.symbol}</span> <span style="color:${valColor};">${e.value.toFixed(2)}${sfx}</span></div>`;
        }
        tooltip.innerHTML = html;
        tooltip.style.display = 'block';

        const x = param.point.x;
        const w = container.clientWidth;
        tooltip.style.top = '8px';
        if (x > w / 2) {
            tooltip.style.left = 'auto';
            tooltip.style.right = (w - x + 12) + 'px';
        } else {
            tooltip.style.right = 'auto';
            tooltip.style.left = (x + 12) + 'px';
        }
    });

    // Build time->value map for crosshair sync (first series as anchor)
    const timeValueMap = {};
    if (seriesData.length > 0) {
        for (const pt of seriesData[0].data) timeValueMap[pt.time] = pt.value;
    }

    return { chart, seriesMap, avgSeries, timeValueMap };
}

export function initPulseCharts(configs) {
    destroyPulseCharts();
    _charts = [];

    for (let ci = 0; ci < configs.length; ci++) {
        const cfg = configs[ci];
        const result = _createChart(cfg.containerId, cfg.seriesData, cfg.legendId, cfg.avgData, cfg.suffix, cfg.baseline);
        if (result) _charts.push(result);
    }

    if (_charts.length < 2) return;

    // Sync zoom/pan via logical range (bar-index based)
    let _syncing = false;
    const timeScales = _charts.map(c => c.chart.timeScale());
    for (const ts of timeScales) {
        ts.subscribeVisibleLogicalRangeChange(range => {
            if (_syncing || !range) return;
            _syncing = true;
            for (const other of timeScales) {
                if (other !== ts) other.setVisibleLogicalRange(range);
            }
            _syncing = false;
        });
    }

    // Sync crosshair position across charts — must pass a real price
    // value so the tooltip callback fires with valid seriesData
    let _syncingCrosshair = false;
    for (const c of _charts) {
        c.chart.subscribeCrosshairMove(param => {
            if (_syncingCrosshair) return;
            _syncingCrosshair = true;
            for (const other of _charts) {
                if (other.chart === c.chart) continue;
                if (param.time) {
                    const anchor = Object.values(other.seriesMap)[0] || other.avgSeries;
                    const price = other.timeValueMap[param.time];
                    if (anchor && price !== undefined) {
                        try { other.chart.setCrosshairPosition(price, param.time, anchor); } catch (_) {}
                    }
                } else {
                    try { other.chart.clearCrosshairPosition(); } catch (_) {}
                }
            }
            _syncingCrosshair = false;
        });
    }
}

export function destroyPulseCharts() {
    for (const c of _charts) {
        try { c.chart.remove(); } catch (_) {}
    }
    _charts = [];
}

// Backward compat alias
export function destroyPerformanceChart() {
    destroyPulseCharts();
}
