// ── Sector Treemap ──────────────────────────────────────────────────
// Renders a squarified treemap of stocks grouped by sector,
// sized by market cap and colored by performance change %.

let _container = null;

/** Render a flag-icons span from a 2-letter country code (passed from C#). */
function countryFlagHtml(code) {
    return code ? `<span class="fi fi-${code}" style="margin-right:4px;"></span>` : '';
}

/** Color a tile by its change %: green (+) or red (-), intensity by magnitude. */
function changeColor(pct) {
    const v = Math.abs(pct * 100);
    const intensity = Math.min(v / 4, 1);  // full saturation at +/-4%
    if (pct > 0) {
        // dark green → bright green
        const r = Math.round(20 * (1 - intensity));
        const g = Math.round(80 + 90 * intensity);
        const b = Math.round(40 + 20 * intensity);
        return `rgb(${r},${g},${b})`;
    }
    if (pct < 0) {
        // dark red → bright red
        const r = Math.round(100 + 140 * intensity);
        const g = Math.round(30 * (1 - intensity));
        const b = Math.round(30 * (1 - intensity));
        return `rgb(${r},${g},${b})`;
    }
    return '#333';
}

function formatPct(v) {
    const p = (v * 100).toFixed(2);
    return v >= 0 ? `+${p}%` : `${p}%`;
}

/** Squarified treemap layout (Bruls, Huizing, van Wijk). */
function squarify(items, x, y, w, h) {
    if (!items.length || w <= 0 || h <= 0) return [];

    const total = items.reduce((s, i) => s + i.value, 0);
    if (total <= 0) return [];

    const rects = [];
    let remaining = [...items];
    let cx = x, cy = y, cw = w, ch = h;

    while (remaining.length > 0) {
        const areaLeft = remaining.reduce((s, i) => s + i.value, 0);
        const isVertical = cw >= ch;
        const side = isVertical ? ch : cw;

        // Greedily add items to current row while aspect ratio improves
        let row = [remaining[0]];
        let rowArea = remaining[0].value / total * w * h;
        remaining = remaining.slice(1);

        const worst = (row) => {
            const rowTotal = row.reduce((s, i) => s + i.value, 0);
            const rowFrac = rowTotal / areaLeft;
            const rowSide = rowFrac * (isVertical ? cw : ch);
            if (rowSide <= 0) return Infinity;
            let maxAR = 0;
            for (const item of row) {
                const itemFrac = item.value / rowTotal;
                const itemSide = itemFrac * side;
                const ar = Math.max(rowSide / itemSide, itemSide / rowSide);
                if (ar > maxAR) maxAR = ar;
            }
            return maxAR;
        };

        while (remaining.length > 0) {
            const candidate = [...row, remaining[0]];
            if (worst(candidate) <= worst(row)) {
                row.push(remaining[0]);
                remaining = remaining.slice(1);
            } else {
                break;
            }
        }

        // Lay out the row
        const rowTotal = row.reduce((s, i) => s + i.value, 0);
        const rowFrac = rowTotal / areaLeft;

        if (isVertical) {
            const rowW = rowFrac * cw;
            let ry = cy;
            for (const item of row) {
                const itemH = (item.value / rowTotal) * ch;
                rects.push({ ...item, x: cx, y: ry, w: rowW, h: itemH });
                ry += itemH;
            }
            cx += rowW;
            cw -= rowW;
        } else {
            const rowH = rowFrac * ch;
            let rx = cx;
            for (const item of row) {
                const itemW = (item.value / rowTotal) * cw;
                rects.push({ ...item, x: rx, y: cy, w: itemW, h: rowH });
                rx += itemW;
            }
            cy += rowH;
            ch -= rowH;
        }
    }
    return rects;
}

/** Build the full two-level treemap: sectors → stocks. */
function buildTreemap(data, containerW, containerH) {
    const totalCap = data.sectors.reduce((s, sec) => s + sec.totalCap, 0);
    if (totalCap <= 0) return [];

    // Level 1: lay out sectors
    const sectorItems = data.sectors.map(sec => ({
        value: sec.totalCap,
        sector: sec
    }));
    const sectorRects = squarify(sectorItems, 0, 0, containerW, containerH);

    // Level 2: lay out stocks within each sector rect
    const tiles = [];
    for (const sr of sectorRects) {
        const pad = 4;
        const headerH = 24;
        const sx = sr.x + pad;
        const sy = sr.y + pad + headerH;
        const sw = sr.w - pad * 2;
        const sh = sr.h - pad * 2 - headerH;

        tiles.push({
            type: 'sector-label',
            label: sr.sector.sector,
            x: sr.x + pad,
            y: sr.y + pad,
            w: sr.w - pad * 2,
            h: headerH
        });

        if (sw <= 0 || sh <= 0) continue;

        const stockItems = sr.sector.stocks.map(st => ({
            value: st.cap,
            stock: st
        }));
        const stockRects = squarify(stockItems, sx, sy, sw, sh);
        for (const stRect of stockRects) {
            tiles.push({
                type: 'stock',
                stock: stRect.stock,
                x: stRect.x,
                y: stRect.y,
                w: stRect.w,
                h: stRect.h
            });
        }
    }
    return tiles;
}

/** Custom tooltip element (shared across all tiles). */
let _tooltip = null;
function ensureTooltip() {
    if (_tooltip) return _tooltip;
    _tooltip = document.createElement('div');
    _tooltip.style.cssText = `
        position: fixed; z-index: 9999; pointer-events: none;
        background: rgba(20,20,35,0.95); color: #e0e0e0;
        border: 1px solid #444; border-radius: 6px;
        padding: 8px 12px; font-size: 0.8rem; line-height: 1.5;
        white-space: nowrap; display: none;
        box-shadow: 0 4px 12px rgba(0,0,0,0.5);
    `;
    document.body.appendChild(_tooltip);
    return _tooltip;
}
function showTooltip(e, html) {
    const tt = ensureTooltip();
    tt.innerHTML = html;
    tt.style.display = 'block';
    tt.style.left = (e.clientX + 14) + 'px';
    tt.style.top = (e.clientY + 14) + 'px';
}
function hideTooltip() {
    if (_tooltip) _tooltip.style.display = 'none';
}

/** Render tiles as DOM elements inside the container. */
function renderTiles(container, tiles) {
    container.innerHTML = '';
    container.style.position = 'relative';
    container.style.overflow = 'hidden';

    for (const tile of tiles) {
        if (tile.type === 'sector-label') {
            const el = document.createElement('div');
            el.style.cssText = `
                position: absolute;
                left: ${tile.x}px; top: ${tile.y}px;
                width: ${tile.w}px; height: ${tile.h}px;
                background: rgba(0,0,0,0.6);
                color: #ccc;
                font-size: 0.75rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                padding: 2px 6px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                line-height: ${tile.h}px;
                z-index: 2;
                pointer-events: none;
            `;
            el.textContent = tile.label;
            container.appendChild(el);
            continue;
        }

        const s = tile.stock;
        const el = document.createElement('div');
        const bg = changeColor(s.change);
        const gap = 1;
        const minW = tile.w - gap * 2;
        const minH = tile.h - gap * 2;

        el.style.cssText = `
            position: absolute;
            left: ${tile.x + gap}px; top: ${tile.y + gap}px;
            width: ${minW}px; height: ${minH}px;
            background: ${bg};
            border-radius: 2px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            cursor: pointer;
            transition: filter 0.15s;
        `;
        const flagHtml = countryFlagHtml(s.countryCode);
        const pctColor = s.change >= 0 ? '#4caf50' : '#ef5350';
        const tooltipHtml = `${flagHtml}<strong>${s.name}</strong><br>${s.symbol} <span style="color:${pctColor};font-weight:600">${formatPct(s.change)}</span>`;
        el.addEventListener('mouseenter', (e) => { el.style.filter = 'brightness(1.3)'; showTooltip(e, tooltipHtml); });
        el.addEventListener('mousemove', (e) => showTooltip(e, tooltipHtml));
        el.addEventListener('mouseleave', () => { el.style.filter = ''; hideTooltip(); });
        el.addEventListener('click', () => { if (s.href) Blazor.navigateTo(s.href); });

        // Adaptive content based on tile size
        if (minW > 50 && minH > 40) {
            const logoSize = Math.min(Math.max(Math.floor(Math.min(minW, minH) * 0.35), 16), 48);
            const img = document.createElement('img');
            img.src = s.logo;
            img.alt = s.symbol;
            img.style.cssText = `
                width: ${logoSize}px; height: ${logoSize}px;
                object-fit: cover;
                border-radius: 50%;
                margin-bottom: 2px;
                background: rgba(255,255,255,0.1);
                pointer-events: none;
            `;
            img.onerror = () => img.style.display = 'none';
            el.appendChild(img);
        }

        if (minW > 35 && minH > 25) {
            const sym = document.createElement('div');
            const fontSize = minW > 80 ? '0.7rem' : '0.55rem';
            sym.style.cssText = `
                color: #fff; font-weight: 700;
                font-size: ${fontSize};
                line-height: 1.1;
                text-align: center;
                pointer-events: none;
                text-shadow: 0 1px 2px rgba(0,0,0,0.6);
            `;
            sym.textContent = s.symbol;
            el.appendChild(sym);
        }

        if (minW > 45 && minH > 45) {
            const pct = document.createElement('div');
            const pctColor = s.change >= 0 ? 'rgba(255,255,255,0.9)' : 'rgba(255,255,255,0.8)';
            pct.style.cssText = `
                color: ${pctColor};
                font-size: 0.55rem;
                font-weight: 500;
                pointer-events: none;
                text-shadow: 0 1px 2px rgba(0,0,0,0.6);
            `;
            pct.textContent = formatPct(s.change);
            el.appendChild(pct);
        }

        container.appendChild(el);
    }
}

let _resizeObserver = null;
let _currentData = null;

export function initTreemap(containerId, data) {
    _container = document.getElementById(containerId);
    if (!_container) return;
    _currentData = data;

    const render = () => {
        const w = _container.clientWidth;
        const h = _container.clientHeight;
        if (w > 0 && h > 0) {
            const tiles = buildTreemap(_currentData, w, h);
            renderTiles(_container, tiles);
        }
    };

    render();

    // Re-render on resize
    _resizeObserver = new ResizeObserver(() => {
        if (_currentData) render();
    });
    _resizeObserver.observe(_container);
}

export function updateTreemap(data) {
    _currentData = data;
    if (!_container) return;
    const w = _container.clientWidth;
    const h = _container.clientHeight;
    if (w > 0 && h > 0) {
        const tiles = buildTreemap(data, w, h);
        renderTiles(_container, tiles);
    }
}

export function destroyTreemap() {
    if (_resizeObserver && _container) {
        _resizeObserver.disconnect();
        _resizeObserver = null;
    }
    if (_container) {
        _container.innerHTML = '';
        _container = null;
    }
    if (_tooltip) {
        _tooltip.remove();
        _tooltip = null;
    }
    _currentData = null;
}
