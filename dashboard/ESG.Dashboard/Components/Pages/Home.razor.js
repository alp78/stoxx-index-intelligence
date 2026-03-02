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
