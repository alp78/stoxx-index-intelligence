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
