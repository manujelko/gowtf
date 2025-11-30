function applyFilters() {
    const searchInput = document.getElementById('workflow-search');
    const statusSelect = document.getElementById('status-filter');
    const runSelect = document.getElementById('run-filter');

    if (!searchInput || !statusSelect || !runSelect) return;

    const searchTerm = searchInput.value.toLowerCase();
    const statusFilter = statusSelect.value;
    const runFilter = runSelect.value;
    
    const rows = document.querySelectorAll('#workflow-table tbody tr');
    
    rows.forEach(row => {
        // Skip "No workflows found" row
        if (row.cells.length === 1) return;
        
        const nameElement = row.querySelector('td:nth-child(1) strong a');
        if (!nameElement) return;
        
        const name = nameElement.textContent.toLowerCase();
        const toggleBtn = row.querySelector('.toggle-btn');
        const isEnabled = toggleBtn ? toggleBtn.textContent.trim() === '✓' : false;
        
        // Get latest run status from the status circles (first one is latest)
        let latestRunStatus = 'none';
        const statusCircle = row.querySelector('.status-circles a:first-child');
        if (statusCircle) {
            if (statusCircle.classList.contains('status-success')) latestRunStatus = 'success';
            else if (statusCircle.classList.contains('status-failed')) latestRunStatus = 'failed';
            else if (statusCircle.classList.contains('status-running')) latestRunStatus = 'running';
        }
        
        let show = true;
        
        // Search filter
        if (searchTerm && !name.includes(searchTerm)) {
            show = false;
        }
        
        // Status filter
        if (show && statusFilter !== 'all') {
            if (statusFilter === 'enabled' && !isEnabled) show = false;
            if (statusFilter === 'disabled' && isEnabled) show = false;
        }
        
        // Run filter
        if (show && runFilter !== 'all') {
            if (runFilter !== latestRunStatus) show = false;
        }
        
        row.style.display = show ? '' : 'none';
    });
}

document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('workflow-search');
    const statusSelect = document.getElementById('status-filter');
    const runSelect = document.getElementById('run-filter');

    if (searchInput) searchInput.addEventListener('input', applyFilters);
    if (statusSelect) statusSelect.addEventListener('change', applyFilters);
    if (runSelect) runSelect.addEventListener('change', applyFilters);
    
    // Initial application
    applyFilters();
});

