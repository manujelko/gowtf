// Graph zoom and pan functionality
(function() {
    let isPanning = false;
    let startPoint = { x: 0, y: 0 };
    let viewBox = { x: 0, y: 0, width: 1000, height: 700 };
    let svg = null;
    let wheelHandler = null;
    let mouseDownHandler = null;
    let mouseMoveHandler = null;
    let mouseUpHandler = null;
    let selectStartHandler = null;

    // Store viewBox state in sessionStorage to persist across HTMX swaps
    function saveViewBox() {
        if (svg) {
            const vb = svg.getAttribute('viewBox');
            sessionStorage.setItem('graphViewBox', vb);
        }
    }

    function loadViewBox() {
        const saved = sessionStorage.getItem('graphViewBox');
        if (saved) {
            const vb = saved.split(' ');
            viewBox = {
                x: parseFloat(vb[0]) || 0,
                y: parseFloat(vb[1]) || 0,
                width: parseFloat(vb[2]) || 1000,
                height: parseFloat(vb[3]) || 700
            };
            return true;
        }
        return false;
    }

    function cleanup() {
        try {
            if (svg && svg.parentNode) {
                if (wheelHandler) {
                    svg.removeEventListener('wheel', wheelHandler);
                }
                if (mouseDownHandler) {
                    svg.removeEventListener('mousedown', mouseDownHandler);
                }
                if (selectStartHandler) {
                    svg.removeEventListener('selectstart', selectStartHandler);
                }
            }
        } catch (e) {
            // SVG might already be removed, ignore
        }
        
        if (mouseMoveHandler) {
            document.removeEventListener('mousemove', mouseMoveHandler);
        }
        if (mouseUpHandler) {
            document.removeEventListener('mouseup', mouseUpHandler);
        }
        
        // Reset handlers
        wheelHandler = null;
        mouseDownHandler = null;
        mouseMoveHandler = null;
        mouseUpHandler = null;
        selectStartHandler = null;
        svg = null;
    }

    function initGraph() {
        // Only cleanup if SVG changed
        if (svg && svg !== document.querySelector('.graph-svg')) {
            cleanup();
        }

        svg = document.querySelector('.graph-svg');
        if (!svg) return;

        // If we already have listeners on this SVG, don't re-initialize
        if (svg === document.querySelector('.graph-svg') && wheelHandler) {
            // Already initialized, just update viewBox from saved state if needed
            const saved = loadViewBox();
            if (saved) {
                const currentVB = svg.getAttribute('viewBox');
                const savedVB = `${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`;
                if (currentVB !== savedVB) {
                    svg.setAttribute('viewBox', savedVB);
                }
            }
            return;
        }

        // Get current viewBox from SVG
        const currentVB = svg.getAttribute('viewBox');
        const vb = currentVB.split(' ');
        viewBox = {
            x: parseFloat(vb[0]) || 0,
            y: parseFloat(vb[1]) || 0,
            width: parseFloat(vb[2]) || 1000,
            height: parseFloat(vb[3]) || 700
        };

        // Try to load saved viewBox
        const hasSaved = loadViewBox();
        if (hasSaved) {
            // Restore saved viewBox
            svg.setAttribute('viewBox', `${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`);
        } else {
            // Save current for next time
            saveViewBox();
        }

        // Create handler functions
        wheelHandler = handleWheel;
        mouseDownHandler = handleMouseDown;
        mouseMoveHandler = handleMouseMove;
        mouseUpHandler = handleMouseUp;
        selectStartHandler = function(e) {
            if (isPanning) e.preventDefault();
        };

        // Mouse wheel zoom
        svg.addEventListener('wheel', wheelHandler, { passive: false });

        // Mouse drag pan
        svg.addEventListener('mousedown', mouseDownHandler);
        document.addEventListener('mousemove', mouseMoveHandler);
        document.addEventListener('mouseup', mouseUpHandler);

        // Prevent text selection while dragging
        svg.addEventListener('selectstart', selectStartHandler);

        // Handle node clicks
        svg.addEventListener('click', function(e) {
            const nodeGroup = e.target.closest('.graph-node-group');
            if (nodeGroup) {
                const node = nodeGroup.querySelector('.graph-node');
                if (node && (e.target === node || e.target.closest('.graph-node-group'))) {
                    const instanceID = node.getAttribute('data-instance-id');
                    if (instanceID) {
                        e.preventDefault();
                        e.stopPropagation();
                        showTaskLogs(instanceID);
                    } else {
                        const taskName = node.getAttribute('data-task-name');
                        alert(`Task "${taskName}" has not started yet. No logs available.`);
                    }
                }
            }
        });
    }

    function handleWheel(e) {
        e.preventDefault();

        const delta = e.deltaY > 0 ? 1.1 : 0.9;
        const rect = svg.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;

        // Calculate mouse position in SVG coordinates
        const svgX = viewBox.x + (mouseX / rect.width) * viewBox.width;
        const svgY = viewBox.y + (mouseY / rect.height) * viewBox.height;

        // Zoom
        const newWidth = viewBox.width * delta;
        const newHeight = viewBox.height * delta;

        // Adjust viewBox to zoom towards mouse position
        viewBox.x = svgX - (mouseX / rect.width) * newWidth;
        viewBox.y = svgY - (mouseY / rect.height) * newHeight;
        viewBox.width = newWidth;
        viewBox.height = newHeight;

        // Limit zoom
        if (viewBox.width < 200) {
            const scale = 200 / viewBox.width;
            viewBox.width = 200;
            viewBox.height *= scale;
            viewBox.x = svgX - (mouseX / rect.width) * viewBox.width;
            viewBox.y = svgY - (mouseY / rect.height) * viewBox.height;
        }
        if (viewBox.width > 5000) {
            const scale = 5000 / viewBox.width;
            viewBox.width = 5000;
            viewBox.height *= scale;
            viewBox.x = svgX - (mouseX / rect.width) * viewBox.width;
            viewBox.y = svgY - (mouseY / rect.height) * viewBox.height;
        }

        updateViewBox();
    }

    function handleMouseDown(e) {
        if (e.button !== 0) return; // Only left mouse button
        // Handle node clicks
        if (e.target.classList.contains('graph-node') || 
            e.target.closest('.graph-node-group')) {
            const nodeGroup = e.target.closest('.graph-node-group');
            if (nodeGroup) {
                const node = nodeGroup.querySelector('.graph-node');
                if (node) {
                    const instanceID = node.getAttribute('data-instance-id');
                    if (instanceID) {
                        e.preventDefault();
                        e.stopPropagation();
                        showTaskLogs(instanceID);
                        return;
                    } else {
                        // No instance yet (task hasn't started)
                        const taskName = node.getAttribute('data-task-name');
                        alert(`Task "${taskName}" has not started yet. No logs available.`);
                    }
                }
            }
            return;
        }
        // Don't pan if clicking on label
        if (e.target.classList.contains('graph-node-label')) {
            return;
        }
        isPanning = true;
        startPoint = { x: e.clientX, y: e.clientY };
        svg.style.cursor = 'grabbing';
        e.preventDefault();
    }

    function handleMouseMove(e) {
        if (!isPanning) return;

        const rect = svg.getBoundingClientRect();
        const dx = (e.clientX - startPoint.x) * (viewBox.width / rect.width);
        const dy = (e.clientY - startPoint.y) * (viewBox.height / rect.height);

        viewBox.x -= dx;
        viewBox.y -= dy;

        startPoint = { x: e.clientX, y: e.clientY };
        updateViewBox();
        e.preventDefault();
    }

    function handleMouseUp(e) {
        if (isPanning) {
            isPanning = false;
            svg.style.cursor = 'grab';
        }
    }

    function updateViewBox() {
        if (svg) {
            const newVB = `${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`;
            const currentVB = svg.getAttribute('viewBox');
            // Only update if different to avoid unnecessary DOM updates
            if (currentVB !== newVB) {
                svg.setAttribute('viewBox', newVB);
            }
            saveViewBox();
        }
    }

    // No longer need MutationObserver since SVG won't be replaced
    function startObserving() {
        // Not needed anymore - SVG stays in DOM
    }

    // Initialize on page load
    function initialize() {
        initGraph();
        startObserving();
        startAutoRefresh();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initialize);
    } else {
        initialize();
    }
    
    // Cleanup on page unload
    window.addEventListener('beforeunload', function() {
        stopAutoRefresh();
        cleanup();
        if (observer) {
            observer.disconnect();
        }
    });

    // Auto-refresh node states without replacing SVG (no HTMX)
    let refreshInterval = null;
    
    function startAutoRefresh() {
        // Get run ID from URL
        const path = window.location.pathname;
        const match = path.match(/\/run\/(\d+)/);
        if (!match) return;
        
        const runID = match[1];
        const url = `/run/${runID}?format=json`;
        
        refreshInterval = setInterval(function() {
            fetch(url)
                .then(response => response.json())
                .then(data => {
                    // Update node states without touching the SVG structure
                    if (data.nodeStates && svg) {
                        const nodes = svg.querySelectorAll('.graph-node');
                        nodes.forEach(node => {
                            const taskID = parseInt(node.getAttribute('data-task-id'));
                            const newState = data.nodeStates[taskID];
                            if (newState) {
                                const currentState = node.getAttribute('data-state');
                                if (currentState !== newState) {
                                    // Update state
                                    node.setAttribute('data-state', newState);
                                    // Update class
                                    node.classList.remove('status-pending', 'status-queued', 'status-running', 
                                                          'status-success', 'status-failed', 'status-skipped');
                                    node.classList.add('status-' + newState);
                                }
                            }
                        });
                    }
                })
                .catch(err => {
                    // Silently fail - network error or run deleted
                });
        }, 2000);
    }
    
    function stopAutoRefresh() {
        if (refreshInterval) {
            clearInterval(refreshInterval);
            refreshInterval = null;
        }
    }

    // Show task logs in a modal
    function showTaskLogs(instanceID) {
        let logRefreshInterval = null;
        let isAutoScrolling = true;

        function loadLogs() {
            fetch(`/api/task-instance/${instanceID}/logs`)
                .then(response => response.json())
                .then(data => {
                    // Update status badge
                    const badge = modal.querySelector('.log-info .badge');
                    if (badge) {
                        badge.className = 'badge status-' + data.state;
                        badge.textContent = data.state;
                    }

                    // Update exit code
                    const exitCodeSpan = modal.querySelector('.log-info .exit-code');
                    if (data.exit_code !== null && data.exit_code !== undefined) {
                        if (!exitCodeSpan) {
                            const infoDiv = modal.querySelector('.log-info');
                            const exitCodeEl = document.createElement('span');
                            exitCodeEl.className = 'exit-code';
                            exitCodeEl.textContent = `Exit Code: ${data.exit_code}`;
                            infoDiv.appendChild(exitCodeEl);
                        } else {
                            exitCodeSpan.textContent = `Exit Code: ${data.exit_code}`;
                        }
                    }

                    // Update stdout
                    const stdoutEl = document.getElementById('stdout-content');
                    if (stdoutEl) {
                        const wasAtBottom = stdoutEl.scrollHeight - stdoutEl.scrollTop <= stdoutEl.clientHeight + 10;
                        stdoutEl.textContent = data.stdout || '(empty)';
                        if (isAutoScrolling && wasAtBottom) {
                            setTimeout(() => {
                                stdoutEl.scrollTop = stdoutEl.scrollHeight;
                            }, 0);
                        }
                    }

                    // Update stderr
                    const stderrEl = document.getElementById('stderr-content');
                    if (stderrEl) {
                        const wasAtBottom = stderrEl.scrollHeight - stderrEl.scrollTop <= stderrEl.clientHeight + 10;
                        stderrEl.textContent = data.stderr || '(empty)';
                        if (isAutoScrolling && wasAtBottom) {
                            setTimeout(() => {
                                stderrEl.scrollTop = stderrEl.scrollHeight;
                            }, 0);
                        }
                    }

                    // Stop refreshing if task is completed (success or failed)
                    if (data.state === 'success' || data.state === 'failed') {
                        if (logRefreshInterval) {
                            clearInterval(logRefreshInterval);
                            logRefreshInterval = null;
                        }
                    }
                })
                .catch(err => {
                    console.error('Failed to load task logs:', err);
                });
        }

        // Create modal
        const modal = document.createElement('div');
        modal.className = 'log-modal';
        modal.innerHTML = `
            <div class="log-modal-content">
                <div class="log-modal-header">
                    <h3>Task Logs: <span id="task-name">Loading...</span></h3>
                    <button class="log-modal-close" onclick="this.closest('.log-modal').remove()">×</button>
                </div>
                <div class="log-modal-body">
                    <div class="log-info">
                        <span class="badge status-pending">Loading...</span>
                    </div>
                    <div class="log-tabs">
                        <button class="log-tab active" data-tab="stdout">Stdout</button>
                        <button class="log-tab" data-tab="stderr">Stderr</button>
                    </div>
                    <div class="log-content">
                        <pre class="log-output active" id="stdout-content">Loading logs...</pre>
                        <pre class="log-output" id="stderr-content">Loading logs...</pre>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        // Load initial logs
        loadLogs();

        // Start auto-refresh every 1 second if task is still running
        logRefreshInterval = setInterval(() => {
            loadLogs();
        }, 1000);

        // Track scroll position to determine if we should auto-scroll
        const stdoutEl = document.getElementById('stdout-content');
        const stderrEl = document.getElementById('stderr-content');
        
        if (stdoutEl) {
            stdoutEl.addEventListener('scroll', function() {
                const isAtBottom = this.scrollHeight - this.scrollTop <= this.clientHeight + 10;
                isAutoScrolling = isAtBottom;
            });
        }
        
        if (stderrEl) {
            stderrEl.addEventListener('scroll', function() {
                const isAtBottom = this.scrollHeight - this.scrollTop <= this.clientHeight + 10;
                isAutoScrolling = isAtBottom;
            });
        }

        // Tab switching
        modal.querySelectorAll('.log-tab').forEach(tab => {
            tab.addEventListener('click', function() {
                modal.querySelectorAll('.log-tab').forEach(t => t.classList.remove('active'));
                modal.querySelectorAll('.log-output').forEach(o => o.classList.remove('active'));
                this.classList.add('active');
                const tabName = this.getAttribute('data-tab');
                const contentEl = document.getElementById(tabName + '-content');
                if (contentEl) {
                    contentEl.classList.add('active');
                    // Auto-scroll to bottom when switching tabs
                    setTimeout(() => {
                        contentEl.scrollTop = contentEl.scrollHeight;
                    }, 0);
                }
            });
        });

        // Close on background click
        modal.addEventListener('click', function(e) {
            if (e.target === modal) {
                if (logRefreshInterval) {
                    clearInterval(logRefreshInterval);
                }
                modal.remove();
            }
        });

        // Close on Escape key
        const closeHandler = function(e) {
            if (e.key === 'Escape') {
                if (logRefreshInterval) {
                    clearInterval(logRefreshInterval);
                }
                modal.remove();
                document.removeEventListener('keydown', closeHandler);
            }
        };
        document.addEventListener('keydown', closeHandler);

        // Update task name after first load
        fetch(`/api/task-instance/${instanceID}/logs`)
            .then(response => response.json())
            .then(data => {
                const taskNameEl = document.getElementById('task-name');
                if (taskNameEl) {
                    taskNameEl.textContent = data.task_name || 'Unknown';
                }
            });
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
})();

