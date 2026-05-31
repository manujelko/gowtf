document.addEventListener('DOMContentLoaded', () => {
    const savedTheme = localStorage.getItem('theme');
    if (savedTheme) {
        document.body.classList.add(savedTheme);
    }

    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        // Update icon based on current theme
        const updateIcon = () => {
            if (document.body.classList.contains('dark')) {
                themeToggle.textContent = '☾';
            } else {
                themeToggle.textContent = '☼';
            }
        };
        
        // Set initial icon
        updateIcon();

        themeToggle.addEventListener('click', () => {
            if (document.body.classList.contains('dark')) {
                document.body.classList.remove('dark');
                document.body.classList.add('light');
                localStorage.setItem('theme', 'light');
            } else if (document.body.classList.contains('light')) {
                document.body.classList.remove('light');
                document.body.classList.add('dark');
                localStorage.setItem('theme', 'dark');
            } else {
                // If no class is set, check system preference
                if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
                    document.body.classList.add('light');
                    localStorage.setItem('theme', 'light');
                } else {
                    document.body.classList.add('dark');
                    localStorage.setItem('theme', 'dark');
                }
            }
            updateIcon();
        });
    }
});


