package server

import (
	"embed"
	"html/template"
)

//go:embed templates/*.html
var templateFS embed.FS

// initTemplates parses templates from the embedded filesystem
// It returns a map where the key is the template name (e.g. "dashboard.html")
// and the value is the complete template set for that page
func initTemplates() (map[string]*template.Template, error) {
	templates := make(map[string]*template.Template)

	// Parse base.html first
	baseContent, err := templateFS.ReadFile("templates/base.html")
	if err != nil {
		return nil, err
	}

	baseTmpl, err := template.New("base.html").Parse(string(baseContent))
	if err != nil {
		return nil, err
	}

	// List of page templates
	pages := []string{"dashboard.html", "run_graph.html", "workflow_grid.html"}

	for _, page := range pages {
		// Clone the base template
		tmpl, err := baseTmpl.Clone()
		if err != nil {
			return nil, err
		}

		// Parse the page template into the clone
		pageContent, err := templateFS.ReadFile("templates/" + page)
		if err != nil {
			return nil, err
		}

		// Parse with the filename as the name
		_, err = tmpl.New(page).Parse(string(pageContent))
		if err != nil {
			return nil, err
		}

		templates[page] = tmpl
	}

	return templates, nil
}
