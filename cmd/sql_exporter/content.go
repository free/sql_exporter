package main

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/free/sql_exporter"
)

const (
	docsUrl   = "https://github.com/free/sql_exporter#readme"
	templates = `
    {{ define "page" -}}
      <html>
      <head>
        <title>Prometheus SQL Exporter</title>
        <style type="text/css">
          body { margin: 0; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 14px; line-height: 1.42857143; color: #333; background-color: #fff; }
          .navbar { display: flex; background-color: #222; margin: 0; border-width: 0 0 1px; border-style: solid; border-color: #080808; }
          .navbar > * { margin: 0; padding: 15px; }
          .navbar * { line-height: 20px; color: #9d9d9d; }
          .navbar a { text-decoration: none; }
          .navbar a:hover, .navbar a:focus { color: #fff; }
          .navbar-header { font-size: 18px; }
          body > * { margin: 15px; padding: 0; }
          pre { padding: 10px; font-size: 13px; background-color: #f5f5f5; border: 1px solid #ccc; }
          h1, h2 { font-weight: 500; }
          a { color: #337ab7; }
          a:hover, a:focus { color: #23527c; }
        </style>
      </head>
      <body>
        <div class="navbar">
          <div class="navbar-header"><a href="/">Prometheus SQL Exporter</a></div>
          <div><a href="{{ .MetricsPath }}">Metrics</a></div>
          <div><a href="/config">Configuration</a></div>
          <div><a href="/debug/pprof">Profiling</a></div>
          <div><a href="{{ .DocsUrl }}">Help</a></div>
        </div>
        {{template "content" .}}
      </body>
      </html>
    {{- end }}

    {{ define "content.home" -}}
      <p>This is a <a href="{{ .DocsUrl }}">Prometheus SQL Exporter</a> instance.
        You are probably looking for its <a href="{{ .MetricsPath }}">metrics</a> handler.</p>
    {{- end }}

    {{ define "content.config" -}}
      <h2>Configuration</h2>
      <pre>{{ .Config }}</pre>
    {{- end }}

    {{ define "content.error" -}}
      <h2>Error</h2>
      <pre>{{ .Err }}</pre>
    {{- end }}
    `
)

type tdata struct {
	MetricsPath string
	DocsUrl     string

	// `/config` only
	Config string

	// `/error` only
	Err error
}

var (
	allTemplates   = template.Must(template.New("").Parse(templates))
	homeTemplate   = pageTemplate("home")
	configTemplate = pageTemplate("config")
	errorTemplate  = pageTemplate("error")
)

func pageTemplate(name string) *template.Template {
	pageTemplate := fmt.Sprintf(`{{define "content"}}{{template "content.%s" .}}{{end}}{{template "page" .}}`, name)
	return template.Must(template.Must(allTemplates.Clone()).Parse(pageTemplate))
}

// HomeHandlerFunc is the HTTP handler for the home page (`/`).
func HomeHandlerFunc(metricsPath string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		homeTemplate.Execute(w, &tdata{
			MetricsPath: metricsPath,
			DocsUrl:     docsUrl,
		})
	}
}

// ConfigHandlerFunc is the HTTP handler for the `/config` page. It outputs the configuration marshaled in YAML format.
func ConfigHandlerFunc(metricsPath string, exporter sql_exporter.Exporter) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		config, err := exporter.Config().YAML()
		if err != nil {
			HandleError(err, metricsPath, w, r)
			return
		}
		configTemplate.Execute(w, &tdata{
			MetricsPath: metricsPath,
			DocsUrl:     docsUrl,
			Config:      string(config),
		})
	}
}

// HandleError is an error handler that other handlers defer to in case of error. It is important to not have written
// anything to w before calling HandleError(), or the 500 status code won't be set (and the content might be mixed up).
func HandleError(err error, metricsPath string, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	errorTemplate.Execute(w, &tdata{
		MetricsPath: metricsPath,
		DocsUrl:     docsUrl,
		Err:         err,
	})
}
