package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/alin-sinpalean/sql_exporter"
	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
)

func init() {
	prometheus.MustRegister(version.NewCollector("sql_exporter"))
}

func main() {
	var (
		showVersion   = flag.Bool("version", false, "Print version information.")
		listenAddress = flag.String("web.listen-address", ":9237", "Address to listen on for web interface and telemetry.")
		metricsPath   = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
		configFile    = flag.String("config.file", "sql_exporter.yml", "SQL Exporter configuration file name.")
	)

	// Override --alsologtostderr default value.
	if alsoLogToStderr := flag.Lookup("alsologtostderr"); alsoLogToStderr != nil {
		alsoLogToStderr.DefValue = "true"
		alsoLogToStderr.Value.Set("true")
	}
	// Override the config.file default with the CONFIG environment variable, if set. If the flag is explicitly set, it
	// will override both.
	if envConfigFile := os.Getenv("CONFIG"); envConfigFile != "" {
		*configFile = envConfigFile
	}
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Print("sql_exporter"))
		os.Exit(0)
	}

	log.Infof("Starting SQL exporter %s %s", version.Info(), version.BuildContext())

	exporter, err := sql_exporter.NewExporter(*configFile, prometheus.DefaultGatherer)
	if err != nil {
		log.Fatalf("Error starting exporter: %s", err)
	}
	// Replace the default gatherer with the exporter, which merges SQL metrics with those from the default gatherer.
	prometheus.DefaultGatherer = exporter

	// Setup and start webserver.
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "OK", http.StatusOK) })
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>SQL Exporter</title></head>
		<body>
		<h1>SQL Exporter</h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>
		`))
	})

	log.Infof("Listening on %s", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
