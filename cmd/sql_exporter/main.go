package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"

	_ "net/http/pprof"

	"github.com/free/sql_exporter"
	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"

	//"github.com/prometheus/common/log"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/exporter-toolkit/web"
)

var (
	showVersion   = flag.Bool("version", false, "Print version information.")
	listenAddress = flag.String("web.listen-address", ":9399", "Address to listen on for web interface and telemetry.")
	metricsPath   = flag.String("web.metrics-path", "/metrics", "Path under which to expose metrics.")
	configFile    = flag.String("config.file", "sql_exporter.yml", "SQL Exporter configuration file name.")
	webcfgFile    = flag.String("web.config", "", "Path to config yaml file that can enable TLS or authentication.")
)

func init() {
	prometheus.MustRegister(version.NewCollector("sql_exporter"))
}

func main() {
	if os.Getenv("DEBUG") != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

	// Override --alsologtostderr default value.
	if alsoLogToStderr := flag.Lookup("alsologtostderr"); alsoLogToStderr != nil {
		alsoLogToStderr.DefValue = "true"
		alsoLogToStderr.Value.Set("true")
	}
	// Override the config.file default with the CONFIG environment variable, if set. If the flag is explicitly set, it
	// will end up overriding either.
	if envConfigFile := os.Getenv("CONFIG"); envConfigFile != "" {
		*configFile = envConfigFile
	}
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Print("sql_exporter"))
		os.Exit(0)
	}

	log.Infof("Starting SQL exporter %s %s", version.Info(), version.BuildContext())

	exporter, err := sql_exporter.NewExporter(*configFile)
	if err != nil {
		log.Fatalf("Error creating exporter: %s", err)
	}

	// Setup and start webserver.
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "OK", http.StatusOK) })
	http.HandleFunc("/", HomeHandlerFunc(*metricsPath))
	http.HandleFunc("/config", ConfigHandlerFunc(*metricsPath, exporter))
	http.Handle(*metricsPath, ExporterHandlerFor(exporter))
	// Expose exporter metrics separately, for debugging purposes.
	http.Handle("/sql_exporter_metrics", promhttp.Handler())

	promlogConfig := &promlog.Config{}
	logger := promlog.New(promlogConfig)
	log.Infoln("Listening on", *listenAddress)
	server := &http.Server{Addr: *listenAddress}
	if err := web.Listen(server, *webcfgFile, logger); err != nil {
		log.Fatal(err)
	}

	//log.Infof("Listening on %s", *listenAddress)
	//log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

// LogFunc is an adapter to allow the use of any function as a promhttp.Logger. If f is a function, LogFunc(f) is a
// promhttp.Logger that calls f.
type LogFunc func(args ...interface{})

// Println implements promhttp.Logger.
func (log LogFunc) Println(args ...interface{}) {
	log(args)
}
