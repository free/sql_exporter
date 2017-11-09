package sql_exporter

import (
	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter collects SQL metrics. It implements prometheus.Collector.
type Exporter struct {
	jobs []*Job
}

// NewExporter returns a new SQL Exporter for the provided config.
func NewExporter(logger log.Logger, configFile string) (*Exporter, error) {
	if configFile == "" {
		configFile = "config.yml"
	}

	// read config
	cfg, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	exp := &Exporter{
		jobs:   make([]*Job, 0, len(cfg.Jobs)),
		logger: logger,
	}

	// dispatch all jobs
	for _, job := range cfg.Jobs {
		if job == nil {
			continue
		}
		if err := job.Init(logger, cfg.Queries); err != nil {
			log.Warningf("Skipping job %q. Failed to initialize: %s", job.Name, err)
			continue
		}
		exp.jobs = append(exp.jobs, job)
		go job.Run()
	}

	return exp, nil
}

// Describe implements prometheus.Collector
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, job := range e.jobs {
		if job == nil {
			continue
		}
		for _, query := range job.Queries {
			if query == nil {
				continue
			}
			if query.desc == nil {
				log.Errorf("Query has no descriptor: %s", query.Name)
				continue
			}
			ch <- query.desc
		}
	}
}

// Collect implements prometheus.Collector
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	for _, job := range e.jobs {
		if job == nil {
			continue
		}
		for _, query := range job.Queries {
			if query == nil {
				continue
			}
			for _, metrics := range query.metrics {
				for _, metric := range metrics {
					ch <- metric
				}
			}
		}
	}
}
