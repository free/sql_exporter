package sql_exporter

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Exporter is a prometheus.Gatherer that gathers SQL metrics from targets and merges them with the default registry.
type Exporter interface {
	prometheus.Gatherer

	Config() *Config
}

type exporter struct {
	config          *Config
	jobs            []Job
	defaultGatherer prometheus.Gatherer
}

// NewExporter returns a new SQL Exporter for the provided config.
func NewExporter(configFile string, defaultGatherer prometheus.Gatherer) (Exporter, error) {
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, 0, len(config.Jobs))
	for _, jc := range config.Jobs {
		job, err := NewJob(jc)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return &exporter{
		config:          config,
		jobs:            jobs,
		defaultGatherer: defaultGatherer,
	}, nil
}

// Gather implements prometheus.Gatherer.
func (e *exporter) Gather() ([]*dto.MetricFamily, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Make sure to cancel the context, to release any resources associated with it.
	defer cancel()

	gatherers := make(prometheus.Gatherers, 0, len(e.jobs)*3)
	gatherers = append(gatherers, e.defaultGatherer)
	for _, j := range e.jobs {
		for _, t := range j.Targets() {
			gatherers = append(gatherers, NewGathererAdapter(ctx, t))
		}
	}
	return gatherers.Gather()
}

func (e *exporter) Config() *Config {
	return e.config
}

// gathererAdapter is a request scoped prometheus.Gatherer that wraps a Target, providing the context to Target.Gather.
type gathererAdapter struct {
	ctx    context.Context
	target Target
}

// NewGathererAdapter creates a prometheus.Gatherer that will invoke target.Gather in the context ctx.
func NewGathererAdapter(ctx context.Context, target Target) prometheus.Gatherer {
	return &gathererAdapter{
		ctx:    ctx,
		target: target,
	}
}

// Gather implements prometheus.Gatherer.
func (ga *gathererAdapter) Gather() ([]*dto.MetricFamily, error) {
	return ga.target.Gather(ga.ctx)
}
