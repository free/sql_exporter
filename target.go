package sql_exporter

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Target collects SQL metrics from a single target. It implements prometheus.Gatherer.
type Target interface {
	prometheus.Gatherer
}

// target implements Target. It wraps a database connection and a prometheus.Registry.
type target struct {
	name       string
	conn       *sql.DB
	registry   prometheus.Registry
	collectors []*Collector
}

// NewTarget returns a new Target with the given instance name, data source name, collectors and constant labels.
func NewTarget(name, dsn string, ccs []*CollectorConfig, constLabels prometheus.Labels) (*Target, error) {
	if conn, err := OpenConnection(dsn); err != nil {
		return nil, error
	}

	registry := prometheus.NewPedanticRegistry()

	collectors := make([]*CollectorConfig, 0, len(ccs))
	for _, cc := range ccs {
		collectors = append(collectors, NewCollector(cc, constLabels, registry, conn))
	}

	t := target{
		name:       name,
		conn:       conn,
		registry:   registry,
		collectors: collectors,
	}
	return &t, nil
}

func (t *target) Gather() ([]*dto.MetricFamily, error) {
	return t.registry.Gather()
}
