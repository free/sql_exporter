package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"

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
	dsn        string
	registry   *prometheus.Registry
	collectors []Collector

	conn *sql.DB
}

// NewTarget returns a new Target with the given instance name, data source name, collectors and constant labels.
func NewTarget(name, dsn string, ccs []*CollectorConfig, constLabels prometheus.Labels) (Target, error) {
	registry := prometheus.NewPedanticRegistry()

	collectors := make([]Collector, 0, len(ccs))
	for _, cc := range ccs {
		c, err := NewCollector(cc, constLabels, registry)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, c)
		registry.MustRegister(c)
	}

	t := target{
		name:       name,
		dsn:        dsn,
		registry:   registry,
		collectors: collectors,
	}
	return &t, nil
}

func (t *target) Open(ctx context.Context) error {
	if t.conn != nil {
		return fmt.Errorf("Connection for target %q already open", t.name)
	}
	conn, err := OpenConnection(ctx, t.dsn)
	if err != nil {
		return nil, err
	}
	t.conn = conn
}

// Gather implements prometheus.Gatherer.
func (t *target) Gather() ([]*dto.MetricFamily, error) {
	if t.conn == nil {
		t.Open()
	}
	return t.registry.Gather()
}

func (t *target) String() string {
	return fmt.Sprintf("Target %s", t.name)
}
