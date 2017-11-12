package sql_exporter

import (
	"database/sql"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Collector is a self-contained group of SQL queries and metrics to collect from a specific database. It implements
// prometheus.Collector.
type Collector interface {
	prometheus.Collector
}

// collector implements Collector. It wraps a collection of queries, metrics and the database to collect them from.
type collector struct {
	config  *CollectorConfig
	queries []*Query
	metrics []*Metric

	conn *sql.DB
}

// NewCollector returns a new Collector with the given configuration and database. The metrics it creates will all have
// the provided const labels applied and will be registered with the provided registry.
func NewCollector(cc *CollectorConfig, constLabels prometheus.Labels, registry prometheus.Registerer) (Collector, error) {
	metrics := make([]*Metric, 0, len(cc.Metrics))
	queries := make([]*Query, 0, len(cc.Metrics))

	for _, mc := range cc.Metrics {
		m, err := NewMetric(mc, constLabels)
		if err != nil {
			return nil, errors.Wrapf(err, "error in metric %q defined by collector %q", mc.Name, cc.Name)
		}
		metrics = append(metrics, m)

		q, err := NewQuery(mc.Query, m)
		if err != nil {
			return nil, errors.Wrapf(err, "error in query defined by collector %q: %s", cc.Name, mc.Query)
		}
		queries = append(queries, q)
	}

	c := collector{
		config:  cc,
		queries: queries,
		metrics: metrics,
	}
	return &c, nil
}

// Describe implements prometheus.Collector.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m.Desc
	}
}

var (
	errorDesc = prometheus.NewDesc(
		"error",
		"error",
		[]string{},
		map[string]string{},
	)
)

// Collect implements prometheus.Collector.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for _, q := range c.queries {
		// TODO: add timeout
		rows, err := q.Run()
		if err != nil {
			// TODO: increment an error counter
			ch <- prometheus.NewInvalidMetric(errorDesc, err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			row, err := q.ScanRow(rows)
			if err != nil {
				ch <- prometheus.NewInvalidMetric(errorDesc,
					errors.Wrapf(err, "error while scanning row in collector %q", c.config.Name))
				continue
			}
			for _, m := range q.metrics {
				m.Collect(row, ch)
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			ch <- prometheus.NewInvalidMetric(errorDesc, err)
		}
	}
}
