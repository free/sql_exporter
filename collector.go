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
	conn    *sql.DB
	queries map[*sql.Stmt]Metrics
	metrics Metrics
}

// NewCollector returns a new Collector with the given configuration and database. The metrics it creates will all have
// the provided const labels applied and will be registered with the provided registry.
func NewCollector(cc *CollectorConfig, constLabels prometheus.Labels, registry prometheus.Registerer, conn *sql.DB) (Collector, error) {
	metrics := make(Metrics, 0, len(cc.Metrics))
	queries := make(map[*sql.Stmt]Metrics, len(cc.Metrics))

	for _, mc := range cc.Metrics {
		m, err := NewMetric(mc)
		if err != nil {
			return nil, errors.Wrapf(err, "Error in metric %q defined by collector %q", mc.Name, cc.Name)
		}
		metrics = append(metrics, m)

		query, err := conn.Prepare(mc.Query)
		if err != nil {
			return nil, errors.Wrapf(err, "Error in query defined by collector %q: %s", cc.Name, mc.Query)
		}
		queries[query] = Metrics{m}
	}

	c := collector{
		config:  cc,
		conn:    conn,
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

// Collect implements prometheus.Collector.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	for query, metrics := range c.queries {
		// TODO: add timeout
		rows, err := query.Query()
		if err != nil {
			// TODO: increment an error counter
			ch <- prometheus.NewInvalidMetric(metrics[0].Desc, err)
		}
		defer rows.Close()

		for rows.Next() {

			for _, m := range metrics {
				m.Collect(ch)
			}
		}
		if err = rows.Err(); err != nil {
			ch <- prometheus.NewInvalidMetric(metrics[0].Desc, err)
		}
	}
}

// scanRow scans the contents of a
func (c *collector) scanRow() (map[string]interface{}, err) {

}
