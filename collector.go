package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
)

// Collector is a self-contained group of SQL queries and metrics to collect from a specific database. It is
// conceptually similar to a prometheus.Collector, but doesn't implement it because it requires a context to run in.
type Collector interface {
	// Collect is the equivalent of prometheus.Collector.Collect() but takes a context to run in and a database to run on.
	Collect(context.Context, *sql.DB, chan<- MetricValue)
}

// collector implements Collector. It wraps a collection of queries, metrics and the database to collect them from.
type collector struct {
	config  *CollectorConfig
	queries []*Query
	metrics []*Metric
}

// NewCollector returns a new Collector with the given configuration and database. The metrics it creates will all have
// the provided const labels applied.
func NewCollector(cc *CollectorConfig, constLabels []*dto.LabelPair) (Collector, error) {
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

// Collect implements Collector.
func (c *collector) Collect(ctx context.Context, conn *sql.DB, ch chan<- MetricValue) {
	for _, q := range c.queries {
		if ctx.Err() != nil {
			ch <- NewInvalidMetric(ctx.Err())
			return
		}
		rows, err := q.Run(ctx, conn)
		if err != nil {
			// TODO: increment an error counter
			ch <- NewInvalidMetric(err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			row, err := q.ScanRow(rows)
			if err != nil {
				ch <- NewInvalidMetric(errors.Wrapf(err, "error while scanning row in collector %q", c.config.Name))
				continue
			}
			for _, m := range q.metrics {
				m.Collect(row, ch)
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			ch <- NewInvalidMetric(err)
		}
	}
}

// String implements fmt.Stringer.
func (c *collector) String() string {
	return fmt.Sprintf("collector %q", c.config.Name)
}
