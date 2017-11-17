package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
)

// Collector is a self-contained group of SQL queries and metric families to collect from a specific database. It is
// conceptually similar to a prometheus.Collector.
type Collector interface {
	// Collect is the equivalent of prometheus.Collector.Collect() but takes a context to run in and a database to run on.
	Collect(context.Context, *sql.DB, chan<- Metric)
}

// collector implements Collector. It wraps a collection of queries, metrics and the database to collect them from.
type collector struct {
	config  *CollectorConfig
	queries []*Query
}

// NewCollector returns a new Collector with the given configuration and database. The metrics it creates will all have
// the provided const labels applied.
func NewCollector(cc *CollectorConfig, constLabels []*dto.LabelPair) (Collector, error) {
	// Maps each query to the list of metric families it populates.
	queryMFs := make(map[*QueryConfig][]*MetricFamily, len(cc.Metrics))

	// Instantiate metric families.
	for _, mc := range cc.Metrics {
		mf, err := NewMetricFamily(mc, constLabels)
		if err != nil {
			return nil, errors.Wrapf(err, "error in metric %q defined by collector %q", mc.Name, cc.Name)
		}
		mfs, found := queryMFs[mc.query]
		if !found {
			mfs = make([]*MetricFamily, 0, 2)
		}
		queryMFs[mc.query] = append(mfs, mf)
	}

	// Instantiate queries.
	queries := make([]*Query, 0, len(cc.Metrics))
	for qc, mfs := range queryMFs {
		q, err := NewQuery(qc, mfs...)
		if err != nil {
			return nil, errors.Wrapf(err, "error in query %q defined by collector %q", qc.Name, cc.Name)
		}
		queries = append(queries, q)
	}

	c := collector{
		config:  cc,
		queries: queries,
	}
	return &c, nil
}

// Collect implements Collector.
func (c *collector) Collect(ctx context.Context, conn *sql.DB, ch chan<- Metric) {
	for _, q := range c.queries {
		if ctx.Err() != nil {
			ch <- NewInvalidMetric(c.String(), ctx.Err())
			return
		}
		rows, err := q.Run(ctx, conn)
		if err != nil {
			// TODO: increment an error counter
			ch <- NewInvalidMetric(c.String(), err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			row, err := q.ScanRow(rows)
			if err != nil {
				ch <- NewInvalidMetric(fmt.Sprintf("error scanning row in collector %q", c.config.Name), err)
				continue
			}
			for _, mf := range q.metricFamilies {
				mf.Collect(row, ch)
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			ch <- NewInvalidMetric(c.String(), err)
		}
	}
}

// String implements fmt.Stringer.
func (c *collector) String() string {
	return fmt.Sprintf("collector %q", c.config.Name)
}
