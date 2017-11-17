package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	// Capacity for the channel to collect metrics.
	capMetricChan = 1000
)

var (
	upMetricConfig = MetricConfig{
		Name:      "up",
		Help:      "1 if the target is reachable, or 0 if the scrape failed",
		Values:    []string{"foo"},
		valueType: prometheus.GaugeValue,
	}
	scrapeDurationConfig = MetricConfig{
		Name:      "scrape_duration_seconds",
		Help:      "How long it took to scrape the target in seconds",
		Values:    []string{"foo"},
		valueType: prometheus.GaugeValue,
	}
)

// Target collects SQL metrics from a single sql.DB instance. It aggregates one or more Collectors and it looks much
// like a prometheus.Gatherer, except its Gather() method takes a Context to run in.
type Target interface {
	// Gather is the equivalent of prometheus.Gatherer.Gather(), but takes a context to run in.
	Gather(ctx context.Context) ([]*dto.MetricFamily, error)
}

// target implements Target. It wraps a sql.DB, which is initially nil but never changes once instantianted.
type target struct {
	name               string
	dsn                string
	collectors         []Collector
	constLabels        prometheus.Labels
	upDesc             *MetricFamily
	scrapeDurationDesc *MetricFamily

	conn *sql.DB
}

// NewTarget returns a new Target with the given instance name, data source name, collectors and constant labels.
func NewTarget(name, dsn string, ccs []*CollectorConfig, constLabels prometheus.Labels) (Target, error) {
	constLabelPairs := make([]*dto.LabelPair, 0, len(constLabels))
	for n, v := range constLabels {
		constLabelPairs = append(constLabelPairs, &dto.LabelPair{
			Name:  proto.String(n),
			Value: proto.String(v),
		})
	}
	sort.Sort(prometheus.LabelPairSorter(constLabelPairs))

	collectors := make([]Collector, 0, len(ccs))
	for _, cc := range ccs {
		c, err := NewCollector(cc, constLabelPairs)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, c)
	}

	upDesc, err := NewMetricFamily(&upMetricConfig, constLabelPairs)
	if err != nil {
		return nil, err
	}
	scrapeDurationDesc, err := NewMetricFamily(&scrapeDurationConfig, constLabelPairs)
	if err != nil {
		return nil, err
	}
	t := target{
		name:               name,
		dsn:                dsn,
		collectors:         collectors,
		constLabels:        constLabels,
		upDesc:             upDesc,
		scrapeDurationDesc: scrapeDurationDesc,
	}
	return &t, nil
}

func (t *target) Gather(ctx context.Context) ([]*dto.MetricFamily, error) {
	var (
		metricChan  = make(chan Metric, capMetricChan)
		errs        prometheus.MultiError
		targetUp    = true
		scrapeStart = time.Now()
	)

	if t.conn == nil {
		// Try creating a DB handle. It won't necessarily open an actual connection, only a driver handle.
		conn, err := OpenConnection(ctx, t.dsn)
		if err != nil {
			log.Errorf("Possible permanent error for target %q: %s", t.name, err)
			errs = append(errs, err)
			targetUp = false
		} else {
			t.conn = conn
		}
	}
	// If we have a handle and the context Check whether the connection is up.
	if t.conn != nil && ctx.Err() != nil {
		if err := t.conn.PingContext(ctx); err != nil {
			targetUp = false
		}
	}
	if ctx.Err() != nil {
		// Report target down because we timed out or got canceled before scraping.
		errs = append(errs, ctx.Err())
		targetUp = false
	}

	var wg sync.WaitGroup
	// Don't bother with the collectors if target is down.
	if targetUp {
		wg.Add(len(t.collectors))
		for _, c := range t.collectors {
			// TODO Is this needed? We're using a single DB connection, collectors will most likely run sequentially anyway.
			go func(collector Collector) {
				defer wg.Done()
				collector.Collect(ctx, t.conn, metricChan)
			}(c)
		}
	}

	// Wait for all collectors (if any) to complete, generate automatic metrics, then close the channel.
	go func() {
		wg.Wait()
		// Export an "up" metric for the target.
		metricChan <- NewMetric(t.upDesc, boolToFloat64(targetUp))
		// And a scrape duration metric
		metricChan <- NewMetric(t.scrapeDurationDesc, float64(time.Since(scrapeStart)))
		close(metricChan)
	}()

	// Drain metricChan in case of premature return.
	defer func() {
		for range metricChan {
		}
	}()

	// Gather.
	dtoMetricFamilies := make(map[string]*dto.MetricFamily, 10)
	for metric := range metricChan {
		metricFamily := metric.Family()
		dtoMetric := &dto.Metric{}
		if err := metric.Write(dtoMetric); err != nil {
			errs = append(errs, err)
			continue
		}
		dtoMetricFamily, ok := dtoMetricFamilies[metricFamily.Name()]
		if !ok {
			dtoMetricFamily = &dto.MetricFamily{}
			dtoMetricFamily.Name = proto.String(metricFamily.Name())
			dtoMetricFamily.Help = proto.String(metricFamily.Help())
			switch {
			case dtoMetric.Gauge != nil:
				dtoMetricFamily.Type = dto.MetricType_GAUGE.Enum()
			case dtoMetric.Counter != nil:
				dtoMetricFamily.Type = dto.MetricType_COUNTER.Enum()
			default:
				errs = append(errs, fmt.Errorf("don't know how to handle metric %v", dtoMetric))
				continue
			}
			dtoMetricFamilies[metricFamily.Name()] = dtoMetricFamily
		}
		dtoMetricFamily.Metric = append(dtoMetricFamily.Metric, dtoMetric)
	}

	// No need to sort metric families, prometheus.Gatherers will do that for us when merging.
	result := make([]*dto.MetricFamily, 0, len(dtoMetricFamilies))
	for _, mf := range dtoMetricFamilies {
		result = append(result, mf)
	}
	return result, errs
}

// String implements fmt.Stringer.
func (t *target) String() string {
	return fmt.Sprintf("target %q", t.name)
}

// boolToFloat64 converts a boolean flag to a float64 value (0.0 or 1.0).
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}
