package sql_exporter

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metric wraps a prometheus.Desc plus information on how to populate its labels and value from a sql.Rows.
type Metric struct {
	Desc   *prometheus.Desc
	config *MetricConfig
}

// Metrics is a slice of Metric references.
type Metrics []*Metric

// NewMetric returns a new Metric with the given instance name, data source name, collectors and constant labels.
func NewMetric(mc *MetricConfig) (*Metric, error) {
	labels := make([]string, 0, len(mc.KeyLabels)+1)
	labels = append(labels, mc.KeyLabels...)
	if mc.ValueLabel != "" {
		labels = append(labels, mc.ValueLabel)
	}
	desc := prometheus.NewDesc(
		mc.Name,
		mc.Help,
		labels,
		constLabels,
	)

	m := metric{Desc: desc, config: mc}
	return &m, nil
}

func (m Metric) Collect(ch chan<- prometheus.Metric) {
}
