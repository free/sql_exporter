package sql_exporter

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric wraps a prometheus.Desc plus information on how to populate its labels and value from a sql.Rows.
type Metric struct {
	Desc   *prometheus.Desc
	config *MetricConfig
	labels []string
}

// NewMetric returns a new Metric with the given instance name, data source name, collectors and constant labels.
func NewMetric(mc *MetricConfig, constLabels prometheus.Labels) (*Metric, error) {
	if len(mc.Values) == 0 {
		return nil, fmt.Errorf("no value column for metric %q", mc.Name)
	}
	if len(mc.Values) > 1 && mc.ValueLabel == "" {
		return nil, fmt.Errorf("multiple values but no value label for metric %q", mc.Name)
	}

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

	m := Metric{
		Desc:   desc,
		config: mc,
		labels: labels,
	}
	return &m, nil
}

func (m Metric) Collect(row map[string]interface{}, ch chan<- prometheus.Metric) {
	labelValues := make([]string, len(m.labels))
	for i, label := range m.config.KeyLabels {
		labelValues[i] = row[label].(string)
	}
	for _, v := range m.config.Values {
		if m.config.ValueLabel != "" {
			labelValues[len(labelValues)-1] = v
		}
		value := row[v].(float64)
		ch <- prometheus.MustNewConstMetric(m.Desc, m.config.valueType, value, labelValues...)
	}
}
