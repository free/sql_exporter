package sql_exporter

import (
	"fmt"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Metric wraps a prometheus.Desc plus information on how to populate its labels and value from a sql.Rows.
type Metric struct {
	config      *MetricConfig
	labels      []string
	constLabels []*dto.LabelPair
}

// NewMetric returns a new Metric with the given instance name, data source name, collectors and constant labels.
func NewMetric(mc *MetricConfig, constLabels []*dto.LabelPair) (*Metric, error) {
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

	m := Metric{
		config:      mc,
		labels:      labels,
		constLabels: constLabels,
	}
	return &m, nil
}

func (m Metric) Collect(row map[string]interface{}, ch chan<- MetricValue) {
	labelValues := make([]string, len(m.labels))
	for i, label := range m.config.KeyLabels {
		labelValues[i] = row[label].(string)
	}
	for _, v := range m.config.Values {
		if m.config.ValueLabel != "" {
			labelValues[len(labelValues)-1] = v
		}
		value := row[v].(float64)
		ch <- NewMetricValue(&m, value, labelValues...)
	}
}

func (m Metric) Name() string {
	return m.config.Name
}

func (m Metric) Help() string {
	return m.config.Help
}

// NewMetricValue returns a metric with one fixed value that cannot be changed.
//
// NewMetricValue panics if the length of labelValues is not consistent with desc.labels.
func NewMetricValue(desc *Metric, value float64, labelValues ...string) MetricValue {
	if len(desc.labels) != len(labelValues) {
		panic(fmt.Sprintf("Metric %q: expected %d labels, got %d", desc.Name(), len(desc.labels), len(labelValues)))
	}
	return &constMetric{
		desc:       desc,
		val:        value,
		labelPairs: makeLabelPairs(desc, labelValues),
	}
}

type MetricValue interface {
	Desc() *Metric
	Write(out *dto.Metric) error
}

type constMetric struct {
	desc       *Metric
	val        float64
	labelPairs []*dto.LabelPair
}

func (m *constMetric) Desc() *Metric {
	return m.desc
}

func (m *constMetric) Write(out *dto.Metric) error {
	out.Label = m.labelPairs
	switch t := m.desc.config.valueType; t {
	case prometheus.CounterValue:
		out.Counter = &dto.Counter{Value: proto.Float64(m.val)}
	case prometheus.GaugeValue:
		out.Gauge = &dto.Gauge{Value: proto.Float64(m.val)}
	default:
		return fmt.Errorf("encountered unknown type %v", t)
	}
	return nil
}

func makeLabelPairs(desc *Metric, labelValues []string) []*dto.LabelPair {
	totalLen := len(desc.labels) + len(desc.constLabels)
	if totalLen == 0 {
		// Super fast path.
		return nil
	}
	if len(desc.labels) == 0 {
		// Moderately fast path.
		return desc.constLabels
	}
	labelPairs := make([]*dto.LabelPair, 0, totalLen)
	for i, label := range desc.labels {
		labelPairs = append(labelPairs, &dto.LabelPair{
			Name:  proto.String(label),
			Value: proto.String(labelValues[i]),
		})
	}
	labelPairs = append(labelPairs, desc.constLabels...)
	sort.Sort(prometheus.LabelPairSorter(labelPairs))
	return labelPairs
}

type invalidMetric struct {
	err error
}

// NewInvalidMetric returns a metric whose Write method always returns the provided error.
func NewInvalidMetric(err error) MetricValue {
	return invalidMetric{err}
}

func (m invalidMetric) Desc() *Metric { return &Metric{} }

func (m invalidMetric) Write(*dto.Metric) error { return m.err }
