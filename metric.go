package sql_exporter

import (
	"fmt"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// MetricFamily describes a metric (name, labes, const labels) and how to populate its labels and values from sql.Rows.
type MetricFamily struct {
	config      *MetricConfig
	labels      []string
	constLabels []*dto.LabelPair
}

// NewMetricFamily creates a new MetricFamily with the given metric config and const labels (e.g. job and instance).
func NewMetricFamily(mc *MetricConfig, constLabels []*dto.LabelPair) (*MetricFamily, error) {
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

	return &MetricFamily{
		config:      mc,
		labels:      labels,
		constLabels: constLabels,
	}, nil
}

// Collect is the equivalent of prometheus.Collector.Collect() but takes a Query output map to populate values from.
func (mf MetricFamily) Collect(row map[string]interface{}, ch chan<- Metric) {
	labelValues := make([]string, len(mf.labels))
	for i, label := range mf.config.KeyLabels {
		labelValues[i] = row[label].(string)
	}
	for _, v := range mf.config.Values {
		if mf.config.ValueLabel != "" {
			labelValues[len(labelValues)-1] = v
		}
		value := row[v].(float64)
		ch <- NewMetric(&mf, value, labelValues...)
	}
}

func (mf MetricFamily) Name() string {
	return mf.config.Name
}

func (mf MetricFamily) Help() string {
	return mf.config.Help
}

// NewMetric returns a metric with one fixed value that cannot be changed.
//
// NewMetric panics if the length of labelValues is not consistent with family.labels.
func NewMetric(family *MetricFamily, value float64, labelValues ...string) Metric {
	if len(family.labels) != len(labelValues) {
		panic(fmt.Sprintf("Metric %q: expected %d labels, got %d", family.Name(), len(family.labels), len(labelValues)))
	}
	return &constMetric{
		family:     family,
		val:        value,
		labelPairs: makeLabelPairs(family, labelValues),
	}
}

type Metric interface {
	Family() *MetricFamily
	Write(out *dto.Metric) error
}

type constMetric struct {
	family     *MetricFamily
	val        float64
	labelPairs []*dto.LabelPair
}

func (m *constMetric) Family() *MetricFamily {
	return m.family
}

func (m *constMetric) Write(out *dto.Metric) error {
	out.Label = m.labelPairs
	switch t := m.family.config.valueType; t {
	case prometheus.CounterValue:
		out.Counter = &dto.Counter{Value: proto.Float64(m.val)}
	case prometheus.GaugeValue:
		out.Gauge = &dto.Gauge{Value: proto.Float64(m.val)}
	default:
		return fmt.Errorf("encountered unknown type %v", t)
	}
	return nil
}

func makeLabelPairs(family *MetricFamily, labelValues []string) []*dto.LabelPair {
	totalLen := len(family.labels) + len(family.constLabels)
	if totalLen == 0 {
		// Super fast path.
		return nil
	}
	if len(family.labels) == 0 {
		// Moderately fast path.
		return family.constLabels
	}
	labelPairs := make([]*dto.LabelPair, 0, totalLen)
	for i, label := range family.labels {
		labelPairs = append(labelPairs, &dto.LabelPair{
			Name:  proto.String(label),
			Value: proto.String(labelValues[i]),
		})
	}
	labelPairs = append(labelPairs, family.constLabels...)
	sort.Sort(prometheus.LabelPairSorter(labelPairs))
	return labelPairs
}

type invalidMetric struct {
	err error
}

// NewInvalidMetric returns a metric whose Write method always returns the provided error.
func NewInvalidMetric(context string, err error) Metric {
	return invalidMetric{errors.Wrap(err, context)}
}

func (m invalidMetric) Family() *MetricFamily { return &MetricFamily{} }

func (m invalidMetric) Write(*dto.Metric) error { return m.err }
