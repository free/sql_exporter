package sql_exporter

import (
	"fmt"
	"sort"
	"math"
	"encoding/json"

	"github.com/free/sql_exporter/config"
	"github.com/free/sql_exporter/errors"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/golang/glog"
)

// MetricDesc is a descriptor for a family of metrics, sharing the same name, help, labes, type.
type MetricDesc interface {
	Name() string
	Help() string
	ValueType() prometheus.ValueType
	ConstLabels() []*dto.LabelPair
	Labels() []string
	LogContext() string
	GlobalConfig() *config.GlobalConfig
}

//
// MetricFamily
//

// MetricFamily implements MetricDesc for SQL metrics, with logic for populating its labels and values from sql.Rows.
type MetricFamily struct {
	config       *config.MetricConfig
	constLabels  []*dto.LabelPair
	labels       []string
	logContext   string
	globalConfig *config.GlobalConfig
}

// NewMetricFamily creates a new MetricFamily with the given metric config and const labels (e.g. job and instance).
func NewMetricFamily(logContext string, mc *config.MetricConfig, constLabels []*dto.LabelPair, gc *config.GlobalConfig) (*MetricFamily, errors.WithContext) {
	logContext = fmt.Sprintf("%s, metric=%q", logContext, mc.Name)

	if len(mc.Values) == 0 {
		return nil, errors.New(logContext, "no value column defined")
	}
	if len(mc.Values) > 1 && mc.ValueLabel == "" {
		return nil, errors.New(logContext, "multiple values but no value label")
	}

	labels := make([]string, 0, len(mc.KeyLabels)+1)
	labels = append(labels, mc.KeyLabels...)
	if mc.ValueLabel != "" {
		labels = append(labels, mc.ValueLabel)
	}

	return &MetricFamily{
		config:       mc,
		constLabels:  constLabels,
		labels:       labels,
		logContext:   logContext,
		globalConfig: gc,
	}, nil
}

// Collect is the equivalent of prometheus.Collector.Collect() but takes a Query output map to populate values from.
func (mf MetricFamily) Collect(row map[string]interface{}, ch chan<- Metric) {
	var userLabels []*dto.LabelPair

	// TODO: move to func()
	if mf.config.JsonLabels != "" && row[mf.config.JsonLabels].(string) != "" {
		userLabels = parseJsonLabels(mf, row[mf.config.JsonLabels].(string))
	}

	labelValues := make([]string, 0, len(mf.labels))
	for _, label := range mf.config.KeyLabels {
		labelValues = append(labelValues, row[label].(string))
	}
	for _, v := range mf.config.Values {
		if mf.config.ValueLabel != "" {
			labelValues[len(labelValues)-1] = v
		}
		value := row[v].(float64)
		ch <- NewMetric(&mf, value, labelValues, userLabels)
	}
}

// Name implements MetricDesc.
func (mf MetricFamily) Name() string {
	return mf.config.Name
}

// Help implements MetricDesc.
func (mf MetricFamily) Help() string {
	return mf.config.Help
}

// ValueType implements MetricDesc.
func (mf MetricFamily) ValueType() prometheus.ValueType {
	return mf.config.ValueType()
}

// ConstLabels implements MetricDesc.
func (mf MetricFamily) ConstLabels() []*dto.LabelPair {
	return mf.constLabels
}

// Labels implements MetricDesc.
func (mf MetricFamily) Labels() []string {
	return mf.labels
}

// LogContext implements MetricDesc.
func (mf MetricFamily) LogContext() string {
	return mf.logContext
}

// GlobalConfig implements MetricDesc.
func (mf MetricFamily) GlobalConfig() *config.GlobalConfig {
	return mf.globalConfig
}

//
// automaticMetricDesc
//

// automaticMetric is a MetricDesc for automatically generated metrics (e.g. `up` and `scrape_duration`).
type automaticMetricDesc struct {
	name        string
	help        string
	valueType   prometheus.ValueType
	labels      []string
	constLabels []*dto.LabelPair
	logContext  string
}

// NewAutomaticMetricDesc creates a MetricDesc for automatically generated metrics.
func NewAutomaticMetricDesc(
	logContext, name, help string, valueType prometheus.ValueType, constLabels []*dto.LabelPair, labels ...string) MetricDesc {
	return &automaticMetricDesc{
		name:        name,
		help:        help,
		valueType:   valueType,
		constLabels: constLabels,
		labels:      labels,
		logContext:  logContext,
	}
}

// Name implements MetricDesc.
func (a automaticMetricDesc) Name() string {
	return a.name
}

// Help implements MetricDesc.
func (a automaticMetricDesc) Help() string {
	return a.help
}

// ValueType implements MetricDesc.
func (a automaticMetricDesc) ValueType() prometheus.ValueType {
	return a.valueType
}

// ConstLabels implements MetricDesc.
func (a automaticMetricDesc) ConstLabels() []*dto.LabelPair {
	return a.constLabels
}

// Labels implements MetricDesc.
func (a automaticMetricDesc) Labels() []string {
	return a.labels
}

// LogContext implements MetricDesc.
func (a automaticMetricDesc) LogContext() string {
	return a.logContext
}

// GlobalConfig implements MetricDesc.
func (a automaticMetricDesc) GlobalConfig() *config.GlobalConfig {
	return nil
}

//
// Metric
//

// A Metric models a single sample value with its meta data being exported to Prometheus.
type Metric interface {
	Desc() MetricDesc
	Write(out *dto.Metric) errors.WithContext
}

// NewMetric returns a metric with one fixed value that cannot be changed.
//
// NewMetric panics if the length of labelValues is not consistent with desc.labels().
func NewMetric(desc MetricDesc, value float64, labelValues []string, userLabels []*dto.LabelPair) Metric {
	if len(desc.Labels()) != len(labelValues) {
		panic(fmt.Sprintf("[%s] expected %d labels, got %d", desc.LogContext(), len(desc.Labels()), len(labelValues)))
	}
	return &constMetric{
		desc:       desc,
		val:        value,
		labelPairs: makeLabelPairs(desc, labelValues, userLabels),
	}
}

// constMetric is a metric with one fixed value that cannot be changed.
type constMetric struct {
	desc       MetricDesc
	val        float64
	labelPairs []*dto.LabelPair
}

// Desc implements Metric.
func (m *constMetric) Desc() MetricDesc {
	return m.desc
}

// Write implements Metric.
func (m *constMetric) Write(out *dto.Metric) errors.WithContext {
	out.Label = m.labelPairs
	switch t := m.desc.ValueType(); t {
	case prometheus.CounterValue:
		out.Counter = &dto.Counter{Value: proto.Float64(m.val)}
	case prometheus.GaugeValue:
		out.Gauge = &dto.Gauge{Value: proto.Float64(m.val)}
	default:
		return errors.Errorf(m.desc.LogContext(), "encountered unknown type %v", t)
	}
	return nil
}

func parseJsonLabels(desc MetricDesc, labels string) []*dto.LabelPair {
	var jsonLabels map[string]string

	config := desc.GlobalConfig()
	maxJsonLabels := 0
	if config != nil {
		maxJsonLabels = config.MaxJsonLabels
	}

	err := json.Unmarshal([]byte(labels), &jsonLabels)
	// errors are logged but ignored
	if err != nil {
		log.Warningf("[%s] Failed to parse JSON labels returned by query - %s", desc.LogContext(), err)
		return nil
	}

//	var userLabels []*dto.LabelPair
	userLabelsMax := int(math.Min(float64(len(jsonLabels)), float64(maxJsonLabels)))
	userLabels := make([]*dto.LabelPair, 0, userLabelsMax)

	idx := 0
	for name, value := range jsonLabels {
		// limit label count
		if idx >= maxJsonLabels {
			log.Warningf("[%s] Count of JSON labels is limited to %d, truncating", desc.LogContext(), maxJsonLabels)
			break
		}
		userLabels = append(userLabels, makeLabelPair(desc, name, value))
		idx = idx + 1
	}
	return userLabels
}

func makeLabelPair(desc MetricDesc, label string, value string) *dto.LabelPair {
	config := desc.GlobalConfig()
	if config != nil {
		if (len(label) > config.MaxLabelNameLen) {
			label = label[:config.MaxLabelNameLen]
		}
		if (len(value) > config.MaxLabelValueLen) {
			value = value[:config.MaxLabelValueLen]
		}
	}

	return &dto.LabelPair{
		Name:  proto.String(label),
		Value: proto.String(value),
	}
}

func makeLabelPairs(desc MetricDesc, labelValues []string, userLabels []*dto.LabelPair) []*dto.LabelPair {
	labels := desc.Labels()
	constLabels := desc.ConstLabels()

	totalLen := len(labels) + len(constLabels) + len(userLabels)
	if totalLen == 0 {
		// Super fast path.
		return nil
	}
	if len(labels) == 0 && len(userLabels) == 0{
		// Moderately fast path.
		return constLabels
	}
	labelPairs := make([]*dto.LabelPair, 0, totalLen)
	for i, label := range labels {
		labelPairs = append(labelPairs, makeLabelPair(desc, label, labelValues[i]))
	}
	labelPairs = append(labelPairs, userLabels...)
	labelPairs = append(labelPairs, constLabels...)
	sort.Sort(labelPairSorter(labelPairs))
	return labelPairs
}

// labelPairSorter implements sort.Interface.
// It provides a sortable version of a slice of dto.LabelPair pointers.

type labelPairSorter []*dto.LabelPair

func (s labelPairSorter) Len() int {
	return len(s)
}

func (s labelPairSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s labelPairSorter) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}

type invalidMetric struct {
	err errors.WithContext
}

// NewInvalidMetric returns a metric whose Write method always returns the provided error.
func NewInvalidMetric(err errors.WithContext) Metric {
	return invalidMetric{err}
}

func (m invalidMetric) Desc() MetricDesc { return nil }

func (m invalidMetric) Write(*dto.Metric) errors.WithContext { return m.err }
