package sql_exporter

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

// LoadConfig attempts to parse the given config file and return a Config object.
func LoadConfig(path string) (*Config, error) {
	f := Config{}

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return &f, err
	}

	err = yaml.Unmarshal(buf, &f)
	return &f, err
}

//
// Top-level config
//

// Config is a collection of jobs and collectors.
type Config struct {
	Globals    GlobalConfig       `yaml:"global"`
	Jobs       []*JobConfig       `yaml:"jobs"`
	Collectors []*CollectorConfig `yaml:"collectors"`

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for Config.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if len(c.Jobs) == 0 {
		return fmt.Errorf("no jobs defined")
	}

	// Populate collector references for all jobs
	colls := make(map[string]*CollectorConfig)
	for _, coll := range c.Collectors {
		// Set the min interval to the global default if not explicitly set.
		if coll.MinInterval < 0 {
			coll.MinInterval = c.Globals.MinInterval
		}
		if _, found := colls[coll.Name]; found {
			return fmt.Errorf("duplicate collector name: %s", coll.Name)
		}
		colls[coll.Name] = coll
	}
	for _, j := range c.Jobs {
		j.collectors = make([]*CollectorConfig, 0, len(j.Collectors))
		for _, cname := range j.Collectors {
			coll, found := colls[cname]
			if !found {
				return fmt.Errorf("unknown collector %q referenced by job %q", cname, j.Name)
			}
			j.collectors = append(j.collectors, coll)
		}
	}

	return checkOverflow(c.XXX, "config")
}

// YAML marshals the config into YAML format.
func (c *Config) YAML() ([]byte, error) {
	return yaml.Marshal(c)
}

// GlobalConfig contains globally applicable defaults.
type GlobalConfig struct {
	MinInterval model.Duration `yaml:"min_interval"` // minimum interval between query executions, default is 0

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for GlobalConfig.
func (g *GlobalConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	g.MinInterval = model.Duration(0)

	type plain GlobalConfig
	if err := unmarshal((*plain)(g)); err != nil {
		return err
	}

	return checkOverflow(g.XXX, "global")
}

//
// Jobs
//

// JobConfig defines a set of collectors to be executed on a set of targets.
type JobConfig struct {
	Name          string          `yaml:"job_name"`       // name of this job
	Collectors    []string        `yaml:"collectors"`     // names of collectors to apply to all targets in this job
	StaticConfigs []*StaticConfig `yaml:"static_configs"` // collections of statically defined targets

	collectors []*CollectorConfig // resolved collector references

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for JobConfig.
func (j *JobConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain JobConfig
	if err := unmarshal((*plain)(j)); err != nil {
		return err
	}

	// Check required fields
	if j.Name == "" {
		return fmt.Errorf("missing name for job %+v", j)
	}

	// At least one collector, no duplicates
	if len(j.Collectors) == 0 {
		return fmt.Errorf("no collectors defined for job %q", j.Name)
	}
	for i, ci := range j.Collectors {
		for _, cj := range j.Collectors[i+1:] {
			if ci == cj {
				return fmt.Errorf("duplicate collector reference %q by job %q", ci, j.Name)
			}
		}
	}

	if len(j.StaticConfigs) == 0 {
		return fmt.Errorf("no targets defined for job %q", j.Name)
	}

	return checkOverflow(j.XXX, "job")
}

// checkLabelCollisions checks for label collisions between StaticConfig labels and Metric labels.
func (j *JobConfig) checkLabelCollisions() error {
	sclabels := make(map[string]interface{})
	for _, s := range j.StaticConfigs {
		for _, l := range s.Labels {
			sclabels[l] = nil
		}
	}

	for _, c := range j.collectors {
		for _, m := range c.Metrics {
			for _, l := range m.KeyLabels {
				if _, ok := sclabels[l]; ok {
					fmt.Errorf("label collision in job %q: label %q is defined both by a static_config and by metric %q of collector %q",
						j.Name, l, m.Name, c.Name)
				}
			}
		}
	}
	return nil
}

// StaticConfig defines a set of targets and optional labels to apply to the metrics collected from them.
type StaticConfig struct {
	Targets map[string]string `yaml:"targets"`          // map of target names to data source names
	Labels  map[string]string `yaml:"labels,omitempty"` // labels to apply to all metrics collected from the targets

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for StaticConfig.
func (s *StaticConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain StaticConfig
	if err := unmarshal((*plain)(s)); err != nil {
		return err
	}

	// Check for empty/duplicate target names/data source names
	tnames := make(map[string]interface{})
	dsns := make(map[string]interface{})
	for tname, dsn := range s.Targets {
		if tname == "" {
			return fmt.Errorf("empty target name in static config %+v", s)
		}
		if _, ok := tnames[tname]; ok {
			return fmt.Errorf("duplicate target name %q in static_config %+v", tname, s)
		}
		tnames[tname] = nil
		if dsn == "" {
			return fmt.Errorf("empty data source name in static config %+v", s)
		}
		if _, ok := dsns[dsn]; ok {
			return fmt.Errorf("duplicate data source name %q in static_config %+v", tname, s)
		}
		dsns[dsn] = nil
	}

	return checkOverflow(s.XXX, "static_config")
}

func (s *StaticConfig) MarshalYAML() (interface{}, error) {
	result := StaticConfig{
		Targets: make(map[string]string, len(s.Targets)),
		Labels:  s.Labels,
	}
	for tname, _ := range s.Targets {
		result.Targets[tname] = "<secret>"
	}
	return result, nil
}

//
// Collectors
//

// CollectorConfig defines a set of metrics and how they are collected.
type CollectorConfig struct {
	Name        string          `yaml:"collector_name"`         // name of this collector
	MinInterval model.Duration  `yaml:"min_interval,omitempty"` // minimum interval between query executions
	Metrics     []*MetricConfig `yaml:"metrics"`                // metrics/queries defined by this collector
	Queries     []*QueryConfig  `yaml:"queries,omitempty"`      // named queries defined by this collector

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for CollectorConfig.
func (c *CollectorConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Default to undefined (a negative value) so it can be overriden by the global default when not explicitly set.
	c.MinInterval = -1

	type plain CollectorConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if len(c.Metrics) == 0 {
		return fmt.Errorf("no metrics defined for collector %q", c.Name)
	}

	// Set metric.query for all metrics: resolve query references (if any) and generate QueryConfigs for literal queries.
	queries := make(map[string]*QueryConfig, len(c.Queries))
	for _, query := range c.Queries {
		queries[query.Name] = query
	}
	for _, metric := range c.Metrics {
		if metric.QueryRef != "" {
			query, found := queries[metric.QueryRef]
			if !found {
				return fmt.Errorf("unresolved query_ref %q in metric %q of collector %q", metric.QueryRef, metric.Name, c.Name)
			}
			metric.query = query
			query.metrics = append(query.metrics, metric)
		} else {
			// For literal queries generate a QueryConfig with a name based off collector and metric name.
			metric.query = &QueryConfig{
				Name:  fmt.Sprintf("%s.%s", c.Name, metric.Name),
				Query: metric.Query,
			}
		}
	}

	return checkOverflow(c.XXX, "collector")
}

// MetricConfig defines a Prometheus metric, the SQL query to populate it and the mapping of columns to metric
// keys/values.
type MetricConfig struct {
	Name       string   `yaml:"metric_name"`           // the Prometheus metric name
	TypeString string   `yaml:"type"`                  // the Prometheus metric type
	Help       string   `yaml:"help"`                  // the Prometheus metric help text
	KeyLabels  []string `yaml:"key_labels,omitempty"`  // expose these columns as labels
	ValueLabel string   `yaml:"value_label,omitempty"` // with multiple value columns, map their names under this label
	Values     []string `yaml:"values"`                // expose each of these columns as a value, keyed by column name
	Query      string   `yaml:"query,omitempty"`       // a literal query
	QueryRef   string   `yaml:"query_ref,omitempty"`   // references a query in the query map

	valueType prometheus.ValueType // TypeString converted to prometheus.ValueType
	query     *QueryConfig         // QueryConfig resolved from QueryRef or generated from Query

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for MetricConfig.
func (m *MetricConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain MetricConfig
	if err := unmarshal((*plain)(m)); err != nil {
		return err
	}

	// Check required fields
	if m.Name == "" {
		return fmt.Errorf("missing name for metric %+v", m)
	}
	if m.TypeString == "" {
		return fmt.Errorf("missing type for metric %q", m.Name)
	}
	if m.Help == "" {
		return fmt.Errorf("missing help for metric %q", m.Name)
	}
	if (m.Query == "") == (m.QueryRef == "") {
		return fmt.Errorf("exactly one of query and query_ref should be specified for metric %q", m.Name)
	}

	switch strings.ToLower(m.TypeString) {
	case "counter":
		m.valueType = prometheus.CounterValue
	case "gauge":
		m.valueType = prometheus.GaugeValue
	default:
		return fmt.Errorf("unsupported metric type: %s", m.TypeString)
	}

	// Check for duplicate key labels
	for i, li := range m.KeyLabels {
		checkLabel(li, "metric", m.Name)
		for _, lj := range m.KeyLabels[i+1:] {
			if li == lj {
				return fmt.Errorf("duplicate key label %q for metric %q", li, m.Name)
			}
		}
		if m.ValueLabel == li {
			return fmt.Errorf("duplicate label %q (defined in both key_labels and value_label) for metric %q", li, m.Name)
		}
	}

	if len(m.Values) == 0 {
		return fmt.Errorf("no values defined for metric %q", m.Name)
	}

	if len(m.Values) > 1 {
		// Multiple value columns but no value label to identify them
		if m.ValueLabel == "" {
			return fmt.Errorf("value_label must be defined for metric with multiple values %q", m.Name)
		}
		checkLabel(m.ValueLabel, "value_label for metric", m.Name)
	}

	return checkOverflow(m.XXX, "metric")
}

// QueryConfig defines a named query, to be referenced by one or multiple metrics.
type QueryConfig struct {
	Name  string `yaml:"query_name"` // the query name, to be referenced via `query_ref`
	Query string `yaml:"query"`      // the named query

	metrics []*MetricConfig // metrics referencing this query

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline" json:"-"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for QueryConfig.
func (q *QueryConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain QueryConfig
	if err := unmarshal((*plain)(q)); err != nil {
		return err
	}

	// Check required fields
	if q.Name == "" {
		return fmt.Errorf("missing name for query %+v", q)
	}
	if q.Query == "" {
		return fmt.Errorf("missing query literal for query %q", q.Name)
	}

	q.metrics = make([]*MetricConfig, 0, 2)

	return checkOverflow(q.XXX, "metric")
}

func checkLabel(label string, ctx ...string) error {
	if label == "" {
		return fmt.Errorf("empty label defined in %s", strings.Join(ctx, " "))
	}
	if label == "job" || label == "instance" {
		return fmt.Errorf("reserved label %q redefined in %s", label, strings.Join(ctx, " "))
	}
	return nil
}

func checkOverflow(m map[string]interface{}, ctx string) error {
	if len(m) > 0 {
		var keys []string
		for k := range m {
			keys = append(keys, k)
		}
		return fmt.Errorf("unknown fields in %s: %s", ctx, strings.Join(keys, ", "))
	}
	return nil
}
