package sql_exporter

import (
	"io/ioutil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

// LoadConfig attempts to parse the given config file and return a Config object.
func LoadConfig(path string) (Config, error) {
	f := Config{}

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return f, err
	}

	if err := yaml.Unmarshal(buf, &f); err != nil {
		return f, err
	}
	return f, nil
}

// Config is a collection of jobs and collectors.
type Config struct {
	Globals    GlobalConfig `yaml:"global,omitempty"`
	Jobs       []*Job       `yaml:"jobs"`
	Collectors []*Collector `yaml:"collectors"`
}

// GlobalConfig contains globally applicable defaults.
type GlobalConfig struct {
	MinInterval common.Duration `yaml:"min_interval,omitempty"` // minimum interval between query executions
}

// JobConfig defines a set of collectors to be executed on a set of targets.
type JobConfig struct {
	Name          string         `yaml:"job_name"`       // name of this job
	Collectors    []string       `yaml:"collectors"`     // names of collectors to apply to all targets in this job
	StaticConfigs []StaticConfig `yaml:"static_configs"` // collections of statically defined targets
}

// StaticConfig defines a set of targets and optional labels to apply to the metrics collected from them.
type StaticConfig struct {
	Targets map[string]string `yaml:"targets"`          // Map of target names to data source names
	Labels  map[string]string `yaml:"labels,omitempty"` // Labels to apply to all metrics collected from the targets
}

// CollectorConfig defines a set of metrics and how they are collected.
type CollectorConfig struct {
	Name        string         `yaml:"collector_name"`         // name of this collector
	MinInterval model.Duration `yaml:"min_interval,omitempty"` // minimum interval between query executions
	Metrics     []MetricConfig `yaml:"metrics"`                // metrics/queries defined by this collector
}

// MetricType defines one of the supported Prometheus metric types.
type MetricType int

const (
	Counter MetricType = 1
	Gauge   MetricType = 2
)

// UnmarshalYAML implements the yaml.Unmarshaler interface for MetricType.
func (t *MetricType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var temp string
	if err := unmarshal(temp); err != nil {
		return err
	}

	switch temp {
	case "counter":
		t = Counter
	case "gauge":
		t = Gauge
	default:
		return fmt.Errorf("Unsupported metric type: %s", t)
	}
	return nil
}

// MetricConfig defines a Prometheus metric, the SQL query to populate it and the mapping between them.
type MetricConfig struct {
	Name     string     `yaml:"name"`      // the Prometheus metric name
	Type     MetricType `yaml:"type"`      // the Prometheus metric type
	Help     string     `yaml:"help"`      // the Prometheus metric help text
	Labels   []string   `yaml:"labels"`    // expose these columns as labels per gauge
	Values   []string   `yaml:"values"`    // expose each of these as an gauge
	Query    string     `yaml:"query"`     // a literal query
	QueryRef string     `yaml:"query_ref"` // references an query in the query map
}

// Job defines a set of collectors to be executed on a set of targets.
type Job struct {
	log    log.Logger
	conns  []*connection
	config JobConfig
}

type connection struct {
	conn     *sqlx.DB
	url      *url.URL
	driver   string
	host     string
	database string
	user     string
}

// Query is an SQL query that is executed on a connection
type Query struct {
	sync.Mutex
	log      log.Logger
	desc     *prometheus.Desc
	metrics  map[*connection][]prometheus.Metric
	Name     string   `yaml:"name"`      // the prometheus metric name
	Help     string   `yaml:"help"`      // the prometheus metric help text
	Labels   []string `yaml:"labels"`    // expose these columns as labels per gauge
	Values   []string `yaml:"values"`    // expose each of these as an gauge
	Query    string   `yaml:"query"`     // a literal query
	QueryRef string   `yaml:"query_ref"` // references an query in the query map
}
