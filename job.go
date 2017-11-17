package sql_exporter

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Job is a collection of targets with the same collectors applied.
type Job interface {
	Targets() []Target
}

// job implements Job. It wraps the corresponding JobConfig and a set of Targets.
type job struct {
	config  *JobConfig
	targets []Target
}

func NewJob(config *JobConfig) (Job, error) {
	j := job{
		config:  config,
		targets: make([]Target, 0, 10),
	}

	for _, sc := range config.StaticConfigs {
		for tname, dsn := range sc.Targets {
			constLabels := prometheus.Labels{
				"job":      config.Name,
				"instance": tname,
			}
			for name, value := range sc.Labels {
				// Shouldn't happen as there are sanity checks in config, but check nonetheless.
				if _, found := constLabels[name]; found {
					return nil, fmt.Errorf("duplicate label %q in job %q", name, config.Name)
				}
				constLabels[name] = value
			}
			t, err := NewTarget(tname, dsn, config.collectors, constLabels)
			if err != nil {
				return nil, err
			}
			j.targets = append(j.targets, t)
		}
	}

	return &j, nil
}

func (j *job) Targets() []Target {
	return j.targets
}
