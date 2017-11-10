package sql_exporter

// Exporter collects SQL metrics. It implements prometheus.Collector.
type Exporter struct {
	jobs []*Job
}

// NewExporter returns a new SQL Exporter for the provided config.
func NewExporter(configFile string) (*Exporter, error) {
	// read config
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	exp := &Exporter{
		jobs:   make([]*Job, 0, len(config.Jobs)),
		logger: logger,
	}

	for _, jc := range config.Jobs {
		if job, err := NewJob(jc); err != nil {
			return nil, err
		}
		exp.jobs = append(exp.jobs, job)
	}
	return exp, nil
}
