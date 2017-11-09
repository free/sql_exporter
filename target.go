package sql_exporter

import (
	"database/sql"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Target collects SQL metrics from a single target. It implements prometheus.Gatherer.
type Target struct {
	conn       *sql.DB
	collectors []*Collector
	logger     log.Logger
}

// NewTarget returns a new SQL Exporter for the provided config.
func NewTarget(logger log.Logger, configFile string) (*Exporter, error) {
	if configFile == "" {
		configFile = "config.yml"
	}
}

func (t *Target) Gather() ([]*dto.MetricFamily, error) {

}
