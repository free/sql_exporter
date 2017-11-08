package sql_exporter

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	_ "github.com/denisenkom/go-mssqldb" // register the MS-SQL driver
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	_ "github.com/go-sql-driver/mysql" // register the MySQL driver
	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse" // register the ClickHouse driver
	_ "github.com/lib/pq"              // register the PostgreSQL driver
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// MetricNameRE matches any invalid metric name
	// characters, see github.com/prometheus/common/model.MetricNameRE
	MetricNameRE = regexp.MustCompile("[^a-zA-Z0-9_:]+")
)

// Init will initialize the metric descriptors
func (j *Job) Init(logger log.Logger, queries map[string]string) error {
	j.log = log.With(logger, "job", j.Name)
	// register each query as an metric
	for _, q := range j.Queries {
		if q == nil {
			level.Warn(j.log).Log("msg", "Skipping invalid query")
			continue
		}
		q.log = log.With(j.log, "query", q.Name)
		if q.Query == "" && q.QueryRef != "" {
			if qry, found := queries[q.QueryRef]; found {
				q.Query = qry
			}
		}
		if q.Query == "" {
			level.Warn(q.log).Log("msg", "Skipping empty query")
			continue
		}
		if q.metrics == nil {
			// we have no way of knowing how many metrics will be returned by the
			// queries, so we just assume that each query returns at least one metric.
			// after the each round of collection this will be resized as necessary.
			q.metrics = make(map[*connection][]prometheus.Metric, len(j.Queries))
		}
		// try to satisfy prometheus naming restrictions
		name := MetricNameRE.ReplaceAllString("sql_"+q.Name, "")
		help := q.Help
		// prepare a new metrics descriptor
		//
		// the tricky part here is that the *order* of labels has to match the
		// order of label values supplied to NewConstMetric later
		q.desc = prometheus.NewDesc(
			name,
			help,
			append(q.Labels, "driver", "host", "database", "user", "col"),
			prometheus.Labels{
				"sql_job": j.Name,
			},
		)
	}
	return nil
}

// Run prepares and runs the job
func (j *Job) Run() {
	if j.log == nil {
		j.log = log.NewNopLogger()
	}
	// if there are no connection URLs for this job it can't be run
	if j.Connections == nil {
		level.Error(j.log).Log("msg", "No conenctions for job", "job", j.Name)
		return
	}
	// make space for the connection objects
	if j.conns == nil {
		j.conns = make([]*connection, 0, len(j.Connections))
	}
	// parse the connection URLs and create an connection object for each
	if len(j.conns) < len(j.Connections) {
		for _, conn := range j.Connections {
			u, err := url.Parse(conn)
			if err != nil {
				level.Error(j.log).Log("msg", "Failed to parse URL", "url", conn, "err", err)
				continue
			}
			user := ""
			if u.User != nil {
				user = u.User.Username()
			}
			// we expose some of the connection variables as labels, so we need to
			// remember them
			j.conns = append(j.conns, &connection{
				conn:     nil,
				url:      u,
				driver:   u.Scheme,
				host:     u.Host,
				database: strings.TrimPrefix(u.Path, "/"),
				user:     user,
			})
		}
	}
	level.Debug(j.log).Log("msg", "Starting")

	// enter the run loop
	// tries to run each query on each connection at approx the interval
	for {
		bo := backoff.NewExponentialBackOff()
		bo.MaxElapsedTime = j.Interval
		if err := backoff.Retry(j.runOnce, bo); err != nil {
			level.Error(j.log).Log("msg", "Failed to run", "err", err)
		}
		level.Debug(j.log).Log("msg", "Sleeping until next run", "sleep", j.Interval.String())
		time.Sleep(j.Interval)
	}
}

func (j *Job) runOnceConnection(conn *connection, done chan int) {
	updated := 0
	defer func() {
		done <- updated
	}()

	// connect to DB if not connected already
	if err := conn.connect(j.Interval); err != nil {
		level.Warn(j.log).Log("msg", "Failed to connect", "err", err)
		return
	}

	for _, q := range j.Queries {
		if q == nil {
			continue
		}
		if q.desc == nil {
			// this may happen if the metric registration failed
			level.Warn(q.log).Log("msg", "Skipping query. Collector is nil")
			continue
		}
		level.Debug(q.log).Log("msg", "Running Query")
		// execute the query on the connection
		if err := q.Run(conn); err != nil {
			level.Warn(q.log).Log("msg", "Failed to run query", "err", err)
			continue
		}
		level.Debug(q.log).Log("msg", "Query finished")
		updated++
	}
}

func (j *Job) runOnce() error {
	doneChan := make(chan int, len(j.conns))

	// execute queries for each connection in parallel
	for _, conn := range j.conns {
		go j.runOnceConnection(conn, doneChan)
	}

	// connections now run in parallel, wait for and collect results
	updated := 0
	for range j.conns {
		updated += <-doneChan
	}

	if updated < 1 {
		return fmt.Errorf("zero queries ran")
	}
	return nil
}

func (c *connection) connect(iv time.Duration) error {
	// already connected
	if c.conn != nil {
		return nil
	}
	dsn := c.url.String()
	switch c.url.Scheme {
	case "mysql":
		dsn = strings.TrimPrefix(dsn, "mysql://")
	case "clickhouse":
		dsn = "tcp://" + strings.TrimPrefix(dsn, "clickhouse://")
	}
	conn, err := sqlx.Connect(c.url.Scheme, dsn)
	if err != nil {
		return err
	}
	// be nice and don't use up too many connections for mere metrics
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(iv * 2)
	c.conn = conn
	return nil
}
