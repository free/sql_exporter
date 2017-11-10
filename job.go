package sql_exporter

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Job is a collection of targets with the same collectors applied. It wraps the corresponding JobConfig and a set of
// Targets.
type Job struct {
	config  *JobConfig
	targets []*Target
}

func NewJob(config *JobConfig) (*Job, error) {
	j := Job{
		config:  config,
		targets: make([]*Target, 0, 10),
	}

	for _, sc := range config.StaticConfigs {
		for tname, dsn := range sc.Targets {
			constLabels := prometheus.Labels{
				"job":      config.Name,
				"instance": tname,
			}
			for name, value := range sc.Labels {
				// Shouldn't happen as there are sanity checks in config, but check nonetheless.
				if _, ok := constLabels[name]; ok {
					return nil, fmt.Errorf("duplicate label %q in job %q", name, config.Name)
				}
				constLabels[name] = value
			}
			if target, err := NewTarget(tname, dsn, config.collectors, constLabels); err != nil {
				return nil, err
			}
			j.targets = append(j.targets, target)
		}
	}

	return j, nil
}

//
//// Run prepares and runs the job
//func (j *Job) Run() {
//	if j.log == nil {
//		j.log = log.NewNopLogger()
//	}
//	// if there are no connection URLs for this job it can't be run
//	if j.Connections == nil {
//		log.Errorf("No conenctions for job %s", j.Name)
//		return
//	}
//	// make space for the connection objects
//	if j.conns == nil {
//		j.conns = make([]*connection, 0, len(j.Connections))
//	}
//	// parse the connection URLs and create an connection object for each
//	if len(j.conns) < len(j.Connections) {
//		for _, conn := range j.Connections {
//			u, err := url.Parse(conn)
//			if err != nil {
//				log.Errorf("Failed to parse URL %s: %s", conn, err)
//				continue
//			}
//			user := ""
//			if u.User != nil {
//				user = u.User.Username()
//			}
//			// we expose some of the connection variables as labels, so we need to
//			// remember them
//			j.conns = append(j.conns, &connection{
//				conn:     nil,
//				url:      u,
//				driver:   u.Scheme,
//				host:     u.Host,
//				database: strings.TrimPrefix(u.Path, "/"),
//				user:     user,
//			})
//		}
//	}
//	log.Debug("Starting")
//
//	// enter the run loop
//	// tries to run each query on each connection at approx the interval
//	for {
//		bo := backoff.NewExponentialBackOff()
//		bo.MaxElapsedTime = j.Interval
//		if err := backoff.Retry(j.runOnce, bo); err != nil {
//			log.Errorf("Failed to run: %s", err)
//		}
//		log.Debugf("Sleeping until next run %s", j.Interval.String())
//		time.Sleep(j.Interval)
//	}
//}
//
//func (j *Job) runOnceConnection(conn *connection, done chan int) {
//	updated := 0
//	defer func() {
//		done <- updated
//	}()
//
//	// connect to DB if not connected already
//	if err := conn.connect(j.Interval); err != nil {
//		log.Warningf("Failed to connect: %s", err)
//		return
//	}
//
//	for _, q := range j.Queries {
//		if q == nil {
//			continue
//		}
//		if q.desc == nil {
//			// this may happen if the metric registration failed
//			log.Warning("Skipping query. Collector is nil")
//			continue
//		}
//		log.Debug("Running Query")
//		// execute the query on the connection
//		if err := q.Run(conn); err != nil {
//			log.Warningf("Failed to run query: %s", err)
//			continue
//		}
//		log.Debug("Query finished")
//		updated++
//	}
//}
//
//func (j *Job) runOnce() error {
//	doneChan := make(chan int, len(j.conns))
//
//	// execute queries for each connection in parallel
//	for _, conn := range j.conns {
//		go j.runOnceConnection(conn, doneChan)
//	}
//
//	// connections now run in parallel, wait for and collect results
//	updated := 0
//	for range j.conns {
//		updated += <-doneChan
//	}
//
//	if updated < 1 {
//		return fmt.Errorf("zero queries ran")
//	}
//	return nil
//}
//
//func (c *connection) connect(iv time.Duration) error {
//	// already connected
//	if c.conn != nil {
//		return nil
//	}
//	dsn := c.url.String()
//	switch c.url.Scheme {
//	case "mysql":
//		dsn = strings.TrimPrefix(dsn, "mysql://")
//	case "clickhouse":
//		dsn = "tcp://" + strings.TrimPrefix(dsn, "clickhouse://")
//	}
//	conn, err := sqlx.Connect(c.url.Scheme, dsn)
//	if err != nil {
//		return err
//	}
//	// be nice and don't use up too many connections for mere metrics
//	conn.SetMaxOpenConns(1)
//	conn.SetMaxIdleConns(1)
//	conn.SetConnMaxLifetime(iv * 2)
//	c.conn = conn
//	return nil
//}
