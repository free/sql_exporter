package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // register the MS-SQL driver
	_ "github.com/go-sql-driver/mysql"   // register the MySQL driver
	log "github.com/golang/glog"
	_ "github.com/kshvakov/clickhouse" // register the ClickHouse driver
	_ "github.com/lib/pq"              // register the PostgreSQL driver
)

func OpenConnection(ctx context.Context, logContext, dsn string) (*sql.DB, error) {
	// Extract driver name from DSN.
	idx := strings.Index(dsn, "://")
	if idx == -1 {
		return nil, fmt.Errorf("missing driver in data source name. Expected format `<driver>://<dsn>`.")
	}
	driver := dsn[:idx]

	// Adjust DSN, where necessary.
	switch driver {
	case "mysql":
		dsn = strings.TrimPrefix(dsn, "mysql://")
	case "clickhouse":
		dsn = "tcp://" + strings.TrimPrefix(dsn, "clickhouse://")
	}

	// Open the DB handle in a separate goroutine so we can terminate early if the context closes.
	var (
		conn *sql.DB
		err  error
		ch   = make(chan error)
	)
	go func() {
		conn, err = sql.Open(driver, dsn)
		close(ch)
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ch:
		if err != nil {
			return nil, err
		}
	}

	// Set it up so we put as little extra load on the DB as possible.
	// TODO Make the number of connections configurable, to increase scraping parallelism.
	conn.SetMaxIdleConns(1)
	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(time.Duration(1 * time.Hour))

	log.V(1).Infof("[%s] Database handle successfully opened with driver %s.", logContext, driver)
	return conn, nil
}

// PingDB is a wrapper around sql.DB.PingContext() that terminates as soon as the context is closed.
//
// sql.DB does not actually pass along the context to the driver when opening a connection (which always happens if the
// database is down) and the driver uses an arbitrary timeout which may well be longer than ours. So we run the ping
// call in a goroutine and terminate immediately if the context is closed.
func PingDB(ctx context.Context, conn *sql.DB) error {
	ch := make(chan error, 1)

	go func() {
		ch <- conn.PingContext(ctx)
		close(ch)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
