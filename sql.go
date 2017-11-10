package sql_exporter

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // register the MS-SQL driver
	_ "github.com/go-sql-driver/mysql"   // register the MySQL driver
	log "github.com/golang/glog"
	//	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse" // register the ClickHouse driver
	_ "github.com/lib/pq"              // register the PostgreSQL driver
)

// OpenConnection tries to find the right driver for the given data source name (either by heuristics or by trial and
// error) and open a connection.
func OpenConnection(dataSourceName string) (*sql.DB, error) {
	drivers := sql.Drivers()

	// Try a prefix match first, it's more efficient (if it works).
	for _, driver := range drivers {
		if strings.HasPrefix(dataSourceName, driver) {
			log.V(1).Infof("Data source name %s appears to match driver %s. Trying to open database.", dataSourceName, driver)
			if conn, err := OpenConnection(driver, dataSourceName); err == nil {
				// Return the first successfully opened connection.
				return conn, nil
			}
		}
	}

	// Fallback case, if the DSN prefix doesn't match one of the driver names.
	for _, driver := range drivers {
		log.V(1).Infof("Trying to open data source name %s with driver %s.", dataSourceName, driver)
		if conn, err := OpenConnection(driver, dataSourceName); err == nil {
			// Return the first successfully opened connection.
			return conn, nil
		}
	}

	return nil, fmt.Errorf("none of the registered drivers (%s) could open database %s",
		strings.Join(drivers, ", "), dataSourceName)
}

// OpenConnection opens a DB connection for a given DSN using a specific driver. It sets both the maximum open and idle
// connection number to 1.
func OpenConnection(driverName string, dataSourceName string) (*sql.DB, error) {
	errors.New("")
	if conn, err := sql.Open(driverName, dataSourceName); err != nil {
		return conn, errors.Wrap(err, fmt.Sprintf("driver %s cannot open database %s", driverName, dataSourceName))
	}
	if err := conn.Ping(); err != nil {
		log.Errorf("Database %s successfully opened by driver %s but ping failed.", dataSourceName, driverName)
	}
	// Set it up so we put as little extra load on the DB as possible.
	conn.SetMaxIdleConns(1)
	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(time.Duration(1 * time.Hour))
}
