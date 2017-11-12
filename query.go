package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"

	log "github.com/golang/glog"
)

// Query wraps a sql.Stmt and all the metrics populated from it. It helps extract keys and values from result rows.
type Query struct {
	metrics []*Metric
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap
	queryString string

	stmt *sql.Stmt
	conn *sql.DB
}

type columnType int
type columnTypeMap map[string]columnType

const (
	columnTypeKey   = 1
	columnTypeValue = 2
)

// NewQuery returns a new Query.
func NewQuery(queryString string, conn *sql.DB, metrics ...*Metric) (*Query, error) {
	columnTypes := make(columnTypeMap)

	for _, m := range metrics {
		for _, kcol := range m.config.KeyLabels {
			if err := setColumnType(kcol, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}
		for _, vcol := range m.config.Values {
			if err := setColumnType(vcol, columnTypeValue, columnTypes); err != nil {
				return nil, err
			}
		}
	}

	q := Query{
		metrics:     metrics,
		columnTypes: columnTypes,
		queryString: queryString,
	}
	return &q, nil
}

// setColumnType stores the provided type for a given column, checking for conflicts in the process.
func setColumnType(columnName string, ctype columnType, columnTypes columnTypeMap) error {
	previousType, found := columnTypes[columnName]
	if found {
		if previousType != ctype {
			return fmt.Errorf("column %q used both as key and value", columnName)
		}
	} else {
		columnTypes[columnName] = ctype
	}
	return nil
}

// Run executes the wrapped prepared statement.
func (q *Query) Run(ctx context.Context, conn *sql.DB) (*sql.Rows, error) {
	if q.stmt == nil {
		var err error
		q.stmt, err = q.conn.PrepareContext(ctx, q.queryString)
		if err != nil {
			log.Warningf("Failed to prepare query %s", q.queryString)
			return nil, err
		}
	}
	return q.stmt.QueryContext(ctx)
}

// ScanRow scans the current row into a map of column name to value, with string values for key columns and float64
// values for value columns
func (q *Query) ScanRow(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create the array to scan the row into, with strings for keys and float64s for values...
	dest := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			dest = append(dest, new(string))
		case columnTypeValue:
			dest = append(dest, new(float64))
		default:
			log.V(1).Infof("Extra column %q returned by query %s", column, q.stmt)
			dest = append(dest, new(interface{}))
		}
	}
	// ...scan the row content into dest...
	err = rows.Scan(dest...)
	if err != nil {
		return nil, err
	}

	// ...and pick all values we're interested in into a map.
	result := make(map[string]interface{}, len(q.columnTypes))
	for i, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			result[column] = *dest[i].(*string)
		case columnTypeValue:
			result[column] = *dest[i].(*float64)
		}
	}
	return result, nil
}
