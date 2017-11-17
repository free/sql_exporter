package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	log "github.com/golang/glog"
)

// Query wraps a sql.Stmt and all the metrics populated from it. It helps extract keys and values from result rows.
type Query struct {
	config         *QueryConfig
	metricFamilies []*MetricFamily
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap

	conn *sql.DB
	stmt *sql.Stmt
}

type columnType int
type columnTypeMap map[string]columnType

const (
	columnTypeKey   = 1
	columnTypeValue = 2
)

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(qc *QueryConfig, metricFamilies ...*MetricFamily) (*Query, error) {
	columnTypes := make(columnTypeMap)

	for _, mf := range metricFamilies {
		for _, kcol := range mf.config.KeyLabels {
			if err := setColumnType(kcol, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}
		for _, vcol := range mf.config.Values {
			if err := setColumnType(vcol, columnTypeValue, columnTypes); err != nil {
				return nil, err
			}
		}
	}

	q := Query{
		config:         qc,
		metricFamilies: metricFamilies,
		columnTypes:    columnTypes,
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

// Run executes the query on the provided database, in the provided context.
func (q *Query) Run(ctx context.Context, conn *sql.DB) (*sql.Rows, error) {
	if q.conn != nil && q.conn != conn {
		panic("Expecting to always run on the same database handle")
	}

	if q.stmt == nil {
		stmt, err := conn.PrepareContext(ctx, q.config.Query)
		if err != nil {
			log.Errorf("Failed to prepare query %q: %s", q.config.Name, err)
			return nil, err
		}
		q.conn = conn
		q.stmt = stmt
	}
	return q.stmt.QueryContext(ctx)
}

// ScanRow scans the current row into a map of column name to value, with string values for key columns and float64
// values for value columns.
func (q *Query) ScanRow(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create the slice to scan the row into, with strings for keys and float64s for values.
	dest := make([]interface{}, 0, len(columns))
	have := make(map[string]bool, len(q.columnTypes))
	for _, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			dest = append(dest, new(string))
			have[column] = true
		case columnTypeValue:
			dest = append(dest, new(float64))
			have[column] = true
		default:
			log.V(1).Infof("Extra column %q returned by query %q", column, q.config.Name)
			dest = append(dest, new(interface{}))
		}
	}
	// Not all requested columns could be mapped, fail.
	if len(have) != len(q.columnTypes) {
		missing := make([]string, len(q.columnTypes)-len(have))
		for c, _ := range q.columnTypes {
			missing = append(missing, c)
		}
		return nil, fmt.Errorf("column(s) [%s] missing from query %q result", strings.Join(missing, "], ["), q.config.Name)
	}

	// Scan the row content into dest.
	err = rows.Scan(dest...)
	if err != nil {
		return nil, err
	}

	// Pick all values we're interested in into a map.
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
