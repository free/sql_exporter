package sql_exporter

import (
	"database/sql"
	"fmt"

	log "github.com/golang/glog"
)

// Query wraps a sql.Stmt and all the metrics populated from it. It helps extract keys and values from result rows.
type Query struct {
	stmt    *sql.Stmt
	metrics Metrics
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap
}

type columnType int
type columnTypeMap map[string]columnType

const (
	columnTypeKey   = 1
	columnTypeValue = 2
)

// NewQuery returns a new Query.
func NewQuery(stmt *sql.Stmt, metrics Metrics) (Query, error) {
	columnTypes := make(columnTypeMap)

	for _, m := range metrics {
		for _, kcol := range m.config.KeyLabels {
			if err := setColumnType(kcol, columnTypeKey, columnTypes); err != nil {
				return err
			}
		}
		for _, vcol := range m.config.Values {
			if err := setColumnType(vcol, columnTypeValue, columnTypes); err != nil {
				return err
			}
		}
	}

	q := Query{
		stmt:    stmt,
		metrics: metrics,
	}
	return &q, nil
}

// setColumnType stores the provided type for a given column, checking for conflicts in the process.
func setColumnType(columnName string, ctype columnType, columnTypes columnTypeMap) error {
	previousType, found := columnTypes[columnName]
	if found {
		if previousType != columnType {
			return fmt.Errorf("column %q used both as key and value", columnName)
		}
	} else {
		columnTypes[columnName] = columnType
	}
	return nil
}

// ScanRow scans the columns of the current row into a map keyed by column name with string values for key columns and
// float64 values for value columns.
func (q *Query) ScanRow(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		log.Errorf("error while scanning row: %s", err)
		return err
	}

	dest := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			dest = append(dest, new(string))
		case columnTypeValue:
			dest = append(dest, new(float64))
		default:
			log.V(1).Infof("Unused column %q returned by query %s", column, q.stmt)
			dest = append(dest, new(interface{}))
		}
	}
	err = rows.Scan(dest...)
	if err != nil {
		return err
	}
}

//
//import (
//	"fmt"
//	"strconv"
//
//	"github.com/go-kit/kit/log"
//	"github.com/go-kit/kit/log/level"
//	"github.com/prometheus/client_golang/prometheus"
//)
//
//// Run executes a single Query on a single connection
//func (q *Query) Run(conn *connection) error {
//	if q.log == nil {
//		q.log = log.NewNopLogger()
//	}
//	if q.desc == nil {
//		return fmt.Errorf("metrics descriptor is nil")
//	}
//	if q.Query == "" {
//		return fmt.Errorf("query is empty")
//	}
//	if conn == nil || conn.conn == nil {
//		return fmt.Errorf("db connection not initialized (should not happen)")
//	}
//	// execute query
//	rows, err := conn.conn.Queryx(q.Query)
//	if err != nil {
//		return err
//	}
//	defer rows.Close()
//
//	updated := 0
//	metrics := make([]prometheus.Metric, 0, len(q.metrics))
//	for rows.Next() {
//		res := make(map[string]interface{})
//		err := rows.MapScan(res)
//		if err != nil {
//			level.Error(q.log).Log("msg", "Failed to scan", "err", err, "host", conn.host, "db", conn.database)
//			continue
//		}
//		m, err := q.updateMetrics(conn, res)
//		if err != nil {
//			level.Error(q.log).Log("msg", "Failed to update metrics", "err", err, "host", conn.host, "db", conn.database)
//			continue
//		}
//		metrics = append(metrics, m...)
//		updated++
//	}
//
//	if updated < 1 {
//		return fmt.Errorf("zero rows returned")
//	}
//
//	// update the metrics cache
//	q.Lock()
//	q.metrics[conn] = metrics
//	q.Unlock()
//
//	return nil
//}
//
//// updateMetrics parses the result set and returns a slice of const metrics
//func (q *Query) updateMetrics(conn *connection, res map[string]interface{}) ([]prometheus.Metric, error) {
//	updated := 0
//	metrics := make([]prometheus.Metric, 0, len(q.Values))
//	for _, valueName := range q.Values {
//		m, err := q.updateMetric(conn, res, valueName)
//		if err != nil {
//			level.Error(q.log).Log(
//				"msg", "Failed to update metric",
//				"value", valueName,
//				"err", err,
//				"host", conn.host,
//				"db", conn.database,
//			)
//			continue
//		}
//		metrics = append(metrics, m)
//		updated++
//	}
//	if updated < 1 {
//		return nil, fmt.Errorf("zero values found")
//	}
//	return metrics, nil
//}
//
//// updateMetrics parses a single row and returns a const metric
//func (q *Query) updateMetric(conn *connection, res map[string]interface{}, valueName string) (prometheus.Metric, error) {
//	var value float64
//	if i, ok := res[valueName]; ok {
//		switch f := i.(type) {
//		case int:
//			value = float64(f)
//		case int32:
//			value = float64(f)
//		case int64:
//			value = float64(f)
//		case uint:
//			value = float64(f)
//		case uint32:
//			value = float64(f)
//		case uint64:
//			value = float64(f)
//		case float32:
//			value = float64(f)
//		case float64:
//			value = float64(f)
//		case []uint8:
//			val, err := strconv.ParseFloat(string(f), 64)
//			if err != nil {
//				return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
//			}
//			value = val
//		case string:
//			val, err := strconv.ParseFloat(f, 64)
//			if err != nil {
//				return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
//			}
//			value = val
//		default:
//			return nil, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
//		}
//	}
//	// make space for all defined variable label columns and the "static" labels
//	// added below
//	labels := make([]string, 0, len(q.Labels)+5)
//	for _, label := range q.Labels {
//		// we need to fill every spot in the slice or the key->value mapping
//		// won't match up in the end.
//		//
//		// ORDER MATTERS!
//		lv := ""
//		if i, ok := res[label]; ok {
//			switch str := i.(type) {
//			case string:
//				lv = str
//			case []uint8:
//				lv = string(str)
//			default:
//				return nil, fmt.Errorf("Column '%s' must be type text (string)", label)
//			}
//		}
//		labels = append(labels, lv)
//	}
//	labels = append(labels, conn.driver)
//	labels = append(labels, conn.host)
//	labels = append(labels, conn.database)
//	labels = append(labels, conn.user)
//	labels = append(labels, valueName)
//	// create a new immutable const metric that can be cached and returned on
//	// every scrape. Remember that the order of the lable values in the labels
//	// slice must match the order of the label names in the descriptor!
//	return prometheus.NewConstMetric(q.desc, prometheus.GaugeValue, value, labels...)
//}
