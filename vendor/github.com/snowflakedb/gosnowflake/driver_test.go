// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"crypto/rsa"
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	user             string
	pass             string
	account          string
	dbname           string
	schemaname       string
	warehouse        string
	rolename         string
	dsn              string
	host             string
	port             string
	protocol         string
	customPrivateKey bool            // Whether user has specified the private key path
	testPrivKey      *rsa.PrivateKey // Valid private key used for all test cases
)

// The tests require the following parameters in the environment variables.
// SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD, SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_DATABASE,
// SNOWFLAKE_TEST_SCHEMA, SNOWFLAKE_TEST_WAREHOUSE.
// Optionally you may specify SNOWFLAKE_TEST_PROTOCOL, SNOWFLAKE_TEST_HOST and SNOWFLAKE_TEST_PORT to specify
// the endpoint.
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user = env("SNOWFLAKE_TEST_USER", "testuser")
	pass = env("SNOWFLAKE_TEST_PASSWORD", "testpassword")
	account = env("SNOWFLAKE_TEST_ACCOUNT", "testaccount")
	dbname = env("SNOWFLAKE_TEST_DATABASE", "testdb")
	schemaname = env("SNOWFLAKE_TEST_SCHEMA", "public")
	rolename = env("SNOWFLAKE_TEST_ROLE", "sysadmin")
	warehouse = env("SNOWFLAKE_TEST_WAREHOUSE", "testwarehouse")

	protocol = env("SNOWFLAKE_TEST_PROTOCOL", "https")
	host = os.Getenv("SNOWFLAKE_TEST_HOST")
	port = env("SNOWFLAKE_TEST_PORT", "443")
	if host == "" {
		host = fmt.Sprintf("%s.snowflakecomputing.com", account)
	} else {
		host = fmt.Sprintf("%s:%s", host, port)
	}

	setupPrivateKey()

	createDSN("UTC")
}

func createDSN(timezone string) {
	dsn = fmt.Sprintf("%s:%s@%s/%s/%s", user, pass, host, dbname, schemaname)

	parameters := url.Values{}
	parameters.Add("timezone", timezone)
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	if warehouse != "" {
		parameters.Add("warehouse", warehouse)
	}
	if rolename != "" {
		parameters.Add("role", rolename)
	}

	if len(parameters) > 0 {
		dsn += "?" + parameters.Encode()
	}
}

// setup creates a test schema so that all tests can run in the same schema
func setup() (string, error) {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	orgSchemaname := schemaname
	if env("TRAVIS", "") == "true" {
		schemaname = fmt.Sprintf("TRAVIS_JOB_%v", env("TRAVIS_JOB_ID", "testschema"))
	} else {
		schemaname = fmt.Sprintf("golang_%v", time.Now().UnixNano())
	}
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		return "", fmt.Errorf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	if _, err = db.Exec(fmt.Sprintf("CREATE OR REPLACE SCHEMA %v", schemaname)); err != nil {
		return "", fmt.Errorf("failed to create schema. %v", err)
	}
	createDSN("UTC")
	return orgSchemaname, nil
}

// teardown drops the test schema
func teardown() error {
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		return fmt.Errorf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	if _, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %v", schemaname)); err != nil {
		return fmt.Errorf("failed to create schema. %v", err)
	}
	return nil
}

func TestMain(m *testing.M) {
	flag.Parse()
	signal.Ignore(syscall.SIGQUIT)
	if value := os.Getenv("SKIP_SETUP"); value != "" {
		os.Exit(m.Run())
	}

	_, err := setup()
	if err != nil {
		panic(err)
	}
	ret := m.Run()
	err = teardown()
	if err != nil {
		panic(err)
	}
	os.Exit(ret)
}

type DBTest struct {
	*testing.T
	db *sql.DB
}

type RowsExtended struct {
	rows      *sql.Rows
	closeChan *chan bool
}

func (rs *RowsExtended) Close() error {
	*rs.closeChan <- true
	close(*rs.closeChan)
	return rs.rows.Close()
}
func (rs *RowsExtended) ColumnTypes() ([]*sql.ColumnType, error) {
	return rs.rows.ColumnTypes()
}
func (rs *RowsExtended) Columns() ([]string, error) {
	return rs.rows.Columns()
}

func (rs *RowsExtended) Err() error {
	return rs.rows.Err()
}
func (rs *RowsExtended) Next() bool {
	return rs.rows.Next()
}
func (rs *RowsExtended) NextResultSet() bool {
	return rs.rows.NextResultSet()
}

func (rs *RowsExtended) Scan(dest ...interface{}) error {
	return rs.rows.Scan(dest...)
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *RowsExtended) {
	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	c0 := make(chan bool, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			fmt.Println("Caught signal, canceling...")
			cancel()
		case <-ctx.Done():
			fmt.Println("Done")
		case <-c0:
		}
		close(c)
	}()

	rs, err := dbt.db.QueryContext(ctx, query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return &RowsExtended{
		rows:      rs,
		closeChan: &c0,
	}
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s [%s]: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustDecimalSize(ct *sql.ColumnType) (pr int64, sc int64) {
	var ok bool
	pr, sc, ok = ct.DecimalSize()
	if !ok {
		dbt.Fatalf("failed to get decimal size. %v", ct)
	}
	return pr, sc
}

func (dbt *DBTest) mustFailDecimalSize(ct *sql.ColumnType) {
	var ok bool
	_, _, ok = ct.DecimalSize()
	if ok {
		dbt.Fatalf("should not return decimal size. %v", ct)
	}
}

func (dbt *DBTest) mustLength(ct *sql.ColumnType) (cLen int64) {
	var ok bool
	cLen, ok = ct.Length()
	if !ok {
		dbt.Fatalf("failed to get length. %v", ct)
	}
	return cLen
}

func (dbt *DBTest) mustFailLength(ct *sql.ColumnType) {
	var ok bool
	_, ok = ct.Length()
	if ok {
		dbt.Fatalf("should not return length. %v", ct)
	}
}

func (dbt *DBTest) mustNullable(ct *sql.ColumnType) (canNull bool) {
	var ok bool
	canNull, ok = ct.Nullable()
	if !ok {
		dbt.Fatalf("failed to get length. %v", ct)
	}
	return canNull
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	_, err = db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

func TestBogusUserPasswordParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", "bogus", pass, host)
	invalidUserPassErrorTests(invalidDNS, t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", user, "INVALID_PASSWORD", host)
	invalidUserPassErrorTests(invalidDNS, t)
}
func invalidUserPassErrorTests(invalidDNS string, t *testing.T) {
	parameters := url.Values{}
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	invalidDNS += "?" + parameters.Encode()
	db, err := sql.Open("snowflake", invalidDNS)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	// actual connection won't happen until run a query
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("should cause an error.")
	}
	if driverErr, ok := err.(*SnowflakeError); ok {
		if driverErr.Number != 390100 {
			t.Fatalf("wrong error code: %v", driverErr)
		}
		if !strings.Contains(driverErr.Error(), "390100") {
			t.Fatalf("error message should included the error code. got: %v", driverErr.Error())
		}
	} else {
		t.Fatalf("wrong error code: %v", err)
	}
}

func TestBogusHostNameParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", user, pass, "INVALID_HOST:1234")
	invalidHostErrorTests(invalidDNS, []string{"no such host", "verify account name is correct", "HTTP Status: 403"}, t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", user, pass, "INVALID_HOST")
	invalidHostErrorTests(invalidDNS, []string{"read: connection reset by peer.", "EOF", "verify account name is correct", "HTTP Status: 403"}, t)
}

func invalidHostErrorTests(invalidDNS string, mstr []string, t *testing.T) {
	parameters := url.Values{}
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	parameters.Add("loginTimeout", "10")
	invalidDNS += "?" + parameters.Encode()
	db, err := sql.Open("snowflake", invalidDNS)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	// actual connection won't happen until run a query
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("should cause an error.")
	}
	found := false
	for _, m := range mstr {
		if strings.Contains(err.Error(), m) {
			found = true
		}
	}
	if !found {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestCommentOnlyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "--"
		// just a comment, no query
		rows, err := dbt.db.Query(query)
		if err == nil {
			rows.Close()
			dbt.fail("query", query, err)
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != 900 { // syntax error
				dbt.fail("query", query, err)
			}
		}
	})
}

func TestEmptyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "select 1 from dual where 1=0"
		// just a comment, no query
		rows := dbt.db.QueryRow(query)
		var v1 interface{}
		err := rows.Scan(&v1)
		if err != sql.ErrNoRows {
			dbt.Errorf("should fail. err: %v", err)
		}
		rows = dbt.db.QueryRowContext(context.Background(), query)
		err = rows.Scan(&v1)
		if err != sql.ErrNoRows {
			dbt.Errorf("should fail. err: %v", err)
		}
	})
}

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOLEAN)")

		// Test for unexpected Data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if rows.Next() {
			dbt.Error("unexpected Data in empty table")
		}

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (true)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != -1 {
			dbt.Fatalf(
				"expected InsertId -1, got %d. Snowflake doesn't support last insert ID", id)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if !out {
				dbt.Errorf("%t should be true", out)
			}

			if rows.Next() {
				dbt.Error("unexpected Data")
			}
		} else {
			dbt.Error("no Data")
		}

		// Update
		res = dbt.mustExec("UPDATE test SET value = ? WHERE value = ?", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out {
				dbt.Errorf("%t should be true", out)
			}

			if rows.Next() {
				dbt.Error("unexpected Data")
			}
		} else {
			dbt.Error("no Data")
		}

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = ?", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 0 {
			dbt.Fatalf("expected 0 affected row, got %d", count)
		}
	})
}

func TestInt(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"INT", "INTEGER"}
		in := int64(42)
		var out int64
		var rows *RowsExtended

		// SIGNED
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat32(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		in := float32(42.23)
		var out float32
		var rows *RowsExtended
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				err := rows.Scan(&out)
				if err != nil {
					dbt.Errorf("failed to scan data: %v", err)
				}
				if in != out {
					dbt.Errorf("%s: %g != %g", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat64(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *RowsExtended
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (42.23)")
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat64Placeholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *RowsExtended
		for _, v := range types {
			dbt.mustExec(fmt.Sprintf("CREATE TABLE test (id int, value %v)", v))
			dbt.mustExec("INSERT INTO test VALUES (1, ?)", expected)
			rows = dbt.mustQuery("SELECT value FROM test WHERE id = ?", 1)
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

// TestUint64Placeholder tests uint64 binding. Should fail as unit64 is not a supported binding value by Go's sql
// package.
func TestUint64Placeholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"INTEGER"}
		expected := uint64(18446744073709551615)
		for _, v := range types {
			dbt.mustExec(fmt.Sprintf("CREATE TABLE test (id int, value %v)", v))
			_, err := dbt.db.Exec("INSERT INTO test VALUES (1, ?)", expected)
			if err == nil {
				dbt.Fatal("should fail as uint64 values with high bit set are not supported.")
			} else {
				glog.V(2).Infof("expected err: %v", err)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestDateTimeTimestampPlaceholder(t *testing.T) {
	createDSN("America/Los_Angeles")
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec(
			"CREATE OR REPLACE TABLE tztest (id int, ntz timestamp_ntz, ltz timestamp_ltz, dt date, tm time)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz,dt,tm) VALUES(1,?,?,?,?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(
			DataTypeTimestampNtz, expected,
			DataTypeTimestampLtz, expected,
			DataTypeDate, expected,
			DataTypeTime, expected)
		if err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT ntz,ltz,dt,tm FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var ntz, vltz, dt, tm time.Time
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			dbt.Errorf("column type error. err: %v", err)
		}
		if columnTypes[0].Name() != "NTZ" {
			dbt.Errorf("expected column name: %v, got: %v", "TEST", columnTypes[0])
		}
		canNull := dbt.mustNullable(columnTypes[0])
		if !canNull {
			dbt.Errorf("expected nullable: %v, got: %v", true, canNull)
		}
		if columnTypes[0].DatabaseTypeName() != "TIMESTAMP_NTZ" {
			dbt.Errorf("expected database type: %v, got: %v", "TIMESTAMP_NTZ", columnTypes[0].DatabaseTypeName())
		}
		dbt.mustFailDecimalSize(columnTypes[0])
		dbt.mustFailLength(columnTypes[0])
		cols, err := rows.Columns()
		if err != nil {
			dbt.Errorf("failed to get columns. err: %v", err)
		}
		if len(cols) != 4 || cols[0] != "NTZ" || cols[1] != "LTZ" || cols[2] != "DT" || cols[3] != "TM" {
			dbt.Errorf("failed to get columns. got: %v", cols)
		}
		if rows.Next() {
			rows.Scan(&ntz, &vltz, &dt, &tm)
			if expected.UnixNano() != ntz.UnixNano() {
				dbt.Errorf("returned TIMESTAMP_NTZ value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, ntz.UnixNano(), ntz)
			}
			if expected.UnixNano() != vltz.UnixNano() {
				dbt.Errorf("returned TIMESTAMP_LTZ value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, vltz.UnixNano(), vltz)
			}
			if expected.Year() != dt.Year() || expected.Month() != dt.Month() || expected.Day() != dt.Day() {
				dbt.Errorf("returned DATE value didn't match. expected: %v:%v, got: %v:%v",
					expected.Unix()*1000, expected, dt.Unix()*1000, dt)
			}
			if expected.Hour() != tm.Hour() || expected.Minute() != tm.Minute() || expected.Second() != tm.Second() || expected.Nanosecond() != tm.Nanosecond() {
				dbt.Errorf("returned TIME value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, tm.UnixNano(), tm)
			}
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})

	createDSN("UTC")
}

func TestBinaryPlaceholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE bintest (id int, b binary)")
		var b = []byte{0x01, 0x02, 0x03}
		dbt.mustExec("INSERT INTO bintest(id,b) VALUES(1, ?)", DataTypeBinary, b)
		rows := dbt.mustQuery("SELECT b FROM bintest WHERE id=?", 1)
		defer rows.Close()
		if rows.Next() {
			var rb []byte
			if err := rows.Scan(&rb); err != nil {
				dbt.Errorf("failed to scan data. err: %v", err)
			}
			if !bytes.Equal(b, rb) {
				dbt.Errorf("failed to match data. expected: %v, got: %v", b, rb)
			}
		} else {
			dbt.Errorf("no data")
		}
		dbt.mustExec("DROP TABLE bintest")
	})
}

func TestBindingInterface(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		var err error
		rows := dbt.mustQuery(
			"SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6")
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
		err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
		if err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		var s string
		var ok bool
		s, ok = v1.(string)
		if !ok || s != "1.00" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v1)
		}
		s, ok = v2.(string)
		if !ok || s != "2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		s, ok = v3.(string)
		if !ok || s != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
		s, ok = v4.(string)
		if !ok || s != "4.2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		}
	})
}

func TestVariousTypes(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(
			"SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6")
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		cc, err := rows.Columns()
		if err != nil {
			dbt.Errorf("columns: %v", cc)
		}
		ct, err := rows.ColumnTypes()
		if err != nil {
			dbt.Errorf("column types: %v", ct)
		}
		var v1 float32
		var v2 int
		var v3 string
		var v4 float64
		var v5 []byte
		var v6 bool
		err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
		if err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if v1 != 1.0 {
			dbt.Errorf("failed to scan. %#v", v1)
		}
		if ct[0].Name() != "C1" || ct[1].Name() != "C2" || ct[2].Name() != "C3" || ct[3].Name() != "C4" || ct[4].Name() != "C5" || ct[5].Name() != "C6" {
			dbt.Errorf("failed to get column names: %#v", ct)
		}
		if ct[0].ScanType() != reflect.TypeOf(float64(0)) {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeOf(float64(0)), ct[0].ScanType())
		}
		if ct[1].ScanType() != reflect.TypeOf(int64(0)) {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeOf(int64(0)), ct[1].ScanType())
		}
		var pr, sc int64
		var cLen int64
		var canNull bool
		pr, sc = dbt.mustDecimalSize(ct[0])
		if pr != 30 || sc != 2 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[0])
		}
		dbt.mustFailLength(ct[0])
		canNull = dbt.mustNullable(ct[0])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[0])
		}
		if cLen != 0 {
			dbt.Errorf("failed to get length. %#v", ct[0])
		}
		if v2 != 2 {
			dbt.Errorf("failed to scan. %#v", v2)
		}
		pr, sc = dbt.mustDecimalSize(ct[1])
		if pr != 38 || sc != 0 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[1])
		}
		dbt.mustFailLength(ct[1])
		canNull = dbt.mustNullable(ct[1])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[1])
		}
		if v3 != "t3" {
			dbt.Errorf("failed to scan. %#v", v3)
		}
		dbt.mustFailDecimalSize(ct[2])
		cLen = dbt.mustLength(ct[2])
		if cLen != 2 {
			dbt.Errorf("failed to get length. %#v", ct[2])
		}
		canNull = dbt.mustNullable(ct[2])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[2])
		}
		if v4 != 4.2 {
			dbt.Errorf("failed to scan. %#v", v4)
		}
		dbt.mustFailDecimalSize(ct[3])
		dbt.mustFailLength(ct[3])
		canNull = dbt.mustNullable(ct[3])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[3])
		}
		if !bytes.Equal(v5, []byte{0xab, 0xcd}) {
			dbt.Errorf("failed to scan. %#v", v5)
		}
		dbt.mustFailDecimalSize(ct[4])
		cLen = dbt.mustLength(ct[4]) // BINARY
		if cLen != 8388608 {
			dbt.Errorf("failed to get length. %#v", ct[4])
		}
		canNull = dbt.mustNullable(ct[4])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[4])
		}
		if !v6 {
			dbt.Errorf("failed to scan. %#v", v6)
		}
		dbt.mustFailDecimalSize(ct[5])
		dbt.mustFailLength(ct[5])
		/*canNull = dbt.mustNullable(ct[5])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[5])
		}*/

	})
}

func TestTimestampTZPlaceholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, tz timestamp_tz)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,tz) VALUES(1, ?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(DataTypeTimestampTz, expected)
		if err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT tz FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var v time.Time
		if rows.Next() {
			rows.Scan(&v)
			if expected.UnixNano() != v.UnixNano() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, v.UnixNano(), v)
			}
			// fmt.Printf("returned value: %v, %v, %v\n", v, v.UnixNano(), expected.UnixNano())
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})
}

func TestString(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"CHAR(255)", "VARCHAR(255)", "TEXT", "STRING"}
		in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
		var out string
		var rows *RowsExtended

		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")

			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %s != %s", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}

		// BLOB (Snowflake doesn't support BLOB type but STRING covers large text data)
		dbt.mustExec("CREATE TABLE test (id int, value STRING)")

		id := 2
		in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
			"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
		dbt.mustExec("INSERT INTO test VALUES (?, ?)", id, in)

		err := dbt.db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out)
		if err != nil {
			dbt.Fatalf("Error on BLOB-Query: %s", err.Error())
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

type tcDateTimeTimestamp struct {
	dbtype  string
	tlayout string
	tests   []timeTest
}

type timeTest struct {
	s string    // source date time string
	t time.Time // expected fetched data
}

func (tt timeTest) genQuery() string {
	return "SELECT '%s'::%s"
}

func (tt timeTest) run(t *testing.T, dbt *DBTest, dbtype, tlayout string) {
	var rows *RowsExtended
	query := fmt.Sprintf(tt.genQuery(), tt.s, dbtype)
	rows = dbt.mustQuery(query)
	defer rows.Close()
	var err error
	if !rows.Next() {
		err = rows.Err()
		if err == nil {
			err = fmt.Errorf("no data")
		}
		dbt.Errorf("%s: %s", dbtype, err)
		return
	}

	var dst interface{}
	err = rows.Scan(&dst)
	if err != nil {
		dbt.Errorf("%s: %s", dbtype, err)
		return
	}
	switch val := dst.(type) {
	case []uint8:
		str := string(val)
		if str == tt.s {
			return
		}
		dbt.Errorf("%s to string: expected %q, got %q",
			dbtype,
			tt.s,
			str,
		)
	case time.Time:
		if val.UnixNano() == tt.t.UnixNano() {
			return
		}
		t.Logf("source:%v, expected: %v, got:%v", tt.s, tt.t, val)
		dbt.Errorf("%s to string: expected %q, got %q",
			dbtype,
			tt.s,
			val.Format(tlayout),
		)
	default:
		fmt.Printf("%#v\n", []interface{}{dbtype, tlayout, tt.s, tt.t})
		dbt.Errorf("%s: unhandled type %T (is '%v')",
			dbtype, val, val,
		)
	}
}

func TestSimpleDateTimeTimestampFetch(t *testing.T) {
	var scan = func(rows *RowsExtended, cd interface{}, ct interface{}, cts interface{}) {
		err := rows.Scan(cd, ct, cts)
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Printf("cd: %v, ct: %v, cts: %v", cd, ct, cts)
		// no error should occurs
	}
	var fetchTypes = []func(*RowsExtended){
		func(rows *RowsExtended) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
		func(rows *RowsExtended) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		for _, f := range fetchTypes {
			rows := dbt.mustQuery("SELECT CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIMESTAMP()")
			defer rows.Close()
			if rows.Next() {
				f(rows)
			} else {
				t.Fatal("no results")
			}
		}
	})
}

func TestDateTime(t *testing.T) {
	afterTime := func(t time.Time, d string) time.Time {
		dur, err := time.ParseDuration(d)
		if err != nil {
			panic(err)
		}
		return t.Add(dur)
	}
	format := "2006-01-02 15:04:05.999999999"
	t0 := time.Time{}
	tstr0 := "0000-00-00 00:00:00.000000000"
	testcases := []tcDateTimeTimestamp{
		{"DATE", format[:10], []timeTest{
			{t: time.Date(2011, 11, 20, 0, 0, 0, 0, time.UTC)},
			{t: time.Date(2, 8, 2, 0, 0, 0, 0, time.UTC), s: "0002-08-02"},
		}},
		{"TIME", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(0)", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(1)", format[11:21], []timeTest{
			{t: afterTime(t0, "12345600ms")},
			{t: t0, s: tstr0[11:21]},
		}},
		{"TIME(6)", format[11:], []timeTest{
			{t: t0, s: tstr0[11:]},
		}},
		{"DATETIME", format[:19], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
		}},
		{"DATETIME(0)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
		}},
		{"DATETIME(1)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 100000000, time.UTC)},
		}},
		{"DATETIME(6)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456000, time.UTC)},
		}},
		{"DATETIME(9)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456789, time.UTC)},
		}},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
}

func TestTimestampLTZ(t *testing.T) {
	format := "2006-01-02 15:04:05.999999999"
	// Set session time zone in Los Angeles, same as machine
	createDSN("America/Los_Angeles")
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Error(err)
	}
	testcases := []tcDateTimeTimestamp{
		{
			dbtype:  "TIMESTAMP_LTZ(9)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2016-12-30 05:02:03",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0, location),
				},
				{
					s: "2016-12-30 05:02:03 -00:00",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0, time.UTC),
				},
				{
					s: "2017-05-12 00:51:42",
					t: time.Date(2017, 5, 12, 0, 51, 42, 0, location),
				},
				{
					s: "2017-03-12 01:00:00",
					t: time.Date(2017, 3, 12, 1, 0, 0, 0, location),
				},
				{
					s: "2017-03-13 04:00:00",
					t: time.Date(2017, 3, 13, 4, 0, 0, 0, location),
				},
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456789, location),
				},
			},
		},
		{
			dbtype:  "TIMESTAMP_LTZ(8)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456780, location),
				},
			},
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
	// Revert timezone to UTC, which is default for the test suit
	createDSN("UTC")
}

func TestTimestampTZ(t *testing.T) {
	sflo := func(offsets string) (loc *time.Location) {
		r, err := LocationWithOffsetString(offsets)
		if err != nil {
			return time.UTC
		}
		return r
	}
	format := "2006-01-02 15:04:05.999999999"
	testcases := []tcDateTimeTimestamp{
		{
			dbtype:  "TIMESTAMP_TZ(9)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2016-12-30 05:02:03 +07:00",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0,
						sflo("+0700")),
				},
				{
					s: "2017-05-23 03:56:41 -09:00",
					t: time.Date(2017, 5, 23, 3, 56, 41, 0,
						sflo("-0900")),
				},
			},
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
}

func TestNULL(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		nullStmt, err := dbt.db.Prepare("SELECT NULL")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nullStmt.Close()

		nonNullStmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nonNullStmt.Close()

		// NullBool
		var nb sql.NullBool
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if nb.Valid {
			dbt.Error("valid NullBool which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if !nb.Valid {
			dbt.Error("invalid NullBool which should be valid")
		} else if !nb.Bool {
			dbt.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
		}

		// NullFloat64
		var nf sql.NullFloat64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if nf.Valid {
			dbt.Error("valid NullFloat64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if !nf.Valid {
			dbt.Error("invalid NullFloat64 which should be valid")
		} else if nf.Float64 != float64(1) {
			dbt.Errorf("unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
		}

		// NullInt64
		var ni sql.NullInt64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if ni.Valid {
			dbt.Error("valid NullInt64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if !ni.Valid {
			dbt.Error("invalid NullInt64 which should be valid")
		} else if ni.Int64 != int64(1) {
			dbt.Errorf("unexpected NullInt64 value: %d (should be 1)", ni.Int64)
		}

		// NullString
		var ns sql.NullString
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if ns.Valid {
			dbt.Error("valid NullString which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if !ns.Valid {
			dbt.Error("invalid NullString which should be valid")
		} else if ns.String != `1` {
			dbt.Error("unexpected NullString value:" + ns.String + " (should be `1`)")
		}

		// nil-bytes
		var b []byte
		// Read nil
		if err = nullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil []byte which should be nil")
		}
		// Read non-nil
		if err = nonNullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil []byte which should be non-nil")
		}
		// Insert nil
		b = nil
		success := false
		if err = dbt.db.QueryRow("SELECT ? IS NULL", b).Scan(&success); err != nil {
			dbt.Fatal(err)
		}
		if !success {
			dbt.Error("inserting []byte(nil) as NULL failed")
			t.Fatal("stopping")
		}
		// Check input==output with input==nil
		b = nil
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil echo from nil input")
		}
		// Check input==output with input!=nil
		b = []byte("")
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}

		// Insert NULL
		dbt.mustExec("CREATE TABLE test (dummmy1 int, value int, dummy2 int)")

		dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", 1, nil, 2)

		var out interface{}
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out != nil {
				dbt.Errorf("%v != nil", out)
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func TestVariant(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(`select parse_json('[{"id":1, "name":"test1"},{"id":2, "name":"test2"}]')`)
		defer rows.Close()
		var v string
		if rows.Next() {
			err := rows.Scan(&v)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatal("no rows")
		}
	})
}

func TestArray(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(`select as_array(parse_json('[{"id":1, "name":"test1"},{"id":2, "name":"test2"}]'))`)
		defer rows.Close()
		var v string
		if rows.Next() {
			err := rows.Scan(&v)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatal("no rows")
		}
	})
}

func TestLargeSetResult(t *testing.T) {
	CustomJSONDecoderEnabled = false
	testLargeSetResult(t, 100000)
}

func TestLargeSetResultWithCustomJSONDecoder(t *testing.T) {
	CustomJSONDecoderEnabled = true
	// less number of rows to avoid Travis timeout
	testLargeSetResult(t, 20000)
}

func testLargeSetResult(t *testing.T, numrows int) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows))
		defer rows.Close()
		cnt := 0
		var idx int
		var v string
		for rows.Next() {
			err := rows.Scan(&idx, &v)
			if err != nil {
				t.Fatal(err)
			}
			if cnt%1000 == 0 {
				glog.V(2).Infof("%v, %v", idx, v)
				glog.V(2).Infof("NextResultSet: %v", rows.NextResultSet())
			}
			cnt++
		}
		glog.V(2).Infof("NextResultSet: %v", rows.NextResultSet())

		if cnt != numrows {
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}
	})
}

func TestPingpongQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		numrows := 1
		rows := dbt.mustQuery("SELECT DISTINCT 1 FROM TABLE(GENERATOR(TIMELIMIT=> 60))")
		defer rows.Close()
		cnt := 0
		for rows.Next() {
			cnt++
		}
		if cnt != numrows {
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}
	})
}

func TestDML(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test(c1 int, c2 string)")
		err := insertData(dbt, false)
		if err != nil {
			dbt.Fatalf("failed to insert data: %v", err)
		}
		results, err := queryTest(dbt)
		if err != nil {
			dbt.Fatalf("failed to query test table: %v", err)
		}
		if len(*results) != 0 {
			dbt.Fatalf("number of returned data didn't match. expected 0, got: %v", len(*results))
		}
		err = insertData(dbt, true)
		if err != nil {
			dbt.Fatalf("failed to insert data: %v", err)
		}
		results, err = queryTest(dbt)
		if err != nil {
			dbt.Fatalf("failed to query test table: %v", err)
		}
		if len(*results) != 2 {
			dbt.Fatalf("number of returned data didn't match. expected 2, got: %v", len(*results))
		}
	})
}
func insertData(dbt *DBTest, commit bool) error {
	tx, err := dbt.db.Begin()
	if err != nil {
		dbt.Fatalf("failed to begin transaction: %v", err)
	}
	res, err := tx.Exec("INSERT INTO test VALUES(1, 'test1'), (2, 'test2')")
	if err != nil {
		dbt.Fatalf("failed to insert value into test: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		dbt.Fatalf("failed to rows affected: %v", err)
	}
	if n != 2 {
		dbt.Fatalf("failed to insert value into test. expected: 2, got: %v", n)
	}
	results, err := queryTestTx(tx)
	if err != nil {
		dbt.Fatalf("failed to query test table: %v", err)
	}
	if len(*results) != 2 {
		dbt.Fatalf("number of returned data didn't match. expected 2, got: %v", len(*results))
	}
	if commit {
		err = tx.Commit()
		if err != nil {
			return err
		}
	} else {
		err = tx.Rollback()
		if err != nil {
			return err
		}
	}
	return err
}

func queryTestTx(tx *sql.Tx) (*map[int]string, error) {
	var c1 int
	var c2 string
	rows, err := tx.Query("SELECT c1, c2 FROM test")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	results := make(map[int]string, 2)
	for rows.Next() {
		err := rows.Scan(&c1, &c2)
		if err != nil {
			return nil, err
		}
		results[c1] = c2
	}
	return &results, nil
}

func queryTest(dbt *DBTest) (*map[int]string, error) {
	var c1 int
	var c2 string
	rows, err := dbt.db.Query("SELECT c1, c2 FROM test")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	results := make(map[int]string, 2)
	for rows.Next() {
		err := rows.Scan(&c1, &c2)
		if err != nil {
			return nil, err
		}
		results[c1] = c2
	}
	return &results, nil
}

// Special cases where rows are already closed
func TestRowsClose(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows, err := dbt.db.Query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("unexpected row after rows.Close()")
		}

		err = rows.Err()
		if err != nil {
			dbt.Fatal(err)
		}
	})
}

func TestResultNoRows(t *testing.T) {
	// DDL
	runTests(t, dsn, func(dbt *DBTest) {
		row, err := dbt.db.Exec("CREATE OR REPLACE TABLE test(c1 int)")
		if err != nil {
			t.Fatalf("failed to execute DDL. err: %v", err)
		}
		_, err = row.RowsAffected()
		if err == nil {
			t.Fatal("should have failed to get RowsAffected")
		}
		_, err = row.LastInsertId()
		if err == nil {
			t.Fatal("should have failed to get LastInsertID")
		}
	})
}

func TestCancelQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := dbt.db.QueryContext(ctx, "SELECT DISTINCT 1 FROM TABLE(GENERATOR(TIMELIMIT=> 100))")

		if err == nil {
			dbt.Fatal("No timeout error returned")
		}

		if err.Error() != "context deadline exceeded" {
			dbt.Fatalf("Timeout error mismatch: expect %v, receive %v", context.DeadlineExceeded, err.Error())
		}
	})
}

func TestInvalidConnection(t *testing.T) {
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	err = db.Close()
	if err != nil {
		t.Error("should not cause error in Close")
	}
	err = db.Close()
	if err != nil {
		t.Error("should not cause error in the second call of Close")
	}
	_, err = db.Exec("CREATE TABLE OR REPLACE test0(c1 int)")
	if err == nil {
		t.Error("should fail to run Exec")
	}
	_, err = db.Query("SELECT CURRENT_TIMESTAMP()")
	if err == nil {
		t.Error("should fail to run Query")
	}
	_, err = db.Begin()
	if err == nil {
		t.Error("should fail to run Begin")
	}
}

func TestPing(t *testing.T) {
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	if err = db.Ping(); err != nil {
		t.Fatalf("failed to ping. %v, err: %v", dsn, err)
	}
	if err = db.PingContext(context.Background()); err != nil {
		t.Fatalf("failed to ping with context. %v, err: %v", dsn, err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("failed to close db. %v, err: %v", dsn, err)
	}
	if err = db.Ping(); err == nil {
		t.Fatal("should have failed to ping")
	}
	if err = db.PingContext(context.Background()); err == nil {
		t.Fatal("should have failed to ping with context")
	}
}

func TestDoubleDollar(t *testing.T) {
	// no escape is required for dollar signs
	runTests(t, dsn, func(dbt *DBTest) {
		sql := `create or replace function dateErr(I double) returns date
language javascript strict
as $$
  var x = [
    0, "1400000000000",
    "2013-04-05",
    [], [1400000000000],
    "x1234",
    Number.NaN, null, undefined,
    {},
    [1400000000000,1500000000000]
  ];
  return x[I];
$$
;`
		dbt.mustExec(sql)
	})
}

func TestTransactionOptions(t *testing.T) {
	var db *sql.DB
	var err error
	var driverErr *SnowflakeError
	var ok bool

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	var tx *sql.Tx
	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatal("failed to start transaction.")
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal("failed to rollback")
	}
	_, err = db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrNoReadOnlyTransaction {
		t.Fatalf("should have returned Snowflake Error: %v", errMsgNoReadOnlyTransaction)
	}
	_, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 100})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrNoDefaultTransactionIsolationLevel {
		t.Fatalf("should have returned Snowflake Error: %v", errMsgNoDefaultTransactionIsolationLevel)
	}
}

func TestTimezoneSessionParameter(t *testing.T) {
	var db *sql.DB
	var err error
	var rows *sql.Rows

	createDSN("America/Los_Angeles")
	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	rows, err = db.Query("SHOW PARAMETERS LIKE 'TIMEZONE'")
	if err != nil {
		t.Errorf("failed to run show parameters. err: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("failed to get timezone.")
	}

	p, err := ScanSnowflakeParameter(rows)
	if err != nil {
		t.Errorf("failed to run get timezone value. err: %v", err)
	}
	if p.Value != "America/Los_Angeles" {
		t.Fatalf("failed to get an expected timezone. got: %v", p.Value)
	}
	createDSN("UTC")
}

func TestLargeSetResultCancel(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		c := make(chan error)
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			// attempt to run a 100 seconds query, but it should be canceled in 1 second
			timelimit := 100
			rows, err := dbt.db.QueryContext(
				ctx,
				fmt.Sprintf("SELECT COUNT(*) FROM TABLE(GENERATOR(timelimit=>%v))", timelimit))
			if err != nil {
				c <- err
				return
			}
			defer rows.Close()
			c <- nil
		}()
		// cancel after 1 second
		time.Sleep(time.Second)
		cancel()
		ret := <-c
		if ret.Error() != "context canceled" {
			t.Fatalf("failed to cancel. err: %v", ret)
		}
		close(c)
	})
}

type tcValidateDatabaseParameter struct {
	dsn       string
	params    map[string]string
	errorCode int
}

func TestValidateDatabaseParameter(t *testing.T) {
	baseDSN := fmt.Sprintf("%s:%s@%s", user, pass, host)
	testcases := []tcValidateDatabaseParameter{
		{
			dsn:       baseDSN + fmt.Sprintf("/%s/%s", "NOT_EXISTS", "NOT_EXISTS"),
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn:       baseDSN + fmt.Sprintf("/%s/%s", dbname, "NOT_EXISTS"),
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn: baseDSN + fmt.Sprintf("/%s/%s", dbname, schemaname),
			params: map[string]string{
				"warehouse": "NOT_EXIST",
			},
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn: baseDSN + fmt.Sprintf("/%s/%s", dbname, schemaname),
			params: map[string]string{
				"role": "NOT_EXIST",
			},
			errorCode: ErrRoleNotExist,
		},
	}
	for idx, tc := range testcases {
		newDSN := tc.dsn
		parameters := url.Values{}
		if protocol != "" {
			parameters.Add("protocol", protocol)
		}
		if account != "" {
			parameters.Add("account", account)
		}
		for k, v := range tc.params {
			parameters.Add(k, v)
		}
		newDSN += "?" + parameters.Encode()
		db, err := sql.Open("snowflake", newDSN)
		// actual connection won't happen until run a query
		if err != nil {
			t.Fatalf("error creating a connection object: %s", err.Error())
		}
		defer db.Close()
		_, err = db.Exec("SELECT 1")
		if err == nil {
			t.Fatal("should cause an error.")
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != tc.errorCode { // not exist error
				t.Errorf("got unexpected error: %v in %v", err, idx)
			}
		}
	}
}

func TestSpecifyWarehouseDatabase(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@%s/%s", user, pass, host, dbname)
	parameters := url.Values{}
	parameters.Add("account", account)
	parameters.Add("warehouse", warehouse)
	// parameters.Add("role", "nopublic") TODO: create nopublic role for test
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	db, err := sql.Open("snowflake", dsn+"?"+parameters.Encode())
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err != nil {
		t.Fatalf("failed to execute a select 1: %v", err)
	}
}

func TestFetchNil(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT * FROM values(3,4),(null, 5) order by 2")
		defer rows.Close()
		var c1 sql.NullInt64
		var c2 sql.NullInt64

		var results []sql.NullInt64
		for rows.Next() {
			err := rows.Scan(&c1, &c2)
			if err != nil {
				dbt.Fatal(err)
			}
			results = append(results, c1)
		}
		if results[1].Valid {
			t.Errorf("First element of second row must be nil (NULL). %v", results)
		}
	})
}

func TestPingInvalidHost(t *testing.T) {
	config := Config{
		Account:      "NOT_EXISTS",
		User:         "BOGUS_USER",
		Password:     "barbar",
		LoginTimeout: 10 * time.Second,
	}

	testURL, err := DSN(&config)
	if err != nil {
		t.Fatalf("failed to parse config. config: %v, err: %v", config, err)
	}

	db, err := sql.Open("snowflake", testURL)
	if err != nil {
		t.Fatalf("failed to initalize the connetion. err: %v", err)
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err == nil {
		t.Fatal("should cause an error")
	}
	driverErr, ok := err.(*SnowflakeError)

	if !ok || ok && driverErr.Number != ErrCodeFailedToConnect { // Failed to connect error
		t.Fatalf("error didn't match")
	}
}

func createDSNWithClientSessionKeepAlive() {
	dsn = fmt.Sprintf("%s:%s@%s/%s/%s", user, pass, host, dbname, schemaname)

	parameters := url.Values{}
	parameters.Add("client_session_keep_alive", "true")
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	if warehouse != "" {
		parameters.Add("warehouse", warehouse)
	}
	if rolename != "" {
		parameters.Add("role", rolename)
	}
	if len(parameters) > 0 {
		dsn += "?" + parameters.Encode()
	}
}

func TestClientSessionKeepAliveParameter(t *testing.T) {
	// This test doesn't really validate the CLIENT_SESSION_KEEP_ALIVE functionality but simply checks
	// the session parameter.
	var db *sql.DB
	var err error
	var rows *sql.Rows

	createDSNWithClientSessionKeepAlive()
	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	rows, err = db.Query("SHOW PARAMETERS LIKE 'CLIENT_SESSION_KEEP_ALIVE'")
	if err != nil {
		t.Errorf("failed to run show parameters. err: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("failed to get timezone.")
	}

	p, err := ScanSnowflakeParameter(rows)
	if err != nil {
		t.Errorf("failed to run get client_session_keep_alive value. err: %v", err)
	}
	if p.Value != "true" {
		t.Fatalf("failed to get an expected client_session_keep_alive. got: %v", p.Value)
	}
	rows, err = db.Query("select count(*) from table(generator(timelimit=>30))")
	if err != nil {
		t.Errorf("failed to run a query: %v", err)
	}
	defer rows.Close()
}

func TestTimePrecision(t *testing.T) {
	var db *sql.DB
	var err error

	if db, err = sql.Open("snowflake", dsn); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()

	_, err = db.Exec("create or replace table z3 (t1 time(5))")
	if err != nil {
		t.Errorf("error while executing query. err : %v", err)
	}
	res, err := db.Query("select * from z3")
	if err != nil {
		t.Errorf("error while executing query. err : %v", err)
	}

	cols, _ := res.ColumnTypes()
	pres, _, _ := cols[0].DecimalSize()
	if pres != 5 {
		t.Fatalf("Wrong value returned. Got %v instead of 5.", pres)
	}
}
