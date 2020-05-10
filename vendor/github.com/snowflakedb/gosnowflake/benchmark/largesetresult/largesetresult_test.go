// This code is to profile a large result set query. It is basically similar to selectmany example code but
// leverages benchmark framework.
package largesetresult

import (
	"flag"
	"log"
	_ "net/http/pprof"
	"os"
	"testing"

	"database/sql"

	"context"
	"os/signal"

	"runtime/debug"

	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

func TestLargeResultSet(t *testing.T) {
	runLargeResultSet()
}

func BenchmarkLargeResultSet(*testing.B) {
	runLargeResultSet()
}

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (dsn string, cfg *sf.Config, err error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	port := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	portStr, _ := strconv.Atoi(port)
	cfg = &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Host:     host,
		Port:     portStr,
		Protocol: protocol,
	}

	dsn, err = sf.DSN(cfg)
	return dsn, cfg, err
}

func runLargeResultSet() {
	if !flag.Parsed() {
		// enable glog for Go Snowflake Driver
		flag.Parse()
	}

	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	defer db.Close()
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}

	query := `select * from
	  (select 0 a union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) A,
	  (select 0 b union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) B,
	  (select 0 c union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) C,
	  (select 0 d union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) E,
	  (select 0 e union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) F,
	  (select 0 f union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) G,
	  (select 0 f union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) H`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v1 int
	var v2 int
	var v3 int
	var v4 int
	var v5 int
	var v6 int
	var v7 int
	counter := 0
	for rows.Next() {
		err := rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if counter%1000000 == 0 {
			debug.FreeOSMemory()
		}
		counter++
	}
}
