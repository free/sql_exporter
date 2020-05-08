package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *sf.Config, error) {
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
	host := env("SNOWFLAKE_TEST_HOST", false)
	port := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	portStr, _ := strconv.Atoi(port)
	cfg := &sf.Config{
		Authenticator: sf.AuthTypeExternalBrowser,
		Account:       account,
		User:          user,
		Host:          host,
		Port:          portStr,
		Protocol:      protocol,
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

// A simple test program for authenticating with an external browser.
// In order for this to work, SSO needs to be set up on Snowflake as per:
// https://docs.snowflake.net/manuals/user-guide/admin-security-fed-auth-configure-snowflake.html
func main() {
	if !flag.Parsed() {
		// enable glog for Go Snowflake Driver
		flag.Parse()
	}
	dsn, cfg, err := getDSN()

	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	// The external browser flow should start with the call to Open
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()
	query := "SELECT 1"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	for rows.Next() {
		err := rows.Scan(&v)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if v != 1 {
			log.Fatalf("failed to get 1. got: %v", v)
		}
		fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!", query)
	}
}
