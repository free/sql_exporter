// Example: Verify SSL/TLS certificate with OCSP revocation check
package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	var targetURL = flag.String("url", "", "target host name, e.g., https://myaccount.snowflakecomputing.com/")
	flag.Parse()
	if *targetURL == "" {
		flag.Usage()
		os.Exit(2)
	}
	c := &http.Client{
		Transport: sf.SnowflakeTransportTest,
		Timeout:   30 * time.Second,
	}
	req, err := http.NewRequest("GET", *targetURL, bytes.NewReader(nil))
	if err != nil {
		log.Fatalf("fail to create a request. err: %v", err)
	}
	res, err := c.Do(req)
	if err != nil {
		log.Fatalf("failed to GET contents. err: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Fatalf("failed to get 200: %v", res.StatusCode)
	}
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("failed to read content body for %v", targetURL)
	}
	log.Println("SUCCESS. Certificate Revocation Check has been completed.")
}
