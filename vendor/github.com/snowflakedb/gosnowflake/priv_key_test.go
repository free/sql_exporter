// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.
// +build go1.10

package gosnowflake

// This file contains variables or functions of test cases that we want to run for go version >= 1.10

// For compile concern, should any newly added variables or functions here must also be added with same
// name or signature but with default or empty content in the priv_key_test.go(See addParseDSNTest)

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

// helper function to generate PKCS8 encoded base64 string of a private key
func generatePKCS8StringSupress(key *rsa.PrivateKey) string {
	// Error would only be thrown when the private key type is not supported
	// We would be safe as long as we are using rsa.PrivateKey
	tmpBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	privKeyPKCS8 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS8
}

// helper function to generate PKCS1 encoded base64 string of a private key
func generatePKCS1String(key *rsa.PrivateKey) string {
	tmpBytes := x509.MarshalPKCS1PrivateKey(key)
	privKeyPKCS1 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS1
}

// helper function to set up private key for testing
func setupPrivateKey() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	privKeyPath := env("SNOWFLAKE_TEST_PRIVATE_KEY", "")
	if privKeyPath == "" {
		customPrivateKey = false
		testPrivKey, _ = rsa.GenerateKey(rand.Reader, 2048)
	} else {
		// path to the DER file
		customPrivateKey = true
		data, _ := ioutil.ReadFile(privKeyPath)
		privKey, _ := x509.ParsePKCS8PrivateKey(data)
		testPrivKey = privKey.(*rsa.PrivateKey)
	}
}

// Helper function to add encoded private key to dsn
func appendPrivateKeyString(dsn *string, key *rsa.PrivateKey) string {
	var b bytes.Buffer
	b.WriteString(*dsn)
	b.WriteString(fmt.Sprintf("&authenticator=%v", AuthTypeJwt.String()))
	b.WriteString(fmt.Sprintf("&privateKey=%s", generatePKCS8StringSupress(key)))
	return b.String()
}

// Integration test for the JWT authentication function
func TestJWTAuthentication(t *testing.T) {
	// For private key generated on the fly, we want to load the public key to the server first
	if !customPrivateKey {
		db, err := sql.Open("snowflake", dsn)
		if err != nil {
			t.Fatalf("error creating a connection object: %s", err.Error())
		}
		// Load server's public key to database
		pubKeyByte, err := x509.MarshalPKIXPublicKey(testPrivKey.Public())
		if err != nil {
			t.Fatalf("error marshaling public key: %s", err.Error())
		}
		_, err = db.Exec("USE ROLE ACCOUNTADMIN")
		if err != nil {
			t.Fatalf("error changin role: %s", err.Error())
		}
		encodedKey := base64.StdEncoding.EncodeToString(pubKeyByte)
		_, err = db.Exec(fmt.Sprintf("ALTER USER %v set rsa_public_key='%v'", user, encodedKey))
		if err != nil {
			t.Fatalf("error setting server's public key: %s", err.Error())
		}
		db.Close()
	}

	// Test that a valid private key can pass
	jwtDSN := appendPrivateKeyString(&dsn, testPrivKey)
	db, err := sql.Open("snowflake", jwtDSN)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	_, err = db.Exec("SELECT 1")
	if err != nil {
		t.Fatalf("error executing: %s", err.Error())
	}
	db.Close()

	// Test that an invalid private key cannot pass
	invalidPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwtDSN = appendPrivateKeyString(&dsn, invalidPrivateKey)
	db, _ = sql.Open("snowflake", jwtDSN)
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatalf("An invalid jwt token can pass")
	}

	db.Close()
}
