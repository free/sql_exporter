// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"net/url"
	"testing"
	"time"
)

func TestUnitPostAuth(t *testing.T) {
	sr := &snowflakeRestful{
		Token:    "token",
		FuncPost: postTestAfterRenew,
	}
	var err error
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppForbiddenError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppUnexpectedError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
}

func postAuthFailServiceIssue(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrCodeServiceUnavailable,
	}
}

func postAuthFailWrongAccount(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrCodeFailedToConnect,
	}
}

func postAuthFailUnknown(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrFailedToAuth,
	}
}

func postAuthSuccessWithErrorCode(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    "98765",
		Message: "wrong!",
	}, nil
}

func postAuthSuccessWithInvalidErrorCode(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    "abcdef",
		Message: "wrong!",
	}, nil
}

func postAuthSuccess(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckSAMLResponse(_ *snowflakeRestful, _ *url.Values, _ map[string]string, jsonBody []byte, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.RawSAMLResponse == "" {
		return nil, errors.New("SAML response is empty")
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

// Checks that the request body generated when authenticating with OAuth
// contains all the necessary values.
func postAuthCheckOAuth(
	_ *snowflakeRestful,
	_ *url.Values, _ map[string]string,
	jsonBody []byte,
	_ time.Duration) (*authResponse, error) {
	var ar authRequest
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Authenticator != AuthTypeOAuth.String() {
		return nil, errors.New("Authenticator is not OAUTH")
	}
	if ar.Data.Token == "" {
		return nil, errors.New("Token is empty")
	}
	if ar.Data.LoginName == "" {
		return nil, errors.New("Login name is empty")
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckPasscode(_ *snowflakeRestful, _ *url.Values, _ map[string]string, jsonBody []byte, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Passcode != "987654321" || ar.Data.ExtAuthnDuoMethod != "passcode" {
		return nil, fmt.Errorf("passcode didn't match. expected: 987654321, got: %v, duo: %v", ar.Data.Passcode, ar.Data.ExtAuthnDuoMethod)
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckPasscodeInPassword(_ *snowflakeRestful, _ *url.Values, _ map[string]string, jsonBody []byte, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Passcode != "" || ar.Data.ExtAuthnDuoMethod != "passcode" {
		return nil, fmt.Errorf("passcode must be empty, got: %v, duo: %v", ar.Data.Passcode, ar.Data.ExtAuthnDuoMethod)
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

// JWT token validate callback function to check the JWT token
// It uses the public key paired with the testPrivKey
func postAuthCheckJWTToken(_ *snowflakeRestful, _ *url.Values, _ map[string]string, jsonBody []byte, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Authenticator != AuthTypeJwt.String() {
		return nil, errors.New("Authenticator is not JWT")
	}

	tokenString := ar.Data.Token

	// Validate token
	_, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Don't forget to validate the alg is what you expect:
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return testPrivKey.Public(), nil
	})
	if err != nil {
		return nil, err
	}

	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func getDefaultSnowflakeConn() *snowflakeConn {
	cfg := Config{
		Account:            "a",
		User:               "u",
		Password:           "p",
		Database:           "d",
		Schema:             "s",
		Warehouse:          "w",
		Role:               "r",
		Region:             "",
		Params:             make(map[string]*string),
		PasscodeInPassword: false,
		Passcode:           "",
		Application:        "testapp",
	}
	sr := &snowflakeRestful{}
	sc := &snowflakeConn{
		rest: sr,
		cfg:  &cfg,
	}
	return sc
}

func TestUnitAuthenticate(t *testing.T) {
	var err error
	var driverErr *SnowflakeError
	var ok bool

	sc := getDefaultSnowflakeConn()
	sr := &snowflakeRestful{
		FuncPostAuth: postAuthFailServiceIssue,
	}
	sc.rest = sr

	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrCodeServiceUnavailable {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailWrongAccount
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrCodeFailedToConnect {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailUnknown
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFailedToAuth {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthSuccessWithErrorCode
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != 98765 {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthSuccessWithInvalidErrorCode
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuth = postAuthSuccess
	var resp *authResponseMain
	resp, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to auth. err: %v", err)
	}
	if resp.SessionInfo.DatabaseName != "dbn" {
		t.Fatalf("failed to get response from auth")
	}
}

func TestUnitAuthenticateSaml(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth: postAuthCheckSAMLResponse,
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeOkta
	sc.cfg.OktaURL = &url.URL{
		Scheme: "https",
		Host:   "blah.okta.com",
	}
	sc.rest = sr
	_, err = authenticate(sc, []byte("HTML data in bytes from"), []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

// Unit test for OAuth.
func TestUnitAuthenticateOAuth(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth: postAuthCheckOAuth,
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Token = "oauthToken"
	sc.cfg.Authenticator = AuthTypeOAuth
	sc.rest = sr
	_, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

func TestUnitAuthenticatePasscode(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth: postAuthCheckPasscode,
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Passcode = "987654321"
	sc.rest = sr

	_, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
	sr.FuncPostAuth = postAuthCheckPasscodeInPassword
	sc.rest = sr
	sc.cfg.PasscodeInPassword = true
	_, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

// Test JWT function in the local environment against the validation function in go
func TestUnitAuthenticateJWT(t *testing.T) {
	var err error

	sr := &snowflakeRestful{
		FuncPostAuth: postAuthCheckJWTToken,
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeJwt
	sc.cfg.JWTExpireTimeout = defaultJWTTimeout
	sc.cfg.PrivateKey = testPrivKey
	sc.rest = sr

	// A valid JWT token should pass
	_, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	// An invalid JWT token should not pass
	invalidPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	sc.cfg.PrivateKey = invalidPrivateKey
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatalf("invalid token passed")
	}
}
