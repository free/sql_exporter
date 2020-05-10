// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.
package gosnowflake

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func postTestError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postTestSuccessButInvalidJSON(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppForbiddenError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppUnexpectedError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInsufficientStorage,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    sessionExpiredCode,
		Success: true,
	}

	ba, err := json.Marshal(er)
	glog.V(2).Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func postTestAfterRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "",
		Success: true,
	}

	ba, err := json.Marshal(er)
	glog.V(2).Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func TestUnitPostQueryHelperError(t *testing.T) {
	sr := &snowflakeRestful{
		Token:    "token",
		FuncPost: postTestError,
	}
	var err error
	var requestID uuid.UUID
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, &requestID)
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestAppBadGatewayError
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, &requestID)
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, &requestID)
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func renewSessionTest(_ context.Context, _ *snowflakeRestful) error {
	return nil
}

func renewSessionTestError(_ context.Context, _ *snowflakeRestful) error {
	return errors.New("failed to renew session in tests")
}

func TestUnitPostQueryHelperRenewSession(t *testing.T) {
	var err error
	orgRequestID := uuid.New()
	postQueryTest := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID *uuid.UUID) (*execResponse, error) {
		// ensure the same requestID is used after the session token is renewed.
		if requestID.String() != orgRequestID.String() {
			t.Fatal("requestID doesn't match")
		}
		dd := &execResponseData{}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "0",
			Success: true,
		}, nil
	}
	sr := &snowflakeRestful{
		Token:            "token",
		FuncPost:         postTestRenew,
		FuncPostQuery:    postQueryTest,
		FuncRenewSession: renewSessionTest,
	}

	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, &orgRequestID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncRenewSession = renewSessionTestError
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, &orgRequestID)
	if err == nil {
		t.Fatal("should have failed to renew session")
	}
}

func TestUnitRenewRestfulSession(t *testing.T) {
	sr := &snowflakeRestful{
		MasterToken: "mtoken",
		Token:       "token",
		FuncPost:    postTestAfterRenew,
	}
	err := renewRestfulSession(context.Background(), sr)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = renewRestfulSession(context.Background(), sr)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = renewRestfulSession(context.Background(), sr)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = renewRestfulSession(context.Background(), sr)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
}

func TestUnitCloseSession(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestAfterRenew,
	}
	err := closeSession(sr)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = closeSession(sr)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = closeSession(sr)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = closeSession(sr)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
}

func TestUnitCancelQuery(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestAfterRenew,
	}
	var requestID uuid.UUID
	requestID = uuid.New()
	err := cancelQuery(sr, &requestID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	requestID = uuid.New()
	err = cancelQuery(sr, &requestID)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestAppBadGatewayError
	requestID = uuid.New()
	err = cancelQuery(sr, &requestID)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	requestID = uuid.New()
	err = cancelQuery(sr, &requestID)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
}
