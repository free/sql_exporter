// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"math/cmplx"
	"reflect"
	"testing"
	"time"
)

type tcGoTypeToSnowflake struct {
	in    interface{}
	tmode string
	out   string
}

func TestGoTypeToSnowflake(t *testing.T) {
	testcases := []tcGoTypeToSnowflake{
		{in: int64(123), tmode: "", out: "FIXED"},
		{in: float64(234.56), tmode: "", out: "REAL"},
		{in: true, tmode: "", out: "BOOLEAN"},
		{in: "teststring", tmode: "", out: "TEXT"},
		{in: nil, tmode: "", out: "TEXT"}, // nil is taken as TEXT
		{in: DataTypeBinary, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampLtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampNtz, tmode: "", out: "CHANGE_TYPE"},
		{in: DataTypeTimestampTz, tmode: "", out: "CHANGE_TYPE"},
		{in: time.Now(), tmode: "TIMESTAMP_NTZ", out: "TIMESTAMP_NTZ"},
		{in: time.Now(), tmode: "TIMESTAMP_TZ", out: "TIMESTAMP_TZ"},
		{in: time.Now(), tmode: "TIMESTAMP_LTZ", out: "TIMESTAMP_LTZ"},
		{in: []byte{1, 2, 3}, tmode: "BINARY", out: "BINARY"},
		// negative
		{in: 123, tmode: "", out: "TEXT"},
		{in: int8(12), tmode: "", out: "TEXT"},
		{in: int32(456), tmode: "", out: "TEXT"},
		{in: uint(456), tmode: "", out: "TEXT"},
		{in: uint8(12), tmode: "", out: "TEXT"},
		{in: uint64(456), tmode: "", out: "TEXT"},
		{in: []byte{100}, tmode: "", out: "TEXT"},
	}
	for _, test := range testcases {
		a := goTypeToSnowflake(test.in, test.tmode)
		if a != test.out {
			t.Errorf("failed. in: %v, tmode: %v, expected: %v, got: %v", test.in, test.tmode, test.out, a)
		}
	}
}

type tcSnowflakeTypeToGo struct {
	in    string
	scale int64
	out   reflect.Type
}

func TestSnowflakeTypeToGo(t *testing.T) {
	testcases := []tcSnowflakeTypeToGo{
		{in: "fixed", scale: 0, out: reflect.TypeOf(int64(0))},
		{in: "fixed", scale: 2, out: reflect.TypeOf(float64(0))},
		{in: "real", scale: 0, out: reflect.TypeOf(float64(0))},
		{in: "text", scale: 0, out: reflect.TypeOf("")},
		{in: "date", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "time", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_ltz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_ntz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "timestamp_tz", scale: 0, out: reflect.TypeOf(time.Now())},
		{in: "object", scale: 0, out: reflect.TypeOf("")},
		{in: "variant", scale: 0, out: reflect.TypeOf("")},
		{in: "array", scale: 0, out: reflect.TypeOf("")},
		{in: "binary", scale: 0, out: reflect.TypeOf([]byte{})},
		{in: "boolean", scale: 0, out: reflect.TypeOf(true)},
	}
	for _, test := range testcases {
		a := snowflakeTypeToGo(test.in, test.scale)
		if a != test.out {
			t.Errorf("failed. in: %v, scale: %v, expected: %v, got: %v",
				test.in, test.scale, test.out, a)
		}
	}
}

func TestValueToString(t *testing.T) {
	v := cmplx.Sqrt(-5 + 12i) // should never happen as Go sql package must have already validated.
	_, err := valueToString(v, "")
	if err == nil {
		t.Errorf("should raise error: %v", v)
	}

	// both localTime and utcTime should yield the same unix timestamp
	localTime := time.Date(2019, 2, 6, 14, 17, 31, 123456789, time.FixedZone("-08:00", -8*3600))
	utcTime := time.Date(2019, 2, 6, 22, 17, 31, 123456789, time.UTC)
	expectedUnixTime := "1549491451123456789" // time.Unix(1549491451, 123456789).Format(time.RFC3339) == "2019-02-06T14:17:31-08:00"

	if s, err := valueToString(localTime, "TIMESTAMP_LTZ"); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}

	if s, err := valueToString(utcTime, "TIMESTAMP_LTZ"); err != nil {
		t.Error("unexpected error")
	} else if s == nil {
		t.Errorf("expected '%v', got %v", expectedUnixTime, s)
	} else if *s != expectedUnixTime {
		t.Errorf("expected '%v', got '%v'", expectedUnixTime, *s)
	}
}

func TestExtractTimestamp(t *testing.T) {
	s := "1234abcdef"
	_, _, err := extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234abc.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
	s = "1234.def"
	_, _, err = extractTimestamp(&s)
	if err == nil {
		t.Errorf("should raise error: %v", s)
	}
}

func TestStringToValue(t *testing.T) {
	var source string
	var dest driver.Value
	var err error
	var rowType *execResponseRowType
	source = "abcdefg"

	types := []string{
		"date", "time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "binary",
	}

	for _, tt := range types {
		rowType = &execResponseRowType{
			Type: tt,
		}
		err = stringToValue(&dest, *rowType, &source)
		if err == nil {
			t.Errorf("should raise error. type: %v, value:%v", tt, source)
		}
	}

	sources := []string{
		"12345K78 2020",
		"12345678 20T0",
	}

	types = []string{
		"timestamp_tz",
	}

	for _, ss := range sources {
		for _, tt := range types {
			rowType = &execResponseRowType{
				Type: tt,
			}
			err = stringToValue(&dest, *rowType, &ss)
			if err == nil {
				t.Errorf("should raise error. type: %v, value:%v", tt, source)
			}
		}
	}

	src := "1549491451.123456789"
	if err = stringToValue(&dest, execResponseRowType{Type: "timestamp_ltz"}, &src); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if ts, ok := dest.(time.Time); !ok {
		t.Errorf("expected type: 'time.Time', got '%v'", reflect.TypeOf(dest))
	} else if ts.UnixNano() != 1549491451123456789 {
		t.Errorf("expected unix timestamp: 1549491451123456789, got %v", ts.UnixNano())
	}
}
