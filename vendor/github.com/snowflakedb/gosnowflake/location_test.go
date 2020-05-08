// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"errors"
	"testing"
)

type tcLocation struct {
	ss  string
	tt  string
	err error
}

func TestWithOffsetString(t *testing.T) {
	testcases := []tcLocation{
		{
			ss:  "+0700",
			tt:  "+0700",
			err: nil,
		},
		{
			ss:  "-1200",
			tt:  "-1200",
			err: nil,
		},
		{
			ss:  "+0710",
			tt:  "+0710",
			err: nil,
		},
		{
			ss: "1200",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"1200"},
			},
		},
		{
			ss: "x1200",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"x1200"},
			},
		},
		{
			ss: "+12001",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"+12001"},
			},
		},
		{
			ss: "x12001",
			tt: "",
			err: &SnowflakeError{
				Number:      ErrInvalidOffsetStr,
				Message:     errMsgInvalidOffsetStr,
				MessageArgs: []interface{}{"x12001"},
			},
		},
		{
			ss:  "-12CD",
			tt:  "",
			err: errors.New("parse int error"), // can this be more specific?
		},
		{
			ss:  "+ABCD",
			tt:  "",
			err: errors.New("parse int error"), // can this be more specific?
		},
	}
	for _, t0 := range testcases {
		loc, err := LocationWithOffsetString(t0.ss)
		if t0.err != nil {
			if t0.err != err {
				driverError1, ok1 := t0.err.(*SnowflakeError)
				driverError2, ok2 := err.(*SnowflakeError)
				if ok1 && ok2 && driverError1.Number != driverError2.Number {
					t.Fatalf("error expected: %v, got: %v", t0.err, err)
				}
			}
		} else {
			if err != nil {
				t.Fatalf("%v", err)
			}
			if t0.tt != loc.String() {
				t.Fatalf("location string didn't match. expected: %v, got: %v", t0.tt, loc)
			}
		}
	}
}
