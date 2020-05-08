// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

type tcDataTypeMode struct {
	tp    driver.Value
	tmode string
	err   error
}

func TestDataTypeMode(t *testing.T) {
	var testcases = []tcDataTypeMode{
		{tp: DataTypeTimestampLtz, tmode: "TIMESTAMP_LTZ", err: nil},
		{tp: DataTypeTimestampNtz, tmode: "TIMESTAMP_NTZ", err: nil},
		{tp: DataTypeTimestampTz, tmode: "TIMESTAMP_TZ", err: nil},
		{tp: DataTypeDate, tmode: "DATE", err: nil},
		{tp: DataTypeTime, tmode: "TIME", err: nil},
		{tp: DataTypeBinary, tmode: "BINARY", err: nil},
		{tp: DataTypeFixed, tmode: "FIXED",
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: DataTypeReal, tmode: "REAL",
			err: fmt.Errorf(errMsgInvalidByteArray, DataTypeFixed)},
		{tp: 123, tmode: "",
			err: fmt.Errorf(errMsgInvalidByteArray, 123)},
	}
	for _, ts := range testcases {
		tmode, err := dataTypeMode(ts.tp)
		if ts.err == nil {
			if err != nil {
				t.Errorf("failed to get datatype mode: %v", err)
			}
			if tmode != ts.tmode {
				t.Errorf("wrong data type: %v", tmode)
			}
		} else {
			if err == nil {
				t.Errorf("should raise an error: %v", ts.err)
			}
		}
	}
}
