// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"strings"
	"testing"
)

func TestErrorMessage(t *testing.T) {
	e := &SnowflakeError{
		Number:  1,
		Message: "test message",
	}
	if !strings.Contains(e.Error(), "000001") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
	e = &SnowflakeError{
		Number:      1,
		Message:     "test message: %v, %v",
		MessageArgs: []interface{}{"C1", "C2"},
	}
	if !strings.Contains(e.Error(), "000001") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "C1") {
		t.Errorf("failed to format error. %v", e)
	}
	e = &SnowflakeError{
		Number:      1,
		Message:     "test message: %v, %v",
		MessageArgs: []interface{}{"C1", "C2"},
		SQLState:    "01112",
	}
	if !strings.Contains(e.Error(), "000001") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "C1") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "01112") {
		t.Errorf("failed to format error. %v", e)
	}
	e = &SnowflakeError{
		Number:      1,
		Message:     "test message: %v, %v",
		MessageArgs: []interface{}{"C1", "C2"},
		SQLState:    "01112",
		QueryID:     "abcdef-abcdef-abcdef",
	}
	if !strings.Contains(e.Error(), "000001") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "test message") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "C1") {
		t.Errorf("failed to format error. %v", e)
	}
	if !strings.Contains(e.Error(), "01112") {
		t.Errorf("failed to format error. %v", e)
	}
	if strings.Contains(e.Error(), "abcdef-abcdef-abcdef") {
		// no quid
		t.Errorf("failed to format error. %v", e)
	}
	e.IncludeQueryID = true
	if !strings.Contains(e.Error(), "abcdef-abcdef-abcdef") {
		// no quid
		t.Errorf("failed to format error. %v", e)
	}
}
