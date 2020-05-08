package gosnowflake

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestBadChunkData(t *testing.T) {
	testDecodeErr(t, "")
	testDecodeErr(t, "null")
	testDecodeErr(t, "42")
	testDecodeErr(t, "\"null\"")
	testDecodeErr(t, "{}")

	testDecodeErr(t, "[[]")
	testDecodeErr(t, "[null]")
	testDecodeErr(t, `[[hello world]]`)

	testDecodeErr(t, `[[""hello world""]]`)
	testDecodeErr(t, `[["\"hello world""]]`)
	testDecodeErr(t, `[[""hello world\""]]`)
	testDecodeErr(t, `[["hello world`)
	testDecodeErr(t, `[["hello world"`)
	testDecodeErr(t, `[["hello world"]`)

	testDecodeErr(t, `[["\uQQQQ"]]`)

	for b := byte(0); b < ' '; b++ {
		testDecodeErr(t, string([]byte{
			'[', '[', '"', b, '"', ']', ']',
		}))
	}
}

func TestValidChunkData(t *testing.T) {
	testDecodeOk(t, "[]")
	testDecodeOk(t, "[  ]")
	testDecodeOk(t, "[[]]")
	testDecodeOk(t, "[ [  ]   ]")
	testDecodeOk(t, "[[],[],[],[]]")
	testDecodeOk(t, "[[] , []  , [], []  ]")

	testDecodeOk(t, "[[null]]")
	testDecodeOk(t, "[[\n\t\r null]]")
	testDecodeOk(t, "[[null,null]]")
	testDecodeOk(t, "[[ null , null ]]")
	testDecodeOk(t, "[[null],[null],[null]]")
	testDecodeOk(t, "[[null],[ null  ] ,  [null]]")

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["false"]]`)
	testDecodeOk(t, `[["true"]]`)
	testDecodeOk(t, `[["42"]]`)

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["hello"]]`)
	testDecodeOk(t, `[["hello world"]]`)

	testDecodeOk(t, `[["/ ' \\ \b \t \n \f \r \""]]`)
	testDecodeOk(t, `[["❄"]]`)
	testDecodeOk(t, `[["\u2744"]]`)
	testDecodeOk(t, `[["\uFfFc"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\ufffd"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\u0000"]]`)       // yes, this is valid
	testDecodeOk(t, `[["\uD834\uDD1E"]]`) // surrogate pair
	testDecodeOk(t, `[["\uD834\u0000"]]`) // corrupt surrogate pair

	testDecodeOk(t, `[["$"]]`)      // "$"
	testDecodeOk(t, `[["\u0024"]]`) // "$"

	testDecodeOk(t, `[["\uC2A2"]]`) // "¢"
	testDecodeOk(t, `[["¢"]]`)      // "¢"

	testDecodeOk(t, `[["\u00E2\u82AC"]]`) // "€"
	testDecodeOk(t, `[["€"]]`)            // "€"

	testDecodeOk(t, `[["\uF090\u8D88"]]`) // "𐍈"
	testDecodeOk(t, `[["𐍈"]]`)            // "𐍈"
}

func TestSmallBufferChunkData(t *testing.T) {
	r := strings.NewReader(`[
	  [null,"hello world"],
	  ["foo bar", null],
	  [null, null] ,
	  ["foo bar",   "hello world" ]
	]`)

	lcd := largeChunkDecoder{
		r, 0, 0,
		0, 0,
		make([]byte, 1),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	if _, err := lcd.decode(); err != nil {
		t.Fatalf("failed with small buffer: %s", err)
	}
}

func TestEnsureBytes(t *testing.T) {
	// the content here doesn't matter
	r := strings.NewReader("0123456789")

	lcd := largeChunkDecoder{
		r, 0, 0,
		3, 8189,
		make([]byte, 8192),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	lcd.ensureBytes(4)

	// we expect the new remainder to be 3 + 10 (length of r)
	if lcd.rem != 13 {
		t.Fatalf("buffer was not refilled correctly")
	}
}

func testDecodeOk(t *testing.T, s string) {
	var rows [][]*string
	if err := json.Unmarshal([]byte(s), &rows); err != nil {
		t.Fatalf("test case is not valid json / [][]*string: %s", s)
	}

	// NOTE we parse and stringify the expected result to
	// remove superficial differences, like whitespace
	expect, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("unreachable: %s", err)
	}

	rows, err = decodeLargeChunk(strings.NewReader(s), 0, 0)
	if err != nil {
		t.Fatalf("expected decode to succeed: %s", err)
	}

	actual, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("json marshal failed: %s", err)
	}
	if string(actual) != string(expect) {
		t.Fatalf(`
		result did not match expected result
		  expect=%s
		   bytes=(%v)

		  acutal=%s
		   bytes=(%v)`,
			string(expect), expect,
			string(actual), actual,
		)
	}
}

func testDecodeErr(t *testing.T, s string) {
	_, err := decodeLargeChunk(strings.NewReader(s), 0, 0)
	if err == nil {
		t.Fatalf("expected decode to fail for input: %s", s)
	}
}
