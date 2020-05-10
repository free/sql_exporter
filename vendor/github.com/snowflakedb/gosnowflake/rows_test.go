// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

// test variables
var (
	rowsInChunk = 123
)

func TestRowsWithoutChunkDownloader(t *testing.T) {
	sts1 := "1"
	sts2 := "Test1"
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 10; i++ {
		cc = append(cc, []*string{&sts1, &sts2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := []execResponseChunk{}
	rows := new(snowflakeRows)
	rows.sc = nil
	rows.RowType = rt
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:                 nil,
		ctx:                context.Background(),
		CurrentChunk:       cc,
		Total:              int64(len(cc)),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "",
		FuncDownload:       nil,
		FuncDownloadHelper: nil,
	}
	rows.ChunkDownloader.start()
	// var dest []driver.Value
	dest := make([]driver.Value, 2)
	for i = 0; i < len(cc); i++ {
		err := rows.Next(dest)
		if err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		if dest[0] != sts1 {
			t.Fatalf("failed to get value. expected: %v, got: %v", sts1, dest[0])
		}
		if dest[1] != sts2 {
			t.Fatalf("failed to get value. expected: %v, got: %v", sts2, dest[1])
		}
	}
	err := rows.Next(dest)
	if err != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
	glog.V(2).Infof("dest: %v", dest)

}

func downloadChunkTest(scd *snowflakeChunkDownloader, idx int) {
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.ChunksMutex.Lock()
	scd.Chunks[idx] = d
	scd.DoneDownloadCond.Broadcast()
	scd.ChunksMutex.Unlock()
}

func TestRowsWithChunkDownloader(t *testing.T) {
	numChunks := 12
	// changed the workers
	backupMaxChunkDownloadWorkers := MaxChunkDownloadWorkers
	MaxChunkDownloadWorkers = 2
	glog.V(2).Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	rows.sc = nil
	rows.RowType = rt
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            nil,
		ctx:           context.Background(),
		CurrentChunk:  cc,
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HAHAHA",
		FuncDownload:  downloadChunkTest,
	}
	rows.ChunkDownloader.start()
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		// fmt.Printf("data: %v\n", dest)
		cnt++
	}
	if cnt != len(cc)+numChunks*rowsInChunk {
		t.Fatalf("failed to get all results. expected:%v, got:%v", len(cc)+numChunks*rowsInChunk, cnt)
	}
	glog.V(2).Infof("dest: %v", dest)
	MaxChunkDownloadWorkers = backupMaxChunkDownloadWorkers
	glog.V(2).Info("END TESTS")
}

func downloadChunkTestError(scd *snowflakeChunkDownloader, idx int) {
	// fail to download 6th and 10th chunk, and retry up to N times and success
	// NOTE: zero based index
	scd.ChunksMutex.Lock()
	defer scd.ChunksMutex.Unlock()
	if (idx == 6 || idx == 10) && scd.ChunksErrorCounter < maxChunkDownloaderErrorCounter {
		scd.ChunksError <- &chunkError{
			Index: idx,
			Error: fmt.Errorf(
				"dummy error. idx: %v, errCnt: %v", idx+1, scd.ChunksErrorCounter)}
		scd.DoneDownloadCond.Broadcast()
		return
	}
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.Chunks[idx] = d
	scd.DoneDownloadCond.Broadcast()
}

func TestRowsWithChunkDownloaderError(t *testing.T) {
	numChunks := 12
	// changed the workers
	backupMaxChunkDownloadWorkers := MaxChunkDownloadWorkers
	MaxChunkDownloadWorkers = 3
	glog.V(2).Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	rows.sc = nil
	rows.RowType = rt
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            nil,
		ctx:           context.Background(),
		CurrentChunk:  cc,
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HOHOHO",
		FuncDownload:  downloadChunkTestError,
	}
	rows.ChunkDownloader.start()
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		// fmt.Printf("data: %v\n", dest)
		cnt++
	}
	if cnt != len(cc)+numChunks*rowsInChunk {
		t.Fatalf("failed to get all results. expected:%v, got:%v", len(cc)+numChunks*rowsInChunk, cnt)
	}
	glog.V(2).Infof("dest: %v", dest)
	MaxChunkDownloadWorkers = backupMaxChunkDownloadWorkers
	glog.V(2).Info("END TESTS")
}

func downloadChunkTestErrorFail(scd *snowflakeChunkDownloader, idx int) {
	// fail to download 6th and 10th chunk, and retry up to N times and fail
	// NOTE: zero based index
	scd.ChunksMutex.Lock()
	defer scd.ChunksMutex.Unlock()
	if idx == 6 && scd.ChunksErrorCounter <= maxChunkDownloaderErrorCounter {
		scd.ChunksError <- &chunkError{
			Index: idx,
			Error: fmt.Errorf(
				"dummy error. idx: %v, errCnt: %v", idx+1, scd.ChunksErrorCounter)}
		scd.DoneDownloadCond.Broadcast()
		return
	}
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.Chunks[idx] = d
	scd.DoneDownloadCond.Broadcast()
}

func TestRowsWithChunkDownloaderErrorFail(t *testing.T) {
	numChunks := 12
	// changed the workers
	glog.V(2).Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	rows.sc = nil
	rows.RowType = rt
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            nil,
		ctx:           context.Background(),
		CurrentChunk:  cc,
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HOHOHO",
		FuncDownload:  downloadChunkTestErrorFail,
	}
	rows.ChunkDownloader.start()
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			glog.V(2).Infof(
				"failure was expected by the number of rows is wrong. expected: %v, got: %v", 715, cnt)
			break
		}
		// fmt.Printf("data: %v\n", dest)
		cnt++
	}
}

func getChunkTestInvalidResponseBody(_ context.Context, _ *snowflakeChunkDownloader, _ string, _ map[string]string, _ time.Duration) (
	*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func TestDownloadChunkInvalidResponseBody(t *testing.T) {
	numChunks := 2
	cm := make([]execResponseChunk, 0)
	for i := 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf(
			"dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	scd := &snowflakeChunkDownloader{
		sc: &snowflakeConn{
			rest: &snowflakeRestful{RequestTimeout: defaultRequestTimeout},
		},
		ctx:                context.Background(),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "HOHOHO",
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunkTestInvalidResponseBody,
	}
	scd.ChunksMutex = &sync.Mutex{}
	scd.DoneDownloadCond = sync.NewCond(scd.ChunksMutex)
	scd.Chunks = make(map[int][][]*string)
	scd.ChunksError = make(chan *chunkError, 1)
	scd.FuncDownload(scd, 1)
	select {
	case errc := <-scd.ChunksError:
		if errc.Index != 1 {
			t.Fatalf("the error should have caused with chunk idx: %v", errc.Index)
		}
	default:
		t.Fatal("should have caused an error and queued in scd.ChunksError")
	}
}

func getChunkTestErrorStatus(_ context.Context, _ *snowflakeChunkDownloader, _ string, _ map[string]string, _ time.Duration) (
	*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func TestDownloadChunkErrorStatus(t *testing.T) {
	numChunks := 2
	cm := make([]execResponseChunk, 0)
	for i := 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf(
			"dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	scd := &snowflakeChunkDownloader{
		sc: &snowflakeConn{
			rest: &snowflakeRestful{RequestTimeout: defaultRequestTimeout},
		},
		ctx:                context.Background(),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "HOHOHO",
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunkTestErrorStatus,
	}
	scd.ChunksMutex = &sync.Mutex{}
	scd.DoneDownloadCond = sync.NewCond(scd.ChunksMutex)
	scd.Chunks = make(map[int][][]*string)
	scd.ChunksError = make(chan *chunkError, 1)
	scd.FuncDownload(scd, 1)
	select {
	case errc := <-scd.ChunksError:
		if errc.Index != 1 {
			t.Fatalf("the error should have caused with chunk idx: %v", errc.Index)
		}
		serr, ok := errc.Error.(*SnowflakeError)
		if !ok {
			t.Fatalf("should have been snowflake error. err: %v", errc.Error)
		}
		if serr.Number != ErrFailedToGetChunk {
			t.Fatalf("message error code is not correct. msg: %v", serr.Number)
		}
	default:
		t.Fatal("should have caused an error and queued in scd.ChunksError")
	}
}
