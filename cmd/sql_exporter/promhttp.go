package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/free/sql_exporter"
	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

const (
	contentTypeHeader     = "Content-Type"
	contentLengthHeader   = "Content-Length"
	contentEncodingHeader = "Content-Encoding"
	acceptEncodingHeader  = "Accept-Encoding"
)

// ExporterHandlerFor returns an http.Handler for the provided Exporter.
func ExporterHandlerFor(exporter sql_exporter.Exporter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := contextFor(req, exporter)
		defer cancel()

		// Go through prometheus.Gatherers to sanitize and sort metrics.
		gatherer := prometheus.Gatherers{exporter.WithContext(ctx)}
		mfs, err := gatherer.Gather()
		if err != nil {
			log.Infof("Error gathering metrics: %s", err)
			if len(mfs) == 0 {
				http.Error(w, "No metrics gathered, "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		contentType := expfmt.Negotiate(req.Header)
		buf := getBuf()
		defer giveBuf(buf)
		writer, encoding := decorateWriter(req, buf)
		enc := expfmt.NewEncoder(writer, contentType)
		var errs prometheus.MultiError
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				errs = append(errs, err)
				log.Infof("Error encoding metric family %q: %s", mf.GetName(), err)
			}
		}
		if closer, ok := writer.(io.Closer); ok {
			closer.Close()
		}
		if errs.MaybeUnwrap() != nil && buf.Len() == 0 {
			http.Error(w, "No metrics encoded, "+errs.Error(), http.StatusInternalServerError)
			return
		}
		header := w.Header()
		header.Set(contentTypeHeader, string(contentType))
		header.Set(contentLengthHeader, fmt.Sprint(buf.Len()))
		if encoding != "" {
			header.Set(contentEncodingHeader, encoding)
		}
		w.Write(buf.Bytes())
	})
}

func contextFor(req *http.Request, exporter sql_exporter.Exporter) (context.Context, context.CancelFunc) {
	timeout := time.Duration(0)
	configTimeout := time.Duration(exporter.Config().Globals.ScrapeTimeout)
	// If a timeout is provided in the Prometheus header, use it.
	if v := req.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); v != "" {
		timeoutSeconds, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Errorf("Failed to parse timeout (`%s`) from Prometheus header: %s", v, err)
		} else {
			timeout = time.Duration(timeoutSeconds * float64(time.Second))

			// Subtract the timeout offset, unless the result would be negative or zero.
			timeoutOffset := time.Duration(exporter.Config().Globals.TimeoutOffset)
			if timeoutOffset > timeout {
				log.Errorf("global.scrape_timeout_offset (`%s`) is greater than Prometheus' scraping timeout (`%s`), ignoring",
					timeoutOffset, timeout)
			} else {
				timeout -= timeoutOffset
			}
		}
	}

	// If the configured scrape timeout is more restrictive, use that instead.
	if configTimeout > 0 && (timeout <= 0 || configTimeout < timeout) {
		timeout = configTimeout
	}

	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

var bufPool sync.Pool

func getBuf() *bytes.Buffer {
	buf := bufPool.Get()
	if buf == nil {
		return &bytes.Buffer{}
	}
	return buf.(*bytes.Buffer)
}

func giveBuf(buf *bytes.Buffer) {
	buf.Reset()
	bufPool.Put(buf)
}

// decorateWriter wraps a writer to handle gzip compression if requested.  It
// returns the decorated writer and the appropriate "Content-Encoding" header
// (which is empty if no compression is enabled).
func decorateWriter(request *http.Request, writer io.Writer) (w io.Writer, encoding string) {
	header := request.Header.Get(acceptEncodingHeader)
	parts := strings.Split(header, ",")
	for _, part := range parts {
		part := strings.TrimSpace(part)
		if part == "gzip" || strings.HasPrefix(part, "gzip;") {
			return gzip.NewWriter(writer), "gzip"
		}
	}
	return writer, ""
}
