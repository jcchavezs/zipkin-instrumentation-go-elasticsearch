package zipkines

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

func TestRequestSuccess(t *testing.T) {
	reporter := recorder.NewReporter()
	tracer, err := zipkin.NewTracer(reporter, zipkin.WithSampler(zipkin.AlwaysSample))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	requestBody := `{"size":25}`
	responseBody := `{"_shards":{"total":6,"successful":6,"skipped":0,"failed":0},"hits":{"total":274}}`
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		reqBody, _ := ioutil.ReadAll(req.Body)
		if want, have := requestBody, string(reqBody); want != have {
			t.Errorf("unexpected query; want %q, have %q", want, have)
		}
		rw.WriteHeader(202)
		rw.Write([]byte(responseBody))
	}))
	defer srv.Close()

	transport := NewTransport(tracer, WithTagTotalHits(), WithTagTotalShards(), WithTagQuery())
	req, err := http.NewRequest("GET", srv.URL+"/_search", bytes.NewBufferString(requestBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	res, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actualBody, _ := ioutil.ReadAll(res.Body)
	if want, have := len(responseBody), len(actualBody); want != have {
		t.Errorf("unexpected response size; want %d, have %d", want, have)
	}

	spans := reporter.Flush()
	if want, have := 1, len(spans); want != have {
		t.Errorf("unexpected spans number; want %d, have %d", want, have)
	}
}
