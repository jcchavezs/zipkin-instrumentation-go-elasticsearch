package zipkines

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
)

type successHitsNShardsResponse struct {
	successHitsResponse
	successShardsResponse
}

type successHitsResponse struct {
	Hits struct {
		Total int `json:"total"`
	} `json:"hits"`
}

type successShardsResponse struct {
	Shards struct {
		Total int `json:"total"`
	} `json:"_shards"`
}

type errorResponse struct {
	Type string `json:"type"`
}

type TraceOpts struct {
	whitelistQueryParams []string
	tagQuery             bool
	tagErrorType         bool
	tagTotalHits         bool
	tagTotalShards       bool
}

type transport struct {
	parent http.RoundTripper
	tracer *zipkin.Tracer
	logger *log.Logger
	opts   TraceOpts
}

func (r *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	span, _ := r.tracer.StartSpanFromContext(req.Context(), "es/"+req.Method, zipkin.Kind(model.Client))
	if span == nil {
		return r.parent.RoundTrip(req)
	}
	defer span.Finish()

	zipkin.TagHTTPMethod.Set(span, req.Method)
	zipkin.TagHTTPPath.Set(span, req.URL.Path)

	if len(r.opts.whitelistQueryParams) > 0 {
		params := req.URL.Query()
		for _, key := range r.opts.whitelistQueryParams {
			if val := params.Get(key); val != "" {
				span.Tag("es.query_params."+key, val)
			}
		}
	}

	if req.Method == "GET" || req.Method == "POST" {
		pieces := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
		if pieces[0] == "_tasks" {
			span.SetName("es/_tasks")
		} else if len(pieces) > 0 && pieces[len(pieces)-1][:1] == "_" {
			span.SetName("es/" + pieces[len(pieces)-1])
		}
	}

	if r.opts.tagQuery && req.Method != "GET" && req.Body != nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			r.logger.Printf("failed to read the request body to tag the query: %v", err)
			io.Copy(ioutil.Discard, req.Body)
			return nil, err
		}
		defer req.Body.Close()
		req.Body = ioutil.NopCloser(bytes.NewBuffer(body))

		if len(body) > 0 {
			span.Tag("es.query", string(body))
		}
	}

	res, rtErr := r.parent.RoundTrip(req)
	if rtErr != nil {
		zipkin.TagError.Set(span, rtErr.Error())
		return nil, rtErr
	}
	zipkin.TagHTTPStatusCode.Set(span, fmt.Sprintf("%d", res.StatusCode))

	if res.StatusCode < 200 || res.StatusCode > 299 {
		if r.opts.tagErrorType {
			resBody, err := ioutil.ReadAll(res.Body)
			if err != nil {
				r.logger.Printf("failed to read the response body to tag the error: %v", err)
				io.Copy(ioutil.Discard, res.Body)
				return nil, err
			}
			defer res.Body.Close()

			resErr := errorResponse{}
			if err := json.Unmarshal(resBody, &resErr); err != nil {
				return nil, err
			}
			zipkin.TagError.Set(span, resErr.Type)
			res.Body = ioutil.NopCloser(bytes.NewBuffer(resBody))
		} else {
			zipkin.TagError.Set(span, fmt.Sprintf("%d", res.StatusCode))
		}

		return res, rtErr
	}

	var resBody []byte
	var err error
	if r.opts.tagTotalHits || r.opts.tagTotalShards {
		resBody, err = ioutil.ReadAll(res.Body)
		if err != nil {
			r.logger.Printf("failed to read the response body to tag the response values: %v", err)
			io.Copy(ioutil.Discard, res.Body)
			return nil, err
		}
		defer res.Body.Close()
		res.Body = ioutil.NopCloser(bytes.NewBuffer(resBody))
	}

	if r.opts.tagTotalHits && r.opts.tagTotalShards {
		sRes := successHitsNShardsResponse{}
		if err := json.Unmarshal(resBody, &sRes); err != nil {
			return res, err
		}

		if sRes.Shards.Total > 0 {
			span.Tag("es.shards.total", fmt.Sprintf("%d", sRes.Shards.Total))
		}
		if sRes.Hits.Total > 0 {
			span.Tag("es.hits.total", fmt.Sprintf("%d", sRes.Hits.Total))
		}
	} else if r.opts.tagTotalHits {
		sRes := successHitsResponse{}
		if err := json.Unmarshal(resBody, &sRes); err != nil {
			return res, err
		}

		if sRes.Hits.Total > 0 {
			span.Tag("es.hits.total", fmt.Sprintf("%d", sRes.Hits.Total))
		}
	} else if r.opts.tagTotalShards {
		sRes := successShardsResponse{}
		if err := json.Unmarshal(resBody, &sRes); err != nil {
			return res, err
		}

		if sRes.Shards.Total > 0 {
			span.Tag("es.shards.total", fmt.Sprintf("%d", sRes.Shards.Total))
		}
	}

	return res, nil
}

type TraceOpt func(r *transport)

// RoundTripper allows to inject a `http.RoundTripper` to be wrapped but it should
// never be used with a traced transport, otherwise traces will be duplicated.
func RoundTripper(rt http.RoundTripper) TraceOpt {
	return func(r *transport) {
		r.parent = rt
	}
}

// WithLogger allows to pass a `log.Logger` into the transport
func WithLogger(l *log.Logger) TraceOpt {
	return func(r *transport) {
		r.logger = l
	}
}

// WithWhitelistQueryParams allows to pass the whitelist of query parameters
// that should be recorded in a ES query, e.g. "_routing"
func WithWhitelistQueryParams(l ...string) TraceOpt {
	return func(r *transport) {
		r.opts.whitelistQueryParams = l
	}
}

// WithTagQuery tags the query sent to ES in non GET requests.
func WithTagQuery() TraceOpt {
	return func(r *transport) {
		r.opts.tagQuery = true
	}
}

// WithTagTotalHits tags the total hits in a successful query response.
func WithTagTotalHits() TraceOpt {
	return func(r *transport) {
		r.opts.tagTotalHits = true
	}
}

// WithTagTotalShards tags the total shards being queried in a successful
// query response.
func WithTagTotalShards() TraceOpt {
	return func(r *transport) {
		r.opts.tagTotalShards = true
	}
}

// NewTransport returns a transport instance including tracing for ES calls
func NewTransport(tracer *zipkin.Tracer, opts ...TraceOpt) http.RoundTripper {
	t := &transport{
		tracer: tracer,
		parent: http.DefaultTransport,
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}
