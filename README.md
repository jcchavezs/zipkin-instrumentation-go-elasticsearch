# Zipkin instrumentation for go-elasticsearch

This library provides instrumentation for go-elasticsearch library

## Getting started

```
go get github.com/jcchavezs/zipkin-instrumentation-go-elasticsearch
```

## Example

func main() {
	cfg := elasticsearch.Config{
        Transport: eszipkin.NewTransport()
    }
	es, _ := elasticsearch.NewClient(cfg)
	log.Println(elasticsearch.Version)
	log.Println(es.Info())
}
