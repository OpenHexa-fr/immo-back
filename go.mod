module github.com/openhexa-fr/immo-back

go 1.24

require (
	github.com/elastic/go-elasticsearch/v8 v8.19.0
	github.com/openhexa-fr/core v0.0.0
)

require (
	github.com/elastic/elastic-transport-go/v8 v8.7.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/otel v1.28.0 // indirect
	go.opentelemetry.io/otel/metric v1.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.28.0 // indirect
)

replace github.com/openhexa-fr/core => ../core
