// Separate module, so grpcproc itself depends on grpc and protobuf only:
// OpenTelemetry is a dependency of this adapter alone. It requires a
// published grpcproc rather than a replace, because a replace is ignored by
// whoever imports this module; bumping the requirement below is how this
// adapter picks up a library change. For local work on both at once, use a
// go.work (ignored by git): go work init . ./otel
module github.com/floatdrop/grpcproc/otel

go 1.27.1

require (
	github.com/floatdrop/grpcproc v0.0.4
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/metric v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
)
