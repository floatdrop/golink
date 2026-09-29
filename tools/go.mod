// Separate module, so grpcproc itself carries no CLI or MCP dependencies. It
// requires a published grpcproc rather than a replace, because a replace is
// ignored by whoever installs from this module. For local work on both at
// once, use a go.work (ignored by git): go work init . ./tools
module github.com/floatdrop/grpcproc/tools

go 1.27.1

require (
	github.com/floatdrop/grpcproc v0.3.0
	github.com/floatdrop/grpcproc/cron v0.3.0
	github.com/floatdrop/grpcproc/leader v0.3.0
	github.com/modelcontextprotocol/go-sdk v1.8.0
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
)
