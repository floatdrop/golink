// Separate module, like benchmarks/: the programs the README embeds, checked
// by CI. It builds against the working tree, so the README always shows code
// that compiles with the grpcproc next to it.
module github.com/floatdrop/grpcproc/examples

go 1.27.1

replace (
	github.com/floatdrop/grpcproc => ../
	github.com/floatdrop/grpcproc/cron => ../cron
	github.com/floatdrop/grpcproc/leader => ../leader
)

require (
	github.com/floatdrop/grpcproc v0.0.6-0.20260928122334-e8395f778226
	github.com/floatdrop/grpcproc/cron v0.0.0
	github.com/floatdrop/grpcproc/leader v0.0.0
	golang.yandex/di v0.18.0
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
)
