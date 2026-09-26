// Separate module, so golink itself does not depend on what it is compared
// with. It measures the working tree: nothing imports this module, so the
// replace below is never seen by anyone else.
module github.com/floatdrop/golink/benchmarks

go 1.27.1

replace github.com/floatdrop/golink => ../

require (
	github.com/anthdm/hollywood v1.0.5
	github.com/floatdrop/golink v0.0.0
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/DataDog/gostackparse v0.7.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/zeebo/errs v1.2.2 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	storj.io/drpc v0.0.33 // indirect
)
