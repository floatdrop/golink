// Separate module, so grpcproc itself does not depend on what it is compared
// with. It measures the working tree: nothing imports this module, so the
// replace below is never seen by anyone else.
module github.com/floatdrop/grpcproc/benchmarks

go 1.27.1

replace github.com/floatdrop/grpcproc => ../

require (
	github.com/anthdm/hollywood v1.0.5
	github.com/asynkron/protoactor-go v0.0.0-20260118094027-288962e52f3f
	github.com/floatdrop/grpcproc v0.0.2-0.20260927045241-731748e1246b
	github.com/tochemey/goakt/v4 v4.5.6
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/DataDog/gostackparse v0.7.0 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.28.0 // indirect
	github.com/Workiva/go-datastructures v1.1.7 // indirect
	github.com/andybalholm/brotli v1.2.4 // indirect
	github.com/asynkron/gofun v0.0.0-20220329210725-34fed760f4c2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.25.0 // indirect
	github.com/bytedance/gopkg v0.1.4 // indirect
	github.com/bytedance/sonic v1.15.4 // indirect
	github.com/bytedance/sonic/loader v0.5.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.7 // indirect
	github.com/deckarep/golang-set/v2 v2.9.0 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/flowchartsman/retry v1.2.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.4 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/memberlist v0.7.0 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/lithammer/shortuuid/v4 v4.2.0 // indirect
	github.com/lmittmann/tint v1.1.2 // indirect
	github.com/miekg/dns v1.1.73 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/orcaman/concurrent-map v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.71.0 // indirect
	github.com/prometheus/otlptranslator v0.0.2 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	github.com/reugn/go-quartz v0.15.2 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/redcon v1.6.4 // indirect
	github.com/tochemey/olric v0.3.21 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/zeebo/errs v1.2.2 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	go.mongodb.org/mongo-driver v1.17.10 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.60.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/arch v0.31.0 // indirect
	golang.org/x/exp v0.0.0-20260908205506-85c1c2202aba // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260911204522-f61a6ca850bd // indirect
	storj.io/drpc v0.0.33 // indirect
)
