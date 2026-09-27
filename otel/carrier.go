package grpcprocotel

import (
	"maps"
	"slices"

	"go.opentelemetry.io/otel/propagation"

	"github.com/floatdrop/grpcproc"
)

// Carrier adapts grpcproc.Metadata to OpenTelemetry propagation, the way
// propagation.HeaderCarrier adapts HTTP headers.
type Carrier grpcproc.Metadata

var _ propagation.TextMapCarrier = Carrier(nil)

func (c Carrier) Get(key string) string { return c[key] }
func (c Carrier) Set(key, value string) { c[key] = value }
func (c Carrier) Keys() []string        { return slices.Sorted(maps.Keys(c)) }
