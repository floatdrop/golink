// Package golinkotel records golink activity with OpenTelemetry: a span for
// every send, call and handled message, chained across processes and nodes
// through golink.Metadata, and metrics keyed by process label.
//
//	h, err := golinkotel.New()                 // global providers and propagator
//	node, err := golink.NewNode(golink.Config{…, Hooks: h})
//	reg, err := h.Observe(node)                // gauges from snapshots
//	defer reg.Unregister()
//
// Inside a handler, h.Extract(ctx, m.Metadata) is a context whose span is the
// one handling m, for the handler's own spans (a database call, an HTTP
// request).
package golinkotel

import (
	"context"
	"errors"
	"maps"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
)

// ScopeName is the instrumentation scope of the meter and tracer.
const ScopeName = "github.com/floatdrop/golink/otel"

// Attribute keys. Metrics never carry a PID: the label is the unit of
// aggregation, and everything else is bounded.
const (
	AttrLabel       = attribute.Key("golink.label")
	AttrCall        = attribute.Key("golink.call")
	AttrRemote      = attribute.Key("golink.remote")
	AttrReason      = attribute.Key("golink.reason")
	AttrMessageType = attribute.Key("golink.message.type")
	AttrPeer        = attribute.Key("golink.peer")
	AttrDirection   = attribute.Key("golink.direction")
	AttrPID         = attribute.Key("golink.pid") // spans only
	AttrErrorType   = attribute.Key("error.type")
)

// Option configures New.
type Option func(*config)

type config struct {
	meters  metric.MeterProvider
	tracers trace.TracerProvider
	prop    propagation.TextMapPropagator
}

// WithMeterProvider records metrics with mp instead of the global provider.
func WithMeterProvider(mp metric.MeterProvider) Option { return func(c *config) { c.meters = mp } }

// WithTracerProvider records spans with tp instead of the global provider.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(c *config) { c.tracers = tp } }

// WithPropagator carries trace context with p instead of the global propagator.
func WithPropagator(p propagation.TextMapPropagator) Option { return func(c *config) { c.prop = p } }

// Hooks implements golink.Hooks with OpenTelemetry. Combine it with other
// hooks through golink.JoinHooks.
type Hooks struct {
	golink.NopHooks
	meter  metric.Meter
	tracer trace.Tracer
	prop   propagation.TextMapPropagator

	sent, received, spawned, exited, deadLetters, linksUp, linksDown metric.Int64Counter
	mailboxWait, handling, callDuration                              metric.Float64Histogram
}

var _ golink.Hooks = (*Hooks)(nil)

// New creates the instruments. With no options it uses the global meter and
// tracer providers and the global propagator, read once, now.
func New(opts ...Option) (*Hooks, error) {
	c := config{meters: otel.GetMeterProvider(), tracers: otel.GetTracerProvider(), prop: otel.GetTextMapPropagator()}
	for _, o := range opts {
		o(&c)
	}
	h := &Hooks{
		meter:  c.meters.Meter(ScopeName),
		tracer: c.tracers.Tracer(ScopeName),
		prop:   c.prop,
	}
	var errs []error
	counter := func(name, desc, unit string) metric.Int64Counter {
		m, err := h.meter.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return m
	}
	seconds := func(name, desc string) metric.Float64Histogram {
		m, err := h.meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
		errs = append(errs, err)
		return m
	}
	h.sent = counter("golink.messages.sent", "Messages and calls sent, by the sender's label.", "{message}")
	h.received = counter("golink.messages.received", "Messages taken from mailboxes, by the receiver's label.", "{message}")
	h.spawned = counter("golink.processes.spawned", "Processes started, by label.", "{process}")
	h.exited = counter("golink.processes.exited", "Processes ended, by label and reason class.", "{process}")
	h.deadLetters = counter("golink.dead_letters", "Messages that could not be delivered, by reason and message type.", "{message}")
	h.linksUp = counter("golink.links.up", "Links established, by peer.", "{link}")
	h.linksDown = counter("golink.links.down", "Links lost, by peer.", "{link}")
	h.mailboxWait = seconds("golink.mailbox.wait", "How long a message waited in the mailbox.")
	h.handling = seconds("golink.process.duration", "How long a process spent on one message, until its next Receive.")
	h.callDuration = seconds("golink.call.duration", "Call latency seen by the caller, with error.type on failure.")
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return h, nil
}

// Extract returns ctx carrying the trace context in md. With a Msg's
// Metadata it is the span handling that message.
func (h *Hooks) Extract(ctx context.Context, md golink.Metadata) context.Context {
	return h.prop.Extract(ctx, Carrier(md))
}

// inject returns a copy of md with the span context of ctx added.
func (h *Hooks) inject(ctx context.Context, md golink.Metadata) golink.Metadata {
	out := make(golink.Metadata, len(md)+2)
	maps.Copy(out, md)
	h.prop.Inject(ctx, Carrier(out))
	return out
}

func typeOf(m proto.Message) string {
	if m == nil {
		return ""
	}
	return string(m.ProtoReflect().Descriptor().FullName())
}

func destination(s golink.SendInfo) string {
	if s.ToName != "" {
		return golink.Name{Node: s.To.Node, Name: s.ToName}.String()
	}
	return s.To.String()
}

// OnSend starts a producer span for a send, a client span for a call, and
// injects it into what leaves.
func (h *Hooks) OnSend(s golink.SendInfo, md golink.Metadata) (golink.Metadata, golink.Done) {
	start := time.Now()
	label := AttrLabel.String(s.FromLabel)
	h.sent.Add(context.Background(), 1, metric.WithAttributes(label, AttrCall.Bool(s.Call), AttrRemote.Bool(s.Remote)))

	op, kind := "send", trace.SpanKindProducer
	if s.Call {
		op, kind = "call", trace.SpanKindClient
	}
	typ := typeOf(s.Body)
	ctx, span := h.tracer.Start(h.Extract(context.Background(), md), op+" "+typ,
		trace.WithSpanKind(kind),
		trace.WithAttributes(
			attribute.String("messaging.system", "golink"),
			attribute.String("messaging.operation.type", op),
			attribute.String("messaging.destination.name", destination(s)),
			AttrMessageType.String(typ), label, AttrRemote.Bool(s.Remote),
			AttrPID.String(s.From.String()),
		))
	return h.inject(ctx, md), func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(AttrErrorType.String(ErrorType(err)))
		}
		span.End()
		if s.Call {
			attrs := []attribute.KeyValue{label, AttrRemote.Bool(s.Remote)}
			if err != nil {
				attrs = append(attrs, AttrErrorType.String(ErrorType(err)))
			}
			h.callDuration.Record(context.Background(), time.Since(start).Seconds(), metric.WithAttributes(attrs...))
		}
	}
}

// OnReceive starts the span that covers handling the message, a consumer
// span (a server span for a call) whose parent is the sender's span, and
// makes it what the process's own sends inherit.
func (h *Hooks) OnReceive(r golink.ReceiveInfo, md golink.Metadata) (golink.Metadata, golink.Done) {
	start := time.Now()
	label := metric.WithAttributes(AttrLabel.String(r.Label))
	h.received.Add(context.Background(), 1, label)
	h.mailboxWait.Record(context.Background(), r.Waited.Seconds(), label)

	typ, kind := typeOf(r.Body), trace.SpanKindConsumer
	if r.Down != nil {
		typ = "golink.Down"
	}
	if r.Call {
		kind = trace.SpanKindServer
	}
	ctx, span := h.tracer.Start(h.Extract(context.Background(), md), "process "+typ,
		trace.WithSpanKind(kind),
		trace.WithAttributes(
			attribute.String("messaging.system", "golink"),
			attribute.String("messaging.operation.type", "process"),
			AttrMessageType.String(typ), AttrLabel.String(r.Label),
			AttrPID.String(r.PID.String()),
			attribute.Float64("golink.mailbox.wait", r.Waited.Seconds()),
		))
	if r.Down != nil {
		span.SetAttributes(AttrReason.String(r.Down.Reason), attribute.String("golink.down.pid", r.Down.PID.String()))
	}
	return h.inject(ctx, md), func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		h.handling.Record(context.Background(), time.Since(start).Seconds(), label)
	}
}

func (h *Hooks) OnSpawn(p golink.ProcessInfo) {
	h.spawned.Add(context.Background(), 1, metric.WithAttributes(AttrLabel.String(p.Label)))
}

func (h *Hooks) OnExit(p golink.ProcessInfo, reason string) {
	h.exited.Add(context.Background(), 1, metric.WithAttributes(AttrLabel.String(p.Label), AttrReason.String(ReasonClass(reason))))
}

func (h *Hooks) OnDeadLetter(_, _ golink.PID, body proto.Message, reason string) {
	h.deadLetters.Add(context.Background(), 1, metric.WithAttributes(AttrReason.String(reason), AttrMessageType.String(typeOf(body))))
}

func (h *Hooks) OnLinkUp(peer golink.NodeID) {
	h.linksUp.Add(context.Background(), 1, metric.WithAttributes(AttrPeer.String(peer.Name)))
}

func (h *Hooks) OnLinkDown(peer golink.NodeID, _ error) {
	h.linksDown.Add(context.Background(), 1, metric.WithAttributes(AttrPeer.String(peer.Name)))
}

// ReasonClass maps an exit reason to a bounded set, for metrics: the
// well-known reasons, "panic", or "error" for anything a process returned.
func ReasonClass(reason string) string {
	switch reason {
	case golink.ReasonNormal, golink.ReasonShutdown, golink.ReasonKilled, golink.ReasonNoProc, golink.ReasonNoConnection, golink.ReasonType:
		return reason
	}
	if strings.HasPrefix(reason, "panic:") {
		return "panic"
	}
	return "error"
}

// ErrorType maps a send or call error to a bounded set, for error.type.
func ErrorType(err error) string {
	switch {
	case errors.Is(err, golink.ErrNoProc):
		return "noproc"
	case errors.Is(err, golink.ErrType):
		return "type"
	case errors.Is(err, golink.ErrNoConnection):
		return "noconnection"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	if _, ok := errors.AsType[*golink.RemoteError](err); ok {
		return "remote"
	}
	return "other"
}
