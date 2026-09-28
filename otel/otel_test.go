package grpcprocotel_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocotel "github.com/floatdrop/grpcproc/otel"
)

type env struct {
	hooks  *grpcprocotel.Hooks
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
}

func setup(t *testing.T) env {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	h, err := grpcprocotel.New(
		grpcprocotel.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))),
		grpcprocotel.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		grpcprocotel.WithPropagator(propagation.TraceContext{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return env{hooks: h, spans: spans, reader: reader}
}

// echo replies Pong{N+1} to calls, errors on N < 0, returns "boom" on
// N == -100 and panics on N == -200.
func echo(p *grpcproc.Process[*testpb.Ping]) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		switch n := m.Body.GetN(); {
		case n == -100:
			return errors.New("boom")
		case n == -200:
			panic("kaboom")
		case n < 0:
			_ = m.Reply(nil, errors.New("negative"))
		case m.IsCall():
			_ = m.Reply(&testpb.Pong{N: n + 1}, nil)
		}
	}
}

func span(t *testing.T, spans []sdktrace.ReadOnlySpan, name, label string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range spans {
		if s.Name() != name {
			continue
		}
		for _, a := range s.Attributes() {
			if a.Key == grpcprocotel.AttrLabel && a.Value.AsString() == label {
				return s
			}
		}
	}
	var names []string
	for _, s := range spans {
		names = append(names, s.Name())
	}
	t.Fatalf("no span %q with label %q among %v", name, label, names)
	return nil
}

func TestTraceChainsAcrossProcessesAndNodes(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a", "b")
	a, b := c.Node("a"), c.Node("b")

	handlerSpan := make(chan trace.SpanContext, 1)
	sink, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		// A handler's own work nests under the span handling the message.
		handlerSpan <- trace.SpanContextFromContext(e.hooks.Extract(t.Context(), m.Metadata))
		return nil
	}, grpcproc.WithLabel("sink"))
	relay, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		return p.Send(sink, m.Body)
	}, grpcproc.WithLabel("relay"))

	if err := a.Send(t.Context(), relay, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	var fromHandler trace.SpanContext
	select {
	case fromHandler = <-handlerSpan:
	case <-time.After(5 * time.Second):
		t.Fatal("sink never ran")
	}
	// Both processes have exited, so their handling spans have ended.
	time.Sleep(50 * time.Millisecond)
	ended := e.spans.Ended()

	send1 := span(t, ended, "send grpcproc.test.v1.Ping", "")
	proc1 := span(t, ended, "process grpcproc.test.v1.Ping", "relay")
	send2 := span(t, ended, "send grpcproc.test.v1.Ping", "relay")
	proc2 := span(t, ended, "process grpcproc.test.v1.Ping", "sink")

	traceID := send1.SpanContext().TraceID()
	for _, s := range []sdktrace.ReadOnlySpan{proc1, send2, proc2} {
		if s.SpanContext().TraceID() != traceID {
			t.Fatalf("%s is in another trace", s.Name())
		}
	}
	chain := []struct{ child, parent sdktrace.ReadOnlySpan }{{proc1, send1}, {send2, proc1}, {proc2, send2}}
	for _, l := range chain {
		if l.child.Parent().SpanID() != l.parent.SpanContext().SpanID() {
			t.Errorf("%s (%s) is not a child of %s", l.child.Name(), l.child.SpanKind(), l.parent.Name())
		}
	}
	if send1.SpanKind() != trace.SpanKindProducer || proc2.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("kinds %v %v", send1.SpanKind(), proc2.SpanKind())
	}
	if fromHandler.SpanID() != proc2.SpanContext().SpanID() {
		t.Fatalf("Extract gave %v, want the handling span %v", fromHandler.SpanID(), proc2.SpanContext().SpanID())
	}
	if attr(send2, "messaging.destination.name") != sink.String() || attr(send2, "grpcproc.remote") != "true" {
		t.Fatalf("attrs %v", send2.Attributes())
	}
}

func attr(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestCallSpansAndDuration(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a")
	a := c.Node("a")
	ep, _ := a.Spawn(echo, grpcproc.WithLabel("echo"))
	caller, _ := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		if _, err := p.Call[*testpb.Pong](t.Context(), ep, &testpb.Ping{N: 1}); err != nil {
			return err
		}
		_, err := p.Call[*testpb.Pong](t.Context(), ep, &testpb.Ping{N: -1})
		if err == nil {
			return errors.New("expected an error")
		}
		return nil
	}, grpcproc.WithLabel("caller"))
	_ = caller
	time.Sleep(100 * time.Millisecond)
	c.Stop("a") // ends the echo's last handling span

	var ok, failed sdktrace.ReadOnlySpan
	for _, s := range e.spans.Ended() {
		if s.Name() == "call grpcproc.test.v1.Ping" {
			if s.Status().Code == codes.Error {
				failed = s
			} else {
				ok = s
			}
		}
	}
	if ok == nil || failed == nil {
		t.Fatal("missing call spans")
	}
	if ok.SpanKind() != trace.SpanKindClient || attr(failed, "error.type") != "remote" || len(failed.Events()) == 0 {
		t.Fatalf("ok=%v failed=%v %v", ok.SpanKind(), failed.Attributes(), failed.Events())
	}
	server := span(t, e.spans.Ended(), "process grpcproc.test.v1.Ping", "echo")
	if server.SpanKind() != trace.SpanKindServer {
		t.Fatalf("callee kind %v", server.SpanKind())
	}

	rm := collect(t, e.reader)
	if n := histCount(rm, "grpcproc.call.duration", grpcprocotel.AttrLabel.String("caller"), grpcprocotel.AttrErrorType.String("remote")); n != 1 {
		t.Fatalf("failed call durations: %d", n)
	}
	if n := histCount(rm, "grpcproc.call.duration", grpcprocotel.AttrLabel.String("caller")); n != 2 {
		t.Fatalf("all call durations: %d", n)
	}
}

func TestMetrics(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a", "b")
	a := c.Node("a")
	reg, err := e.hooks.Observe(a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()

	// Exits: normal, error and panic.
	for _, n := range []int64{0, -100, -200} {
		ep, _ := a.Spawn(echo, grpcproc.WithLabel("echo"))
		if n == 0 {
			_ = a.Exit(t.Context(), ep, grpcproc.ReasonNormal)
			continue
		}
		_ = a.Send(t.Context(), ep, &testpb.Ping{N: n})
	}
	// A dead letter of the wrong type.
	ep, _ := a.Spawn(echo, grpcproc.WithLabel("echo"))
	_ = a.SendTo(t.Context(), ep.PID(), &testpb.Pong{})
	// A backlog: a blocked process with two waiting messages.
	release := make(chan struct{})
	defer close(release)
	busy, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
			<-release
		}
	}, grpcproc.WithLabel("busy"))
	for range 3 {
		_ = a.Send(t.Context(), busy, &testpb.Ping{})
	}
	// A link to b, then its loss.
	remote, _ := c.Node("b").Spawn(echo, grpcproc.WithLabel("echo"))
	if _, err := a.Call[*testpb.Pong](t.Context(), remote, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	rm := collect(t, e.reader)
	checks := []struct {
		name  string
		attrs []attribute.KeyValue
		want  int64
	}{
		// Both nodes share these hooks: four echoes on a, one on b.
		{"grpcproc.processes.spawned", []attribute.KeyValue{grpcprocotel.AttrLabel.String("echo")}, 5},
		{"grpcproc.processes.exited", []attribute.KeyValue{grpcprocotel.AttrReason.String("normal")}, 1},
		{"grpcproc.processes.exited", []attribute.KeyValue{grpcprocotel.AttrReason.String("error")}, 1},
		{"grpcproc.processes.exited", []attribute.KeyValue{grpcprocotel.AttrReason.String("panic")}, 1},
		{"grpcproc.dead_letters", []attribute.KeyValue{grpcprocotel.AttrReason.String("type"), grpcprocotel.AttrMessageType.String("grpcproc.test.v1.Pong")}, 1},
		{"grpcproc.messages.sent", []attribute.KeyValue{grpcprocotel.AttrLabel.String(""), grpcprocotel.AttrCall.Bool(true), grpcprocotel.AttrRemote.Bool(true)}, 1},
		{"grpcproc.messages.received", []attribute.KeyValue{grpcprocotel.AttrLabel.String("busy")}, 1},
		// a's stream to b, and b's reply stream arriving at a.
		{"grpcproc.links.up", []attribute.KeyValue{grpcprocotel.AttrPeer.String("b")}, 2},
		{"grpcproc.processes", []attribute.KeyValue{grpcprocotel.AttrLabel.String("busy")}, 1},
		{"grpcproc.mailbox.depth", []attribute.KeyValue{grpcprocotel.AttrLabel.String("busy")}, 2},
	}
	for _, ch := range checks {
		if got := intValue(rm, ch.name, ch.attrs...); got != ch.want {
			t.Errorf("%s%v = %d, want %d", ch.name, ch.attrs, got, ch.want)
		}
	}
	if got := floatValue(rm, "grpcproc.mailbox.oldest", grpcprocotel.AttrLabel.String("busy")); got <= 0 {
		t.Errorf("oldest = %v", got)
	}
	if got := intValue(rm, "grpcproc.link.messages", grpcprocotel.AttrPeer.String("b"), grpcprocotel.AttrDirection.String("out")); got == 0 {
		t.Error("no outbound link messages")
	}
	if got := intValue(rm, "grpcproc.link.bytes", grpcprocotel.AttrPeer.String("b"), grpcprocotel.AttrDirection.String("in")); got == 0 {
		t.Error("no inbound link bytes")
	}
	if histCount(rm, "grpcproc.mailbox.wait", grpcprocotel.AttrLabel.String("busy")) != 1 {
		t.Error("no mailbox wait recorded")
	}

	c.Kill("b")
	time.Sleep(20 * time.Millisecond)
	if got := intValue(collect(t, e.reader), "grpcproc.links.down", grpcprocotel.AttrPeer.String("b")); got < 1 {
		t.Errorf("links down = %d", got)
	}
}

func TestDownIsTraced(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a")
	a := c.Node("a")
	ep, _ := a.Spawn(echo)
	got := make(chan struct{})
	_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(ep)
		_ = p.Exit(ep, "stop")
		if _, err := p.Receive(); err != nil {
			return err
		}
		close(got)
		return errors.New("done") // abnormal exit ends the handling span with an error
	}, grpcproc.WithLabel("watcher"))
	<-got
	time.Sleep(20 * time.Millisecond)
	s := span(t, e.spans.Ended(), "process grpcproc.Down", "watcher")
	if attr(s, "grpcproc.reason") != "stop" || s.Status().Code != codes.Error {
		t.Fatalf("%v %v", s.Attributes(), s.Status())
	}
}

func TestExitedIsTraced(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a")
	a := c.Node("a")
	ep, _ := a.Spawn(echo)
	got := make(chan struct{})
	_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.SetTrapExit(true)
		p.Link(ep)
		_ = p.Exit(ep, "stop")
		if _, err := p.Receive(); err != nil {
			return err
		}
		close(got)
		_, err := p.Receive()
		return err
	}, grpcproc.WithLabel("linker"))
	<-got
	time.Sleep(20 * time.Millisecond)
	s := span(t, e.spans.Ended(), "process grpcproc.Exited", "linker")
	if attr(s, "grpcproc.reason") != "stop" || attr(s, "grpcproc.exited.pid") != ep.PID().String() {
		t.Fatalf("%v", s.Attributes())
	}
}

func TestClassification(t *testing.T) {
	reasons := map[string]string{
		grpcproc.ReasonNormal: "normal", grpcproc.ReasonKilled: "killed", grpcproc.ReasonShutdown: "shutdown",
		grpcproc.ReasonNoProc: "noproc", grpcproc.ReasonNoConnection: "noconnection", grpcproc.ReasonType: "type",
		actor.ReasonMaxRestarts: "max restarts", "timeout": "timeout", "replaced": "replaced", "demoted": "demoted",
		"panic: x": "panic", "db timeout for user 42": "error",
	}
	for in, want := range reasons {
		if got := grpcprocotel.ReasonClass(in); got != want {
			t.Errorf("ReasonClass(%q) = %q", in, got)
		}
	}
	errs := map[error]string{
		grpcproc.ErrNoProc: "noproc", grpcproc.ErrType: "type",
		&grpcproc.LinkError{Peer: "b", Err: errors.New("x")}: "noconnection",
		context.DeadlineExceeded:                             "timeout", context.Canceled: "canceled",
		fmt.Errorf("wrapped: %w", &grpcproc.RemoteError{Msg: "m"}): "remote",
		errors.New("encode"): "other",
	}
	for in, want := range errs {
		if got := grpcprocotel.ErrorType(in); got != want {
			t.Errorf("ErrorType(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestCarrier(t *testing.T) {
	c := grpcprocotel.Carrier{}
	c.Set("b", "2")
	c.Set("a", "1")
	if c.Get("a") != "1" || c.Get("zz") != "" || fmt.Sprint(c.Keys()) != "[a b]" {
		t.Fatalf("%v", c)
	}
}

func TestDefaultsUseGlobals(t *testing.T) {
	if _, err := grpcprocotel.New(); err != nil {
		t.Fatal(err)
	}
}

// failing makes every instrument fail to be created.
type failingProvider struct{ noop.MeterProvider }

func (failingProvider) Meter(string, ...metric.MeterOption) metric.Meter { return failingMeter{} }

type failingMeter struct{ noop.Meter }

var errNoInstruments = errors.New("no instruments")

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errNoInstruments
}

func (failingMeter) Int64ObservableGauge(string, ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	return nil, errNoInstruments
}

func TestInstrumentErrors(t *testing.T) {
	if _, err := grpcprocotel.New(grpcprocotel.WithMeterProvider(failingProvider{})); !errors.Is(err, errNoInstruments) {
		t.Fatalf("New: %v", err)
	}
	// Observe fails on its own instruments even when New's succeeded.
	h, err := grpcprocotel.New(grpcprocotel.WithMeterProvider(observeFailing{}))
	if err != nil {
		t.Fatal(err)
	}
	c := grpcproctest.New(t, "a")
	if _, err := h.Observe(c.Node("a")); !errors.Is(err, errNoInstruments) {
		t.Fatalf("Observe: %v", err)
	}
}

type observeFailing struct{ noop.MeterProvider }

func (observeFailing) Meter(string, ...metric.MeterOption) metric.Meter { return observeFailingMeter{} }

type observeFailingMeter struct{ noop.Meter }

func (observeFailingMeter) Int64ObservableGauge(string, ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	return nil, errNoInstruments
}

// ---------- metric helpers ----------

func collect(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func find(rm metricdata.ResourceMetrics, name string) metricdata.Aggregation {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

func has(set attribute.Set, attrs []attribute.KeyValue) bool {
	for _, a := range attrs {
		if v, ok := set.Value(a.Key); !ok || v != a.Value {
			return false
		}
	}
	return true
}

// intValue sums an int sum or gauge over the data points matching attrs.
func intValue(rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	var total int64
	switch d := find(rm, name).(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			if has(p.Attributes, attrs) {
				total += p.Value
			}
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			if has(p.Attributes, attrs) {
				total += p.Value
			}
		}
	}
	return total
}

func floatValue(rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) float64 {
	var total float64
	if d, ok := find(rm, name).(metricdata.Gauge[float64]); ok {
		for _, p := range d.DataPoints {
			if has(p.Attributes, attrs) {
				total += p.Value
			}
		}
	}
	return total
}

func histCount(rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) uint64 {
	var total uint64
	if d, ok := find(rm, name).(metricdata.Histogram[float64]); ok {
		for _, p := range d.DataPoints {
			if has(p.Attributes, attrs) {
				total += p.Count
			}
		}
	}
	return total
}

func TestDestinationByName(t *testing.T) {
	e := setup(t)
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(e.hooks)}, "a")
	a := c.Node("a")
	_, _ = a.Spawn(echo, grpcproc.WithName("svc"))
	if _, err := a.Call[*testpb.Pong](t.Context(), grpcproc.Named[*testpb.Ping]("a", "svc"), &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	for _, s := range e.spans.Ended() {
		if s.Name() == "call grpcproc.test.v1.Ping" {
			if got := attr(s, "messaging.destination.name"); got != "{svc@a}" {
				t.Fatalf("destination %q", got)
			}
			return
		}
	}
	t.Fatal("no call span")
}
