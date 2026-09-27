package grpcproc_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// tracer is a Hooks that behaves like a tracer would: it stamps a span id on
// what leaves and on what is being handled, and records when each ends.
type tracer struct {
	grpcproc.NopHooks
	name string

	mu     sync.Mutex
	seq    int
	events []string
}

func (tr *tracer) log(s string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.events = append(tr.events, s)
}

func (tr *tracer) next() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.seq++
	return tr.name + string(rune('0'+tr.seq))
}

func (tr *tracer) Events() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.events)
}

func stamp(md grpcproc.Metadata, key, val string) grpcproc.Metadata {
	out := grpcproc.Metadata{key: val}
	for k, v := range md {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func (tr *tracer) OnSend(s grpcproc.SendInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	id := tr.next()
	kind := "send"
	if s.Call {
		kind = "call"
	}
	tr.log(kind + " " + id + " parent=" + md["span"] + " label=" + s.FromLabel)
	return stamp(md, "span", id), func(err error) {
		msg := "end " + id
		if err != nil {
			msg += " err=" + err.Error()
		}
		tr.log(msg)
	}
}

func (tr *tracer) OnReceive(r grpcproc.ReceiveInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	id := tr.next()
	what := "msg"
	if r.Down != nil {
		what = "down"
	}
	tr.log("recv " + id + " parent=" + md["span"] + " " + what + " label=" + r.Label)
	return stamp(md, "span", id), func(err error) {
		msg := "handled " + id
		if err != nil {
			msg += " err=" + err.Error()
		}
		tr.log(msg)
	}
}

func TestMetadataFlowsThroughProcesses(t *testing.T) {
	tr := &tracer{name: "s"}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(tr)}, "a", "b")
	a, b := c.Node("a"), c.Node("b")

	sink, got := collector(t, b)
	// relay forwards what it receives: its sends inherit the message's
	// metadata (tenant) and the span OnReceive stamped.
	relay, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if err := p.Send(sink, proto.Message(m.Body)); err != nil {
				return err
			}
		}
	}, grpcproc.WithLabel("relay"))
	ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"})
	if err := a.SendContext(ctx, relay, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	m := recv(t, got)
	if m.Metadata["tenant"] != "acme" {
		t.Fatalf("tenant lost: %v", m.Metadata)
	}
	// The chain: node send s1 -> relay handles as s2 -> relay's send s3 is
	// a child of s2 -> the collector handles it as s4, child of s3.
	if m.Metadata["span"] != "s4" {
		t.Fatalf("span chain: %v\n%s", m.Metadata, strings.Join(tr.Events(), "\n"))
	}
	events := tr.Events()
	for _, want := range []string{
		"send s1 parent= label=",
		"recv s2 parent=s1 msg label=relay",
		"send s3 parent=s2 label=relay",
		"recv s4 parent=s3 msg label=collector",
		"end s1",
		"end s3",
	} {
		if !slices.Contains(events, want) {
			t.Errorf("missing %q in\n%s", want, strings.Join(events, "\n"))
		}
	}
	// Handling ends at the next Receive: send another message and s2 closes.
	_ = a.SendContext(ctx, relay, &testpb.Ping{N: 2})
	recv(t, got)
	if !slices.Contains(tr.Events(), "handled s2") {
		t.Fatalf("handling not ended:\n%s", strings.Join(tr.Events(), "\n"))
	}
}

func TestInheritanceEndsWithHandling(t *testing.T) {
	c := grpcproctest.New(t, "a")
	a := c.Node("a")
	sink, got := collector(t, a)
	// After a ReceiveTimeout, nothing is being handled: sends inherit nothing.
	p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		_ = p.Send(sink, proto.Message(&testpb.Ping{N: 1})) // inherits
		ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"extra": "1"})
		_ = p.SendContext(ctx, sink, proto.Message(&testpb.Ping{N: 2})) // inherits and adds
		if _, err := p.ReceiveTimeout(time.Millisecond); err == nil {
			t.Error("expected timeout")
		}
		return p.Send(sink, proto.Message(&testpb.Ping{N: 3})) // inherits nothing
	})
	_ = a.SendContext(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"}), p, &testpb.Ping{})
	if m := recv(t, got); m.Metadata["tenant"] != "acme" {
		t.Fatalf("1: %v", m.Metadata)
	}
	if m := recv(t, got); m.Metadata["tenant"] != "acme" || m.Metadata["extra"] != "1" {
		t.Fatalf("2: %v", m.Metadata)
	}
	if m := recv(t, got); len(m.Metadata) != 0 {
		t.Fatalf("3: %v", m.Metadata)
	}
}

func TestDoneReportsOutcomes(t *testing.T) {
	tr := &tracer{name: "d"}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(tr)}, "a")
	a := c.Node("a")
	e, _ := a.Spawn(echo, grpcproc.WithLabel("echo"))
	// A call ends with its error.
	if _, err := a.Call[*testpb.Pong](t.Context(), e, &testpb.Ping{N: -1}); err == nil {
		t.Fatal("expected error")
	}
	// A send that cannot route ends with that error.
	_ = a.SendTo(grpcproc.Named[*testpb.Ping]("nowhere", "x"), &testpb.Ping{})
	// A process that exits abnormally ends its handling with the reason.
	_ = a.Send(e, &testpb.Ping{N: -100})
	w, ch := watcher(t, a)
	w.Monitor(e)
	recv(t, ch)
	time.Sleep(20 * time.Millisecond)
	ev := strings.Join(tr.Events(), "\n")
	for _, want := range []string{"err=negative: -1", "err=grpcproc: link to nowhere", "err=boom"} {
		if !strings.Contains(ev, want) {
			t.Errorf("missing %q in\n%s", want, ev)
		}
	}
	// A Down is received like a message.
	if !strings.Contains(ev, " down label=") {
		t.Errorf("no down receive in\n%s", ev)
	}
}

func TestJoinHooks(t *testing.T) {
	if grpcproc.JoinHooks() != nil || grpcproc.JoinHooks(nil, nil) != nil {
		t.Fatal("empty join must be nil")
	}
	one := &tracer{name: "x"}
	if grpcproc.JoinHooks(nil, one) != grpcproc.Hooks(one) {
		t.Fatal("single hook must be returned as is")
	}
	first, second := &tracer{name: "f"}, &tracer{name: "s"}
	counts := &countingHooks{}
	h := grpcproc.JoinHooks(first, counts, second)
	// Metadata threads through in order; Done runs in reverse.
	md, done := h.OnSend(grpcproc.SendInfo{}, grpcproc.Metadata{"span": "root"})
	if md["span"] != "s1" {
		t.Fatalf("%v", md)
	}
	if got := second.Events()[0]; got != "send s1 parent=f1 label=" {
		t.Fatal(got)
	}
	done(errors.New("x"))
	md, done = h.OnReceive(grpcproc.ReceiveInfo{}, nil)
	done(nil)
	if md["span"] != "s2" || first.Events()[len(first.Events())-1] != "handled f2" {
		t.Fatalf("%v %v", md, first.Events())
	}
	// Hooks that start nothing leave no Done.
	if _, d := grpcproc.JoinHooks(grpcproc.NopHooks{}, counts).OnSend(grpcproc.SendInfo{}, nil); d != nil {
		t.Fatal("no Done expected")
	}
	if _, d := grpcproc.JoinHooks(grpcproc.NopHooks{}, first).OnReceive(grpcproc.ReceiveInfo{}, nil); d == nil {
		t.Fatal("single Done expected")
	}
	h.OnSpawn(grpcproc.ProcessInfo{})
	h.OnExit(grpcproc.ProcessInfo{}, "")
	h.OnDeadLetter(grpcproc.PID{}, grpcproc.PID{}, nil, "")
	h.OnLinkUp(grpcproc.NodeID{})
	h.OnLinkDown(grpcproc.NodeID{}, nil)
	if counts.spawns.Load() != 1 || counts.exits.Load() != 1 || counts.deadLetters.Load() != 1 || counts.linkUps.Load() != 1 || counts.linkDowns.Load() != 1 {
		t.Fatal("join did not fan out")
	}
}
