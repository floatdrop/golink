package client_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/internal/testpb"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestParseTarget(t *testing.T) {
	pid, err := client.ParseTarget("<orders.eu.7.42>")
	if err != nil || pid.GetPid().GetNode() != "orders.eu" || pid.GetPid().GetIncarnation() != 7 || pid.GetPid().GetId() != 42 {
		t.Fatalf("%v %v", pid, err)
	}
	if name, err := client.ParseTarget("svc"); err != nil || name.GetName() != "svc" {
		t.Fatalf("%v %v", name, err)
	}
	for _, bad := range []string{"", "<a.1.2", "<a.2>", "<a.x.2>", "<a.1.y>", "<.1.2>"} {
		if _, err := client.ParseTarget(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestParseLevelAndSort(t *testing.T) {
	for in, want := range map[string]int{"debug": -4, "INFO": 0, "warn": 4, "error": 8, "12": 12} {
		if l, err := client.ParseLevel(in); err != nil || int(l) != want {
			t.Errorf("%s: %v %v", in, l, err)
		}
	}
	if _, err := client.ParseLevel("loud"); err == nil {
		t.Fatal("accepted loud")
	}
	ps := []client.ProcessView{{PID: "1", Mailbox: 1, Received: 5, Sent: 1}, {PID: "2", Mailbox: 3, Received: 1, Sent: 9}}
	for by, first := range map[string]string{"pid": "1", "mailbox": "2", "received": "1", "sent": "2"} {
		cp := append([]client.ProcessView(nil), ps...)
		if err := client.SortProcesses(cp, by); err != nil || cp[0].PID != first {
			t.Errorf("%s: %v %v", by, cp, err)
		}
	}
	if err := client.SortProcesses(ps, "age"); err == nil {
		t.Fatal("accepted age")
	}
	for d, want := range map[time.Duration]string{0: "", 1500 * time.Microsecond: "1.5ms", 2500 * time.Millisecond: "2.5s", 90*time.Second + 400*time.Millisecond: "1m30s"} {
		if got := client.Short(d); got != want {
			t.Errorf("Short(%v) = %q", d, got)
		}
	}
}

func TestAgainstACluster(t *testing.T) {
	f := testcluster.Start(t)
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()

	n, err := c.Node(ctx, "")
	if err != nil || n.Name != "a" || n.Processes < 5 || len(n.Links) != 2 || n.Links[0].Peer != "b" || n.Uptime == "" {
		t.Fatalf("%+v %v", n, err)
	}
	nodes, err := c.Cluster(ctx)
	if err != nil || len(nodes) != 2 || nodes[1].Name != "b" {
		t.Fatalf("%+v %v", nodes, err)
	}

	ps, err := c.Processes(ctx, "", client.Filter{MinMailbox: 1})
	if err != nil || len(ps) != 1 || ps[0].PID != f.Stuck.String() || ps[0].Mailbox != 3 || ps[0].State != "running" || ps[0].OldestWait == "" {
		t.Fatalf("%+v %v", ps, err)
	}
	ps, _ = c.Processes(ctx, "", client.Filter{Name: "w"})
	if len(ps) != 2 || ps[0].Parent != f.Sup.String() {
		t.Fatalf("%+v", ps)
	}
	ps, _ = c.Processes(ctx, "b", client.Filter{State: "idle"})
	if len(ps) != 1 || ps[0].Names[0] != "echo" {
		t.Fatalf("%+v", ps)
	}
	if _, err := c.Processes(ctx, "", client.Filter{State: "sleeping"}); err == nil {
		t.Fatal("accepted a bad state")
	}

	p, err := c.Process(ctx, "", "talker", true, time.Second)
	if err != nil || p.Inspect["state"] != "ready" || p.PID != f.Talker.String() {
		t.Fatalf("%+v %v", p, err)
	}
	p, err = c.Process(ctx, "", "stuck", true, 20*time.Millisecond)
	if err != nil || !strings.Contains(p.InspectError, "busy") {
		t.Fatalf("%+v %v", p, err)
	}
	if p, err = c.Process(ctx, "", f.Echo.String(), false, 0); err != nil || p.Names[0] != "echo" {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := c.Process(ctx, "", "<bad", false, 0); err == nil {
		t.Fatal("accepted a bad pid")
	}
	if _, err := c.Process(ctx, "", "nobody", false, 0); err == nil {
		t.Fatal("found nobody")
	}

	if err := c.SetLogLevel(ctx, "", "talker", "debug"); err != nil {
		t.Fatal(err)
	}
	if p, _ := c.Process(ctx, "", "talker", false, 0); p.LogLevel != "DEBUG" {
		t.Fatalf("%+v", p)
	}
	for _, bad := range [][2]string{{"<bad", "debug"}, {"talker", "loud"}} {
		if err := c.SetLogLevel(ctx, "", bad[0], bad[1]); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}

	// Watch: events arrive; returning false ends it.
	events := make(chan client.EventView, 16)
	go func() {
		_ = c.Watch(ctx, "", func(e client.EventView) bool {
			events <- e
			return e.Kind != "exit"
		})
	}()
	var exited client.EventView
	deadline := time.After(5 * time.Second)
	for exited.Kind == "" {
		_, _ = grpcproc.Spawn(f.C.Node("a"), func(*grpcproc.Process[*testpb.Ping]) error { return errors.New("bye") }, grpcproc.WithLabel("short"))
		select {
		case e := <-events:
			if e.Kind == "exit" {
				exited = e
			}
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("no events")
		}
	}
	if exited.Process == nil || exited.Process.Label != "short" || exited.Reason != "bye" {
		t.Fatalf("%+v", exited)
	}

	if err := c.Exit(ctx, "", "w1", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.Exit(ctx, "", "<bad", ""); err == nil {
		t.Fatal("accepted a bad pid")
	}
	// A watch whose context ends returns nil; one that cannot start, an error.
	wctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := c.Watch(wctx, "", func(client.EventView) bool { return true }); err != nil {
		t.Fatalf("ended watch: %v", err)
	}
	if err := c.Watch(ctx, "nowhere", func(client.EventView) bool { return true }); err == nil {
		t.Fatal("watched an unknown node")
	}
}
