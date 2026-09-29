package grpcproc

import (
	"errors"
	"testing"
	"testing/synctest"
)

// The answer of a call that asked for a watch can come just before the link
// it came by breaks, and the caller take it only after: the watch worked, so
// its Down comes, with noconnection. A Down that came first keeps its reason.
func TestWatchAnsweredBeforeTheLinkBroke(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
		n.procs[p.pid.ID] = p
		watched := PID{Node: "b", Incarnation: 2, ID: 9}
		for _, first := range []string{"", "early"} {
			ref := Ref{Node: "a", ID: n.nextRef.Add(1)}
			if !p.awaitWatch(ref, "b", false) {
				t.Fatal("not awaited")
			}
			if first != "" {
				n.deliverDown(watched, p.pid, ref.ID, first)
			}
			p.noteWatched(ref, watched) // the answer came
			n.nodeDown("b", errors.New("cut"))
			p.settleWatch(ref, true)
			it, ok := p.mbox.tryPop()
			want := first
			if want == "" {
				want = ReasonNoConnection
			}
			if !ok || it.down == nil || it.down.Ref != ref || it.down.PID != watched || it.down.Reason != want {
				t.Fatalf("%+v, want the Down of %v with %q", it.down, watched, want)
			}
		}
	})
}

// A watch is not placed on a process that has exited, which it could find
// between its exit and its leaving the node's processes.
func TestWatchForAnExitedProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		taker := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
		c := openCall{from: PID{Node: "b", Incarnation: 2, ID: 7}, ref: 3}
		if !taker.queueCall(item{from: c.from, ref: c.ref, watch: true}, nil) {
			t.Fatal("not queued")
		}
		gone := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 6}, exited: true}
		if err := taker.watchFor(c, gone); !errors.Is(err, ErrNoProc) {
			t.Fatal(err)
		}
		if len(gone.watchers) != 0 || !taker.open[c].watched.IsZero() {
			t.Fatal("a watch placed on a process that has exited")
		}
	})
}
