package grpcproc

import (
	"testing"
	"time"
)

// A link's Down that arrives after Unlink took the link is dropped; Link
// from a process that has exited, or to itself by name, does nothing.
func TestLinkEdges(t *testing.T) {
	n := newTestNode(t, "a")
	p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, name: "me", mbox: newQueue[item](true)}
	p.ctx, p.cancel = n.ctx, func(error) { t.Fatal("ended by an unlinked process") }
	n.mu.Lock()
	n.procs[5] = p
	n.mu.Unlock()
	t.Cleanup(func() { n.mu.Lock(); delete(n.procs, 5); n.mu.Unlock() })

	r := Ref{Node: "a", ID: 99}
	p.links = map[Ref]monitorTarget{r: {pid: PID{Node: "b", Incarnation: 2, ID: 7}}}
	delete(p.links, r) // as Unlink does, before the Down comes
	n.deliverDown(PID{Node: "b", Incarnation: 2, ID: 7}, p.pid, r.ID, "boom")
	if p.mbox.len() != 0 {
		t.Fatal("a Down for an unlinked process was delivered")
	}

	p.Link(Name{Node: "a", Name: "me"})
	if len(p.links) != 0 {
		t.Fatal("linked to itself by name")
	}
	p.exited = true
	p.Link(PID{Node: "a", Incarnation: 1, ID: 6})
	if len(p.links) != 0 {
		t.Fatal("an exited process linked")
	}
}

// A link or monitor still on its way when its process exits does not stay
// on the target, even when the exit took it back before it arrived: it is
// taken back again once placed. Here the target's lock keeps the watch from
// being placed until the exit has taken it back.
func TestWatchPlacedAsTheWatcherExits(t *testing.T) {
	for _, link := range []bool{false, true} {
		n := newTestNode(t, "a")
		target := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 6}, mbox: newQueue[item](true)}
		p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
		n.mu.Lock()
		n.procs[6] = target
		n.mu.Unlock()
		target.mu.Lock()
		placed := make(chan struct{})
		go func() {
			if link {
				p.Link(target.pid)
			} else {
				p.Monitor(target.pid)
			}
			close(placed)
		}()
		for { // on its way: in p's maps, held at the target
			p.mu.Lock()
			held := len(p.links)+len(p.monitors) == 1
			if held { // the exit, which has taken it back already
				p.exited, p.links, p.monitors = true, nil, nil
			}
			p.mu.Unlock()
			if held {
				break
			}
			time.Sleep(time.Millisecond)
		}
		target.mu.Unlock()
		<-placed
		if len(target.watchers) != 0 {
			t.Fatalf("link %v: left on the target", link)
		}
		n.mu.Lock()
		delete(n.procs, 6)
		n.mu.Unlock()
	}
}
