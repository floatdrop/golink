package grpcproc_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// FuzzScenario reads the fuzzer's bytes as a run of operations on a
// three-node cluster: processes spawned, sending, calling, monitoring and
// linking each other, asked to exit or ending by themselves, and nodes
// partitioned, healed, killed and restarted, with time passing between.
// Then it checks what grpcproc promises of any run, whatever the order the
// goroutines ran in:
//
//   - messages from one sender reach a process in the order they were sent,
//     each at most once, and only the process they were sent to: never one
//     of a node that restarted since;
//   - a call returns the answer to its own request, or an error;
//   - a monitor fires at most once, for its target, and exactly once for a
//     target that ended; once a Down says the target ended, nothing more
//     arrives from it;
//   - a link to a target that ended ends a process that does not trap
//     exits, and gives one that does exactly one Exited;
//   - and when the cluster stops, nothing is left running.
//
// The processes act on commands from their own goroutines, so that what
// each one sends is sent by it, in order with its own exit, as in a real
// program. The run is in a synctest bubble: timeouts cost nothing, and a
// goroutine left behind fails it.
func FuzzScenario(f *testing.F) {
	for _, seed := range scenarioSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}
		synctest.Test(t, func(t *testing.T) {
			s := newScenario(t)
			s.run(data)
			s.settle()
			s.check()
		})
	})
}

var scenarioNodes = []string{"a", "b", "c"}

// Operations, the first byte of each; the bytes after it are its operands.
const (
	opSpawn     = iota // node, trap
	opSend             // from, to: a process sends
	opNodeSend         // node, to: a node sends
	opCall             // from, to: a process calls
	opNodeCall         // node, to: a node calls
	opMonitor          // watcher, target
	opLink             // linker, target
	opExit             // target, reason: the node asks it to exit
	opQuit             // target, reason: it ends by itself
	opPartition        // node, node: partition, or heal if partitioned
	opKill             // node: kill, or restart if killed
	opSleep            // duration
	opWait             // until every goroutine waits
	opCount
)

// scenarioSeeds are runs worth starting from: each operation at least
// once, and the interleavings the core's bugs came from.
var scenarioSeeds = [][]byte{
	{opSpawn, 0, 0, opSpawn, 1, 0, opSend, 0, 1, opNodeSend, 2, 0, opCall, 1, 0, opNodeCall, 0, 1, opWait},
	{opSpawn, 0, 0, opSpawn, 1, 0, opMonitor, 0, 1, opExit, 1, 3, opWait},
	{opSpawn, 0, 0, opSpawn, 1, 0, opMonitor, 0, 1, opPartition, 0, 1, opWait, opPartition, 0, 1, opSleep, 2},
	{opSpawn, 0, 1, opSpawn, 1, 0, opLink, 0, 1, opQuit, 1, 1, opWait},
	{opSpawn, 0, 0, opSpawn, 2, 0, opLink, 0, 1, opKill, 2, opWait, opKill, 2, opSpawn, 2, 0, opSend, 0, 2},
	{opSpawn, 0, 0, opSpawn, 1, 0, opSend, 1, 0, opSend, 1, 0, opQuit, 1, 0, opMonitor, 0, 1, opWait},
	{opSpawn, 1, 0, opSpawn, 2, 0, opCall, 0, 1, opPartition, 1, 2, opCall, 0, 1, opSleep, 3, opPartition, 1, 2, opCall, 0, 1},
	{opSpawn, 0, 1, opSpawn, 1, 1, opLink, 0, 1, opLink, 1, 0, opMonitor, 0, 1, opKill, 1, opExit, 0, 2},
}

// event is what a process saw or did, in the order it did.
type event struct {
	kind   string // "msg", "down", "exited", "monitored", "linked", "called"
	from   grpcproc.PID
	n      int64
	ref    grpcproc.Ref
	pid    grpcproc.PID
	reason string
	target int
	reply  int64
	err    error
}

type probe struct {
	i    int
	node string
	pid  grpcproc.PID
	trap bool

	mu     sync.Mutex
	log    []event
	ended  bool
	reason string
}

func (pr *probe) record(e event) {
	pr.mu.Lock()
	pr.log = append(pr.log, e)
	pr.mu.Unlock()
}

type scenario struct {
	t *testing.T
	c *grpcproctest.Cluster

	mu     sync.Mutex
	probes []*probe
	byPID  map[grpcproc.PID]*probe

	killed map[string]bool
	parted map[[2]string]bool
	// cut is every pair of nodes whose links broke at some point: partitioned,
	// or one of them killed. A monitor across a cut may fire with noconnection.
	cut    map[[2]string]bool
	seq    map[string]int64 // (sender, receiver) -> the last n sent
	calls  []event          // the nodes' own calls
	ops    []string         // what ran, to say when a check fails
	linked map[[2]int]bool
}

func newScenario(t *testing.T) *scenario {
	return &scenario{
		t:      t,
		c:      grpcproctest.New(t, scenarioNodes...),
		byPID:  map[grpcproc.PID]*probe{},
		killed: map[string]bool{},
		parted: map[[2]string]bool{},
		cut:    map[[2]string]bool{},
		seq:    map[string]int64{},
		linked: map[[2]int]bool{},
	}
}

func pair(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

func (s *scenario) probe(i int) *probe {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes[i]
}

// next is the n of the next message from sender to receiver: the receiver's
// index in the top bits, so that one delivered elsewhere is seen, and a
// count per sender below, so that one out of order is.
func (s *scenario) next(sender string, to int) int64 {
	k := fmt.Sprintf("%s->%d", sender, to)
	s.seq[k]++
	return int64(to)<<40 | s.seq[k]
}

// run reads data as operations, until it runs out.
func (s *scenario) run(data []byte) {
	arg := func(i int) byte {
		if i < len(data) {
			return data[i]
		}
		return 0
	}
	for i := 0; i < len(data); {
		op, a, b := int(data[i])%opCount, arg(i+1), arg(i+2)
		i++
		switch op {
		case opSpawn:
			s.spawn(scenarioNodes[int(a)%3], b&1 == 1)
			i += 2
		case opSend, opCall, opMonitor, opLink:
			s.command(op, int(a), int(b))
			i += 2
		case opNodeSend, opNodeCall:
			s.fromNode(op, scenarioNodes[int(a)%3], int(b))
			i += 2
		case opExit:
			s.exit(int(a), fmt.Sprintf("asked%d", b%4))
			i += 2
		case opQuit:
			s.command(op, int(a), int(b))
			i += 2
		case opPartition:
			s.partition(scenarioNodes[int(a)%3], scenarioNodes[int(b)%3])
			i += 2
		case opKill:
			s.kill(scenarioNodes[int(a)%3])
			i++
		case opSleep:
			d := []time.Duration{time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second}[int(a)%4]
			s.ops = append(s.ops, fmt.Sprintf("sleep %v", d))
			time.Sleep(d)
			i++
		case opWait:
			s.ops = append(s.ops, "wait")
			synctest.Wait()
		}
	}
}

func (s *scenario) spawn(node string, trap bool) {
	if s.killed[node] {
		return
	}
	s.mu.Lock()
	pr := &probe{i: len(s.probes), node: node, trap: trap}
	s.mu.Unlock()
	addr, err := s.c.Node(node).Spawn(s.body(pr))
	if err != nil {
		s.t.Fatalf("spawn on %s: %v\n%s", node, err, s.history())
	}
	pr.pid = addr.PID()
	s.mu.Lock()
	s.probes = append(s.probes, pr)
	s.byPID[pr.pid] = pr
	s.mu.Unlock()
	s.ops = append(s.ops, fmt.Sprintf("spawn p%d on %s (%v), trap %v", pr.i, node, pr.pid, trap))
}

// body is a probe's process: it records what it gets, answers calls, and
// does what commands say.
func (s *scenario) body(pr *probe) func(*grpcproc.Process[proto.Message]) error {
	return func(p *grpcproc.Process[proto.Message]) (err error) {
		defer func() {
			reason := "normal"
			if ee, ok := errors.AsType[*grpcproc.ExitError](context.Cause(p.Context())); ok {
				reason = ee.Reason
			} else if err != nil {
				reason = err.Error()
			}
			pr.mu.Lock()
			pr.ended, pr.reason = true, reason
			pr.mu.Unlock()
		}()
		p.SetTrapExit(pr.trap)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			switch body := m.Body.(type) {
			case nil:
				if m.Down != nil {
					pr.record(event{kind: "down", ref: m.Down.Ref, pid: m.Down.PID, reason: m.Down.Reason})
				} else if m.Exited != nil {
					pr.record(event{kind: "exited", pid: m.Exited.PID, reason: m.Exited.Reason})
				}
			case *testpb.Ping:
				pr.record(event{kind: "msg", from: m.From, n: body.GetN()})
				if m.IsCall() {
					_ = m.Reply(&testpb.Pong{N: body.GetN() + 1}, nil)
				}
			case *testpb.Command:
				if err := s.do(p, pr, body); err != nil {
					return err
				}
			}
		}
	}
}

// do carries out a command on the probe's own goroutine.
func (s *scenario) do(p *grpcproc.Process[proto.Message], pr *probe, c *testpb.Command) error {
	if c.GetOp() == testpb.Command_OP_EXIT {
		return errors.New(c.GetReason())
	}
	target := s.probe(int(c.GetTarget()))
	switch c.GetOp() {
	case testpb.Command_OP_SEND:
		_ = p.SendTo(target.pid, &testpb.Ping{N: c.GetN()})
	case testpb.Command_OP_CALL:
		ctx, cancel := context.WithTimeout(p.Context(), 5*time.Second)
		r, err := p.CallTo[*testpb.Pong](ctx, target.pid, &testpb.Ping{N: c.GetN()})
		cancel()
		pr.record(event{kind: "called", n: c.GetN(), reply: r.GetN(), err: err, target: target.i})
	case testpb.Command_OP_MONITOR:
		pr.record(event{kind: "monitored", ref: p.Monitor(target.pid), target: target.i})
	case testpb.Command_OP_LINK:
		pr.record(event{kind: "linked", target: target.i})
		p.Link(target.pid)
	}
	return nil
}

// command tells probe from to do op to probe to, by a message from from's
// node, so that it runs in order with what that node told it before.
func (s *scenario) command(op, from, to int) {
	s.mu.Lock()
	n := len(s.probes)
	s.mu.Unlock()
	if n == 0 {
		return
	}
	pf, pt := s.probe(from%n), s.probe(to%n)
	c := &testpb.Command{Target: int64(pt.i)}
	switch op {
	case opSend:
		c.Op, c.N = testpb.Command_OP_SEND, s.next(pf.pid.String(), pt.i)
	case opCall:
		c.Op, c.N = testpb.Command_OP_CALL, s.next(pf.pid.String(), pt.i)
	case opMonitor:
		if pf == pt {
			return
		}
		c.Op = testpb.Command_OP_MONITOR
	case opLink:
		// A link to itself, or a second to the same target, is not what
		// the checks below model.
		if pf == pt || s.linked[[2]int{pf.i, pt.i}] {
			return
		}
		s.linked[[2]int{pf.i, pt.i}] = true
		c.Op = testpb.Command_OP_LINK
	case opQuit:
		c.Op, c.Reason = testpb.Command_OP_EXIT, fmt.Sprintf("quit%d", to%4)
	}
	if s.killed[pf.node] {
		return
	}
	_ = s.c.Node(pf.node).SendTo(context.Background(), pf.pid, c)
	s.ops = append(s.ops, fmt.Sprintf("p%d: %v p%d n=%d %s", pf.i, c.GetOp(), pt.i, c.GetN(), c.GetReason()))
}

func (s *scenario) fromNode(op int, node string, to int) {
	s.mu.Lock()
	n := len(s.probes)
	s.mu.Unlock()
	if n == 0 || s.killed[node] {
		return
	}
	pt := s.probe(to % n)
	nd := s.c.Node(node)
	ping := &testpb.Ping{N: s.next(nd.PID().String(), pt.i)}
	if op == opNodeSend {
		_ = nd.SendTo(context.Background(), pt.pid, ping)
		s.ops = append(s.ops, fmt.Sprintf("%s sends p%d n=%d", node, pt.i, ping.N))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	r, err := nd.CallTo[*testpb.Pong](ctx, pt.pid, ping)
	cancel()
	s.calls = append(s.calls, event{kind: "called", n: ping.N, reply: r.GetN(), err: err, target: pt.i})
	s.ops = append(s.ops, fmt.Sprintf("%s calls p%d n=%d: %d %v", node, pt.i, ping.N, r.GetN(), err))
}

func (s *scenario) exit(to int, reason string) {
	s.mu.Lock()
	n := len(s.probes)
	s.mu.Unlock()
	if n == 0 {
		return
	}
	pt := s.probe(to % n)
	for _, node := range scenarioNodes {
		if !s.killed[node] {
			_ = s.c.Node(node).Exit(context.Background(), pt.pid, reason)
			s.ops = append(s.ops, fmt.Sprintf("%s exits p%d: %s", node, pt.i, reason))
			return
		}
	}
}

func (s *scenario) partition(a, b string) {
	if a == b {
		return
	}
	k := pair(a, b)
	if s.parted[k] {
		s.c.Heal(a, b)
		s.ops = append(s.ops, fmt.Sprintf("heal %s %s", a, b))
	} else {
		s.c.Partition(a, b)
		s.cut[k] = true
		s.ops = append(s.ops, fmt.Sprintf("partition %s %s", a, b))
	}
	s.parted[k] = !s.parted[k]
}

func (s *scenario) kill(node string) {
	if s.killed[node] {
		s.c.Restart(node)
		s.ops = append(s.ops, fmt.Sprintf("restart %s as %v", node, s.c.Node(node).ID()))
	} else {
		s.c.Kill(node)
		for _, other := range scenarioNodes {
			s.cut[pair(node, other)] = true
		}
		s.ops = append(s.ops, fmt.Sprintf("kill %s", node))
	}
	s.killed[node] = !s.killed[node]
}

// settle heals every partition and lets everything in flight land: calls
// time out, Downs arrive, links end what they end.
func (s *scenario) settle() {
	for k, parted := range s.parted {
		if parted {
			s.c.Heal(k[0], k[1])
		}
	}
	time.Sleep(time.Minute)
	synctest.Wait()
}

func (s *scenario) history() string { return strings.Join(s.ops, "\n") }

func (s *scenario) fail(format string, args ...any) {
	s.t.Helper()
	s.t.Fatalf("%s\nafter:\n%s", fmt.Sprintf(format, args...), s.history())
}

func (s *scenario) check() {
	s.mu.Lock()
	probes := s.probes
	s.mu.Unlock()
	answered := func(e event) {
		if e.err == nil && e.reply != e.n+1 {
			s.fail("call n=%d to p%d answered %d", e.n, e.target, e.reply)
		}
	}
	for _, e := range s.calls {
		answered(e)
	}
	for _, w := range probes {
		w.mu.Lock()
		log, ended := w.log, w.ended
		w.mu.Unlock()

		last := map[grpcproc.PID]int64{}  // sender -> the last n from it
		downs := map[grpcproc.Ref]event{} // ref -> its Down
		gone := map[grpcproc.PID]bool{}   // a Down said it ended
		exiteds := map[grpcproc.PID]int{}
		for _, e := range log {
			switch e.kind {
			case "msg":
				if to := int(e.n >> 40); to != w.i {
					s.fail("p%d got n=%d, sent to p%d", w.i, e.n, to)
				}
				if e.n <= last[e.from] {
					s.fail("p%d got n=%d from %v after n=%d", w.i, e.n, e.from, last[e.from])
				}
				last[e.from] = e.n
				if gone[e.from] {
					s.fail("p%d got n=%d from %v after a Down said it ended", w.i, e.n, e.from)
				}
			case "down":
				if prev, ok := downs[e.ref]; ok {
					s.fail("p%d got two Downs for %v: %q and %q", w.i, e.ref, prev.reason, e.reason)
				}
				downs[e.ref] = e
				if e.reason != grpcproc.ReasonNoConnection {
					gone[e.pid] = true
				}
			case "exited":
				exiteds[e.pid]++
				if exiteds[e.pid] > 1 {
					s.fail("p%d got %d Exiteds for %v", w.i, exiteds[e.pid], e.pid)
				}
			case "called":
				answered(e)
			}
		}
		for _, e := range log {
			target := func() (*probe, bool, string) {
				t := probes[e.target]
				t.mu.Lock()
				defer t.mu.Unlock()
				return t, t.ended, t.reason
			}
			switch e.kind {
			case "monitored":
				t, tEnded, _ := target()
				d, fired := downs[e.ref]
				switch {
				case fired && d.pid != t.pid:
					s.fail("p%d's monitor %v of p%d (%v) fired for %v", w.i, e.ref, t.i, t.pid, d.pid)
				case !ended && tEnded && !fired:
					s.fail("p%d's monitor %v of p%d never fired, though p%d ended", w.i, e.ref, t.i, t.i)
				case fired && !tEnded && !s.cut[pair(w.node, t.node)]:
					s.fail("p%d's monitor %v of p%d fired (%q), though p%d runs and no link between them broke", w.i, e.ref, t.i, d.reason, t.i)
				}
			case "linked":
				t, tEnded, _ := target()
				switch {
				case tEnded && !w.trap && !ended:
					s.fail("p%d linked to p%d, which ended, and runs on", w.i, t.i)
				case tEnded && w.trap && !ended && exiteds[t.pid] != 1:
					s.fail("p%d traps exits, linked to p%d, which ended, and got %d Exiteds", w.i, t.i, exiteds[t.pid])
				}
			}
		}
	}
}
