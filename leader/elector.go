package leader

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// ReasonDemoted is the exit reason a singleton is told to exit with when
// its node stops leading.
const ReasonDemoted = "demoted"

// electors is what the supervisor runs, again at every restart.
type electors[S proto.Message] struct {
	spec Spec[S]
	// cur is the running elector, for inspect, which grpcproc runs on the
	// elector's own goroutine.
	cur *elector[S]
}

func (es *electors[S]) inspect() map[string]string { return es.cur.inspect() }

func (es *electors[S]) run(p *grpcproc.Process[proto.Message]) error {
	e := &elector[S]{
		spec:  es.spec,
		p:     p,
		self:  p.Node().Name(),
		name:  ElectorName(es.spec.Cluster),
		peers: map[string]*peer{},
		role:  Follower,
		state: &leaderv1.State{Version: &leaderv1.Version{}},
	}
	es.cur = e
	return e.loop()
}

// peer is another node's elector, as this one knows it.
type peer struct {
	node  string
	relay grpcproc.Addr[proto.Message]
	// inView says it counts towards the quorum. Every voter does.
	inView bool
	// declared says Peers or Membership named it: it may take part.
	declared bool
	// greetAt is when to greet it next, while it is out of the view or has
	// not been heard from since it was lost.
	greetAt time.Time
	// down says the relay's monitor fired: it watches again once the peer
	// is heard from.
	down bool
	// ghostSince is when its link broke, if it has not been heard from
	// since.
	ghostSince time.Time
	// On a leader: when it last acknowledged a heartbeat (or joined the
	// view), the state version it holds, and which version it was last
	// sent, when.
	lastAck     time.Time
	acked       *leaderv1.Version
	sentVersion *leaderv1.Version
	sent        time.Time
}

type phase uint8

const (
	idle phase = iota
	starting
	running
	stopping
)

var phases = [...]string{"none", "starting", "running", "stopping"}

// singleton is the elector's singleton: its lifecycle, and the term it was
// started for.
type singleton struct {
	phase phase
	term  uint64
	pid   grpcproc.PID
	ref   grpcproc.Ref
}

// pending is a Checkpoint call waiting for a majority to hold its state.
type pending struct {
	seq uint64
	m   grpcproc.Msg[proto.Message]
}

// resignation is a hand-over being carried out: a Resign call, or a
// cordoned leader's own (whose m is no call, and answers nobody).
type resignation struct {
	m        grpcproc.Msg[proto.Message]
	to       string // the follower to hand over to, or empty for the most recent
	deadline time.Time
}

// elector is one node's part in an election. It is touched by its own
// goroutine only.
type elector[S proto.Message] struct {
	spec       Spec[S]
	p          *grpcproc.Process[proto.Message]
	self, name string

	term     uint64
	votedFor string
	role     Role // Follower, Candidate or Leader
	leader   string
	votes    map[string]bool // a candidate's
	preVotes map[string]bool // a follower's, while it asks whether it would win

	heard        time.Time // the leader's last heartbeat
	electionAt   time.Time // when a follower campaigns
	heartbeatAt  time.Time // when a leader asserts itself next
	quietUntil   time.Time // no vote before: see loop
	backoffUntil time.Time // no campaign before, after a failure
	fails        int       // failures in a row

	peers   map[string]*peer
	state   *leaderv1.State
	pending []pending
	single  singleton
	resign  *resignation

	bridge   grpcproc.Ref // the Membership bridge, while it runs
	bridgeAt time.Time    // when to start it again
}

func (e *elector[S]) static() bool { return len(e.spec.Voters) > 0 }

func (e *elector[S]) loop() error {
	if e.static() && !slices.Contains(e.spec.Voters, e.self) {
		return fmt.Errorf("leader: %s is not among the Voters", e.self)
	}
	now := time.Now()
	// Votes live in memory: this node may have voted in the current term
	// before it restarted. It votes again only once a leader elected with
	// that vote would have been heard from.
	e.quietUntil = now.Add(2 * e.spec.ElectionTimeout)
	e.electionAt = e.quietUntil.Add(e.timeout())
	for _, node := range slices.Concat(e.spec.Voters, e.spec.Peers) {
		if node != e.self {
			e.add(node).declared = true
		}
	}
	for {
		now := time.Now()
		e.timers(now)
		e.reconcile(now)
		m, err := e.p.ReceiveTimeout(time.Until(e.wake(now)))
		switch {
		case errors.Is(err, context.DeadlineExceeded):
		case err != nil:
			return err
		default:
			e.handle(m, time.Now())
		}
	}
}

// timeout is a random election timeout.
func (e *elector[S]) timeout() time.Duration {
	return e.spec.ElectionTimeout + rand.N(e.spec.ElectionTimeout)
}

// wake is when the elector has something to do next, at the latest a
// heartbeat interval from now: ghosts, greetings and hand-overs are
// looked at then.
func (e *elector[S]) wake(now time.Time) time.Time {
	t := e.electionAt
	if e.role == Leader {
		t = e.heartbeatAt
	}
	if limit := now.Add(e.spec.HeartbeatInterval); limit.Before(t) {
		return limit
	}
	return t
}

func (e *elector[S]) timers(now time.Time) {
	e.peersNow(now)
	if e.role == Leader {
		if e.cordoned(e.self) && e.resign == nil {
			e.resign = &resignation{deadline: now.Add(4 * e.spec.ElectionTimeout)}
		}
		if !now.Before(e.heartbeatAt) {
			e.tick(now)
		}
	} else if !now.Before(e.electionAt) {
		e.electionAt = now.Add(e.timeout())
		if !now.Before(e.backoffUntil) && !e.cordoned(e.self) && (e.static() || len(e.view()) >= e.spec.MinClusterSize) {
			e.preVote(now)
		}
	}
	if e.resign != nil && !now.Before(e.resign.deadline) {
		e.handOver(now, true)
	}
}

// peersNow looks after the peers: in a dynamic view ghosts leave it, and
// peers lost, or named but out of the view, are greeted every GhostTTL, to
// hear from them once they are back.
func (e *elector[S]) peersNow(now time.Time) {
	for _, p := range e.peers {
		if !e.static() && p.inView && e.spec.Membership == nil && !p.ghostSince.IsZero() && now.Sub(p.ghostSince) >= e.spec.GhostTTL {
			p.inView = false
		}
		lost := p.inView && (p.down || !p.ghostSince.IsZero())
		absent := !p.inView && p.declared
		if (lost || absent) && !now.Before(p.greetAt) {
			e.greet(p, now, false)
		}
	}
	if !e.static() && e.spec.Membership != nil && e.bridge == (grpcproc.Ref{}) && !now.Before(e.bridgeAt) {
		// It fails only while the node stops.
		_, e.bridge, _ = e.p.SpawnMonitor[proto.Message](bridge(e.spec.Membership), grpcproc.LinkParent(), grpcproc.WithLabel("leader membership"))
	}
}

// reconcile starts the singleton while this node leads, and stops it while
// it does not, or runs for a term that is over.
func (e *elector[S]) reconcile(now time.Time) {
	lead := e.role == Leader && e.resign == nil
	switch e.single.phase {
	case idle:
		if lead {
			e.start(now)
		}
	case running:
		if !lead || e.single.term != e.term {
			e.single.phase = stopping
			_ = e.p.Exit(e.single.pid, ReasonDemoted)
		}
	}
}

func (e *elector[S]) handle(m grpcproc.Msg[proto.Message], now time.Time) {
	if m.Down != nil {
		e.down(*m.Down, now)
		return
	}
	switch b := m.Body.(type) {
	case *leaderv1.Peer:
		if m.From.Node != e.self {
			e.peer(m.From.Node, b, now)
		}
	case *leaderv1.PeerDown:
		e.peerDown(b, now)
	case *leaderv1.MemberEvent:
		e.member(b, now)
	case *leaderv1.Started:
		e.started(b, now)
	case *leaderv1.Checkpoint:
		e.checkpoint(m, b, now)
	case *leaderv1.StatusRequest:
		_ = m.Reply(e.status(), nil)
	case *leaderv1.Resign:
		if err := e.canResign(b.GetTo()); err != nil {
			_ = m.Reply(nil, err)
			return
		}
		// reconcile stops the singleton; handOver goes on once it has.
		e.resign = &resignation{m: m, to: b.GetTo(), deadline: now.Add(4 * e.spec.ElectionTimeout)}
	case *leaderv1.Cordon:
		e.cordon(m, b, now)
	default:
		if m.IsCall() {
			_ = m.Reply(nil, fmt.Errorf("leader: an elector does not answer %s", proto.MessageName(m.Body)))
		}
		e.p.Log().Warn("leader: dropped a message", "from", m.From, "type", proto.MessageName(m.Body))
	}
}

// canResign says why this node cannot hand over to, if it cannot.
func (e *elector[S]) canResign(to string) error {
	switch p := e.peers[to]; {
	case e.role != Leader:
		return ErrNotLeader
	case e.resign != nil:
		return errors.New("leader: already resigning")
	case to == "":
		return nil
	case to == e.self:
		return fmt.Errorf("leader: %s leads already", to)
	case p == nil || !p.inView:
		return fmt.Errorf("leader: %s is not in the view", to)
	case e.cordoned(to):
		return fmt.Errorf("leader: %s is cordoned", to)
	}
	return nil
}

// cordoned reports whether node may not lead.
func (e *elector[S]) cordoned(node string) bool {
	return slices.Contains(e.state.GetCordoned(), node)
}

// cordon changes who may lead, as a new version of the state, and answers
// once a majority holds it.
func (e *elector[S]) cordon(m grpcproc.Msg[proto.Message], c *leaderv1.Cordon, now time.Time) {
	node, on := c.GetNode(), !c.GetOff()
	if e.role != Leader {
		_ = m.Reply(nil, ErrNotLeader)
		return
	}
	if node != e.self && e.peers[node] == nil {
		_ = m.Reply(nil, fmt.Errorf("leader: %s takes no part in this election", node))
		return
	}
	if e.cordoned(node) == on {
		_ = m.Reply(&emptypb.Empty{}, nil)
		return
	}
	cordoned := slices.DeleteFunc(slices.Clone(e.state.GetCordoned()), func(n string) bool { return n == node })
	if on {
		cordoned = append(cordoned, node)
		if !slices.ContainsFunc(e.view(), func(n string) bool { return !slices.Contains(cordoned, n) }) {
			_ = m.Reply(nil, fmt.Errorf("leader: cordoning %s would leave no node of the view to lead", node))
			return
		}
		slices.Sort(cordoned)
	}
	e.change(m, &leaderv1.State{Value: e.state.GetValue(), Cordoned: cordoned}, now)
}

// change makes st, stamped with a new version, the state, and replicates
// it. A call is answered once a majority holds it.
func (e *elector[S]) change(m grpcproc.Msg[proto.Message], st *leaderv1.State, now time.Time) {
	seq := e.state.GetVersion().GetSeq() + 1
	st.Version = &leaderv1.Version{Term: e.term, Seq: seq}
	e.state = st
	if m.IsCall() {
		e.pending = append(e.pending, pending{seq: seq, m: m})
	}
	e.commit()
	e.heartbeatAt = now // replicate at once
}

// add starts knowing node, with a relay to its elector.
func (e *elector[S]) add(node string) *peer {
	p := &peer{node: node, inView: true, lastAck: time.Now()}
	// It fails only while the node stops.
	p.relay, _ = e.p.Spawn(relay(node, e.name), grpcproc.LinkParent(), grpcproc.WithLabel("leader relay"))
	e.peers[node] = p
	if !e.static() {
		e.greet(p, time.Now(), false)
	}
	return p
}

func (e *elector[S]) greet(p *peer, now time.Time, reply bool) {
	if !reply {
		p.greetAt = now.Add(e.spec.GhostTTL)
	}
	e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_Hello{Hello: &leaderv1.Hello{Reply: reply}}})
}

func (e *elector[S]) send(p *peer, m *leaderv1.Peer) { _ = p.relay.Send(e.p.Context(), e.p, m) }

// view is the nodes whose majority elects a leader, this one included,
// sorted.
func (e *elector[S]) view() []string {
	v := []string{e.self}
	for node, p := range e.peers {
		if p.inView {
			v = append(v, node)
		}
	}
	slices.Sort(v)
	return v
}

func (e *elector[S]) quorum() int { return len(e.view())/2 + 1 }

// inView is the peers of the view, in order.
func (e *elector[S]) inView() []*peer {
	var ps []*peer
	for _, node := range e.view() {
		if node != e.self {
			ps = append(ps, e.peers[node])
		}
	}
	return ps
}

// peer handles a message from another node's elector.
func (e *elector[S]) peer(from string, m *leaderv1.Peer, now time.Time) {
	p := e.admit(from, m, now)
	if p == nil {
		return
	}
	// A node that heard from its leader lately ignores a vote request, and
	// its term with it, unless the leader handed over: a node that was cut
	// off and campaigned alone cannot depose a leader the others still
	// hear from.
	if rv := m.GetRequestVote(); rv != nil && !rv.GetTransfer() && e.leading(now) {
		return
	}
	// A pre-vote request's term, and a granted pre-vote's, is one nobody has
	// begun; a refused pre-vote carries its voter's term.
	pre := m.GetRequestVote().GetPre() || m.GetVote().GetPre() && m.GetVote().GetGranted()
	if m.GetTerm() > e.term && !pre {
		if e.role == Leader {
			e.stepDown(now, "a newer term began")
		}
		e.term, e.votedFor, e.role, e.leader, e.preVotes = m.GetTerm(), "", Follower, "", nil
	}
	switch k := m.GetKind().(type) {
	case *leaderv1.Peer_RequestVote:
		e.vote(p, m.GetTerm(), k.RequestVote, now)
	case *leaderv1.Peer_Vote:
		if st := k.Vote.GetState(); st != nil && e.role != Leader && older(e.state.GetVersion(), st.GetVersion()) {
			e.state = st // refused for holding older state: now it does not
		}
		switch {
		case !k.Vote.GetGranted():
		case pre && e.preVotes != nil && m.GetTerm() == e.term+1:
			e.preVotes[from] = true
			if counted(e.view(), e.preVotes) >= e.quorum() {
				e.campaign(now, false)
			}
		case !pre && e.role == Candidate && m.GetTerm() == e.term:
			e.votes[from] = true
			if counted(e.view(), e.votes) >= e.quorum() {
				e.becomeLeader(now)
			}
		}
	case *leaderv1.Peer_Heartbeat:
		e.heartbeat(p, m.GetTerm(), k.Heartbeat, now)
	case *leaderv1.Peer_Ack:
		if e.role == Leader && m.GetTerm() == e.term {
			p.lastAck, p.acked = now, k.Ack.GetVersion()
			e.commit()
			e.handOver(now, false)
		}
	case *leaderv1.Peer_TimeoutNow:
		if m.GetTerm() == e.term && from == e.leader && e.role == Follower && !e.cordoned(e.self) {
			e.campaign(now, true)
		}
	}
}

// admit is the peer from, which it has just heard from, or nil if it is not
// one to listen to: a node that is not a voter, or, with Membership, one
// that Membership has not reported up (nor Peers named).
func (e *elector[S]) admit(from string, m *leaderv1.Peer, now time.Time) *peer {
	p := e.peers[from]
	switch {
	case p == nil && (e.static() || e.spec.Membership != nil):
		return nil
	case p == nil:
		p = e.add(from)
	case !p.inView && e.spec.Membership != nil && !p.declared:
		return nil
	case !p.inView:
		p.inView, p.lastAck = true, now
	}
	if h := m.GetHello(); h != nil && !h.GetReply() {
		e.greet(p, now, true)
	}
	p.ghostSince = time.Time{}
	if p.down {
		p.down = false
		_ = p.relay.Send(e.p.Context(), e.p, &leaderv1.Watch{})
	}
	return p
}

// leading reports whether this node is the leader, or heard from one within
// an election timeout.
func (e *elector[S]) leading(now time.Time) bool {
	return e.role == Leader || e.leader != "" && now.Sub(e.heard) < e.spec.ElectionTimeout
}

func (e *elector[S]) vote(p *peer, term uint64, rv *leaderv1.RequestVote, now time.Time) {
	behind := older(rv.GetVersion(), e.state.GetVersion())
	eligible := !behind && !e.cordoned(p.node)
	v := &leaderv1.Vote{Pre: rv.GetPre()}
	if behind {
		v.State = e.state
	}
	if rv.GetPre() {
		// Would it vote, were the term to begin? Nothing changes here. A
		// yes is for that term; a no carries this node's, which a
		// candidate that restarted, and knows no term, takes.
		v.Granted = eligible && term > e.term
		if !v.Granted {
			term = e.term
		}
		e.send(p, &leaderv1.Peer{Term: term, Kind: &leaderv1.Peer_Vote{Vote: v}})
		return
	}
	v.Granted = eligible && term == e.term && (e.votedFor == "" || e.votedFor == p.node) && !now.Before(e.quietUntil)
	if v.Granted {
		e.votedFor = p.node
		e.electionAt = now.Add(e.timeout())
	}
	e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_Vote{Vote: v}})
}

func (e *elector[S]) heartbeat(p *peer, term uint64, hb *leaderv1.Heartbeat, now time.Time) {
	if term == e.term && e.role != Leader {
		e.role, e.leader, e.heard, e.preVotes = Follower, p.node, now, nil
		e.electionAt = now.Add(e.timeout())
		if st := hb.GetState(); st != nil {
			e.state = st
		}
	}
	// An older term's leader learns of this one from the answer.
	e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_Ack{Ack: &leaderv1.Ack{Version: e.state.GetVersion()}}})
}

// older reports whether a is an older state version than b.
func older(a, b *leaderv1.Version) bool {
	return a.GetTerm() < b.GetTerm() || a.GetTerm() == b.GetTerm() && a.GetSeq() < b.GetSeq()
}

// counted is how many of view have a vote in votes.
func counted(view []string, votes map[string]bool) int {
	n := 0
	for _, node := range view {
		if votes[node] {
			n++
		}
	}
	return n
}

// preVote asks the view whether it would elect this node, before it
// campaigns: see RequestVote.pre.
func (e *elector[S]) preVote(now time.Time) {
	e.preVotes = map[string]bool{e.self: true}
	if counted(e.view(), e.preVotes) >= e.quorum() {
		e.campaign(now, false)
		return
	}
	rv := &leaderv1.RequestVote{Version: e.state.GetVersion(), Pre: true}
	for _, p := range e.inView() {
		e.send(p, &leaderv1.Peer{Term: e.term + 1, Kind: &leaderv1.Peer_RequestVote{RequestVote: rv}})
	}
}

func (e *elector[S]) campaign(now time.Time, transfer bool) {
	e.term++
	e.votedFor, e.role, e.leader, e.preVotes = e.self, Candidate, "", nil
	e.votes = map[string]bool{e.self: true}
	e.electionAt = now.Add(e.timeout())
	if counted(e.view(), e.votes) >= e.quorum() {
		e.becomeLeader(now)
		return
	}
	rv := &leaderv1.RequestVote{Version: e.state.GetVersion(), Transfer: transfer}
	for _, p := range e.inView() {
		e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_RequestVote{RequestVote: rv}})
	}
}

func (e *elector[S]) becomeLeader(now time.Time) {
	e.role, e.leader, e.votes = Leader, e.self, nil
	// The state is this term's now, newer than any an older leader made
	// and a follower might still hold: they all take it.
	e.state = &leaderv1.State{Version: &leaderv1.Version{Term: e.term, Seq: e.state.GetVersion().GetSeq() + 1}, Value: e.state.GetValue(), Cordoned: e.state.GetCordoned()}
	for _, p := range e.peers {
		p.lastAck, p.acked, p.sentVersion = now, nil, nil
	}
	e.p.Log().Info("leader: elected", "cluster", e.spec.Cluster, "term", e.term)
	e.tick(now)
}

// tick is a leader's heartbeat: it steps down if it has not heard from a
// majority lately, and asserts itself otherwise, sending the state to the
// followers that do not hold it.
func (e *elector[S]) tick(now time.Time) {
	e.heartbeatAt = now.Add(e.spec.HeartbeatInterval)
	stale := 2 * e.spec.ElectionTimeout
	reached := 1
	for _, p := range e.inView() {
		if now.Sub(p.lastAck) < stale {
			reached++
		}
	}
	switch {
	case !e.static() && len(e.view()) < e.spec.MinClusterSize:
		e.stepDown(now, "the view is smaller than MinClusterSize")
		return
	case reached < e.quorum():
		e.stepDown(now, "a majority has not answered")
		return
	}
	for _, p := range e.inView() {
		hb := &leaderv1.Heartbeat{}
		if v := e.state.GetVersion(); !proto.Equal(p.acked, v) && (!proto.Equal(p.sentVersion, v) || now.Sub(p.sent) >= e.spec.ElectionTimeout) {
			hb.State, p.sentVersion, p.sent = e.state, v, now
		}
		e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_Heartbeat{Heartbeat: hb}})
	}
}

// stepDown makes a leader a follower: its checkpoints still waiting fail,
// and a Resign waiting is answered, as leadership moved on.
func (e *elector[S]) stepDown(now time.Time, why string) {
	e.role, e.leader = Follower, ""
	e.electionAt = now.Add(e.timeout())
	for _, pc := range e.pending {
		_ = pc.m.Reply(nil, ErrNotLeader)
	}
	e.pending = nil
	if r := e.resign; r != nil {
		e.resign = nil
		_ = r.m.Reply(&emptypb.Empty{}, nil)
	}
	e.p.Log().Info("leader: stepped down", "cluster", e.spec.Cluster, "term", e.term, "why", why)
}

// failed gives up leadership after the singleton could not start or exited
// by itself, and holds off campaigning for a backoff that doubles with each
// failure in a row.
func (e *elector[S]) failed(now time.Time, reason string) {
	e.fails++
	backoff := e.spec.ElectionTimeout << min(e.fails, 6)
	e.backoffUntil = now.Add(backoff)
	e.p.Log().Warn("leader: the singleton failed", "cluster", e.spec.Cluster, "term", e.term, "reason", reason, "backoff", backoff)
	if e.role == Leader {
		e.stepDown(now, "the singleton failed")
	}
}

// commit answers the checkpoints a majority now holds.
func (e *elector[S]) commit() {
	v := e.state.GetVersion()
	for len(e.pending) > 0 {
		pc := e.pending[0]
		held := 1
		for _, p := range e.inView() {
			if p.acked.GetTerm() == v.GetTerm() && p.acked.GetSeq() >= pc.seq {
				held++
			}
		}
		if held < e.quorum() {
			return
		}
		_ = pc.m.Reply(&emptypb.Empty{}, nil)
		e.pending = e.pending[1:]
	}
}

func (e *elector[S]) checkpoint(m grpcproc.Msg[proto.Message], c *leaderv1.Checkpoint, now time.Time) {
	if e.role != Leader || c.GetTerm() != e.term {
		if m.IsCall() {
			_ = m.Reply(nil, ErrNotLeader)
		}
		return
	}
	e.change(m, &leaderv1.State{Value: c.GetState(), Cordoned: e.state.GetCordoned()}, now)
}

// handOver goes on with a Resign: once the singleton has stopped, and the
// follower with the latest state holds this leader's (or force says not to
// wait any longer), it tells the follower to campaign, and steps down.
func (e *elector[S]) handOver(now time.Time, force bool) {
	r := e.resign
	if r == nil || e.single.phase != idle && !force {
		return
	}
	var next *peer
	for _, p := range e.inView() {
		if (r.to == "" || p.node == r.to) && !e.cordoned(p.node) && now.Sub(p.lastAck) < 2*e.spec.ElectionTimeout && (next == nil || older(next.acked, p.acked)) {
			next = p
		}
	}
	if next != nil && !force && !proto.Equal(next.acked, e.state.GetVersion()) {
		return // the heartbeats bring it up to date
	}
	e.resign = nil
	e.backoffUntil = now.Add(2 * e.spec.ElectionTimeout)
	var err error
	if next == nil {
		err = ErrNoSuccessor
	} else {
		e.send(next, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_TimeoutNow{TimeoutNow: &leaderv1.TimeoutNow{}}})
	}
	e.stepDown(now, "resigned")
	_ = r.m.Reply(&emptypb.Empty{}, err)
}

// start starts the singleton for this term, from a process of its own:
// Confirm and the singleton's start may take their time, and heartbeats
// must go on meanwhile.
func (e *elector[S]) start(now time.Time) {
	state, err := decode[S](e.state.GetValue())
	if err != nil {
		e.failed(now, err.Error())
		return
	}
	term, n, top := e.term, e.p.Node(), e.p.Parent()
	lease := &Lease[S]{n: n, elector: e.p.PID(), term: term, log: e.p.Log()}
	confirm, build := e.spec.Confirm, e.spec.Singleton
	e.single = singleton{phase: starting, term: term}
	// It fails only while the node stops.
	_, _ = e.p.Spawn(func(h *grpcproc.Process[proto.Message]) error {
		started := &leaderv1.Started{Term: term}
		pid, err := startSingleton(h.Context(), h, top, term, confirm, func() (actor.ChildSpec, error) { return build(lease, state) })
		if err != nil {
			started.Error = err.Error()
		} else {
			started.Pid = pid.Proto()
		}
		return h.SendTo(h.Parent(), started)
	}, grpcproc.LinkParent(), grpcproc.WithLabel("leader starter"))
}

func startSingleton(ctx context.Context, from grpcproc.Caller, top grpcproc.PID, term uint64, confirm func(context.Context, uint64) error, build func() (actor.ChildSpec, error)) (grpcproc.PID, error) {
	if confirm != nil {
		if err := confirm(ctx, term); err != nil {
			return grpcproc.PID{}, fmt.Errorf("leader: Confirm: %w", err)
		}
	}
	child, err := build()
	if err != nil {
		return grpcproc.PID{}, fmt.Errorf("leader: Singleton: %w", err)
	}
	return actor.StartChild(ctx, from, top, child.WithRestart(actor.Temporary).WithSignificant(false))
}

func (e *elector[S]) started(b *leaderv1.Started, now time.Time) {
	if b.GetError() != "" {
		e.single = singleton{}
		e.failed(now, b.GetError())
		return
	}
	pid := grpcproc.PIDFromProto(b.GetPid())
	// reconcile stops it at once if the term it was started for is over.
	e.single = singleton{phase: running, term: b.GetTerm(), pid: pid, ref: e.p.Monitor(pid)}
}

func (e *elector[S]) down(d grpcproc.Down, now time.Time) {
	switch {
	case d.Ref == e.bridge:
		e.bridge, e.bridgeAt = grpcproc.Ref{}, now.Add(e.spec.GhostTTL)
		e.p.Log().Warn("leader: the Membership watch ended; watching again later", "reason", d.Reason)
	case d.Ref == e.single.ref && e.single.phase == running:
		e.single = singleton{}
		e.failed(now, "the singleton exited: "+d.Reason)
	case d.Ref == e.single.ref:
		e.single, e.fails = singleton{}, 0
		e.handOver(now, false)
	}
}

// peerDown handles a relay's news that its peer's elector is gone.
func (e *elector[S]) peerDown(b *leaderv1.PeerDown, now time.Time) {
	p := e.peers[b.GetNode()]
	if p == nil {
		return // not from a relay of this elector's
	}
	p.down = true
	if b.GetReason() == grpcproc.ReasonNoConnection {
		// Maybe cut off, maybe gone: it counts, for now.
		if p.ghostSince.IsZero() {
			p.ghostSince = now
		}
		return
	}
	// Its elector exited, or it never ran one.
	if !e.static() {
		p.inView = false
		p.greetAt = now.Add(e.spec.GhostTTL)
	}
	if p.node == e.leader {
		e.leader = ""
		e.electionAt = now.Add(rand.N(e.spec.ElectionTimeout))
	}
}

func (e *elector[S]) member(b *leaderv1.MemberEvent, now time.Time) {
	if b.GetNode() == e.self {
		return
	}
	p := e.peers[b.GetNode()]
	switch {
	case !b.GetUp():
		if p != nil {
			p.inView, p.declared, p.ghostSince = false, false, time.Time{}
		}
	case p == nil:
		e.add(b.GetNode()).declared = true
	default:
		p.declared = true
		if !p.inView {
			p.inView, p.ghostSince, p.lastAck = true, time.Time{}, now
			e.greet(p, now, false)
		}
	}
}

// shown is the role to report: Unclustered for a follower whose view is too
// small to elect anyone.
func (e *elector[S]) shown() Role {
	if e.role == Follower && !e.static() && len(e.view()) < e.spec.MinClusterSize {
		return Unclustered
	}
	return e.role
}

func (e *elector[S]) status() *leaderv1.Status {
	st := &leaderv1.Status{
		Role:     e.shown().String(),
		Term:     e.term,
		Leader:   e.leader,
		View:     e.view(),
		Quorum:   uint32(e.quorum()),
		Version:  e.state.GetVersion(),
		Cordoned: e.state.GetCordoned(),
	}
	if e.single.phase == running {
		st.Singleton = e.single.pid.Proto()
	}
	return st
}

// inspect publishes what the elector believes: its role and term, the
// leader, the view and who in it cannot be reached, the state's version,
// and the singleton.
func (e *elector[S]) inspect() map[string]string {
	v := e.state.GetVersion()
	out := map[string]string{
		"role":      e.shown().String(),
		"term":      strconv.FormatUint(e.term, 10),
		"leader":    e.leader,
		"voted_for": e.votedFor,
		"view":      strings.Join(e.view(), ","),
		"quorum":    strconv.Itoa(e.quorum()),
		"state":     strconv.FormatUint(v.GetTerm(), 10) + "." + strconv.FormatUint(v.GetSeq(), 10),
		"singleton": phases[e.single.phase],
	}
	if e.single.phase == running {
		out["singleton"] = e.single.pid.String()
	}
	var unreachable []string
	for _, p := range e.inView() {
		if p.down || !p.ghostSince.IsZero() {
			unreachable = append(unreachable, p.node)
		}
	}
	if len(unreachable) > 0 {
		out["unreachable"] = strings.Join(unreachable, ",")
	}
	if len(e.pending) > 0 {
		out["checkpoints_waiting"] = strconv.Itoa(len(e.pending))
	}
	if c := e.state.GetCordoned(); len(c) > 0 {
		out["cordoned"] = strings.Join(c, ",")
	}
	if now := time.Now(); now.Before(e.backoffUntil) {
		out["backoff"] = e.backoffUntil.Sub(now).Round(time.Millisecond).String()
	}
	return out
}

// relay carries its elector's messages to the elector of node, and
// monitors it: a send or a monitor that waits for a dial to a node that
// does not answer holds the relay, not the elector.
func relay(node, name string) func(*grpcproc.Process[proto.Message]) error {
	to := grpcproc.Name{Node: node, Name: name}
	return func(p *grpcproc.Process[proto.Message]) error {
		ref := p.Monitor(to)
		for {
			m, err := p.Receive()
			switch {
			case err != nil:
				return err
			case m.Down != nil:
				if m.Down.Ref == ref {
					_ = p.SendTo(p.Parent(), &leaderv1.PeerDown{Node: node, Reason: m.Down.Reason})
				}
			default:
				if _, ok := m.Body.(*leaderv1.Watch); ok {
					ref = p.Monitor(to)
					continue
				}
				_ = p.SendTo(to, m.Body)
			}
		}
	}
}

// bridge turns what membership reports into messages to its elector.
func bridge(membership grpcproc.Membership) func(*grpcproc.Process[proto.Message]) error {
	return func(p *grpcproc.Process[proto.Message]) error {
		events, err := membership.Watch(p.Context())
		if err != nil {
			return err
		}
		for ev := range events {
			_ = p.SendTo(p.Parent(), &leaderv1.MemberEvent{Node: ev.Member.Name, Up: ev.Up})
		}
		return p.Context().Err()
	}
}
