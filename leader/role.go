package leader

import (
	"context"
	"errors"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/types/known/emptypb"

	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// stance is where this node stands in the election. It is Role, with the
// pre-vote a follower holds before it campaigns made a stance of its own;
// Status shows a pre-candidate as the follower it still is.
type stance uint8

const (
	follower stance = iota
	preCandidate
	candidate
	leading
)

func (s stance) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

// role is the stance as Status shows it.
func (s stance) role() Role {
	return [...]Role{Follower, Follower, Candidate, Leader}[s]
}

// turn is what the election's events carry: the elector, when, and what
// the event tells.
type turn struct {
	e        *elector
	now      time.Time
	term     uint64 // evNewerTerm: the term that began
	transfer bool   // evTimeoutNow: the leader handed over
	from     string // evTimeoutNow: who sent it
	why      string // events a leader steps down on: why
}

var (
	evElectionTimeout = fsm.Define[turn]("election timeout")
	evPreVoteQuorum   = fsm.Define[turn]("pre-vote quorum")
	evVoteQuorum      = fsm.Define[turn]("vote quorum")
	evTimeoutNow      = fsm.Define[turn]("timeout now")
	// evLeaderOfTerm is a heartbeat from the leader of this node's term.
	evLeaderOfTerm = fsm.Define[turn]("heartbeat of this term")
	// evNewerTerm is any message of a term newer than this node's, but a
	// pre-vote's, whose term nobody has begun.
	evNewerTerm       = fsm.Define[turn]("newer term")
	evLostQuorum      = fsm.Define[turn]("lost quorum")
	evSingletonFailed = fsm.Define[turn]("singleton failed")
	evHandedOver      = fsm.Define[turn]("handed over")
)

var (
	errHeldOff = errors.New("held off: a backoff, a cordon, or a view too small")
	errNotOurs = errors.New("not from this term's leader, or this node is cordoned")
	// notLeading is the one group: Graphviz draws a state in one cluster
	// only, so overlapping groups would draw as other sets than they are.
	notLeading = fsm.NewGroup("not leading", follower, preCandidate, candidate)
)

// election is Raft's election, with pre-votes, check-quorum and
// leadership transfer: where this node stands, and what moves it. What
// each stance does with a message that moves nothing (granting a vote,
// counting one, a follower's heartbeat) is the elector's.
//
// It is built in init, for the same reason as lifecycle.
var election *fsm.Machine[stance]

func init() {
	election = fsm.MustNew("election",
		fsm.Initial(follower),
		fsm.FromGroup(notLeading).On(evElectionTimeout).To(preCandidate).
			Guard("no backoff, not cordoned, the view may elect", mayCampaign),
		fsm.From(preCandidate).On(evPreVoteQuorum).To(candidate),
		fsm.From(follower).On(evTimeoutNow).To(candidate).
			Guard("sent by this term's leader, to a node not cordoned", fromOurLeader),
		fsm.From(candidate).On(evVoteQuorum).To(leading),
		fsm.FromEach(preCandidate, candidate).On(evLeaderOfTerm).To(follower),
		fsm.From(leading).On(evLostQuorum).To(follower),
		fsm.From(leading).On(evSingletonFailed).To(follower),
		fsm.From(leading).On(evHandedOver).To(follower),
		fsm.FromGroup(notLeading).On(evNewerTerm).To(follower),
		fsm.From(leading).On(evNewerTerm).To(follower),

		fsm.OnEnterWith(preCandidate, askPreVotes),
		fsm.OnExitWith(preCandidate, dropPreVotes),
		fsm.OnEnterWith(candidate, campaign),
		fsm.OnExitWith(candidate, dropVotes),
		fsm.OnEnterWith(leading, lead),
		fsm.OnExitWith(leading, stepDown),
		fsm.OnEnterVia(follower, evNewerTerm, adoptTerm),
	)
}

// move fires ev at the elector's stance. As with live, a refusal is no
// news: a stance that does not take ev has nothing to do on it, a guard
// that holds it back says so, and a term that could not be saved ends the
// elector's loop.
func (e *elector) move(ev fsm.Event[turn], t turn) {
	t.e = e
	_, _ = election.Fire(e.p.Context(), &e.role, ev, t)
}

func mayCampaign(_ context.Context, t turn) error {
	e := t.e
	if t.now.Before(e.backoffUntil) || e.cordoned(e.self) || !e.static() && len(e.view()) < e.spec.MinClusterSize {
		return errHeldOff
	}
	return nil
}

func fromOurLeader(_ context.Context, t turn) error {
	if t.term != t.e.term || t.from != t.e.leader || t.e.cordoned(t.e.self) {
		return errNotOurs
	}
	return nil
}

// adoptTerm takes a newer term, with no vote cast in it yet. It runs on
// entry, after a leader's stepDown, which tells of the term it led in.
func adoptTerm(_ context.Context, _ fsm.Transition[stance], t turn) {
	t.e.term, t.e.votedFor, t.e.leader = t.term, "", ""
}

// askPreVotes asks the view whether it would elect this node, before it
// campaigns: see RequestVote.pre. A view of one elects it at once.
func askPreVotes(_ context.Context, _ fsm.Transition[stance], t turn) {
	e := t.e
	e.preVotes = map[string]bool{e.self: true}
	if counted(e.view(), e.preVotes) >= e.quorum() {
		e.move(evPreVoteQuorum, turn{now: t.now})
		return
	}
	rv := &leaderv1.RequestVote{Version: e.state.GetVersion(), Pre: true}
	for _, p := range e.inView() {
		e.send(p, &leaderv1.Peer{Term: e.term + 1, Kind: &leaderv1.Peer_RequestVote{RequestVote: rv}})
	}
}

func dropPreVotes(_ context.Context, _ fsm.Transition[stance], t turn) { t.e.preVotes = nil }

// campaign begins a term, with this node's vote for itself, and asks the
// view for theirs. Nothing tells of the term before it is saved (see
// elector.save), so it needs no Save of its own. A view of one elects it at
// once.
func campaign(_ context.Context, _ fsm.Transition[stance], t turn) {
	e := t.e
	e.term++
	e.votedFor, e.leader = e.self, ""
	e.votes = map[string]bool{e.self: true}
	e.electionAt = t.now.Add(e.timeout())
	if counted(e.view(), e.votes) >= e.quorum() {
		e.move(evVoteQuorum, turn{now: t.now})
		return
	}
	rv := &leaderv1.RequestVote{Version: e.state.GetVersion(), Transfer: t.transfer}
	for _, p := range e.inView() {
		e.send(p, &leaderv1.Peer{Term: e.term, Kind: &leaderv1.Peer_RequestVote{RequestVote: rv}})
	}
}

func dropVotes(_ context.Context, _ fsm.Transition[stance], t turn) { t.e.votes = nil }

// lead takes up leadership: the state is this term's now, newer than any
// an older leader made and a follower might still hold, so they all take
// it; and the first heartbeat goes out at once.
func lead(_ context.Context, _ fsm.Transition[stance], t turn) {
	e := t.e
	e.leader = e.self
	e.state = &leaderv1.State{Version: &leaderv1.Version{Term: e.term, Seq: e.state.GetVersion().GetSeq() + 1}, Value: e.state.GetValue(), Cordoned: e.state.GetCordoned()}
	for _, p := range e.peers {
		p.lastAck, p.acked, p.sentVersion = t.now, nil, nil
	}
	e.p.Log().Info("leader: elected", "cluster", e.spec.Cluster, "term", e.term)
	e.tick(t.now)
}

// stepDown makes a leader a follower: its checkpoints still waiting fail,
// and a Resign waiting is answered, as leadership moved on.
func stepDown(_ context.Context, _ fsm.Transition[stance], t turn) {
	e := t.e
	e.leader = ""
	e.electionAt = t.now.Add(e.timeout())
	for _, pc := range e.pending {
		_ = pc.m.Reply(nil, ErrNotLeader)
	}
	e.pending = nil
	if r := e.resign; r != nil {
		e.resign = nil
		_ = r.m.Reply(&emptypb.Empty{}, nil)
	}
	e.p.Log().Info("leader: stepped down", "cluster", e.spec.Cluster, "term", e.term, "why", t.why)
}
