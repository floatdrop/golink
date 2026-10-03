package leader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// stance is where this node stands in the election. It is Role, with the
// pre-vote a follower holds before it campaigns, and the hand-over a leader
// carries out when it resigns, made stances of their own; Status shows a
// pre-candidate as the follower it still is, and a leader handing over as
// the leader it still is.
type stance uint8

const (
	follower stance = iota
	preCandidate
	candidate
	leading
	handingOver
)

func (s stance) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leading", "handing over"}[s]
}

// role is the stance as Status shows it.
func (s stance) role() Role {
	return [...]Role{Follower, Follower, Candidate, Leader, Leader}[s]
}

// turn is what the election's events carry: the elector, when, and what
// the event tells. One type for all of them, so that the hooks of every
// stance see the elector.
type turn struct {
	e        *elector
	now      time.Time
	term     uint64 // the message's term
	from     string // the node a message came from
	p        *peer  // the peer it came from, to answer it
	transfer bool   // evTimeoutNow: the leader handed over

	rv      *leaderv1.RequestVote // evVoteRequest
	state   *leaderv1.State       // evLeaderOfTerm: the state the heartbeat carries, if any
	version *leaderv1.Version     // evAck: the state the follower holds
	resign  *resignation          // evResign
	why     string                // events a leader steps down on: why
}

var (
	evElectionTimeout = fsm.Define[turn]("election timeout")
	evTimeoutNow      = fsm.Define[turn]("timeout now")
	evVoteRequest     = fsm.Define[turn]("vote request")
	evPreVoteGranted  = fsm.Define[turn]("pre-vote")
	evVoteGranted     = fsm.Define[turn]("vote")
	// evPreVoteQuorum and evVoteQuorum are fired after every vote counted:
	// their guards say whether the votes make a majority.
	evPreVoteQuorum = fsm.Define[turn]("pre-vote quorum")
	evVoteQuorum    = fsm.Define[turn]("vote quorum")
	evLeaderOfTerm  = fsm.Define[turn]("heartbeat")
	evAck           = fsm.Define[turn]("ack")
	// evNewerTerm is any message of a term newer than this node's, but a
	// pre-vote's, whose term nobody has begun.
	evNewerTerm       = fsm.Define[turn]("newer term")
	evResign          = fsm.Define[turn]("resign")
	evHandedOver      = fsm.Define[turn]("handed over")
	evLostQuorum      = fsm.Define[turn]("lost quorum")
	evSingletonFailed = fsm.Define[turn]("singleton failed")
)

var (
	errHeldOff       = errors.New("held off: a backoff, a cordon, or a view too small")
	errNotOurs       = errors.New("not from this term's leader, or this node is cordoned")
	errOtherTerm     = errors.New("of another term")
	errNoMajority    = errors.New("no majority yet")
	errResigningOnce = errors.New("leader: already resigning")

	// Two disjoint groups, inside a third: DOT draws them so.
	notLeading = fsm.NewGroup("not leading", follower, preCandidate, candidate)
	leader     = fsm.NewGroup("leader", leading, handingOver)
	anyStance  = fsm.NewGroup("any stance", follower, preCandidate, candidate, leading, handingOver)
)

// election is Raft's election, with pre-votes, check-quorum and
// leadership transfer: where this node stands, and what it does with each
// message. The internal transitions (Stay) are the messages a stance takes
// without moving: a vote request, a vote, a follower's heartbeat, a
// leader's acks.
//
// Like lifecycle, it is fired through the elector (see machines).
var election = fsm.MustNew("election",
	fsm.Initial(follower),

	// Every stance answers a vote request, and follows a newer term;
	// whether it grants the vote is the vote's own rule (see
	// elector.vote).
	fsm.FromGroup(anyStance).On(evVoteRequest).Stay().Action(answerVote),
	fsm.FromGroup(anyStance).On(evNewerTerm).To(follower),

	// Campaigning: the pre-vote, then the vote, each counted as it comes
	// and settled by a quorum event whose guard counts.
	fsm.FromGroup(notLeading).On(evElectionTimeout).To(preCandidate).
		Guard("no backoff, not cordoned, the view may elect", mayCampaign),
	fsm.From(preCandidate).On(evPreVoteGranted).Stay().
		Guard("for the term it would begin", forNextTerm).Action(countPreVote),
	fsm.From(preCandidate).On(evPreVoteQuorum).To(candidate).
		Guard("a majority of the view would elect it", preVoteMajority),
	// A pre-candidate still follows its leader, which may hand over to it.
	fsm.FromEach(follower, preCandidate).On(evTimeoutNow).To(candidate).
		Guard("sent by this term's leader, to a node not cordoned", fromOurLeader),
	fsm.From(candidate).On(evVoteGranted).Stay().
		Guard("of this term", ofThisTerm).Action(countVote),
	fsm.From(candidate).On(evVoteQuorum).To(leading).
		Guard("a majority of the view voted for it", voteMajority),

	// A heartbeat of this term makes a campaigning node a follower, and a
	// follower stays one: the group's rule, and the follower's own.
	fsm.FromGroup(notLeading).On(evLeaderOfTerm).To(follower).
		Guard("of this term", ofThisTerm).Action(follow),
	fsm.From(follower).On(evLeaderOfTerm).Stay().
		Guard("of this term", ofThisTerm).Action(follow),

	// Leading, and handing over to a successor, are both the leader.
	fsm.FromGroup(leader).On(evAck).Stay().
		Guard("of this term", ofThisTerm).Action(recordAck),
	fsm.From(leading).On(evResign).To(handingOver).
		Guard("a successor it may hand over to", maySucceed),
	fsm.From(handingOver).On(evResign).Stay().
		Guard("never: a hand-over is under way", func(context.Context, turn) error { return errResigningOnce }),
	fsm.From(handingOver).On(evHandedOver).To(follower),
	fsm.FromGroup(leader).On(evLostQuorum).To(follower),
	fsm.FromGroup(leader).On(evSingletonFailed).To(follower),

	fsm.OnEnterWith(preCandidate, askPreVotes),
	fsm.OnExitWith(preCandidate, dropPreVotes),
	fsm.OnEnterWith(candidate, campaign),
	fsm.OnExitWith(candidate, dropVotes),
	fsm.OnEnterWith(leading, lead),
	fsm.OnEnterVia(handingOver, evResign, beginHandOver),
	fsm.OnExitGroupWith(leader, stepDown),
	fsm.OnEnterVia(follower, evNewerTerm, adoptTerm),
)

// move fires ev at the elector's stance, and reports whether the stance
// took it, moving or not. A refusal is no news: a stance that does not take
// ev has nothing to do on it, and a guard that holds it back says so.
func (e *elector) move(ev fsm.Event[turn], t turn) bool {
	t.e = e
	return fire(e, e.machines.election, &e.role, ev, t)
}

// fire fires ev at st with TryFire, and reports whether it was taken. An
// error, a Save that failed in an action or a callback that wrote the
// state, ends the elector's loop.
func fire[S comparable, A any](e *elector, m *fsm.Machine[S], st *S, ev fsm.Event[A], a A) bool {
	_, fired, err := m.TryFire(e.p.Context(), st, ev, a)
	if err != nil {
		e.err = cmp.Or(e.err, err)
	}
	return fired
}

// beginResign has this node hand over, and answers m if it cannot: with
// what a guard said (already handing over, a successor it may not hand
// over to), or ErrNotLeader from a stance that takes no Resign. Taken,
// reconcile stops the singleton, and the hand-over goes on once it has.
func (e *elector) beginResign(m grpcproc.Msg[proto.Message], r *resignation, now time.Time) {
	_, err := e.machines.election.Fire(e.p.Context(), &e.role, evResign, turn{e: e, now: now, resign: r})
	_, unknown := errors.AsType[*fsm.NoTransitionError[stance]](err)
	switch ge, refused := errors.AsType[*fsm.GuardError[stance]](err); {
	case refused:
		_ = m.Reply(nil, ge.Err)
	case unknown:
		_ = m.Reply(nil, ErrNotLeader)
	default:
		// Taken; or a callback wrote the state, which ends the loop, as in fire.
		e.err = cmp.Or(e.err, err)
	}
}

func ofThisTerm(_ context.Context, t turn) error {
	if t.term != t.e.term {
		return errOtherTerm
	}
	return nil
}

func forNextTerm(_ context.Context, t turn) error {
	if t.term != t.e.term+1 {
		return errOtherTerm
	}
	return nil
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

func preVoteMajority(_ context.Context, t turn) error { return t.e.majority(t.e.preVotes) }

func voteMajority(_ context.Context, t turn) error { return t.e.majority(t.e.votes) }

// majority says whether votes hold a majority of the view.
func (e *elector) majority(votes map[string]bool) error {
	if v := e.view(); counted(v, votes) < len(v)/2+1 {
		return errNoMajority
	}
	return nil
}

// maySucceed checks the successor a Resign names, if it names one.
func maySucceed(_ context.Context, t turn) error {
	e, to := t.e, t.resign.to
	switch p := e.peers[to]; {
	case to == "":
		return nil
	case to == e.self:
		return fmt.Errorf("leader: %s leads already", to)
	case p == nil || !p.standing.counts():
		return fmt.Errorf("leader: %s is not in the view", to)
	case e.cordoned(to):
		return fmt.Errorf("leader: %s is cordoned", to)
	}
	return nil
}

func answerVote(_ context.Context, t turn) error {
	t.e.vote(t.p, t.term, t.rv, t.now)
	return nil
}

func countPreVote(_ context.Context, t turn) error {
	t.e.preVotes[t.from] = true
	return nil
}

func countVote(_ context.Context, t turn) error {
	t.e.votes[t.from] = true
	return nil
}

// follow takes the leader of this term: it heard from it now, so the
// election timeout starts over, and it takes the state the heartbeat
// carries.
func follow(_ context.Context, t turn) error {
	e := t.e
	e.leader, e.heard = t.from, t.now
	e.electionAt = t.now.Add(e.timeout())
	if t.state != nil {
		e.state = t.state
	}
	return nil
}

// recordAck notes what a follower holds, and answers the checkpoints a
// majority now holds.
func recordAck(_ context.Context, t turn) error {
	t.p.lastAck, t.p.acked = t.now, t.version
	t.e.commit()
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
	if e.move(evPreVoteQuorum, turn{now: t.now}) {
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
	if e.move(evVoteQuorum, turn{now: t.now}) {
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

// beginHandOver holds the Resign until the hand-over is done (see
// elector.handOver).
func beginHandOver(_ context.Context, _ fsm.Transition[stance], t turn) { t.e.resign = t.resign }

// stepDown makes a leader a follower, from leading or from handing over:
// its checkpoints still waiting fail, and a Resign waiting is answered, as
// leadership moved on.
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
