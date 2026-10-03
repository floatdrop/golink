package leader

import (
	"context"
	"errors"
	"time"

	"github.com/floatdrop/fsm"

	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// standing is where a peer stands with this node: whether it is in the
// view, counting towards the quorum, and whether its relay watches its
// elector, which it stops doing once that elector is gone or out of reach.
type standing uint8

const (
	// live: in the view, and watched.
	live standing = iota
	// ghost: in the view, out of reach since ghostSince; it may be back.
	ghost
	// silent: in the view, and running no elector this node can see.
	silent
	// away: out of the view, as Membership reported it, and watched.
	away
	// gone: out of the view, and not watched.
	gone
)

func (s standing) String() string {
	return [...]string{"live", "ghost", "silent", "away", "gone"}[s]
}

// news is what the peers' events carry: the elector, the peer, and when.
type news struct {
	e   *elector
	p   *peer
	now time.Time
}

var (
	// evHeard is a message from the peer's elector.
	evHeard = fsm.Define[news]("heard from")
	// evLinkLost and evElectorGone are the relay's monitor firing: with
	// noconnection, and with any other reason.
	evLinkLost    = fsm.Define[news]("link lost")
	evElectorGone = fsm.Define[news]("elector gone")
	evReportedUp  = fsm.Define[news]("reported up")
	evReportedOff = fsm.Define[news]("reported down")
	// evGreetDue and evGhostTimeout are fired at every peer on every turn:
	// their guards say whether it is time.
	evGreetDue     = fsm.Define[news]("greet")
	evGhostTimeout = fsm.Define[news]("ghost timeout")
)

var (
	errNotDue     = errors.New("not yet")
	errNotNamed   = errors.New("not reported up by Membership, nor named in Peers")
	errNotExpired = errors.New("not a ghost for GhostTTL, or Membership decides")
)

// The rules both kinds of view share: a peer in the view is live, a ghost
// or silent; a message brings it back, a broken link makes a watched peer
// a ghost, and one this node no longer watches is greeted every GhostTTL,
// to hear from it once it is back. Only a watched peer's relay reports its
// link or its elector: an unwatched one takes neither.
var viewRules = fsm.Rules(
	fsm.From(live).On(evHeard).Stay(),
	fsm.FromEach(ghost, silent).On(evHeard).To(live),
	fsm.From(live).On(evLinkLost).To(ghost),
	fsm.FromEach(ghost, silent).On(evGreetDue).Stay().
		Guard("GhostTTL since the last greeting", greetDue).Action(sendGreeting),
	fsm.OnEnterWith(ghost, lostSince),
	fsm.OnExitWith(ghost, foundAgain),
)

// fixedView is a fixed set of Voters: every one counts, whatever becomes of
// it, so none leaves the view, and one whose elector is gone is silent. An
// elector fires it, or openView, as its Spec calls for (see machines).
var fixedView = fsm.MustNew("fixed view",
	fsm.Initial(live),
	viewRules,
	fsm.From(live).On(evElectorGone).To(silent),
	fsm.OnExitGroupWith(fsm.NewGroup("unwatched", ghost, silent), watchAgain),
)

// openView is a dynamic view, from Peers, Membership and whoever talks: a
// peer whose elector is gone leaves it, as does a ghost after GhostTTL
// unless Membership decides, and Membership's reports move peers in and
// out. Away is where Membership's report put a peer, and only its report of
// it up brings it back: unnamed, it is neither heard from nor greeted.
var openView = fsm.MustNew("open view",
	fsm.Initial(live),
	viewRules,
	fsm.FromEach(live, away).On(evElectorGone).To(gone).Action(greetLater),
	fsm.From(away).On(evLinkLost).To(gone),
	fsm.From(ghost).On(evGhostTimeout).To(gone).
		Guard("a ghost for GhostTTL, with no Membership", ghostExpired),
	fsm.From(gone).On(evHeard).To(live).
		Guard("named by Peers or reported up, and not reported down since, or there is no Membership", admissible),
	fsm.From(gone).On(evGreetDue).Stay().
		Guard("named, and GhostTTL since the last greeting", namedAndDue).Action(sendGreeting),

	fsm.From(live).On(evReportedOff).To(away).Action(undeclare),
	fsm.FromEach(ghost, silent).On(evReportedOff).To(gone).Action(undeclare),
	fsm.FromEach(away, gone).On(evReportedOff).Stay().Action(undeclare),
	fsm.From(away).On(evReportedUp).To(live).Action(declare),
	fsm.From(gone).On(evReportedUp).To(silent).Action(declare),
	fsm.FromEach(live, ghost, silent).On(evReportedUp).Stay().Action(declare),

	fsm.OnEnterGroupWith(fsm.NewGroup("view", live, ghost, silent), joined),
	fsm.OnEnterVia(live, evReportedUp, greetNow),
	fsm.OnEnterVia(silent, evReportedUp, greetNow),
	fsm.OnExitGroupWith(fsm.NewGroup("unwatched", ghost, silent, gone), watchAgain),
)

// counts reports whether a peer standing so is in the view.
func (s standing) counts() bool { return s == live || s == ghost || s == silent }

// unreachable reports whether a peer standing so is in the view but out of
// reach.
func (s standing) unreachable() bool { return s == ghost || s == silent }

// tell fires ev at p's standing, and reports whether p took it.
func (e *elector) tell(p *peer, ev fsm.Event[news], now time.Time) bool {
	return fire(e, e.machines.view, &p.standing, ev, news{e: e, p: p, now: now})
}

func greetDue(_ context.Context, n news) error {
	if n.now.Before(n.p.greetAt) {
		return errNotDue
	}
	return nil
}

func namedAndDue(ctx context.Context, n news) error {
	if !n.p.declared {
		return errNotNamed
	}
	return greetDue(ctx, n)
}

func ghostExpired(_ context.Context, n news) error {
	if n.e.spec.Membership != nil || n.now.Sub(n.p.ghostSince) < n.e.spec.GhostTTL {
		return errNotExpired
	}
	return nil
}

func admissible(_ context.Context, n news) error {
	if n.e.spec.Membership != nil && !n.p.declared {
		return errNotNamed
	}
	return nil
}

func sendGreeting(_ context.Context, n news) error {
	n.e.greet(n.p, n.now, false)
	return nil
}

// greetLater holds off greeting a peer whose elector is gone for GhostTTL.
func greetLater(_ context.Context, n news) error {
	n.p.greetAt = n.now.Add(n.e.spec.GhostTTL)
	return nil
}

func declare(_ context.Context, n news) error {
	n.p.declared = true
	return nil
}

func undeclare(_ context.Context, n news) error {
	n.p.declared = false
	return nil
}

func lostSince(_ context.Context, _ fsm.Transition[standing], n news) { n.p.ghostSince = n.now }

func foundAgain(_ context.Context, _ fsm.Transition[standing], n news) { n.p.ghostSince = time.Time{} }

// joined counts a peer that enters the view as acknowledging the leader
// now, so a leader does not take it for one that stopped answering.
func joined(_ context.Context, _ fsm.Transition[standing], n news) { n.p.lastAck = n.now }

// greetNow greets a peer Membership brought into the view.
func greetNow(_ context.Context, _ fsm.Transition[standing], n news) {
	n.e.greet(n.p, n.now, false)
}

// watchAgain has the relay watch the peer's elector again, as it was heard
// from.
func watchAgain(_ context.Context, _ fsm.Transition[standing], n news) {
	_ = n.p.relay.Send(n.e.p.Context(), n.e.p, &leaderv1.Watch{})
}
