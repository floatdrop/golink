// Package leader elects one node of a cluster to run a singleton: a child
// spec that runs on the leader only, started when its node wins an election
// and told to exit when it loses. Leadership is the singleton's lifetime:
// its Init runs on becoming leader, its context ends on losing it, and no
// handler has to ask whether it still leads.
//
//	leader.Start(node, leader.Spec[*schedpb.State]{
//		Cluster: "scheduler",
//		Voters:  []string{"a", "b", "c"},
//		Singleton: func(l *leader.Lease[*schedpb.State], last *schedpb.State) (actor.ChildSpec, error) {
//			return actor.Child("scheduler", func() *Scheduler { return &Scheduler{lease: l, state: last} }), nil
//		},
//	})
//
// The election is Raft's without the log: terms, votes and heartbeats
// between one elector process per node, registered as ElectorName(cluster).
// A node leads with the votes of a majority, and steps down once it has not
// heard from a majority for two election timeouts.
//
// The singleton can carry state across leaders. Lease.Checkpoint replicates
// its state to the followers and returns once a majority holds it; a
// follower votes only for a candidate whose state is at least as recent as
// its own, so whichever node leads next starts its singleton from every
// checkpoint that returned. Resign hands over on purpose: it stops the
// singleton, brings a follower up to date and has it campaign at once.
//
// Terms, votes and state live in memory, and a cluster that loses a
// majority of its nodes at once loses them, unless Spec.Store keeps them:
// then each node saves its own before it tells anyone of them, as Raft
// does, and a cluster that restarts whole carries on from where it was
// (File keeps them in a file). Two nodes may briefly both believe they
// lead (one cut off from the others that has not noticed yet); Lease.Term
// is the fencing token that lets an external resource tell their writes
// apart, and Spec.Confirm can make leadership wait for an external lock.
package leader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// Spec describes a node's part in a cluster's election, and what runs on
// the leader. S is the singleton's state: proto.Message for any, or
// emptypb.Empty for none.
type Spec[S proto.Message] struct {
	// Cluster names the election: every node that runs one of that name
	// takes part in it. Required.
	Cluster string
	// Voters, if set, is the fixed set of nodes whose majority elects a
	// leader, this node among them. A voter that is down still counts:
	// two majorities of one set always share a node, so no partition can
	// elect two leaders.
	Voters []string
	// Peers, with Voters empty, are nodes taken to take part, from the
	// start. They and the nodes Membership reports make the view: a
	// majority of the view elects a leader. A node that runs no elector
	// for Cluster leaves the view as soon as that is known, and so does
	// one whose elector exits; one that cannot be reached stays in it for
	// GhostTTL, or until Membership reports it gone. Without Membership,
	// a node that talks to this one joins it.
	Peers []string
	// Membership, with Voters empty, reports the nodes of the grpcproc
	// cluster: each one it reports up joins the view, and each one it
	// reports down leaves it. Only those it reports up, and Peers, take
	// part. Unset, it is the node's own Config.Membership, if the node has
	// one, so the cluster the node follows is the one given once.
	Membership grpcproc.Membership
	// MinClusterSize is the smallest view, this node included, that may
	// elect a leader, with Voters empty. Default 3.
	MinClusterSize int
	// ElectionTimeout is how long a follower goes without hearing from a
	// leader before it campaigns: a random time between it and twice it.
	// Default 150ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often a leader asserts itself. It must be
	// shorter than ElectionTimeout. Default 50ms.
	HeartbeatInterval time.Duration
	// GhostTTL is how long a node of the view that cannot be reached still
	// counts, with Voters empty and no Membership. Default 5s.
	GhostTTL time.Duration
	// Store, if set, keeps this node's term, vote and state across restarts
	// of its elector and of the node (File keeps them in a file): with one
	// on every node, a cluster that restarts whole starts again from its
	// last checkpoint, and its terms go on growing. Without it they live in
	// memory. A Save that fails ends the elector, and its supervisor starts
	// it again from what the Store holds.
	Store Store
	// Confirm, if set, runs when this node wins an election, before its
	// singleton starts: an error withholds leadership, and the node steps
	// down and campaigns again only after a backoff, longer after each
	// failure in a row. It is where leadership waits for an external lock
	// (an etcd lease, a Kubernetes Lease).
	Confirm func(ctx context.Context, term uint64) error
	// Singleton builds what runs on the leader, given the state the last
	// leader checkpointed (the zero S before any has). Required. Its
	// Restart is Temporary whatever it says: a singleton that exits ends
	// its node's leadership, and runs again wherever the next election
	// says, after the same backoff as Confirm's. For restarts in place,
	// make it an actor.ChildSupervisor.
	Singleton func(l *Lease[S], state S) (actor.ChildSpec, error)
}

// ElectorName is the name a cluster's elector is registered under on
// every node that takes part in it.
func ElectorName(cluster string) string { return "leader/" + cluster }

// Start runs n's part in spec's election: a supervisor, whose opts these
// are, of the elector and, while n leads, the singleton. It returns the
// supervisor.
func Start[S proto.Message](n *grpcproc.Node, spec Spec[S], opts ...grpcproc.SpawnOption) (grpcproc.PID, error) {
	sup, err := spec.supervisor()
	if err != nil {
		return grpcproc.PID{}, err
	}
	if len(spec.Voters) > 0 && !slices.Contains(spec.Voters, n.Name()) {
		return grpcproc.PID{}, fmt.Errorf("leader: %s is not among the Voters", n.Name())
	}
	return actor.Supervise(n, sup, opts...)
}

// Child is Start for a supervisor: a child, registered as name unless name
// is empty, that runs its node's part in spec's election.
func Child[S proto.Message](name string, spec Spec[S], opts ...grpcproc.SpawnOption) (actor.ChildSpec, error) {
	sup, err := spec.supervisor()
	if err != nil {
		return actor.ChildSpec{}, err
	}
	return actor.ChildSupervisor(name, sup, opts...), nil
}

// supervisor checks s, fills in its defaults, and describes the supervisor
// that runs its elector.
func (s Spec[S]) supervisor() (actor.Spec, error) {
	s.MinClusterSize = cmp.Or(s.MinClusterSize, 3)
	s.ElectionTimeout = cmp.Or(s.ElectionTimeout, 150*time.Millisecond)
	s.HeartbeatInterval = cmp.Or(s.HeartbeatInterval, 50*time.Millisecond)
	s.GhostTTL = cmp.Or(s.GhostTTL, 5*time.Second)
	switch {
	case s.Cluster == "":
		return actor.Spec{}, errors.New("leader: a Spec needs a Cluster")
	case s.Singleton == nil:
		return actor.Spec{}, errors.New("leader: a Spec needs a Singleton")
	case len(s.Voters) > 0 && (len(s.Peers) > 0 || s.Membership != nil):
		return actor.Spec{}, errors.New("leader: Voters, or Peers and Membership, not both")
	case len(slices.Compact(slices.Sorted(slices.Values(s.Voters)))) != len(s.Voters):
		return actor.Spec{}, errors.New("leader: a node is among the Voters twice")
	case s.MinClusterSize < 1, s.ElectionTimeout < 0, s.GhostTTL < 0:
		return actor.Spec{}, errors.New("leader: a negative MinClusterSize, ElectionTimeout or GhostTTL")
	case s.HeartbeatInterval <= 0 || s.HeartbeatInterval >= s.ElectionTimeout:
		return actor.Spec{}, errors.New("leader: HeartbeatInterval must be positive and shorter than ElectionTimeout")
	}
	e := &electors[S]{spec: s}
	name := ElectorName(s.Cluster)
	return actor.Spec{
		// The singleton is the elector's: if the elector restarts, it has
		// forgotten its term, and its singleton must go.
		Strategy: actor.OneForAll,
		Children: []actor.ChildSpec{actor.ChildFunc(name, e.run, grpcproc.WithLabel("leader"), grpcproc.WithInspect(e.inspect))},
	}, nil
}

// Role is what an elector is to its cluster.
type Role uint8

const (
	// Unclustered: the view is smaller than MinClusterSize, and nobody
	// may be elected.
	Unclustered Role = iota
	// Follower: following a leader, or waiting to campaign.
	Follower
	// Candidate: campaigning.
	Candidate
	// Leader: elected, and running the singleton.
	Leader
)

var roles = [...]string{"unclustered", "follower", "candidate", "leader"}

func (r Role) String() string {
	if int(r) < len(roles) {
		return roles[r]
	}
	return "Role(" + strconv.Itoa(int(r)) + ")"
}

// An elector's answers cross nodes as text: they come back as a
// *grpcproc.RemoteError, which is the sentinel to errors.Is.
var (
	// ErrNotLeader is the answer to Checkpoint once the Lease's term is
	// over, and to Resign on a node that does not lead.
	ErrNotLeader = errors.New("leader: not the leader")
	// ErrNoSuccessor is Resign's answer on a leader with no follower to
	// hand over to (Transfer's, when its follower did not catch up): it
	// stepped down, and campaigns again after a backoff.
	ErrNoSuccessor = errors.New("leader: no follower to hand over to")
	// ErrNoLeader is the answer of Transfer, Cordon and Uncordon while the
	// node asked knows of no leader.
	ErrNoLeader = errors.New("leader: no leader is known")
)

// Info is what a node's elector believes.
type Info struct {
	Role Role
	Term uint64
	// Leader is the leader's node, or empty while none is known.
	Leader string
	// View is the nodes whose majority elects a leader, this one included,
	// sorted.
	View   []string
	Quorum int
	// Singleton is the singleton, on the leader, once it runs.
	Singleton grpcproc.PID
	// Cordoned is the nodes that may not lead, sorted.
	Cordoned []string
}

// Status asks n's elector for cluster what it believes.
func Status(ctx context.Context, n *grpcproc.Node, cluster string) (Info, error) {
	st, err := n.CallTo[*leaderv1.Status](ctx, electorOf(n, cluster), &leaderv1.StatusRequest{})
	if err != nil {
		return Info{}, err
	}
	role := slices.Index(roles[:], st.GetRole())
	return Info{
		Role:      Role(role),
		Term:      st.GetTerm(),
		Leader:    st.GetLeader(),
		View:      st.GetView(),
		Quorum:    int(st.GetQuorum()),
		Singleton: grpcproc.PIDFromProto(st.GetSingleton()),
		Cordoned:  st.GetCordoned(),
	}, nil
}

// Resign has the leader of cluster, on n, hand over: it stops its
// singleton, whose Terminate may still checkpoint, brings the follower
// with the latest state up to date, and has it campaign at once. It
// returns once the follower has been told to, with ErrNotLeader if n does
// not lead. Call it before stopping a leader's node, for the next leader to
// start from the singleton's last state.
func Resign(ctx context.Context, n *grpcproc.Node, cluster string) error {
	_, err := n.CallTo[*emptypb.Empty](ctx, electorOf(n, cluster), &leaderv1.Resign{})
	return err
}

// Transfer moves the leadership of cluster to the node to: the leader, which
// n's elector names, hands over to it as Resign does, once to holds its
// state. to must be in the leader's view, and not cordoned.
func Transfer(ctx context.Context, n *grpcproc.Node, cluster, to string) error {
	return viaLeader(ctx, n, cluster, &leaderv1.Resign{To: to})
}

// Cordon keeps node from leading cluster, until Uncordon: it campaigns no
// more, voters refuse it, and if it leads, it hands over. The leader, which
// n's elector names, records it in the state it replicates, and Cordon
// returns once a majority holds it: node stays cordoned across its own
// restarts, and across leaders. It is for taking a host out of the running,
// to work on it. A cordon that would leave no node of the view to lead is
// refused.
func Cordon(ctx context.Context, n *grpcproc.Node, cluster, node string) error {
	return viaLeader(ctx, n, cluster, &leaderv1.Cordon{Node: node})
}

// Uncordon lets node lead cluster again.
func Uncordon(ctx context.Context, n *grpcproc.Node, cluster, node string) error {
	return viaLeader(ctx, n, cluster, &leaderv1.Cordon{Node: node, Off: true})
}

// viaLeader calls the elector of the leader that n's elector knows, with
// m. A leader that changed meanwhile answers ErrNotLeader: ask again.
func viaLeader(ctx context.Context, n *grpcproc.Node, cluster string, m proto.Message) error {
	info, err := Status(ctx, n, cluster)
	if err != nil {
		return err
	}
	if info.Leader == "" {
		return ErrNoLeader
	}
	_, err = n.CallTo[*emptypb.Empty](ctx, grpcproc.Name{Node: info.Leader, Name: ElectorName(cluster)}, m)
	return err
}

func electorOf(n *grpcproc.Node, cluster string) grpcproc.Name {
	return grpcproc.Name{Node: n.Name(), Name: ElectorName(cluster)}
}

// Lease is a leader's hold on its term, as its singleton sees it.
type Lease[S proto.Message] struct {
	n       *grpcproc.Node
	elector grpcproc.PID
	term    uint64
	log     *slog.Logger
}

// Term is the term this node leads in. Terms grow with every election, so
// it is a fencing token: a resource that remembers the highest it has seen
// can refuse a deposed leader's writes.
func (l *Lease[S]) Term() uint64 { return l.term }

// Checkpoint replicates state to the cluster's other electors, and returns
// once a majority of the cluster holds it: from then on whichever node
// leads next starts its singleton from it, or from a later one. It fails
// with ErrNotLeader once the term is over.
func (l *Lease[S]) Checkpoint(ctx context.Context, state S) error {
	c, err := l.checkpoint(state)
	if err != nil {
		return err
	}
	_, err = l.n.CallTo[*emptypb.Empty](ctx, l.elector, c)
	return err
}

// Save is Checkpoint without the wait, for a caller that must not block:
// state is replicated, and the next leader starts from it if a majority
// held it by then. Once the term is over it is dropped.
func (l *Lease[S]) Save(state S) {
	c, err := l.checkpoint(state)
	if err == nil {
		err = l.n.SendTo(context.Background(), l.elector, c)
	}
	if err != nil {
		l.log.Error("leader: Save", "err", err)
	}
}

func (l *Lease[S]) checkpoint(state S) (*leaderv1.Checkpoint, error) {
	a, err := anypb.New(state)
	if err != nil {
		return nil, fmt.Errorf("leader: checkpoint: %w", err)
	}
	return &leaderv1.Checkpoint{Term: l.term, State: a}, nil
}

// decode is the state a singleton starts from: the zero S before any
// checkpoint.
func decode[S proto.Message](a *anypb.Any) (S, error) {
	var s S
	if a == nil {
		return s, nil
	}
	m, err := a.UnmarshalNew()
	if err != nil {
		return s, fmt.Errorf("leader: the checkpointed state: %w", err)
	}
	s, ok := m.(S)
	if !ok {
		return s, fmt.Errorf("leader: the checkpointed state is a %s", proto.MessageName(m))
	}
	return s, nil
}
