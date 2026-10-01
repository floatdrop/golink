// Package grpcproc gives goroutines Erlang-style network transparency on top of
// the gRPC server a service already runs. A process is addressed by a PID or
// a name; Send, Call, Monitor, Link and Exit work the same whether the target
// lives in this binary or on another node.
package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// PID identifies a process anywhere in the cluster. It is a plain comparable
// value that can travel inside messages.
type PID struct {
	Node        string
	Incarnation uint64
	ID          uint64
}

func (p PID) String() string {
	return "<" + p.Node + "." + strconv.FormatUint(p.Incarnation, 10) + "." + strconv.FormatUint(p.ID, 10) + ">"
}

// IsZero reports whether p is the zero PID.
func (p PID) IsZero() bool { return p == PID{} }

// Proto returns p as a grpcproc.v1.PID, for a PID that travels inside an
// application's own messages; nil for the zero PID.
func (p PID) Proto() *grpcprocv1.PID {
	if p.IsZero() {
		return nil
	}
	return &grpcprocv1.PID{Node: p.Node, Incarnation: p.Incarnation, Id: p.ID}
}

// PIDFromProto returns the PID a grpcproc.v1.PID holds: the zero PID for nil.
func PIDFromProto(p *grpcprocv1.PID) PID {
	return PID{Node: p.GetNode(), Incarnation: p.GetIncarnation(), ID: p.GetId()}
}

func (p PID) target() (PID, string) { return p, "" }

// Name addresses a process by the name it registered on a node.
type Name struct {
	Node string
	Name string
}

func (n Name) String() string        { return "{" + n.Name + "@" + n.Node + "}" }
func (n Name) target() (PID, string) { return PID{Node: n.Node}, n.Name }

// Target is anything a message can be addressed to: a PID, a Name, or an Addr.
type Target interface {
	target() (pid PID, name string)
}

// Ref identifies a monitor. It is unique per watching node.
type Ref struct {
	Node string
	ID   uint64
}

func (r Ref) String() string { return "#" + r.Node + "." + strconv.FormatUint(r.ID, 10) }

// Down is delivered to a watcher when the process it monitors exits, or when
// that process's node becomes unreachable.
type Down struct {
	Ref Ref
	// PID is the process that exited. For a monitor placed by name, when no
	// process held the name or its node became unreachable, which process
	// it would have been is not known: PID has only its Node, and Name says
	// what was monitored.
	PID    PID
	Name   string // the registered or global name, when the monitor was placed by one
	Reason string
}

// Exited is what a process that traps exits (see Process.SetTrapExit)
// receives when a process it is linked to exits, or that process's node
// becomes unreachable; a process that does not trap exits ends instead. See
// Process.Link.
type Exited struct {
	// PID is the process that exited. For a link placed by name, when no
	// process held the name or its node became unreachable, which process
	// it would have been is not known: PID has only its Node, and Name says
	// what was linked to.
	PID    PID
	Name   string // the registered or global name, when the link was placed by one
	Reason string
}

// Metadata is propagated with every message, unchanged by grpcproc: trace
// context, tenant, anything the application's interceptors would carry.
type Metadata map[string]string

// Exit reasons used by grpcproc itself. Applications use any string.
const (
	ReasonNormal       = "normal"
	ReasonNoProc       = "noproc"
	ReasonNoConnection = "noconnection"
	ReasonShutdown     = "shutdown"
	ReasonKilled       = "killed"
	ReasonType         = "type"
	ReasonDenied       = "denied"        // a dead letter a peer's Policy refused
	ReasonNameLost     = "name lost"     // a global name's claim was lost (see Process.Claim)
	ReasonNameConflict = "name conflict" // a KeepOnLoss claim found its name taken (see KeepOnLoss)
)

var (
	ErrNoProc       = errors.New("grpcproc: no such process")
	ErrNoConnection = errors.New("grpcproc: no connection to node")
	ErrLinkBusy     = errors.New("grpcproc: link queue full") // see Config.MaxQueued
	ErrNameTaken    = errors.New("grpcproc: name already registered")
	ErrNotLocal     = errors.New("grpcproc: pid does not belong to this node")
	ErrNodeStopped  = errors.New("grpcproc: node stopped")
	ErrNotCall      = errors.New("grpcproc: message is not a call")
	ErrType         = errors.New("grpcproc: process does not accept this message type")
)

// RemoteError is the error a Call handler returned, carried back to the
// caller. Only its text crosses the wire, so errors.Is matches it by its
// text: a handler that answers with a sentinel error, one of grpcproc's or
// its own, gives the caller an error that is that sentinel to errors.Is.
type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return e.Msg }

// Is reports whether target has the error's text; see RemoteError.
func (e *RemoteError) Is(target error) bool { return e.Msg == target.Error() }

// ExitError is the cause of a process's context when it was asked to exit,
// or a process it is linked to exited.
type ExitError struct{ Reason string }

func (e *ExitError) Error() string { return "grpcproc: exit: " + e.Reason }

func exitReasonOf(ctx context.Context) (string, bool) {
	if ee, ok := errors.AsType[*ExitError](context.Cause(ctx)); ok {
		return ee.Reason, true
	}
	return "", false
}

// LinkError reports that a link to a peer closed or could not open; Err says
// why.
type LinkError struct {
	Peer string
	Err  error
	// Unsent reports that the message never left this node, so sending it
	// again cannot deliver it twice: the peer could not be reached, dials to
	// it are backed off, or its link was full (ErrLinkBusy). Otherwise it
	// may have been handled: a Call whose link broke while it waited for the
	// reply.
	Unsent bool
}

func (e *LinkError) Error() string { return fmt.Sprintf("grpcproc: link to %s: %v", e.Peer, e.Err) }
func (e *LinkError) Unwrap() error { return e.Err }

// Is reports true for ErrNoConnection: every link failure is one, a full
// link's included.
func (*LinkError) Is(target error) bool { return target == ErrNoConnection }
