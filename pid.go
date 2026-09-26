// Package golink gives goroutines Erlang-style network transparency on top of
// the gRPC server a service already runs. A process is addressed by a PID or
// a name; Send, Call, Monitor and Exit work the same whether the target lives
// in this binary or on another node.
package golink

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	// PID is the process that exited. For a monitor placed by name on a
	// node that became unreachable, which process held the name is not
	// known: PID has only its Node, and Name says what was monitored.
	PID    PID
	Name   string // the registered name, when the monitor was placed by name
	Reason string
}

// Metadata is propagated with every message, unchanged by golink: trace
// context, tenant, anything the application's interceptors would carry.
type Metadata map[string]string

// Exit reasons used by golink itself. Applications use any string.
const (
	ReasonNormal       = "normal"
	ReasonNoProc       = "noproc"
	ReasonNoConnection = "noconnection"
	ReasonShutdown     = "shutdown"
	ReasonKilled       = "killed"
	ReasonType         = "type"
)

var (
	ErrNoProc       = errors.New("golink: no such process")
	ErrNoConnection = errors.New("golink: no connection to node")
	ErrNameTaken    = errors.New("golink: name already registered")
	ErrNotLocal     = errors.New("golink: pid does not belong to this node")
	ErrNodeStopped  = errors.New("golink: node stopped")
	ErrNotCall      = errors.New("golink: message is not a call")
	ErrType         = errors.New("golink: process does not accept this message type")
)

// RemoteError is the error a Call handler returned, carried back to the caller.
type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return e.Msg }

// ExitError is the cause of a process's context when it was asked to exit.
type ExitError struct{ Reason string }

func (e *ExitError) Error() string { return "golink: exit: " + e.Reason }

func exitReasonOf(ctx context.Context) (string, bool) {
	if ee, ok := errors.AsType[*ExitError](context.Cause(ctx)); ok {
		return ee.Reason, true
	}
	return "", false
}

// LinkError wraps the transport error that closed a link to a peer.
type LinkError struct {
	Peer string
	Err  error
}

func (e *LinkError) Error() string { return fmt.Sprintf("golink: link to %s: %v", e.Peer, e.Err) }
func (e *LinkError) Unwrap() error { return e.Err }

// Is reports true for ErrNoConnection: every link failure is one.
func (*LinkError) Is(target error) bool { return target == ErrNoConnection }
