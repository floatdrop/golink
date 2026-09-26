package golink

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// Hooks is the single tap for observability. Every method is called
// synchronously on the goroutine doing the work, so implementations must be
// cheap and must not call back into the node. All methods are optional: embed
// NopHooks and override what you need. Metrics, tracing, dead-letter logging
// and event streams are all built on this interface, outside the core.
type Hooks interface {
	OnSpawn(ProcessInfo)
	OnExit(info ProcessInfo, reason string)
	// OnSend runs for local and remote sends and calls, before delivery.
	OnSend(from, to PID, body proto.Message, md Metadata)
	// OnReceive runs when a process takes a message from its mailbox; waited
	// is how long the message sat there.
	OnReceive(pid PID, body proto.Message, waited time.Duration)
	// OnDeadLetter runs when a message could not be delivered: no such
	// process (ReasonNoProc), wrong type (ReasonType), node unreachable
	// (ReasonNoConnection). body is nil for a Down that had nowhere to go.
	OnDeadLetter(from, to PID, body proto.Message, reason string)
	OnLinkUp(peer NodeID)
	OnLinkDown(peer NodeID, err error)
}

// NopHooks implements Hooks with no-ops, for embedding.
type NopHooks struct{}

func (NopHooks) OnSpawn(ProcessInfo)                          {}
func (NopHooks) OnExit(ProcessInfo, string)                   {}
func (NopHooks) OnSend(PID, PID, proto.Message, Metadata)     {}
func (NopHooks) OnReceive(PID, proto.Message, time.Duration)  {}
func (NopHooks) OnDeadLetter(PID, PID, proto.Message, string) {}
func (NopHooks) OnLinkUp(NodeID)                              {}
func (NopHooks) OnLinkDown(NodeID, error)                     {}

// NodeID names one incarnation of a node.
type NodeID struct {
	Name        string
	Incarnation uint64
}

func (n NodeID) String() string { return n.Name + "#" + itoa(n.Incarnation) }
