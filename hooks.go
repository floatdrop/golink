package grpcproc

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// SendInfo describes a message or call about to leave its sender.
type SendInfo struct {
	From      PID
	FromLabel string // "" when sent from outside any process
	To        PID    // To.ID is 0 when the target is a name
	ToName    string
	Body      proto.Message
	Call      bool // a Call, which ends when answered
	Remote    bool // the target is on another node
}

// ReceiveInfo describes a message a process has just taken from its mailbox.
type ReceiveInfo struct {
	PID    PID
	Label  string
	From   PID
	Body   proto.Message // nil for a Down
	Down   *Down
	Call   bool // the sender waits for a Reply
	Waited time.Duration
}

// Done ends what a hook started. For a send it runs once the message is
// handed to delivery, with the error Send returns; for a call, once the call
// returns, with its error; for a received message, when the process next
// calls Receive (nil) or exits (nil if normally, else its reason).
type Done func(err error)

// Hooks is the single tap for observability. Every method is called
// synchronously on the goroutine doing the work, so implementations must be
// cheap and must not call back into the node. Embed NopHooks and override
// what you need; combine several with JoinHooks.
//
// OnSend and OnReceive return the metadata to use from then on, which is how
// trace context is propagated: OnSend may add to what leaves, and what
// OnReceive returns is what the process's own sends inherit while it handles
// the message. They must not modify md, which may be shared; return a copy
// to change it.
type Hooks interface {
	OnSpawn(ProcessInfo)
	OnExit(info ProcessInfo, reason string)
	OnSend(s SendInfo, md Metadata) (Metadata, Done)
	OnReceive(r ReceiveInfo, md Metadata) (Metadata, Done)
	// OnDeadLetter runs when a message could not be delivered: no such
	// process (ReasonNoProc), wrong type (ReasonType), or queued on a link
	// that broke before or while writing it (ReasonNoConnection). A message
	// that could not be sent at all is not one: its sender gets the error.
	// body is nil for a message that could not be decoded.
	OnDeadLetter(from, to PID, body proto.Message, reason string)
	OnLinkUp(peer NodeID)
	OnLinkDown(peer NodeID, err error)
}

// NopHooks implements Hooks with no-ops, for embedding.
type NopHooks struct{}

func (NopHooks) OnSpawn(ProcessInfo)                                   {}
func (NopHooks) OnExit(ProcessInfo, string)                            {}
func (NopHooks) OnSend(_ SendInfo, md Metadata) (Metadata, Done)       { return md, nil }
func (NopHooks) OnReceive(_ ReceiveInfo, md Metadata) (Metadata, Done) { return md, nil }
func (NopHooks) OnDeadLetter(PID, PID, proto.Message, string)          {}
func (NopHooks) OnLinkUp(NodeID)                                       {}
func (NopHooks) OnLinkDown(NodeID, error)                              {}

// JoinHooks combines several Hooks into one. Metadata passes through them in
// order, each seeing what the previous returned; Done functions run in
// reverse order, so the first hook's span encloses the others'. Nil entries
// are skipped; JoinHooks of none is nil.
func JoinHooks(hs ...Hooks) Hooks {
	var list joined
	for _, h := range hs {
		if h != nil {
			list = append(list, h)
		}
	}
	switch len(list) {
	case 0:
		return nil
	case 1:
		return list[0]
	}
	return list
}

type joined []Hooks

func (j joined) OnSpawn(i ProcessInfo) {
	for _, h := range j {
		h.OnSpawn(i)
	}
}

func (j joined) OnExit(i ProcessInfo, reason string) {
	for _, h := range j {
		h.OnExit(i, reason)
	}
}

func (j joined) OnSend(s SendInfo, md Metadata) (Metadata, Done) {
	var dones []Done
	for _, h := range j {
		var d Done
		md, d = h.OnSend(s, md)
		if d != nil {
			dones = append(dones, d)
		}
	}
	return md, joinDone(dones)
}

func (j joined) OnReceive(r ReceiveInfo, md Metadata) (Metadata, Done) {
	var dones []Done
	for _, h := range j {
		var d Done
		md, d = h.OnReceive(r, md)
		if d != nil {
			dones = append(dones, d)
		}
	}
	return md, joinDone(dones)
}

func (j joined) OnDeadLetter(from, to PID, body proto.Message, reason string) {
	for _, h := range j {
		h.OnDeadLetter(from, to, body, reason)
	}
}

func (j joined) OnLinkUp(peer NodeID) {
	for _, h := range j {
		h.OnLinkUp(peer)
	}
}

func (j joined) OnLinkDown(peer NodeID, err error) {
	for _, h := range j {
		h.OnLinkDown(peer, err)
	}
}

func joinDone(dones []Done) Done {
	switch len(dones) {
	case 0:
		return nil
	case 1:
		return dones[0]
	}
	return func(err error) {
		for i := len(dones) - 1; i >= 0; i-- {
			dones[i](err)
		}
	}
}

// NodeID names one incarnation of a node.
type NodeID struct {
	Name        string
	Incarnation uint64
}

func (n NodeID) String() string { return n.Name + "#" + itoa(n.Incarnation) }
