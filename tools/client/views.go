package client

import (
	"time"

	"github.com/floatdrop/grpcproc"
)

// NodeView is a node as grpcprocctl shows it and its MCP tools return it.
type NodeView struct {
	Name        string     `json:"name" jsonschema:"node name"`
	Incarnation uint64     `json:"incarnation,omitempty" jsonschema:"changes every time the node starts"`
	Advertise   string     `json:"advertise,omitempty" jsonschema:"address peers dial"`
	Uptime      string     `json:"uptime,omitempty"`
	Processes   int        `json:"processes"`
	Spawned     uint64     `json:"spawned"`
	Exited      uint64     `json:"exited"`
	DeadLetters uint64     `json:"dead_letters" jsonschema:"messages that could not be delivered"`
	Links       []LinkView `json:"links,omitempty"`
	Error       string     `json:"error,omitempty" jsonschema:"why the node could not be inspected"`
}

// LinkView is one direction of traffic with a peer.
type LinkView struct {
	Peer        string `json:"peer"`
	Incarnation uint64 `json:"incarnation"`
	Direction   string `json:"direction" jsonschema:"out: this node sends on it; in: the peer does"`
	State       string `json:"state"`
	Age         string `json:"age,omitempty"`
	Reconnects  uint64 `json:"reconnects"`
	Messages    uint64 `json:"messages"`
	Bytes       uint64 `json:"bytes"`
	LastError   string `json:"last_error,omitempty"`
}

// ProcessView is a process as grpcprocctl shows it and its MCP tools return it.
type ProcessView struct {
	PID           string            `json:"pid"`
	Names         []string          `json:"names,omitempty"`
	Label         string            `json:"label" jsonschema:"what metrics aggregate by; the message type by default"`
	Type          string            `json:"type" jsonschema:"Go type of the messages it accepts"`
	Parent        string            `json:"parent,omitempty" jsonschema:"the process that started it, such as its supervisor"`
	State         string            `json:"state" jsonschema:"idle (waiting in Receive), running, waiting-reply (in a Call), exiting"`
	Uptime        string            `json:"uptime"`
	Mailbox       int               `json:"mailbox" jsonschema:"messages waiting"`
	MailboxPeak   int               `json:"mailbox_peak"`
	OldestWait    string            `json:"oldest_wait,omitempty" jsonschema:"how long the oldest waiting message has waited"`
	Received      uint64            `json:"received"`
	Sent          uint64            `json:"sent"`
	CallsInFlight uint32            `json:"calls_in_flight,omitempty"`
	LastMessage   string            `json:"last_message,omitempty" jsonschema:"type of the last message it took"`
	Monitors      int               `json:"monitors,omitempty" jsonschema:"processes it watches"`
	Watchers      int               `json:"watchers,omitempty" jsonschema:"processes watching it"`
	LogLevel      string            `json:"log_level"`
	Inspect       map[string]string `json:"inspect,omitempty" jsonschema:"what the process says about itself"`
	InspectError  string            `json:"inspect_error,omitempty" jsonschema:"why inspect is empty: busy, or gone"`
}

// EventView is something that happened on a node.
type EventView struct {
	Time    string       `json:"time"`
	Kind    string       `json:"kind" jsonschema:"spawn, exit, link-up, link-down or dead-letter"`
	Missed  uint64       `json:"missed,omitempty" jsonschema:"events lost before this one"`
	Process *ProcessView `json:"process,omitempty"`
	Reason  string       `json:"reason,omitempty"`
	Peer    string       `json:"peer,omitempty"`
	Error   string       `json:"error,omitempty"`
	From    string       `json:"from,omitempty"`
	To      string       `json:"to,omitempty"`
	Type    string       `json:"type,omitempty" jsonschema:"message type of a dead letter"`
}

// Short renders a duration for people: whole seconds past a minute,
// milliseconds below.
func Short(d time.Duration) string {
	switch {
	case d <= 0:
		return ""
	case d >= time.Minute:
		return d.Round(time.Second).String()
	case d >= time.Second:
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func (c *Client) since(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return Short(c.now().Sub(t))
}

func pidString(p grpcproc.PID) string {
	if p.IsZero() {
		return ""
	}
	return p.String()
}

func (c *Client) nodeView(n grpcproc.NodeInfo) NodeView {
	v := NodeView{
		Name: n.ID.Name, Incarnation: n.ID.Incarnation, Advertise: n.Advertise,
		Uptime: c.since(n.StartedAt), Processes: n.Processes,
		Spawned: n.Spawned, Exited: n.Exited, DeadLetters: n.DeadLetters,
	}
	for _, l := range n.Links {
		dir := "in"
		if l.Outbound {
			dir = "out"
		}
		v.Links = append(v.Links, LinkView{
			Peer: l.Peer.Name, Incarnation: l.Peer.Incarnation, Direction: dir, State: l.State.String(),
			Age: c.since(l.EstablishedAt), Reconnects: l.Reconnects, Messages: l.Messages, Bytes: l.Bytes, LastError: l.LastError,
		})
	}
	return v
}

func (c *Client) processView(p grpcproc.ProcessInfo) ProcessView {
	return ProcessView{
		PID: p.PID.String(), Names: p.Names, Label: p.Label, Type: p.Type, Parent: pidString(p.Parent),
		State: p.State.String(), Uptime: c.since(p.StartedAt),
		Mailbox: p.Mailbox.Depth, MailboxPeak: p.Mailbox.Peak, OldestWait: Short(p.Mailbox.OldestAge),
		Received: p.Received, Sent: p.Sent, CallsInFlight: p.CallsInFlight, LastMessage: p.LastMessage,
		Monitors: p.Monitors, Watchers: p.Watchers, LogLevel: p.LogLevel.String(),
	}
}

func (c *Client) eventView(e grpcproc.Event) EventView {
	v := EventView{Time: e.Time.Format(time.RFC3339Nano), Kind: e.Kind.String(), Missed: e.Missed,
		Reason: e.Reason, Error: e.Err, From: pidString(e.From), To: pidString(e.To), Type: e.Type}
	if e.Kind == grpcproc.EventSpawn || e.Kind == grpcproc.EventExit {
		p := c.processView(e.Process)
		v.Process = &p
	}
	if e.Peer.Name != "" {
		v.Peer = e.Peer.String()
	}
	return v
}
