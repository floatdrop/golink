package grpcproc

import (
	"log/slog"
	"strconv"
	"time"
)

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

// ProcessState is where a process is in its loop.
type ProcessState uint8

const (
	StateIdle         ProcessState = iota // blocked in Receive
	StateRunning                          // between Receive calls
	StateWaitingReply                     // blocked in Call
	StateExiting
)

var processStates = [...]string{"idle", "running", "waiting-reply", "exiting"}

func (s ProcessState) String() string { return enumName(processStates[:], int(s), "ProcessState") }

// MailboxInfo is a snapshot of a mailbox.
type MailboxInfo struct {
	Depth int
	Peak  int
	// OldestAge is how long the oldest queued message has waited, 0 when
	// empty. It is read from when its batch started queueing: exact for the
	// first message of a burst, an upper bound for later ones.
	OldestAge time.Duration
}

// ProcessInfo is a snapshot of one process, the same for local inspection,
// the Inspector service and any tool built on either.
type ProcessInfo struct {
	PID           PID
	Name          string // WithName; "" for an unnamed process
	Label         string // WithLabel; the low-cardinality key for metrics
	Type          string // the Go type of M, for display
	Parent        PID    // the spawning process, for Process.Spawn and SpawnMonitor; zero for Node.Spawn
	State         ProcessState
	StartedAt     time.Time
	Mailbox       MailboxInfo
	Received      uint64
	Sent          uint64
	CallsInFlight uint32
	LastMessage   string // proto full name of the last body taken from the mailbox
	Monitors      int    // held by this process
	Links         int    // process links held by this one: whose exits end it, or reach it as an Exited
	Watchers      int    // processes monitoring or linked to this one
	Wakeups       uint64 // Receive returns
	LogLevel      slog.Level
	TrapExit      bool // see Process.SetTrapExit
}

// LinkState is the state of a link to a peer.
type LinkState uint8

const (
	LinkConnecting LinkState = iota
	LinkUp
	LinkDown
)

var linkStates = [...]string{"connecting", "up", "down"}

func (s LinkState) String() string { return enumName(linkStates[:], int(s), "LinkState") }

// enumName is v's entry in names, or type(v) for a value outside them: a
// peer or a newer Inspector can send one, and a String must not panic.
func enumName(names []string, v int, typ string) string {
	if v < len(names) && names[v] != "" {
		return names[v]
	}
	return typ + "(" + strconv.Itoa(v) + ")"
}

// LinkInfo describes one direction of traffic with a peer.
type LinkInfo struct {
	Peer          NodeID
	Outbound      bool // this node opened the stream
	State         LinkState
	EstablishedAt time.Time
	Reconnects    uint64
	Messages      uint64 // envelopes: messages, calls, replies, monitors, downs
	Bytes         uint64 // message bodies carried, not counting framing
	Queued        int    // outbound: envelopes waiting to be written
	QueuedBytes   int    // outbound: their bodies' bytes, as Bytes counts them
	LastError     string // why dials to the peer failed; set with RetryAt
	// RetryAt is set on an outbound link that is down because dials to the
	// peer failed: sends to the peer fail at once until then, and the first
	// send after it dials again (Config.DialBackoff). It may be in the past:
	// nothing has been sent since, or that dial is under way. Such a link's
	// Peer has no Incarnation, which only a dial that succeeds learns.
	RetryAt time.Time
}

// NodeInfo is a snapshot of the node.
type NodeInfo struct {
	ID          NodeID
	Advertise   string
	StartedAt   time.Time
	Processes   int
	Spawned     uint64
	Exited      uint64
	DeadLetters uint64
	Links       []LinkInfo // ordered by peer name, outbound before inbound
}
