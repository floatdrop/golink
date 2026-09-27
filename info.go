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

func (s ProcessState) String() string {
	return [...]string{"idle", "running", "waiting-reply", "exiting"}[s]
}

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
	Names         []string
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
	Watchers      int    // processes monitoring this one
	Wakeups       uint64 // Receive returns
	LogLevel      slog.Level
}

// LinkState is the state of a link to a peer.
type LinkState uint8

const (
	LinkConnecting LinkState = iota
	LinkUp
	LinkDown
)

func (s LinkState) String() string { return [...]string{"connecting", "up", "down"}[s] }

// LinkInfo describes one direction of traffic with a peer.
type LinkInfo struct {
	Peer          NodeID
	Outbound      bool // this node opened the stream
	State         LinkState
	EstablishedAt time.Time
	Reconnects    uint64
	Messages      uint64 // envelopes: messages, calls, replies, monitors, downs
	Bytes         uint64 // message bodies carried, not counting framing
	LastError     string
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
