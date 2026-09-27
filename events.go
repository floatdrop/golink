package grpcproc

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// EventKind says what an Event reports.
type EventKind uint8

const (
	EventSpawn EventKind = iota + 1
	EventExit
	EventLinkUp
	EventLinkDown
	EventDeadLetter
)

var eventKinds = [...]string{"", "spawn", "exit", "link-up", "link-down", "dead-letter"}

func (k EventKind) String() string { return enumName(eventKinds[:], int(k), "EventKind") }

// Event is something that happened on a node. Which fields are set depends
// on Kind:
//
//	EventSpawn       Process
//	EventExit        Process, Reason
//	EventLinkUp      Peer
//	EventLinkDown    Peer, Err
//	EventDeadLetter  From, To, Type (proto full name), Reason
type Event struct {
	Kind    EventKind
	Time    time.Time
	Process ProcessInfo
	Reason  string
	Peer    NodeID
	Err     string
	From    PID
	To      PID
	Type    string
	// Missed is how many events this subscriber lost, because its buffer
	// was full, since the event before this one.
	Missed uint64
}

type subscriber struct {
	mu     sync.Mutex
	ch     chan Event
	closed bool
	missed uint64
}

func (s *subscriber) deliver(ev Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	ev.Missed = s.missed
	select {
	case s.ch <- ev:
		s.missed = 0
	default:
		s.missed++
	}
}

func (s *subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

type subscribers struct {
	mu   sync.Mutex
	list atomic.Pointer[[]*subscriber] // copy-on-write; read without the lock
}

func (s *subscribers) add(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next []*subscriber
	if cur := s.list.Load(); cur != nil {
		next = slices.Clone(*cur)
	}
	next = append(next, sub)
	s.list.Store(&next)
}

func (s *subscribers) remove(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.list.Load()
	if cur == nil {
		return
	}
	next := slices.DeleteFunc(slices.Clone(*cur), func(x *subscriber) bool { return x == sub })
	s.list.Store(&next)
}

// active reports whether anyone listens, so callers can skip building an Event.
func (s *subscribers) active() bool {
	cur := s.list.Load()
	return cur != nil && len(*cur) > 0
}

func (s *subscribers) publish(ev Event) {
	cur := s.list.Load()
	if cur == nil {
		return
	}
	ev.Time = time.Now()
	for _, sub := range *cur {
		sub.deliver(ev)
	}
}

// Subscribe streams the node's events until ctx is done or the node stops,
// then closes the channel. Publishing never blocks the node: when the
// channel's buffer is full the event is dropped, and the next event this
// subscriber receives carries the count in Missed.
func (n *Node) Subscribe(ctx context.Context, buffer int) <-chan Event {
	sub := &subscriber{ch: make(chan Event, max(buffer, 1))}
	n.subs.add(sub)
	stop := func() {
		n.subs.remove(sub)
		sub.close()
	}
	context.AfterFunc(ctx, stop)
	context.AfterFunc(n.ctx, stop)
	return sub.ch
}
