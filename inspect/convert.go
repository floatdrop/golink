package inspect

import (
	"log/slog"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/grpcproc"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// The To functions turn grpcproc snapshots into wire messages; the exported
// ones turn wire messages back, for clients (a CLI, a UI, a test).

func nodeIDTo(n grpcproc.NodeID) *inspectv1.NodeID {
	return &inspectv1.NodeID{Name: n.Name, Incarnation: n.Incarnation}
}

func nodeIDFrom(n *inspectv1.NodeID) grpcproc.NodeID {
	return grpcproc.NodeID{Name: n.GetName(), Incarnation: n.GetIncarnation()}
}

// timeTo and timeFrom carry a zero time as an absent timestamp, so that it
// stays zero across the wire rather than become the Unix epoch.
func timeTo(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeFrom(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func stateTo(s grpcproc.ProcessState) inspectv1.ProcessState {
	return inspectv1.ProcessState(s + 1)
}

func linkStateTo(s grpcproc.LinkState) inspectv1.LinkState {
	return inspectv1.LinkState(s + 1)
}

// stateFrom and linkStateFrom invert stateTo and linkStateTo. A wire value
// that no state has (unspecified, or from a newer node) becomes 255, which
// is none either, rather than wrap around into one that is.
func stateFrom(s inspectv1.ProcessState) grpcproc.ProcessState {
	return grpcproc.ProcessState(enumFrom(int32(s)))
}

func linkStateFrom(s inspectv1.LinkState) grpcproc.LinkState {
	return grpcproc.LinkState(enumFrom(int32(s)))
}

func enumFrom(v int32) uint8 {
	if v < 1 || v > math.MaxUint8 {
		return math.MaxUint8
	}
	return uint8(v - 1)
}

func nodeInfoTo(n grpcproc.NodeInfo) *inspectv1.NodeInfo {
	out := &inspectv1.NodeInfo{
		Id:          nodeIDTo(n.ID),
		Advertise:   n.Advertise,
		Metadata:    n.Metadata,
		StartedAt:   timeTo(n.StartedAt),
		Processes:   uint32(n.Processes),
		Spawned:     n.Spawned,
		Exited:      n.Exited,
		DeadLetters: n.DeadLetters,
	}
	for _, l := range n.Links {
		out.Links = append(out.Links, &inspectv1.Link{
			Peer:          nodeIDTo(l.Peer),
			Outbound:      l.Outbound,
			State:         linkStateTo(l.State),
			EstablishedAt: timeTo(l.EstablishedAt),
			Reconnects:    l.Reconnects,
			Messages:      l.Messages,
			Bytes:         l.Bytes,
			LastError:     l.LastError,
			RetryAt:       timeTo(l.RetryAt),
			Queued:        uint32(l.Queued),
			QueuedBytes:   uint64(l.QueuedBytes),
		})
	}
	return out
}

func processInfoTo(p grpcproc.ProcessInfo) *inspectv1.ProcessInfo {
	return &inspectv1.ProcessInfo{
		Pid:       p.PID.Proto(),
		Name:      p.Name,
		Label:     p.Label,
		Type:      p.Type,
		Globals:   p.Globals,
		Parent:    p.Parent.Proto(),
		State:     stateTo(p.State),
		StartedAt: timeTo(p.StartedAt),
		Mailbox: &inspectv1.Mailbox{
			Depth:     uint32(p.Mailbox.Depth),
			Peak:      uint32(p.Mailbox.Peak),
			OldestAge: durationpb.New(p.Mailbox.OldestAge),
		},
		Received:      p.Received,
		Sent:          p.Sent,
		CallsInFlight: p.CallsInFlight,
		LastMessage:   p.LastMessage,
		Monitors:      uint32(p.Monitors),
		Watchers:      uint32(p.Watchers),
		Wakeups:       p.Wakeups,
		LogLevel:      int32(p.LogLevel),
		Links:         uint32(p.Links),
		TrapExit:      p.TrapExit,
	}
}

func eventTo(ev grpcproc.Event) *inspectv1.Event {
	out := &inspectv1.Event{Time: timeTo(ev.Time), Missed: ev.Missed}
	switch ev.Kind {
	case grpcproc.EventSpawn:
		out.Kind = &inspectv1.Event_Spawned{Spawned: processInfoTo(ev.Process)}
	case grpcproc.EventExit:
		out.Kind = &inspectv1.Event_Exited{Exited: &inspectv1.Exited{Process: processInfoTo(ev.Process), Reason: ev.Reason}}
	case grpcproc.EventLinkUp:
		out.Kind = &inspectv1.Event_LinkUp{LinkUp: nodeIDTo(ev.Peer)}
	case grpcproc.EventLinkDown:
		out.Kind = &inspectv1.Event_LinkDown{LinkDown: &inspectv1.LinkDown{Peer: nodeIDTo(ev.Peer), Error: ev.Err}}
	case grpcproc.EventDeadLetter:
		out.Kind = &inspectv1.Event_DeadLetter{DeadLetter: &inspectv1.DeadLetter{
			From: ev.From.Proto(), To: ev.To.Proto(), Type: ev.Type, Reason: ev.Reason,
		}}
	}
	return out
}

// NodeInfo converts a wire NodeInfo back to grpcproc's.
func NodeInfo(n *inspectv1.NodeInfo) grpcproc.NodeInfo {
	out := grpcproc.NodeInfo{
		ID:          nodeIDFrom(n.GetId()),
		Advertise:   n.GetAdvertise(),
		Metadata:    n.GetMetadata(),
		Processes:   int(n.GetProcesses()),
		Spawned:     n.GetSpawned(),
		Exited:      n.GetExited(),
		DeadLetters: n.GetDeadLetters(),
		StartedAt:   timeFrom(n.GetStartedAt()),
	}
	for _, l := range n.GetLinks() {
		out.Links = append(out.Links, grpcproc.LinkInfo{
			Peer:          nodeIDFrom(l.GetPeer()),
			Outbound:      l.GetOutbound(),
			State:         linkStateFrom(l.GetState()),
			EstablishedAt: timeFrom(l.GetEstablishedAt()),
			Reconnects:    l.GetReconnects(),
			Messages:      l.GetMessages(),
			Bytes:         l.GetBytes(),
			LastError:     l.GetLastError(),
			RetryAt:       timeFrom(l.GetRetryAt()),
			Queued:        int(l.GetQueued()),
			QueuedBytes:   int(l.GetQueuedBytes()),
		})
	}
	return out
}

// ProcessInfo converts a wire ProcessInfo back to grpcproc's.
func ProcessInfo(p *inspectv1.ProcessInfo) grpcproc.ProcessInfo {
	return grpcproc.ProcessInfo{
		PID:     grpcproc.PIDFromProto(p.GetPid()),
		Parent:  grpcproc.PIDFromProto(p.GetParent()), // the zero PID for none
		Name:    p.GetName(),
		Label:   p.GetLabel(),
		Type:    p.GetType(),
		Globals: p.GetGlobals(),
		State:   stateFrom(p.GetState()),
		Mailbox: grpcproc.MailboxInfo{
			Depth:     int(p.GetMailbox().GetDepth()),
			Peak:      int(p.GetMailbox().GetPeak()),
			OldestAge: p.GetMailbox().GetOldestAge().AsDuration(),
		},
		StartedAt:     timeFrom(p.GetStartedAt()),
		Received:      p.GetReceived(),
		Sent:          p.GetSent(),
		CallsInFlight: p.GetCallsInFlight(),
		LastMessage:   p.GetLastMessage(),
		Monitors:      int(p.GetMonitors()),
		Links:         int(p.GetLinks()),
		Watchers:      int(p.GetWatchers()),
		Wakeups:       p.GetWakeups(),
		LogLevel:      slog.Level(p.GetLogLevel()),
		TrapExit:      p.GetTrapExit(),
	}
}

// Event converts a wire Event back to grpcproc's.
func Event(e *inspectv1.Event) grpcproc.Event {
	out := grpcproc.Event{Time: timeFrom(e.GetTime()), Missed: e.GetMissed()}
	switch k := e.GetKind().(type) {
	case *inspectv1.Event_Spawned:
		out.Kind, out.Process = grpcproc.EventSpawn, ProcessInfo(k.Spawned)
	case *inspectv1.Event_Exited:
		out.Kind, out.Process, out.Reason = grpcproc.EventExit, ProcessInfo(k.Exited.GetProcess()), k.Exited.GetReason()
	case *inspectv1.Event_LinkUp:
		out.Kind, out.Peer = grpcproc.EventLinkUp, nodeIDFrom(k.LinkUp)
	case *inspectv1.Event_LinkDown:
		out.Kind, out.Peer, out.Err = grpcproc.EventLinkDown, nodeIDFrom(k.LinkDown.GetPeer()), k.LinkDown.GetError()
	case *inspectv1.Event_DeadLetter:
		d := k.DeadLetter
		out.Kind, out.From, out.To, out.Type, out.Reason = grpcproc.EventDeadLetter, grpcproc.PIDFromProto(d.GetFrom()), grpcproc.PIDFromProto(d.GetTo()), d.GetType(), d.GetReason()
	}
	return out
}
