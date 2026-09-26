package inspect

import (
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/golink"
	inspectv1 "github.com/floatdrop/golink/proto/golink/inspect/v1"
	golinkv1 "github.com/floatdrop/golink/proto/golink/v1"
)

// The To functions turn golink snapshots into wire messages; the exported
// ones turn wire messages back, for clients (a CLI, a UI, a test).

func pidTo(p golink.PID) *golinkv1.PID {
	if p.IsZero() {
		return nil
	}
	return &golinkv1.PID{Node: p.Node, Incarnation: p.Incarnation, Id: p.ID}
}

func pidFrom(p *golinkv1.PID) golink.PID {
	return golink.PID{Node: p.GetNode(), Incarnation: p.GetIncarnation(), ID: p.GetId()}
}

func nodeIDTo(n golink.NodeID) *inspectv1.NodeID {
	return &inspectv1.NodeID{Name: n.Name, Incarnation: n.Incarnation}
}

func nodeIDFrom(n *inspectv1.NodeID) golink.NodeID {
	return golink.NodeID{Name: n.GetName(), Incarnation: n.GetIncarnation()}
}

func timeTo(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func stateTo(s golink.ProcessState) inspectv1.ProcessState {
	return inspectv1.ProcessState(s + 1)
}

func linkStateTo(s golink.LinkState) inspectv1.LinkState {
	return inspectv1.LinkState(s + 1)
}

func nodeInfoTo(n golink.NodeInfo) *inspectv1.NodeInfo {
	out := &inspectv1.NodeInfo{
		Id:          nodeIDTo(n.ID),
		Advertise:   n.Advertise,
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
		})
	}
	return out
}

func processInfoTo(p golink.ProcessInfo) *inspectv1.ProcessInfo {
	return &inspectv1.ProcessInfo{
		Pid:       pidTo(p.PID),
		Names:     p.Names,
		Label:     p.Label,
		Type:      p.Type,
		Parent:    pidTo(p.Parent),
		State:     stateTo(p.State),
		StartedAt: timestamppb.New(p.StartedAt),
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
	}
}

func eventTo(ev golink.Event) *inspectv1.Event {
	out := &inspectv1.Event{Time: timestamppb.New(ev.Time), Missed: ev.Missed}
	switch ev.Kind {
	case golink.EventSpawn:
		out.Kind = &inspectv1.Event_Spawned{Spawned: processInfoTo(ev.Process)}
	case golink.EventExit:
		out.Kind = &inspectv1.Event_Exited{Exited: &inspectv1.Exited{Process: processInfoTo(ev.Process), Reason: ev.Reason}}
	case golink.EventLinkUp:
		out.Kind = &inspectv1.Event_LinkUp{LinkUp: nodeIDTo(ev.Peer)}
	case golink.EventLinkDown:
		out.Kind = &inspectv1.Event_LinkDown{LinkDown: &inspectv1.LinkDown{Peer: nodeIDTo(ev.Peer), Error: ev.Err}}
	case golink.EventDeadLetter:
		out.Kind = &inspectv1.Event_DeadLetter{DeadLetter: &inspectv1.DeadLetter{
			From: pidTo(ev.From), To: pidTo(ev.To), Type: ev.Type, Reason: ev.Reason,
		}}
	}
	return out
}

// NodeInfo converts a wire NodeInfo back to golink's.
func NodeInfo(n *inspectv1.NodeInfo) golink.NodeInfo {
	out := golink.NodeInfo{
		ID:          nodeIDFrom(n.GetId()),
		Advertise:   n.GetAdvertise(),
		Processes:   int(n.GetProcesses()),
		Spawned:     n.GetSpawned(),
		Exited:      n.GetExited(),
		DeadLetters: n.GetDeadLetters(),
	}
	if n.GetStartedAt() != nil {
		out.StartedAt = n.GetStartedAt().AsTime()
	}
	for _, l := range n.GetLinks() {
		li := golink.LinkInfo{
			Peer:       nodeIDFrom(l.GetPeer()),
			Outbound:   l.GetOutbound(),
			State:      golink.LinkState(l.GetState() - 1),
			Reconnects: l.GetReconnects(),
			Messages:   l.GetMessages(),
			Bytes:      l.GetBytes(),
			LastError:  l.GetLastError(),
		}
		if l.GetEstablishedAt() != nil {
			li.EstablishedAt = l.GetEstablishedAt().AsTime()
		}
		out.Links = append(out.Links, li)
	}
	return out
}

// ProcessInfo converts a wire ProcessInfo back to golink's.
func ProcessInfo(p *inspectv1.ProcessInfo) golink.ProcessInfo {
	out := golink.ProcessInfo{
		PID:   pidFrom(p.GetPid()),
		Names: p.GetNames(),
		Label: p.GetLabel(),
		Type:  p.GetType(),
		State: golink.ProcessState(p.GetState() - 1),
		Mailbox: golink.MailboxInfo{
			Depth:     int(p.GetMailbox().GetDepth()),
			Peak:      int(p.GetMailbox().GetPeak()),
			OldestAge: p.GetMailbox().GetOldestAge().AsDuration(),
		},
		StartedAt:     p.GetStartedAt().AsTime(),
		Received:      p.GetReceived(),
		Sent:          p.GetSent(),
		CallsInFlight: p.GetCallsInFlight(),
		LastMessage:   p.GetLastMessage(),
		Monitors:      int(p.GetMonitors()),
		Watchers:      int(p.GetWatchers()),
		Wakeups:       p.GetWakeups(),
		LogLevel:      slog.Level(p.GetLogLevel()),
	}
	if p.GetParent() != nil {
		out.Parent = pidFrom(p.GetParent())
	}
	return out
}

// Event converts a wire Event back to golink's.
func Event(e *inspectv1.Event) golink.Event {
	out := golink.Event{Time: e.GetTime().AsTime(), Missed: e.GetMissed()}
	switch k := e.GetKind().(type) {
	case *inspectv1.Event_Spawned:
		out.Kind, out.Process = golink.EventSpawn, ProcessInfo(k.Spawned)
	case *inspectv1.Event_Exited:
		out.Kind, out.Process, out.Reason = golink.EventExit, ProcessInfo(k.Exited.GetProcess()), k.Exited.GetReason()
	case *inspectv1.Event_LinkUp:
		out.Kind, out.Peer = golink.EventLinkUp, nodeIDFrom(k.LinkUp)
	case *inspectv1.Event_LinkDown:
		out.Kind, out.Peer, out.Err = golink.EventLinkDown, nodeIDFrom(k.LinkDown.GetPeer()), k.LinkDown.GetError()
	case *inspectv1.Event_DeadLetter:
		d := k.DeadLetter
		out.Kind, out.From, out.To, out.Type, out.Reason = golink.EventDeadLetter, pidFrom(d.GetFrom()), pidFrom(d.GetTo()), d.GetType(), d.GetReason()
	}
	return out
}
