// Package actor is optional structure on top of golink processes: a handler
// loop in the style of gen_server, and supervisors that restart what fails.
// It uses only golink's public API, so it can be ignored or replaced.
//
// An actor is a plain struct holding its dependencies, built by a
// constructor, so it fits a DI container:
//
//	type Orders struct{ repo *Repo }
//
//	func (o *Orders) HandleMessage(p *golink.Process[*orderspb.Order], m golink.Msg[*orderspb.Order]) error { … }
//	func (o *Orders) HandleCall(p *golink.Process[*orderspb.Order], m golink.Msg[*orderspb.Order]) (proto.Message, error) { … }
//
//	addr, err := actor.Spawn(node, &Orders{repo: repo}, golink.WithName("orders"))
package actor

import (
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
)

// Handler handles the messages sent to an actor whose mailbox holds M.
// Returning an error ends the actor with that error as its exit reason;
// ErrStop ends it normally.
type Handler[M proto.Message] interface {
	HandleMessage(p *golink.Process[M], m golink.Msg[M]) error
}

// CallHandler answers calls: the returned message, or error, is the reply,
// and the actor carries on. Return ErrNoReply to answer later with
// p.Reply(m, …), from any goroutine; ErrStop to reply and then stop.
//
// Without it, a call is answered with an error.
type CallHandler[M proto.Message] interface {
	HandleCall(p *golink.Process[M], m golink.Msg[M]) (proto.Message, error)
}

// DownHandler is told when a process the actor monitors exits. Without it,
// Downs are ignored.
type DownHandler[M proto.Message] interface {
	HandleDown(p *golink.Process[M], d golink.Down) error
}

// Initializer runs before the first message. An error ends the actor
// before it handles anything, and Terminate is not called.
type Initializer[M proto.Message] interface {
	Init(p *golink.Process[M]) error
}

// Terminator runs when the actor ends, however it ends: err is nil after
// ErrStop, the handler's error, an *golink.ExitError after Exit,
// context.Canceled when the node stops, or "panic: …" (after which the panic
// continues, and golink reports it).
type Terminator[M proto.Message] interface {
	Terminate(p *golink.Process[M], err error)
}

var (
	// ErrStop, returned from a handler, ends the actor normally. From
	// HandleCall, the reply is sent first.
	ErrStop = errors.New("actor: stop")
	// ErrNoReply, returned from HandleCall, means the actor will answer
	// later with p.Reply(m, …).
	ErrNoReply = errors.New("actor: reply later")
)

// Run turns a Handler into a process function for golink.Spawn.
func Run[M proto.Message](h Handler[M]) func(*golink.Process[M]) error {
	calls, _ := h.(CallHandler[M])
	downs, _ := h.(DownHandler[M])
	init, _ := h.(Initializer[M])
	term, _ := h.(Terminator[M])
	return func(p *golink.Process[M]) (err error) {
		if init != nil {
			if err := init.Init(p); err != nil {
				return err
			}
		}
		if term != nil {
			defer func() {
				if r := recover(); r != nil {
					term.Terminate(p, fmt.Errorf("panic: %v", r))
					panic(r)
				}
				term.Terminate(p, err)
			}()
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			switch {
			case m.Down != nil:
				if downs != nil {
					err = downs.HandleDown(p, *m.Down)
				}
			case m.IsCall():
				err = call(p, calls, m)
			default:
				err = h.HandleMessage(p, m)
			}
			if errors.Is(err, ErrStop) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
}

func call[M proto.Message](p *golink.Process[M], calls CallHandler[M], m golink.Msg[M]) error {
	if calls == nil {
		_ = p.Reply(m, nil, fmt.Errorf("actor: %s does not handle calls", reflect.TypeFor[M]()))
		return nil
	}
	resp, err := calls.HandleCall(p, m)
	switch {
	case errors.Is(err, ErrNoReply):
		return nil
	case errors.Is(err, ErrStop):
		_ = p.Reply(m, resp, nil)
		return ErrStop
	}
	_ = p.Reply(m, resp, err)
	return nil
}

// Spawn starts h as a process on n: golink.Spawn(n, Run(h), opts...).
func Spawn[M proto.Message](n *golink.Node, h Handler[M], opts ...golink.SpawnOption) (golink.Addr[M], error) {
	return golink.Spawn(n, Run(h), opts...)
}
