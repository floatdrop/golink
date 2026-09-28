package leader

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
)

// Call calls the process registered as name on whichever node leads
// cluster, the singleton, with req, and returns its answer, typed as R:
//
//	scheduled, err := leader.Call[*schedpb.Scheduled](ctx, p, "scheduler", "scheduler", &schedpb.Schedule{…})
//
// from is who calls, a *grpcproc.Node or a *grpcproc.Process from inside its
// handler, as for grpcproc.Addr.Call; its node's elector for cluster says
// who leads, and must run.
//
// Leadership moves, and Call follows it, asking again for as long as ctx
// allows, but only when the call cannot have reached the singleton: while
// no leader is known, when the node named has no process called name (it
// no longer leads, or has not started its singleton yet), or when the call
// never left this node (a *grpcproc.LinkError whose Unsent is set). A call
// that did leave may have been handled, and delivery is at most once: if its
// link breaks, or ctx ends while it waits, Call returns that error rather
// than call again, and the caller decides whether the request is safe to
// repeat. An answer that is an error is returned as it is.
func Call[R, M proto.Message](ctx context.Context, from grpcproc.Caller, cluster, name string, req M) (R, error) {
	n := nodeOf(from)
	wait := 10 * time.Millisecond
	for {
		var zero R
		info, err := Status(ctx, n, cluster)
		if err != nil {
			return zero, err
		}
		if info.Leader == "" {
			err = ErrNoLeader
		} else {
			var resp R
			resp, err = grpcproc.Named[M](info.Leader, name).Call[R](ctx, from, req)
			if !undelivered(err) {
				return resp, err
			}
		}
		select {
		case <-ctx.Done():
			return zero, errors.Join(err, context.Cause(ctx))
		case <-time.After(wait):
		}
		wait = min(2*wait, 500*time.Millisecond)
	}
}

// nodeOf is the node of a Caller: the node itself, or a process's.
func nodeOf(from grpcproc.Caller) *grpcproc.Node {
	if p, ok := from.(interface{ Node() *grpcproc.Node }); ok {
		return p.Node()
	}
	return from.(*grpcproc.Node)
}

// undelivered reports whether err says a call never reached the process it
// was for, so that calling again cannot handle it twice. An answer is
// looked at first: errors.Is matches a RemoteError by its text, and a
// singleton that answered with ErrNoProc's text answered.
func undelivered(err error) bool {
	if _, ok := errors.AsType[*grpcproc.RemoteError](err); ok {
		return false
	}
	if le, ok := errors.AsType[*grpcproc.LinkError](err); ok {
		return le.Unsent
	}
	return errors.Is(err, grpcproc.ErrNoProc)
}
