// Package inspect serves golink.inspect.v1.Inspector: a node's processes,
// links and events over gRPC, for tools and for people. It is optional —
// register it next to the node on the same server, behind whatever
// interceptors guard the application's other services.
//
//	node.Register(grpcServer)
//	inspect.New(node, inspect.WithPeers(dialer.Peer)).Register(grpcServer)
package inspect

import (
	"cmp"
	"context"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/golink"
	inspectv1 "github.com/floatdrop/golink/proto/golink/inspect/v1"
)

// PeerFunc returns the Inspector of another node, to forward a request to.
type PeerFunc func(ctx context.Context, node string) (inspectv1.InspectorClient, error)

// Option configures a Server.
type Option func(*Server)

// WithPeers lets the server answer for other nodes by forwarding to their
// Inspector. Without it, a request for another node is FailedPrecondition.
func WithPeers(f PeerFunc) Option { return func(s *Server) { s.peers = f } }

// ReadOnly refuses SetLogLevel, Send and Exit with PermissionDenied.
func ReadOnly() Option { return func(s *Server) { s.readOnly = true } }

// Server implements golink.inspect.v1.Inspector for one node.
type Server struct {
	inspectv1.UnimplementedInspectorServer
	node     *golink.Node
	peers    PeerFunc
	readOnly bool
}

// New returns an Inspector for node.
func New(node *golink.Node, opts ...Option) *Server {
	s := &Server{node: node}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register mounts the Inspector on r.
func (s *Server) Register(r grpc.ServiceRegistrar) { inspectv1.RegisterInspectorServer(r, s) }

const (
	defaultInspectTimeout = time.Second
	defaultWatchBuffer    = 256
)

// remote returns the Inspector to forward to, or nil when the request is
// about this node. A target PID on another node routes there when the
// request names no node.
func (s *Server) remote(ctx context.Context, node string, target *inspectv1.Target) (inspectv1.InspectorClient, string, error) {
	if node == "" {
		node = target.GetPid().GetNode()
	}
	if node == "" || node == s.node.Name() {
		return nil, "", nil
	}
	if s.peers == nil {
		return nil, node, status.Errorf(codes.FailedPrecondition, "inspect: %q is not this node (%s) and no peer dialer is configured", node, s.node.Name())
	}
	c, err := s.peers(ctx, node)
	if err != nil {
		return nil, node, status.Errorf(codes.Unavailable, "inspect: reach %s: %v", node, err)
	}
	return c, node, nil
}

// peerErr says which node a forwarded request failed on, keeping its code.
func peerErr(node string, err error) error {
	st := status.Convert(err)
	return status.Errorf(st.Code(), "inspect: node %s: %s", node, st.Message())
}

func (s *Server) writable() error {
	if s.readOnly {
		return status.Error(codes.PermissionDenied, "inspect: server is read-only")
	}
	return nil
}

// local resolves a target on this node to a PID.
func (s *Server) local(t *inspectv1.Target) (golink.PID, error) {
	switch k := t.GetKind().(type) {
	case *inspectv1.Target_Pid:
		return pidFrom(k.Pid), nil
	case *inspectv1.Target_Name:
		if pid, ok := s.node.Whereis(k.Name); ok {
			return pid, nil
		}
		return golink.PID{}, status.Errorf(codes.NotFound, "inspect: no process named %q", k.Name)
	default:
		return golink.PID{}, status.Error(codes.InvalidArgument, "inspect: target is required")
	}
}

// addr turns a target into something golink can send to, without requiring
// the name to resolve here first.
func (s *Server) addr(t *inspectv1.Target) (golink.Target, error) {
	switch k := t.GetKind().(type) {
	case *inspectv1.Target_Pid:
		return pidFrom(k.Pid), nil
	case *inspectv1.Target_Name:
		return golink.Name{Node: s.node.Name(), Name: k.Name}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "inspect: target is required")
	}
}

func (s *Server) GetNode(ctx context.Context, req *inspectv1.GetNodeRequest) (*inspectv1.GetNodeResponse, error) {
	if c, node, err := s.remote(ctx, req.GetNode(), nil); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.GetNodeResponse, error) { return c.GetNode(ctx, req) })
	}
	return &inspectv1.GetNodeResponse{Node: nodeInfoTo(s.node.Info())}, nil
}

func (s *Server) ListProcesses(ctx context.Context, req *inspectv1.ListProcessesRequest) (*inspectv1.ListProcessesResponse, error) {
	if c, node, err := s.remote(ctx, req.GetNode(), nil); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.ListProcessesResponse, error) { return c.ListProcesses(ctx, req) })
	}
	resp := &inspectv1.ListProcessesResponse{}
	for _, p := range s.node.Processes() {
		if matches(p, req) {
			resp.Processes = append(resp.Processes, processInfoTo(p))
		}
	}
	return resp, nil
}

func matches(p golink.ProcessInfo, req *inspectv1.ListProcessesRequest) bool {
	switch {
	case req.GetLabel() != "" && p.Label != req.GetLabel():
		return false
	case req.GetState() != inspectv1.ProcessState_PROCESS_STATE_UNSPECIFIED && stateTo(p.State) != req.GetState():
		return false
	case p.Mailbox.Depth < int(req.GetMinMailbox()):
		return false
	case req.GetName() != "":
		for _, n := range p.Names {
			if strings.Contains(n, req.GetName()) {
				return true
			}
		}
		return false
	}
	return true
}

func (s *Server) GetProcess(ctx context.Context, req *inspectv1.GetProcessRequest) (*inspectv1.GetProcessResponse, error) {
	if c, node, err := s.remote(ctx, req.GetNode(), req.GetTarget()); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.GetProcessResponse, error) { return c.GetProcess(ctx, req) })
	}
	pid, err := s.local(req.GetTarget())
	if err != nil {
		return nil, err
	}
	info, ok := s.node.Process(pid)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "inspect: no process %s", pid)
	}
	resp := &inspectv1.GetProcessResponse{Process: processInfoTo(info)}
	if req.GetInspect() {
		timeout := cmp.Or(req.GetInspectTimeout().AsDuration(), defaultInspectTimeout)
		ictx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		m, err := s.node.Inspect(ictx, pid)
		if err != nil {
			resp.InspectError = err.Error()
		}
		resp.Inspect = m
	}
	return resp, nil
}

func (s *Server) SetLogLevel(ctx context.Context, req *inspectv1.SetLogLevelRequest) (*inspectv1.SetLogLevelResponse, error) {
	if err := s.writable(); err != nil {
		return nil, err
	}
	if c, node, err := s.remote(ctx, req.GetNode(), req.GetTarget()); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.SetLogLevelResponse, error) { return c.SetLogLevel(ctx, req) })
	}
	pid, err := s.local(req.GetTarget())
	if err != nil {
		return nil, err
	}
	if err := s.node.SetLogLevel(pid, slog.Level(req.GetLevel())); err != nil {
		return nil, status.Errorf(codes.NotFound, "inspect: %v", err)
	}
	return &inspectv1.SetLogLevelResponse{}, nil
}

func (s *Server) Send(ctx context.Context, req *inspectv1.SendRequest) (*inspectv1.SendResponse, error) {
	if err := s.writable(); err != nil {
		return nil, err
	}
	if c, node, err := s.remote(ctx, req.GetNode(), req.GetTarget()); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.SendResponse, error) { return c.Send(ctx, req) })
	}
	to, err := s.addr(req.GetTarget())
	if err != nil {
		return nil, err
	}
	if req.GetBody() == nil {
		return nil, status.Error(codes.InvalidArgument, "inspect: body is required")
	}
	body, err := req.GetBody().UnmarshalNew()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "inspect: body: %v", err)
	}
	if err := s.node.SendTo(to, body); err != nil {
		return nil, status.Errorf(codes.Unavailable, "inspect: %v", err)
	}
	return &inspectv1.SendResponse{}, nil
}

func (s *Server) Exit(ctx context.Context, req *inspectv1.ExitRequest) (*inspectv1.ExitResponse, error) {
	if err := s.writable(); err != nil {
		return nil, err
	}
	if c, node, err := s.remote(ctx, req.GetNode(), req.GetTarget()); c != nil || err != nil {
		return forward(node, err, func() (*inspectv1.ExitResponse, error) { return c.Exit(ctx, req) })
	}
	to, err := s.addr(req.GetTarget())
	if err != nil {
		return nil, err
	}
	// Exit is local here, so it cannot fail to route.
	_ = s.node.Exit(to, cmp.Or(req.GetReason(), golink.ReasonKilled))
	return &inspectv1.ExitResponse{}, nil
}

func (s *Server) Watch(req *inspectv1.WatchRequest, stream grpc.ServerStreamingServer[inspectv1.WatchResponse]) error {
	ctx := stream.Context()
	c, node, err := s.remote(ctx, req.GetNode(), nil)
	if err != nil {
		return err
	}
	if c != nil {
		upstream, err := c.Watch(ctx, req)
		if err != nil {
			return peerErr(node, err)
		}
		for {
			resp, err := upstream.Recv()
			if err != nil {
				return peerErr(node, err)
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
	events := s.node.Subscribe(ctx, cmp.Or(int(req.GetBuffer()), defaultWatchBuffer))
	for ev := range events {
		if err := stream.Send(&inspectv1.WatchResponse{Event: eventTo(ev)}); err != nil {
			return err
		}
	}
	if ctx.Err() == nil {
		return status.Error(codes.Unavailable, "inspect: node stopped")
	}
	return nil
}

// forward runs a call on a peer's Inspector, unless routing it failed.
func forward[T any](node string, routeErr error, call func() (T, error)) (T, error) {
	var zero T
	if routeErr != nil {
		return zero, routeErr
	}
	v, err := call()
	if err != nil {
		return zero, peerErr(node, err)
	}
	return v, nil
}
