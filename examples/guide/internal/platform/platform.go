// Package platform is what every program of the shop runs, whichever
// services it hosts: its node, the gRPC server the node is served on, the
// Inspector beside it, and the root of its supervision tree. The services'
// packages know none of it but the node and the configuration.
package platform

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"golang.yandex/di"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/inspect"
)

// Module registers the node and what it stands on. They are built in this
// order, started in it and stopped in reverse: the gRPC server and its
// listener, the node served on it, the Inspector beside the node, and the
// root supervisor, which starts the node once the services run. So the
// services' trees stop first, the node closes its links after them, and the
// server stops last.
func Module(s *di.Scope) {
	s.Wire[grpcproc.Resolver](peers)
	s.Wire[net.Listener](listen).
		// Closed here too, for a start that fails before the server serves.
		OnStop(func(_ context.Context, ln net.Listener) error { _ = ln.Close(); return nil })
	s.Wire[*server](newServer).
		Eager().
		OnStart(func(_ context.Context, srv *server) error {
			go func() {
				// Serve returns nil after GracefulStop, and ErrServerStopped
				// when a rollback stopped the server before it got here.
				if err := srv.grpc.Serve(srv.ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
					s.Shutdown(err) // the listener died: stop the program
				}
			}()
			return nil
		}).
		OnStop(func(ctx context.Context, srv *server) error {
			// GracefulStop takes no context: if ctx ends first, Stop cuts
			// what is left short.
			stop := context.AfterFunc(ctx, srv.grpc.Stop)
			srv.grpc.GracefulStop()
			if !stop() {
				return ctx.Err()
			}
			return nil
		})
	s.Wire[*grpcproc.Node](newNode).
		Eager().
		OnStop(func(ctx context.Context, n *grpcproc.Node) error { return n.Stop(ctx) })
	s.Wire[*inspect.Server](newInspector).
		Eager().
		OnStop(func(_ context.Context, i *inspect.Server) error { return i.Close() })
	s.Wire[*Root](newRoot).
		Needs(di.AllOf[actor.ChildSpec]()).
		Eager().
		OnStart(func(ctx context.Context, r *Root) error { return r.start(ctx) }).
		Go(func(ctx context.Context, r *Root) error { return r.wait(ctx) }).
		OnStop(func(ctx context.Context, r *Root) error { return r.stop(ctx) })
}

// peers resolves the other nodes from the configuration. A deployment with
// a registry takes its resolver from grpcproc/etcd instead, with the node's
// Registrar and Membership, and advertises an address peers can dial; the
// services do not change.
func peers(cfg Config) grpcproc.Resolver { return grpcproc.StaticResolver(cfg.Peers) }

// listen binds the gRPC port when the program starts, so a busy one fails
// the start rather than the first peer's dial.
func listen(cfg Config) (net.Listener, error) { return net.Listen("tcp", cfg.Listen) }

// dialOptions are for every connection to another node: the node's links
// and the Inspector's forwarding. Keepalive is what turns a peer that went
// silent into a broken link, and so into Downs and failed calls. Plaintext
// keeps the guide short; a real deployment puts mTLS here.
func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
	}
}

// server is the program's gRPC server and the listener it serves. The node
// and the Inspector register on it when they are built, before it serves.
type server struct {
	grpc *grpc.Server
	ln   net.Listener
}

func newServer(ln net.Listener) *server {
	// Accepts the pings dialOptions sends.
	policy := keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}
	return &server{grpc: grpc.NewServer(grpc.KeepaliveEnforcementPolicy(policy)), ln: ln}
}

func newNode(cfg Config, r grpcproc.Resolver, srv *server, log *slog.Logger) (*grpcproc.Node, error) {
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name:        cfg.Node,
		Advertise:   cfg.Listen,
		Resolver:    r,
		DialOptions: dialOptions(),
		Logger:      log,
	})
	if err != nil {
		return nil, err
	}
	n.Register(srv.grpc)
	return n, nil
}

// newInspector serves the node to grpcprocctl, and reaches the other nodes'
// Inspectors the way the node reaches the nodes. It is read-only, since it
// shares the node's port and nothing here authenticates: exit and loglevel
// are refused. A deployment that wants them puts the Inspector behind the
// interceptors that guard its other services.
func newInspector(n *grpcproc.Node, r grpcproc.Resolver, srv *server) *inspect.Server {
	i := inspect.New(n, inspect.WithResolver(r, dialOptions()...), inspect.ReadOnly())
	i.Register(srv.grpc)
	return i
}
