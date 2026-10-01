package inspect_test

import (
	"testing"
	"testing/synctest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// claimant spawns on n a process that holds name.
func claimant(t *testing.T, n *grpcproc.Node, name string) grpcproc.PID {
	t.Helper()
	claimed := make(chan error, 1)
	addr, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		_, err := p.Claim(p.Context(), name)
		claimed <- err
		if err != nil {
			return err
		}
		<-p.Context().Done()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-claimed; err != nil {
		t.Fatal(err)
	}
	return addr.PID()
}

func TestNames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a", "b")
		room1 := claimant(t, c.Node("b"), "room:1")
		claimant(t, c.Node("b"), "room:2")
		claimant(t, c.Node("b"), "ledger")
		a := client(c, "a")

		got, err := a.LookupName(t.Context(), &inspectv1.LookupNameRequest{Name: "room:1"})
		if err != nil || !got.GetFound() || grpcproc.PIDFromProto(got.GetPid()) != room1 {
			t.Fatalf("lookup: %v %v", got, err)
		}
		got, err = a.LookupName(t.Context(), &inspectv1.LookupNameRequest{Node: "b", Name: "nobody"})
		if err != nil || got.GetFound() {
			t.Fatalf("lookup of nobody, forwarded: %v %v", got, err)
		}
		list, err := a.ListNames(t.Context(), &inspectv1.ListNamesRequest{Prefix: "room:", Limit: 1})
		if err != nil || len(list.GetNames()) != 1 || list.GetNames()[0].GetName() != "room:1" {
			t.Fatalf("list: %v %v", list, err)
		}
		list, err = a.ListNames(t.Context(), &inspectv1.ListNamesRequest{Node: "b"})
		if err != nil || len(list.GetNames()) != 3 {
			t.Fatalf("list all, forwarded: %v %v", list, err)
		}
		proc, err := a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Target: byPID(room1)})
		if err != nil || len(inspect.ProcessInfo(proc.GetProcess()).Globals) != 1 {
			t.Fatalf("process: %v %v", proc, err)
		}
	})
}

// unlisted is a Names that cannot list.
type unlisted struct{ grpcproc.Names }

func TestNamesUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{
			grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
				if name == "a" {
					cfg.Names = nil
				} else {
					cfg.Names = unlisted{cfg.Names}
				}
			}),
			grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) { inspect.New(n).Register(s) }),
		}, "a", "b")
		a, b := client(c, "a"), client(c, "b")
		_, err := a.LookupName(t.Context(), &inspectv1.LookupNameRequest{Name: "x"})
		code(t, err, codes.FailedPrecondition)
		_, err = a.ListNames(t.Context(), &inspectv1.ListNamesRequest{})
		code(t, err, codes.FailedPrecondition)
		_, err = b.ListNames(t.Context(), &inspectv1.ListNamesRequest{})
		code(t, err, codes.Unimplemented)
	})
}
