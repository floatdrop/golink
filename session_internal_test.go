package grpcproc

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc/metadata"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// sessionStream hands over the Hello the server sends.
type sessionStream struct {
	*fakeStream
	hello chan *grpcprocv1.Hello
}

func (s sessionStream) Send(f *grpcprocv1.Frame) error {
	s.hello <- f.GetEnvelopes()[0].GetHello()
	return nil
}

// A peer that dials with more ended sessions than this node counts ended one
// that this node did not see end: the links of that session go, and fail
// what was pending on them, and this node takes the peer's count, which its
// Hello carries back.
func TestLinkFromAPeerThatEndedItsSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		n.mu.Lock()
		n.newest["b"] = 2
		n.mu.Unlock()
		queuedLink(t, n, NodeID{Name: "b", Incarnation: 2})
		pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pendingMu.Lock()
		n.pending[1] = pc
		n.pendingMu.Unlock()

		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(
			mdNode, "b", mdIncarnation, "2", mdVersion, strconv.Itoa(protoVersion), mdSession, "3"))
		fs := sessionStream{&fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}, make(chan *grpcprocv1.Hello, 1)}
		served := make(chan error, 1)
		go func() { served <- n.serveLink(fs) }()
		if h := <-fs.hello; h.GetSession() != 3 {
			t.Fatalf("the Hello counts %d ended sessions", h.GetSession())
		}
		if r := <-pc.ch; !errors.Is(r.err, ErrNoConnection) || !strings.Contains(r.err.Error(), "it ended its session with a") {
			t.Fatalf("pending call: %v", r.err)
		}
		synctest.Wait()
		n.mu.Lock()
		epoch, in := n.epochs["b"], n.in["b"]
		n.mu.Unlock()
		if epoch != 3 || in == nil {
			t.Fatalf("epoch %d, inbound link %v", epoch, in)
		}
		fs.end()
		if err := <-served; err != nil {
			t.Fatal(err)
		}
	})
}
