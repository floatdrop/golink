package grpcproc

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// newTestNode is node name of incarnation 1, stopped when the test ends.
func newTestNode(t *testing.T, name string) *Node {
	t.Helper()
	n, err := NewNode(Config{Name: name, Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	return n
}

// testConn is a client connection that is never dialed, for links built by
// hand.
func testConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return cc
}

// testOutLink is an outbound link from n to peer built by hand, with no
// stream and no writer until the test gives it them.
func testOutLink(t *testing.T, n *Node, peer NodeID) *outLink {
	return &outLink{node: n, peer: peer, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
		cancel: func() {}}
}

// queuedLink registers a link from n to peer that nothing writes, so that
// what n sends to peer stays in its queue. It is taken away before n stops,
// which would wait for it to drain.
func queuedLink(t *testing.T, n *Node, peer NodeID) *outLink {
	l := testOutLink(t, n, peer)
	n.mu.Lock()
	n.out[peer.Name] = l
	n.mu.Unlock()
	t.Cleanup(func() {
		n.mu.Lock()
		delete(n.out, peer.Name)
		n.mu.Unlock()
		l.close(nil)
	})
	return l
}

// fakeStream is a server stream whose Send fails or whose Recv ends at will.
type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	sendErr error
	recvErr error
	recv    chan *grpcprocv1.Frame
	ended   sync.Once
}

// end ends the stream: Recv returns recvErr from then on, as a real stream's
// does once its peer or its server has ended it. It may be called again.
func (f *fakeStream) end() { f.ended.Do(func() { close(f.recv) }) }

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) Send(*grpcprocv1.Frame) error {
	return f.sendErr
}

func (f *fakeStream) Recv() (*grpcprocv1.Frame, error) {
	if env, ok := <-f.recv; ok {
		return env, nil
	}
	return nil, f.recvErr
}

type fakeClientStream struct {
	grpc.ClientStream
	sendErr     error
	onSend      func()
	onCloseSend func()
}

func (f *fakeClientStream) Send(*grpcprocv1.Frame) error {
	if f.onSend != nil {
		f.onSend()
	}
	return f.sendErr
}

func (f *fakeClientStream) Recv() (*grpcprocv1.Frame, error) { select {} }

func (f *fakeClientStream) CloseSend() error {
	if f.onCloseSend != nil {
		f.onCloseSend()
	}
	return nil
}
