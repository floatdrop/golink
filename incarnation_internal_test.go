package grpcproc

import (
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// helloStream reports the Hello it sends.
type helloStream struct {
	*fakeStream
	hello chan struct{}
}

func (h helloStream) Send(*grpcprocv1.Frame) error {
	close(h.hello)
	return nil
}

// A dial that reached b#2 drops a's links with b#1 before its link goes in.
// If a hears b#3 is up meanwhile, the dial is refused.
func TestDialRefusedWhenANewerIncarnationCameMeanwhile(t *testing.T) {
	e := newSettleEnv(t)
	e.links(t, 1)
	admitted := make(chan error, 1)
	go func() {
		e.n.mu.Lock()
		defer e.n.mu.Unlock()
		e.n.newest["b"] = 1
		admitted <- e.n.admit(NodeID{Name: "b", Incarnation: 2})
	}()
	<-e.h.entered
	e.n.memberEvent(MemberEvent{Member: Member{Name: "b", Incarnation: 3}, Up: true})
	close(e.h.release)
	if err := <-admitted; err == nil || err.Error() != "grpcproc: b#2 is an old incarnation: a has seen b#3" {
		t.Fatalf("got %v", err)
	}
}

// A link from b#3 waits to go in behind a change to b's links, its Hello
// sent, while a hears b#4 is up: then it is refused.
func TestLinkRefusedWhenANewerIncarnationCameMeanwhile(t *testing.T) {
	e := newSettleEnv(t)
	e.links(t, 2)
	disconnected := make(chan struct{})
	go func() { e.n.Disconnect("b"); close(disconnected) }()
	<-e.h.entered
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "3", mdVersion, "1"))
	fs := helloStream{&fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}, make(chan struct{})}
	served := make(chan error, 1)
	go func() { served <- e.n.serveLink(fs) }()
	<-fs.hello
	e.n.memberEvent(MemberEvent{Member: Member{Name: "b", Incarnation: 4}, Up: true})
	close(e.h.release)
	<-disconnected
	select {
	case err := <-served:
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "b#3 is an old incarnation: a has seen b#4") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		close(fs.recv)
		t.Fatalf("b#3's link went in: %v", e.n.Peers())
	}
}
