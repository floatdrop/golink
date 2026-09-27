package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// Protocol version carried in the handshake. Bumped on incompatible change.
const protoVersion = 1

// maxFrame is roughly how large a Frame the writer builds, well under gRPC's
// default 4 MiB receive limit. An envelope larger than that goes alone.
const maxFrame = 1 << 20

const (
	mdNode        = "grpcproc-node"
	mdIncarnation = "grpcproc-incarnation"
	mdVersion     = "grpcproc-version"
)

// Topology: a node opens one Link stream to each peer it sends to and only
// writes on it; it only reads from streams peers opened to it. Two streams per
// active pair, but no simultaneous-dial tie-breaking, and one writer per
// (node -> node) direction keeps per-sender ordering, as in Erlang.

type linkStats struct {
	messages, bytes atomic.Uint64
	established     time.Time
	reconnects      uint64
	lastErr         atomic.Pointer[string]
	state           atomic.Uint32
}

func (s *linkStats) fail(err error) {
	if err != nil {
		s.lastErr.Store(new(err.Error()))
	}
	s.state.Store(uint32(LinkDown))
}

func (s *linkStats) fill(li *LinkInfo) {
	li.State = LinkState(s.state.Load())
	li.EstablishedAt = s.established
	li.Reconnects = s.reconnects
	li.Messages = s.messages.Load()
	li.Bytes = s.bytes.Load()
	if e := s.lastErr.Load(); e != nil {
		li.LastError = *e
	}
}

// ---------- outbound ----------

type outLink struct {
	peer     NodeID
	cc       *grpc.ClientConn
	stream   grpc.BidiStreamingClient[grpcprocv1.Frame, grpcprocv1.Frame]
	cancel   context.CancelFunc
	q        *queue[*grpcprocv1.Envelope]
	done     chan struct{}
	once     sync.Once
	closing  atomic.Bool
	drained  chan struct{} // closed by writeLoop once closing is set and the queue is empty
	recvDone chan struct{} // closed when the server ends the stream
	linkStats
}

func (l *outLink) send(env *grpcprocv1.Envelope) error {
	if !l.q.push(env) {
		return &LinkError{Peer: l.peer.Name, Err: ErrNoConnection}
	}
	return nil
}

func (l *outLink) info() LinkInfo {
	li := LinkInfo{Peer: l.peer, Outbound: true}
	l.fill(&li)
	return li
}

func (l *outLink) start(n *Node) {
	go l.writeLoop(n)
	go func() {
		// The server never sends after Hello; Recv returning is the close signal.
		_, err := l.stream.Recv()
		close(l.recvDone)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		n.connLost(l.peer.Name, l, nil, err, false)
	}()
}

func (l *outLink) writeLoop(n *Node) {
	for {
		select {
		case <-l.q.notify:
		case <-l.done:
			return
		}
		// Everything queued since the last write goes out together: under
		// load, many envelopes share one gRPC message.
		for batch := l.q.drain(); len(batch) > 0; {
			k, body := frameOf(batch)
			if err := l.stream.Send(&grpcprocv1.Frame{Envelopes: batch[:k]}); err != nil {
				n.connLost(l.peer.Name, l, nil, err, false)
				return
			}
			l.messages.Add(uint64(k))
			l.bytes.Add(uint64(body))
			batch = batch[k:]
		}
		if l.closing.Load() && l.q.len() == 0 {
			_ = l.stream.CloseSend()
			close(l.drained)
			return
		}
	}
}

// shutdown flushes what is queued, half-closes the stream and waits for the
// peer to end it, so nothing sent is lost to a cancel racing with the data.
func (l *outLink) shutdown(ctx context.Context) {
	l.closing.Store(true)
	select {
	case l.q.notify <- struct{}{}:
	default:
	}
	select {
	case <-l.drained:
		select {
		case <-l.recvDone:
		case <-l.done:
		case <-ctx.Done():
		}
	case <-l.done:
	case <-ctx.Done():
	}
	l.close(nil)
}

func (l *outLink) close(err error) {
	l.once.Do(func() {
		l.fail(err)
		close(l.done)
		l.q.close() // undelivered envelopes; senders learn through Down / call failure
		l.cancel()
		_ = l.cc.Close()
	})
}

type dialOp struct {
	done chan struct{}
	l    *outLink
	err  error
}

// getOut returns the link to peer, dialing if there is none. Concurrent
// callers share one dial, which runs on its own goroutine: a caller whose ctx
// ends stops waiting, and the dial goes on for the others.
func (n *Node) getOut(ctx context.Context, peer string) (*outLink, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrNodeStopped
	}
	if l := n.out[peer]; l != nil {
		n.mu.Unlock()
		return l, nil
	}
	if err := ctx.Err(); err != nil {
		n.mu.Unlock()
		return nil, err // it would not wait for the dial, so it starts none
	}
	d := n.dialing[peer]
	if d == nil {
		d = &dialOp{done: make(chan struct{})}
		n.dialing[peer] = d
		// Under n.mu, and only while not stopped: Stop's Wait sees every Add.
		n.dialWG.Go(func() { n.finishDial(peer, d) })
	}
	n.mu.Unlock()
	select {
	case <-d.done:
		return d.l, d.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// finishDial dials peer for d's waiters and installs the link.
func (n *Node) finishDial(peer string, d *dialOp) {
	l, err := n.dial(peer)
	n.mu.Lock()
	delete(n.dialing, peer)
	if err == nil {
		if n.stopped {
			l.close(ErrNodeStopped)
			l, err = nil, ErrNodeStopped
		} else {
			n.out[peer] = l
		}
	}
	n.mu.Unlock()
	if err == nil {
		l.start(n)
		n.linkUp(l.peer)
	}
	d.l, d.err = l, err
	close(d.done)
}

func (n *Node) dial(peer string) (*outLink, error) {
	// Not derived from n.ctx: a dial that Stop overtakes completes and is
	// then discarded by finishDial, rather than failing half-way with a
	// misleading error. Stop waits for it.
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()
	addr, err := n.cfg.Resolver.Resolve(ctx, peer)
	if err != nil {
		return nil, &LinkError{Peer: peer, Err: err}
	}
	cc, err := grpc.NewClient(addr, n.cfg.DialOptions...)
	if err != nil {
		return nil, &LinkError{Peer: peer, Err: err}
	}
	// The stream outlives n.ctx: processes exiting on Stop still need it to
	// deliver their Down{shutdown}. Stop closes it after they are gone.
	sctx, scancel := context.WithCancel(context.Background())
	sctx = metadata.AppendToOutgoingContext(sctx,
		mdNode, n.id.Name,
		mdIncarnation, strconv.FormatUint(n.id.Incarnation, 10),
		mdVersion, strconv.Itoa(protoVersion),
	)
	stream, err := grpcprocv1.NewNodeClient(cc).Link(sctx)
	var inc uint64
	if err == nil {
		inc, err = handshake(ctx, peer, stream)
	}
	if err != nil {
		scancel()
		_ = cc.Close()
		return nil, &LinkError{Peer: peer, Err: err}
	}
	l := &outLink{
		established: time.Now(),
		peer:        NodeID{Name: peer, Incarnation: inc},
		cc:          cc,
		stream:      stream,
		cancel:      scancel,
		q:           newQueue[*grpcprocv1.Envelope](false),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
		recvDone:    make(chan struct{}),
	}
	n.mu.Lock()
	l.reconnects = n.dials[peer]
	n.dials[peer]++
	n.mu.Unlock()
	l.state.Store(uint32(LinkUp))
	return l, nil
}

// handshake waits for the server's Hello within ctx and checks it names the
// peer we meant to reach.
func handshake(ctx context.Context, peer string, stream grpc.BidiStreamingClient[grpcprocv1.Frame, grpcprocv1.Frame]) (uint64, error) {
	type res struct {
		f   *grpcprocv1.Frame
		err error
	}
	ch := make(chan res, 1)
	go func() {
		f, err := stream.Recv()
		ch <- res{f, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return 0, r.err
		}
		var h *grpcprocv1.Hello
		if envs := r.f.GetEnvelopes(); len(envs) == 1 && envs[0].GetKind() == grpcprocv1.Kind_KIND_HELLO {
			h = envs[0].GetHello()
		}
		if h == nil {
			return 0, errors.New("grpcproc: handshake: expected Hello")
		}
		if h.GetVersion() != protoVersion {
			return 0, fmt.Errorf("grpcproc: handshake: peer speaks protocol %d, this node %d", h.GetVersion(), protoVersion)
		}
		if h.GetNode() != peer {
			return 0, fmt.Errorf("grpcproc: handshake: dialed %q but reached %q", peer, h.GetNode())
		}
		return h.GetIncarnation(), nil
	case <-ctx.Done():
		return 0, fmt.Errorf("grpcproc: handshake: %w", ctx.Err())
	}
}

// ---------- inbound ----------

type inLink struct {
	peer   NodeID
	closed chan struct{}
	once   sync.Once
	// mu is held while a frame is dispatched and while the link closes, so
	// nothing is dispatched after close returns: a Down{noconnection} that
	// follows a close is never overtaken by a message from the same link.
	mu      sync.Mutex
	stopped bool
	linkStats
}

func (l *inLink) info() LinkInfo {
	li := LinkInfo{Peer: l.peer, Outbound: false}
	l.fill(&li)
	return li
}

func (l *inLink) close() {
	l.once.Do(func() {
		l.mu.Lock()
		l.stopped = true
		l.mu.Unlock()
		close(l.closed)
	})
}

// deliver dispatches a frame's envelopes in order, unless the link has
// closed; it reports whether it did.
func (l *inLink) deliver(n *Node, f *grpcprocv1.Frame) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	for _, env := range f.GetEnvelopes() {
		n.dispatch(l.peer.Name, env)
	}
	_, body := frameOf(f.GetEnvelopes())
	l.messages.Add(uint64(len(f.GetEnvelopes())))
	l.bytes.Add(uint64(body))
	return true
}

// frameOf says how many leading envelopes of batch fit in one frame (at
// least one) and how many message-body bytes they carry.
func frameOf(batch []*grpcprocv1.Envelope) (n, body int) {
	size := 0
	for i, env := range batch {
		b := bodySize(env)
		est := 64 + b
		for k, v := range env.GetMetadata() {
			est += len(k) + len(v) + 8
		}
		if i > 0 && size+est > maxFrame {
			return i, body
		}
		size += est
		body += b
	}
	return len(batch), body
}

func bodySize(env *grpcprocv1.Envelope) int { return len(env.GetBody()) }

// serveLink is the server side of a link: the peer's stream of frames to
// this node.
func (n *Node) serveLink(stream grpc.BidiStreamingServer[grpcprocv1.Frame, grpcprocv1.Frame]) error {
	ctx := stream.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	peer := NodeID{Name: first(md, mdNode)}
	peer.Incarnation, _ = strconv.ParseUint(first(md, mdIncarnation), 10, 64)
	version, _ := strconv.Atoi(first(md, mdVersion))
	switch {
	case version != protoVersion:
		return status.Errorf(codes.FailedPrecondition, "grpcproc: protocol version %d, this node speaks %d", version, protoVersion)
	case peer.Name == "" || peer.Name == n.id.Name:
		return status.Errorf(codes.InvalidArgument, "grpcproc: bad node name %q", peer.Name)
	}
	if n.cfg.Authorize != nil {
		if err := n.cfg.Authorize(ctx, peer); err != nil {
			n.log.Warn("rejected peer", "peer", peer, "err", err)
			return status.Errorf(codes.PermissionDenied, "grpcproc: %v", err)
		}
	}
	if err := stream.Send(&grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{{Kind: grpcprocv1.Kind_KIND_HELLO, Hello: &grpcprocv1.Hello{
		Node: n.id.Name, Incarnation: n.id.Incarnation, Version: protoVersion,
	}}}}); err != nil {
		return err
	}

	l := &inLink{peer: peer, closed: make(chan struct{}), established: time.Now()}
	l.state.Store(uint32(LinkUp))

	// A new stream from a peer we already have one from means the peer lost
	// its session with us (it restarted, or its side broke): end the old one.
	n.mu.Lock()
	old := n.in[peer.Name]
	n.mu.Unlock()
	if old != nil {
		n.connLost(peer.Name, nil, old, errors.New("replaced by a new link"), false)
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return status.Error(codes.Unavailable, ErrNodeStopped.Error())
	}
	n.in[peer.Name] = l
	n.mu.Unlock()
	n.linkUp(peer)

	// Recv cannot be interrupted, so it runs on its own goroutine, which also
	// dispatches what it reads (no hop through a channel), and the handler
	// can return when the link is closed from this side. The stream's own
	// context is deliberately not selected on: a peer's cancel must be seen
	// through Recv, after every envelope that preceded it.
	errs := make(chan error, 1)
	go func() {
		for {
			f, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			if !l.deliver(n, f) {
				return
			}
		}
	}()
	var err error
	select {
	case err = <-errs:
	case <-l.closed:
		err = errors.New("closed")
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	n.connLost(peer.Name, nil, l, err, false)
	return nil
}

// connLost is called when a link to or from peer breaks.
//
// The two directions are independent streams, so an envelope the peer sent
// can still be in flight on the inbound link when the outbound one fails.
// The peer is therefore declared down only once the inbound link has ended
// (everything it sent has then been dispatched, in order), or when there is
// no inbound link at all. An outbound failure alone just drops that link; the
// next send dials again. force (Disconnect) tears both down at once.
func (n *Node) connLost(peer string, out *outLink, in *inLink, err error, force bool) {
	n.mu.Lock()
	if (out != nil && n.out[peer] != out) || (in != nil && n.in[peer] != in) {
		n.mu.Unlock()
		if out != nil {
			out.close(err)
		}
		if in != nil {
			in.fail(err)
			in.close()
		}
		return // stale; already handled
	}
	if in == nil && !force {
		curIn := n.in[peer]
		delete(n.out, peer)
		n.mu.Unlock()
		out.close(err)
		if curIn != nil {
			n.log.Debug("outbound link lost; waiting for inbound", "peer", peer, "err", err)
			return
		}
		n.nodeDown(peer, err)
		n.linkDown(out.peer, err)
		return
	}
	curOut, curIn := n.out[peer], n.in[peer]
	delete(n.out, peer)
	delete(n.in, peer)
	n.mu.Unlock()
	var id NodeID
	if curOut != nil {
		id = curOut.peer
		curOut.close(err)
	}
	if curIn != nil {
		id = curIn.peer
		curIn.fail(err)
		curIn.close()
	}
	n.nodeDown(peer, err)
	n.linkDown(id, err)
}

func (n *Node) linkUp(peer NodeID) {
	if n.hooks != nil {
		n.hooks.OnLinkUp(peer)
	}
	n.subs.publish(Event{Kind: EventLinkUp, Peer: peer})
}

func (n *Node) linkDown(peer NodeID, err error) {
	if n.hooks != nil {
		n.hooks.OnLinkDown(peer, err)
	}
	ev := Event{Kind: EventLinkDown, Peer: peer}
	if err != nil {
		ev.Err = err.Error()
	}
	n.subs.publish(ev)
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}
