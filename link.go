package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
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
}

// fill reports a link that is up: a link leaves n.out or n.in, under n.mu,
// before it closes, and Info reads only those.
func (s *linkStats) fill(li *LinkInfo) {
	li.State = LinkUp
	li.EstablishedAt = s.established
	li.Reconnects = s.reconnects
	li.Messages = s.messages.Load()
	li.Bytes = s.bytes.Load()
}

// ---------- outbound ----------

type outLink struct {
	node     *Node
	peer     NodeID
	cc       *grpc.ClientConn
	stream   grpc.BidiStreamingClient[grpcprocv1.Frame, grpcprocv1.Frame]
	cancel   context.CancelFunc
	q        *queue[*grpcprocv1.Envelope]
	done     chan struct{}
	once     sync.Once
	closing  atomic.Bool
	inflight atomic.Int64 // envelopes the writer has taken and not yet written
	linkStats
}

func (l *outLink) send(env *grpcprocv1.Envelope) error {
	if !l.q.push(env) {
		return &LinkError{Peer: l.peer.Name, Err: ErrNoConnection, Unsent: true}
	}
	return nil
}

func (l *outLink) info() LinkInfo {
	li := LinkInfo{Peer: l.peer, Outbound: true, Queued: l.q.len() + int(l.inflight.Load())}
	l.fill(&li)
	return li
}

func (l *outLink) start() {
	n := l.node
	go l.writeLoop()
	go func() {
		// The server never sends after Hello; Recv returning is the close signal.
		_, err := l.stream.Recv()
		if errors.Is(err, io.EOF) {
			err = nil
		}
		n.outLost(l, err)
	}()
}

func (l *outLink) writeLoop() {
	n := l.node
	for {
		select {
		case <-l.q.notify:
		case <-l.done:
			return
		}
		// Everything queued since the last write goes out together: under
		// load, many envelopes share one gRPC message.
		batch := l.q.drain()
		l.inflight.Store(int64(len(batch)))
		for len(batch) > 0 {
			k, body := frameOf(batch)
			if err := l.stream.Send(&grpcprocv1.Frame{Envelopes: batch[:k]}); err != nil {
				l.inflight.Store(0)
				// The frame may have gone out before the stream broke: its
				// messages are dead letters, and its calls fail with the
				// peer, as possibly handled. The rest of the batch never
				// went: back on the queue, where closing the link fails its
				// calls as unsent.
				n.lost(l.peer.Name, batch[:k], nil)
				if !l.q.putBack(batch[k:]) {
					n.lost(l.peer.Name, batch[k:], err) // the link has closed meanwhile
				}
				n.outLost(l, err)
				return
			}
			l.messages.Add(uint64(k))
			l.bytes.Add(uint64(body))
			l.inflight.Add(-int64(k))
			batch = batch[k:]
		}
		// Sealed as it half-closes: a send that comes later fails at once, as
		// unsent, rather than wait in a queue nothing will write.
		if l.closing.Load() && l.q.sealIfEmpty() {
			_ = l.stream.CloseSend()
			return
		}
	}
}

// shutdown starts flushing what is queued, after which the writer
// half-closes the stream. finish then waits for the peer to end it, so that
// nothing sent is lost to a cancel racing with the data: the stream's reader
// then closes the link (outLost). Stop starts every link's shutdown before it
// waits for any: a peer that never ends its stream costs the others nothing.
func (l *outLink) shutdown() {
	l.closing.Store(true)
	select {
	case l.q.notify <- struct{}{}:
	default:
	}
}

// finish reports whether ctx cut the flush short.
func (l *outLink) finish(ctx context.Context) (cut bool) {
	select {
	case <-l.done:
	case <-ctx.Done():
		cut = true
	}
	l.close(ErrNodeStopped)
	return cut
}

func (l *outLink) close(err error) {
	l.once.Do(func() {
		close(l.done)
		// What is still queued was never written: its messages are dead
		// letters, and its calls fail now, as unsent.
		cause := err
		if cause == nil {
			cause = ErrNoConnection
		}
		l.node.lost(l.peer.Name, l.q.close(), cause)
		l.cancel()
		_ = l.cc.Close()
	})
}

// lost handles envelopes a link to peer failed to write. Their messages
// are dead letters (ReasonNoConnection). If unsent is set, the envelopes
// surely never left, and a call among them fails at once with it, as a
// LinkError that says so; otherwise its call fails when the peer is
// declared down, as one that may have been handled. The other envelopes are
// left alone: the peer learns of a lost reply or Down when its link from
// this node ends, a lost monitor fires when this node declares the peer
// down, and a lost exit is lost, as in Erlang.
func (n *Node) lost(peer string, envs []*grpcprocv1.Envelope, unsent error) {
	for _, env := range envs {
		kind := env.GetKind()
		if kind != grpcprocv1.Kind_KIND_SEND && kind != grpcprocv1.Kind_KIND_CALL {
			continue
		}
		body, _ := decodeBody(env)
		from := PID{Node: n.id.Name, Incarnation: env.GetFromIncarnation(), ID: env.GetFromId()}
		to := PID{Node: peer, Incarnation: env.GetToIncarnation(), ID: env.GetToId()}
		n.deadLetter(from, to, body, ReasonNoConnection)
		// Counted before failCall, as deliver does before replying: a caller
		// woken here sees its dead letter.
		if kind == grpcprocv1.Kind_KIND_CALL && unsent != nil {
			n.failCall(env.GetRef(), &LinkError{Peer: peer, Err: unsent, Unsent: true})
		}
	}
}

// failCall ends a pending call with err, unless it has ended already.
func (n *Node) failCall(ref uint64, err error) {
	if pc := n.takePending(ref); pc != nil {
		pc.ch <- callResult{err: err}
	}
}

// outTo returns the link to peer, or nil if there is none. The caller holds
// n.mu, shared or not.
func (n *Node) outTo(peer string) (*outLink, error) {
	if n.stopped {
		return nil, ErrNodeStopped
	}
	return n.out[peer], nil
}

type dialOp struct {
	done      chan struct{}
	l         *outLink
	err       error
	forgotten bool // Disconnect or Membership's Up came during the dial; guarded by n.mu
	// answers are replies and Downs that dispatch made while the dial was
	// under way, written first once the link is up, and cut the inbound
	// links they answer, to be cut if they cannot go; guarded by n.mu.
	answers []*grpcprocv1.Envelope
	cut     []*inLink
}

// getOut returns the link to peer, dialing if there is none. Concurrent
// callers share one dial, which runs on its own goroutine: a caller whose ctx
// ends stops waiting, and the dial goes on for the others.
func (n *Node) getOut(ctx context.Context, peer string) (*outLink, error) {
	// A send over a live link only reads, so it shares n.mu.
	n.mu.RLock()
	l, err := n.outTo(peer)
	n.mu.RUnlock()
	if l != nil || err != nil {
		return l, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err // it would not wait for the dial, so it starts none
	}
	n.mu.Lock()
	if l, err := n.outTo(peer); l != nil || err != nil { // it came up, or the node stopped
		n.mu.Unlock()
		return l, err
	}
	d, err := n.dialFor(peer)
	n.mu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case <-d.done:
		return d.l, d.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialFor returns the dial to peer, starting one if none is under way, or
// why sends to peer fail at once. Called with n.mu held, while the node is
// not stopped and has no link to peer.
func (n *Node) dialFor(peer string) (*dialOp, error) {
	d := n.dialing[peer]
	// While dials to peer fail, sends fail at once rather than wait for one.
	// The first send after the wait starts the next dial, and waits for it
	// alone: a hung peer then holds one sender at a time, not all of them.
	if r := n.backedOff(peer); r != nil && (d != nil || time.Now().Before(r.at)) {
		next := "the next dial is under way"
		if d == nil {
			next = fmt.Sprintf("next dial in %v", max(time.Until(r.at), time.Millisecond).Round(time.Millisecond))
		}
		return nil, &LinkError{Peer: peer, Err: fmt.Errorf("dialing it failed, %s: %w", next, r.err), Unsent: true}
	}
	if d == nil {
		d = &dialOp{done: make(chan struct{})}
		n.dialing[peer] = d
		// Under n.mu, and only while not stopped: Stop's Wait sees every Add.
		n.dialWG.Go(func() { n.finishDial(peer, d) })
	}
	return d, nil
}

// redial is a peer whose last dial failed: sends to it fail at once until at.
type redial struct {
	at   time.Time
	wait time.Duration // before jitter; the next failure doubles it
	err  error         // why the last dial failed
	why  string        // err's text, taken outside n.mu
}

// backedOff returns peer's backoff, or nil. One whose wait ended more than
// DialBackoff ago is forgotten: nothing has been sent to the peer for that
// long, so its next failure starts the doubling afresh. Called with n.mu held.
func (n *Node) backedOff(peer string) *redial {
	r := n.backoff[peer]
	if r != nil && time.Since(r.at) > n.cfg.DialBackoff {
		delete(n.backoff, peer)
		return nil
	}
	return r
}

// failedDial backs off from peer after a dial to it failed. Called with n.mu
// held.
func (n *Node) failedDial(peer string, err error, why string) {
	limit := n.cfg.DialBackoff
	if limit < 0 {
		return
	}
	wait := limit / 32
	if r := n.backedOff(peer); r != nil {
		wait = limit
		if r.wait <= limit/2 { // not 2*r.wait > limit, which overflows near the largest Duration
			wait = 2 * r.wait
		}
	}
	// Up to a fifth shorter, so that nodes that lost the same peer do not
	// all dial it again at the same moment.
	jittered := wait - time.Duration(rand.Float64()*float64(wait)/5)
	n.backoff[peer] = &redial{at: time.Now().Add(jittered), wait: wait, err: err, why: why}
}

// forget ends the backoff from peer, for Disconnect or Membership reporting
// the peer up. A dial under way that fails anyway starts none, since it began
// before the news. Called with n.mu held.
func (n *Node) forget(peer string) {
	delete(n.backoff, peer)
	if d := n.dialing[peer]; d != nil {
		d.forgotten = true
	}
}

// finishDial dials peer for d's waiters and installs the link.
func (n *Node) finishDial(peer string, d *dialOp) {
	l, err := n.dial(peer)
	var why string
	if err != nil {
		why = err.Error() // outside n.mu: the Resolver's or an interceptor's error
	}
	var discard *outLink
	n.mu.Lock()
	delete(n.dialing, peer)
	answers, cut := d.answers, d.cut
	d.answers, d.cut = nil, nil
	if err != nil {
		if !n.stopped && !d.forgotten {
			n.failedDial(peer, err, why)
		}
		err = &LinkError{Peer: peer, Err: err, Unsent: true}
	} else {
		delete(n.backoff, peer)
		if n.stopped {
			discard, l, err = l, nil, ErrNodeStopped
		} else {
			// Ahead of anything a sender queues once it sees the link.
			for _, env := range answers {
				l.q.push(env)
			}
			answers = nil
			n.out[peer] = l
		}
	}
	// Answers that will not go: the peer waits for them over its links to
	// this node they answer, which are cut, as routeOrCut does. Not a link
	// that has replaced them since: its session is owed nothing.
	if _, ok := errors.AsType[*LinkError](err); !ok || len(answers) == 0 {
		cut = nil
	}
	n.mu.Unlock()
	for _, in := range cut {
		in.abort(errors.Unwrap(err))
	}
	if discard != nil {
		discard.close(ErrNodeStopped) // outside n.mu, as every close
	}
	if err == nil {
		l.start()
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
		if ctx.Err() != nil {
			// The dial's deadline, not the caller's: do not wrap it.
			err = fmt.Errorf("grpcproc: no address within DialTimeout (%v): %v", n.cfg.DialTimeout, err)
		}
		return nil, err
	}
	cc, err := grpc.NewClient(addr, n.cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	// The stream outlives n.ctx: processes exiting on Stop still need it to
	// deliver their Down{shutdown}. Stop closes it after they are gone.
	sctx, scancel := context.WithCancel(context.Background())
	sctx = metadata.AppendToOutgoingContext(sctx,
		mdNode, n.id.Name,
		mdIncarnation, strconv.FormatUint(n.id.Incarnation, 10),
		mdVersion, strconv.Itoa(protoVersion),
	)
	// Opening the stream waits for a connection, and the handshake for the
	// peer's Hello. Both wait on sctx, which outlives the dial, so the dial's
	// deadline ends them by cancelling it. Nothing else would: the wait for a
	// connection lasts gRPC's connect timeout (20s), or forever with
	// WaitForReady, and the wait for the Hello forever.
	stop := context.AfterFunc(ctx, scancel)
	stream, err := grpcprocv1.NewNodeClient(cc).Link(sctx)
	var inc uint64
	if err == nil {
		inc, err = handshake(peer, stream)
	}
	if !stop() {
		// The deadline passed and cancelled the stream, whatever the dial
		// got to. What gRPC reported stays in the message: a wait for a
		// connection that WaitForReady kept through failed connects says
		// why they failed.
		what := "connection"
		if stream != nil {
			what = "Hello"
		}
		msg := fmt.Sprintf("grpcproc: no %s within DialTimeout (%v)", what, n.cfg.DialTimeout)
		if err != nil {
			msg += ": " + status.Convert(err).Message()
		}
		err = errors.New(msg)
	}
	if err != nil {
		scancel()
		_ = cc.Close()
		return nil, err
	}
	l := &outLink{
		node:        n,
		established: time.Now(),
		peer:        NodeID{Name: peer, Incarnation: inc},
		cc:          cc,
		stream:      stream,
		cancel:      scancel,
		q:           newQueue[*grpcprocv1.Envelope](false),
		done:        make(chan struct{}),
	}
	n.mu.Lock()
	l.reconnects = n.dials[peer]
	n.dials[peer]++
	n.mu.Unlock()
	return l, nil
}

// handshake waits for the server's Hello and checks it names the peer we
// meant to reach. The dial's deadline ends the wait by cancelling the stream.
func handshake(peer string, stream grpc.BidiStreamingClient[grpcprocv1.Frame, grpcprocv1.Frame]) (uint64, error) {
	f, err := stream.Recv()
	if err != nil {
		return 0, err
	}
	var h *grpcprocv1.Hello
	if envs := f.GetEnvelopes(); len(envs) == 1 && envs[0].GetKind() == grpcprocv1.Kind_KIND_HELLO {
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
}

// ---------- inbound ----------

type inLink struct {
	peer NodeID
	// done is closed when the link ends from this side: closed (why is nil),
	// or cut, when this node cannot route a reply or a Down back to the peer
	// (routeOrCut; why says why).
	done chan struct{}
	once sync.Once
	why  error
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

func (l *inLink) end(why error) {
	l.once.Do(func() {
		l.why = why
		close(l.done)
	})
}

// abort ends the link from its handler, which tells the peer why. It takes
// no lock, so it may run while the link dispatches a frame.
func (l *inLink) abort(err error) { l.end(err) }

func (l *inLink) close() {
	l.mu.Lock()
	l.stopped = true
	l.mu.Unlock()
	l.end(nil)
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

	l := &inLink{peer: peer, done: make(chan struct{}), established: time.Now()}
	// A new stream from a peer we already have one from means the peer lost
	// its session with us (it restarted, or its side broke): the old one ends
	// first. The new link goes in only once no change to the peer's links is
	// under way, so the old session's Downs are queued, and its link events
	// published, before anything from the new one.
	n.mu.Lock()
	for {
		if n.stopped {
			n.mu.Unlock()
			return status.Error(codes.Unavailable, ErrNodeStopped.Error())
		}
		if n.settling[peer.Name] > 0 {
			n.settled.Wait()
			continue
		}
		if n.in[peer.Name] == nil {
			break
		}
		oldOut, oldIn := n.takeLinks(peer.Name)
		n.mu.Unlock()
		n.linksLost(oldOut, oldIn, errors.New("replaced by a new link"))
		n.mu.Lock()
	}
	n.in[peer.Name] = l
	n.settling[peer.Name]++ // until it is announced
	// The peer reached this node, which shows it is up, not that this node
	// can reach it: the next send dials it at once, and the wait keeps
	// doubling if that fails too.
	if r := n.backoff[peer.Name]; r != nil {
		n.backoff[peer.Name] = &redial{at: time.Now(), wait: r.wait, err: r.err, why: r.why}
	}
	n.mu.Unlock()
	func() {
		defer n.settle(peer.Name) // even if a hook panics and an interceptor recovers
		n.linkUp(peer)
	}()

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
	case <-l.done:
		if l.why != nil {
			n.inLost(l, l.why)
			return status.Errorf(codes.Unavailable, "grpcproc: %s cannot reach %s back: %v", n.id.Name, peer.Name, l.why)
		}
		err = errors.New("closed")
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	n.inLost(l, err)
	return nil
}

// outLost handles the outbound link l breaking.
//
// The two directions are independent streams, so an envelope the peer sent
// can still be in flight on the inbound link when the outbound one fails.
// The peer is therefore declared down only once the inbound link has ended
// (everything it sent has then been dispatched, in order), or when there is
// no inbound link at all. An outbound failure alone just drops that link; the
// next send dials again.
func (n *Node) outLost(l *outLink, err error) {
	peer := l.peer.Name
	n.mu.Lock()
	current := n.out[peer] == l
	in := n.in[peer]
	down := current && in == nil
	if current {
		delete(n.out, peer)
	}
	if down {
		n.settling[peer]++
	}
	n.mu.Unlock()
	if down {
		defer n.settle(peer)
	}
	l.close(err)
	switch {
	case down:
		n.peerDown(l.peer, err)
	case current:
		n.log.Debug("outbound link lost; waiting for inbound", "peer", peer, "err", err)
	}
}

// inLost handles the inbound link l ending: the peer is down, and the link
// to it goes too.
func (n *Node) inLost(l *inLink, err error) {
	peer := l.peer.Name
	n.mu.Lock()
	current := n.in[peer] == l
	var out *outLink
	if current {
		out, _ = n.takeLinks(peer)
	}
	n.mu.Unlock()
	if !current {
		l.close() // already handled
		return
	}
	n.linksLost(out, l, err)
}

// takeLinks removes both links with peer, in the critical section that
// judged them. If it took any, the peer's links are settling until
// linksLost has declared it down. Called with n.mu held.
func (n *Node) takeLinks(peer string) (*outLink, *inLink) {
	out, in := n.out[peer], n.in[peer]
	if out != nil || in != nil {
		delete(n.out, peer)
		delete(n.in, peer)
		n.settling[peer]++
	}
	return out, in
}

// linksLost closes links taken with takeLinks, at least one of them, and
// declares their peer down.
func (n *Node) linksLost(out *outLink, in *inLink, err error) {
	var id NodeID
	if out != nil {
		id = out.peer
	}
	if in != nil {
		id = in.peer
	}
	defer n.settle(id.Name)
	if out != nil {
		out.close(err)
	}
	if in != nil {
		in.close()
	}
	n.peerDown(id, err)
}

// settle ends a change to peer's links begun under n.mu (see settling).
func (n *Node) settle(peer string) {
	n.mu.Lock()
	if n.settling[peer]--; n.settling[peer] == 0 {
		delete(n.settling, peer)
	}
	n.mu.Unlock()
	n.settled.Broadcast()
}

func (n *Node) peerDown(id NodeID, err error) {
	n.nodeDown(id.Name, err)
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
