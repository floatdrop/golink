package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestCallDeadlineReachesCallee(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		local, gotLocal := answering(t, a)
		remote, gotRemote := answering(t, b)

		d := time.Now().Add(time.Minute)
		ctx, cancel := context.WithDeadline(t.Context(), d)
		defer cancel()

		// Locally the callee sees the caller's own deadline.
		if _, err := local.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		if got, ok := recv(t, gotLocal).Deadline(); !ok || !got.Equal(d) {
			t.Fatalf("local deadline %v %v, want %v", got, ok, d)
		}

		// Remotely it is the time left, counted from arrival: the caller's
		// deadline, or a little after it.
		if _, err := remote.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		if got, ok := recv(t, gotRemote).Deadline(); !ok || got.Before(d) || got.Sub(d) > time.Second {
			t.Fatalf("remote deadline %v %v, want from %v", got, ok, d)
		}

		// A deadline too far off for unix nanoseconds, remote or local, is
		// still one in the future, not an overflow into the past.
		far, cancelFar := context.WithDeadline(t.Context(), time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		defer cancelFar()
		for _, to := range []struct {
			addr grpcproc.Addr[*testpb.Ping]
			got  <-chan grpcproc.Msg[*testpb.Ping]
		}{{local, gotLocal}, {remote, gotRemote}} {
			if _, err := to.addr.Call[*testpb.Pong](far, a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			m := recv(t, to.got)
			if got, ok := m.Deadline(); !ok || got.Year() < 2262 {
				t.Fatalf("far deadline became %v %v", got, ok)
			}
			mctx, cancel := m.Context(t.Context())
			if mctx.Err() != nil {
				t.Fatal("a far deadline ended the context at once")
			}
			cancel()
		}

		// A caller with no deadline, and a send, give none.
		for _, to := range []struct {
			addr grpcproc.Addr[*testpb.Ping]
			got  <-chan grpcproc.Msg[*testpb.Ping]
		}{{local, gotLocal}, {remote, gotRemote}} {
			if _, err := to.addr.Call[*testpb.Pong](context.Background(), a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			if got, ok := recv(t, to.got).Deadline(); ok {
				t.Fatalf("a call with no deadline has one: %v", got)
			}
			if err := to.addr.Send(ctx, a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			m := recv(t, to.got)
			if got, ok := m.Deadline(); ok || m.IsCall() {
				t.Fatalf("a send has a deadline: %v", got)
			}
		}
	})
}

// A callee's work for a caller that gave up can stop: the message's
// context ends when the caller stops waiting, and keeps its metadata.
func TestMsgContextEndsWithTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		ended := make(chan error, 1)
		slow, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ctx, cancel := m.Context(p.Context())
			defer cancel()
			if grpcproc.MetadataFrom(ctx)["tenant"] != "acme" {
				t.Error("metadata lost")
			}
			<-ctx.Done() // work that outlives its caller's patience
			ended <- ctx.Err()
			return m.Reply(&testpb.Pong{}, nil) // nobody waits for it
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"}), 50*time.Millisecond)
		defer cancel()
		if _, err := slow.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want the caller's deadline, got %v", err)
		}
		select {
		case err := <-ended:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("the callee's context ended with %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the callee's context outlived its caller")
		}
	})
}

// A call whose ctx is already done sends nothing, even over a live link.
func TestCallWithADoneContextSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		got := make(chan struct{}, 1)
		echo, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- struct{}{}
				_ = m.Reply(m.Body, nil)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
			t.Fatal(err) // the link is up
		}
		<-got
		done, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := echo.Call[*testpb.Ping](done, a, &testpb.Ping{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
		// Anything sent would arrive before this call's own message.
		if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		<-got
		select {
		case <-got:
			t.Fatal("the call with a done ctx was sent")
		default:
		}
	})
}

func TestLeftoverCallsFailFast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		block := make(chan struct{})
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { <-block; return nil })
		errc := make(chan error, 1)
		go func() { _, err := a.CallTo[*testpb.Pong](t.Context(), e, &testpb.Ping{}); errc <- err }()
		time.Sleep(50 * time.Millisecond)
		close(block)
		select {
		case err := <-errc:
			if !errors.Is(err, grpcproc.ErrNoProc) {
				t.Fatalf("got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("call hung")
		}
	})
}

func TestCallFailsWhenCalleeExitsMidCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		// The callee takes the request and exits without replying.
		quitter, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.Receive()
			return err
		})
		if _, err := a.CallTo[*testpb.Pong](ctx(t), quitter, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("got %v", err)
		}
	})
}

// A process can hand a call to another to answer, on its node or another:
// the answer leaves from the node that took the call.
func TestReplyThroughAnotherProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		for _, answerer := range []string{"a", "b"} {
			for _, caller := range []string{"a", "b"} {
				jobs := make(chan grpcproc.Msg[*testpb.Ping], 1)
				front, err := c.Node("a").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
					m, err := p.Receive()
					if err != nil {
						return err
					}
					jobs <- m
					_, err = p.Receive() // stays up: nothing answers for it on exit
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Node(answerer).Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
					return (<-jobs).Reply(&testpb.Ping{N: 7}, nil)
				}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				resp, err := front.Call[*testpb.Ping](ctx, c.Node(caller), &testpb.Ping{N: 1})
				cancel()
				if err != nil || resp.GetN() != 7 {
					t.Fatalf("taken on a, answered on %s, called from %s: %v, %v", answerer, caller, resp, err)
				}
			}
		}
	})
}

// A restarted node reuses its call refs, so a reply meant for its earlier
// incarnation must not answer the new one's call with the same ref.
func TestReplyToAnEarlierIncarnationIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		held, release, replied := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		if _, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			close(held)
			<-release
			replied <- m.Reply(&testpb.Ping{}, nil) // to b's first incarnation
			return nil
		}, grpcproc.WithName("hold")); err != nil {
			t.Fatal(err)
		}
		received := make(chan struct{}, 1)
		if _, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if m.Body.GetN() == 1 {
					received <- struct{}{} // and never answers
					continue
				}
				_ = m.Reply(m.Body, nil)
			}
		}, grpcproc.WithName("x")); err != nil {
			t.Fatal(err)
		}

		go func() {
			_, _ = grpcproc.Named[*testpb.Ping]("a", "hold").Call[*testpb.Ping](context.Background(), c.Node("b"), &testpb.Ping{})
		}()
		<-held
		c.Kill("b")
		b := c.Restart("b")

		// The new b's first call has the ref the old b's had.
		pending := make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			_, err := grpcproc.Named[*testpb.Ping]("a", "x").Call[*testpb.Ping](ctx, b, &testpb.Ping{N: 1})
			pending <- err
		}()
		<-received
		close(release)
		if err := <-replied; err != nil {
			t.Fatalf("the stale reply was not sent: %v", err)
		}
		// A reply sent after the stale one reaches b after it, on the same link.
		if _, err := grpcproc.Named[*testpb.Ping]("a", "x").Call[*testpb.Ping](t.Context(), b, &testpb.Ping{N: 2}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-pending:
			t.Fatalf("the call was answered by a reply to the earlier b: %v", err)
		default:
		}
		cancel()
		if err := <-pending; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
