// Blocking I/O: a line server whose listener and connections are processes.
// A process owns its goroutine, so one that takes no messages may block on
// I/O; one that must also take messages leaves the blocking reads to a
// goroutine of its own, which only sends.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/connpb"
)

// listener accepts connections and serves each in a process of its own,
// linked to it, so that they end when it does. Nothing is sent to it, so it
// may block in Accept: its exit closes the listener, which ends the wait.
func listener(ln net.Listener) func(*grpcproc.Process[proto.Message]) error {
	return func(p *grpcproc.Process[proto.Message]) error {
		context.AfterFunc(p.Context(), func() { _ = ln.Close() })
		for {
			c, err := ln.Accept()
			if err != nil {
				if cause := context.Cause(p.Context()); cause != nil {
					return cause // asked to exit: the reason is the process's
				}
				return err
			}
			conn, err := p.Spawn(serve(c), grpcproc.LinkParent(), grpcproc.WithLabel("conn"))
			if err != nil {
				_ = c.Close()
				continue
			}
			// Writes reach a connection as messages, from any process.
			_ = p.Send(conn, write("welcome to "+p.Node().Name()))
		}
	}
}

// serve runs one connection, and answers each line with it in upper case.
// Its goroutine owns the connection and takes messages; the blocking reads
// are another goroutine's, which touches nothing of the process's and only
// sends what it reads to the process's mailbox, as the node.
func serve(c net.Conn) func(*grpcproc.Process[*connpb.Event]) error {
	return func(p *grpcproc.Process[*connpb.Event]) error {
		defer func() { _ = c.Close() }()
		context.AfterFunc(p.Context(), func() { _ = c.Close() }) // an exit ends the read
		node, self := p.Node(), p.Addr()
		go func() {
			lines := bufio.NewScanner(c)
			for lines.Scan() {
				_ = node.Send(context.Background(), self, received(lines.Text()))
			}
			if p.Context().Err() == nil { // not the process's own exit closing c
				_ = node.Send(context.Background(), self, closed(lines.Err()))
			}
		}()
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			switch e := m.Body.Kind.(type) {
			case *connpb.Event_Received:
				if _, err := fmt.Fprintln(c, strings.ToUpper(e.Received)); err != nil {
					return err
				}
			case *connpb.Event_Write:
				if _, err := fmt.Fprintln(c, e.Write); err != nil {
					return err
				}
			case *connpb.Event_Closed:
				return nil // the peer hung up
			}
		}
	}
}

func main() {
	ctx := context.Background()
	node, err := grpcproc.NewNode(grpcproc.Config{Name: "edge", Resolver: grpcproc.StaticResolver{}})
	check(err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	check(err)
	server, err := node.Spawn(listener(ln), grpcproc.WithName("listener"))
	check(err)

	// A client, as the outside world sees the server.
	c, err := net.Dial("tcp", ln.Addr().String())
	check(err)
	defer func() { _ = c.Close() }()
	lines := bufio.NewScanner(c)
	lines.Scan()
	fmt.Println("<", lines.Text())
	for _, s := range []string{"hello", "blocking reads"} {
		_, err := fmt.Fprintln(c, s)
		check(err)
		fmt.Println(">", s)
		lines.Scan()
		fmt.Println("<", lines.Text())
	}

	// The listener's exit closes its listener and ends the connection's
	// process, linked to it, whose exit closes the connection.
	check(node.Exit(ctx, server, "closing"))
	fmt.Println("connection open:", lines.Scan())

	check(node.Stop(ctx))
}

func received(line string) *connpb.Event {
	return &connpb.Event{Kind: &connpb.Event_Received{Received: line}}
}

func write(line string) *connpb.Event {
	return &connpb.Event{Kind: &connpb.Event_Write{Write: line}}
}

func closed(err error) *connpb.Event {
	reason := "EOF"
	if err != nil {
		reason = err.Error()
	}
	return &connpb.Event{Kind: &connpb.Event_Closed{Closed: reason}}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
