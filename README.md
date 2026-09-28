# <img src="site/public/favicon.svg" alt="" height="32" align="absmiddle"> grpcproc

[![CI](https://github.com/floatdrop/grpcproc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/floatdrop/grpcproc/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/grpcproc.svg)](https://pkg.go.dev/github.com/floatdrop/grpcproc)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/floatdrop/grpcproc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Erlang-style processes for Go, on the gRPC server you already run.

A process is a goroutine with a typed mailbox and an address any node in the
cluster can use. `Send`, `Call`, `Monitor` and `Exit` work the same whether
the process runs in this binary or on another node, and supervisors restart
what fails.

```sh
go get github.com/floatdrop/grpcproc
```

**[Documentation](https://floatdrop.github.io/grpcproc/)** ·
[Getting started](https://floatdrop.github.io/grpcproc/start/) ·
[Concepts](https://floatdrop.github.io/grpcproc/concepts/actors/) ·
[API reference](https://pkg.go.dev/github.com/floatdrop/grpcproc)

## A quick look

Two nodes, each on its own gRPC server. A process on one, a call and a
monitor from the other:

[embedmd]:# (examples/quickstart/main.go go /\/\/ inventory is a process/ /check\(warehouse.Stop\(ctx\)\)\n}/)
```go
// inventory is a process: a goroutine with a mailbox. The mailbox holds
// *shoppb.Reserve and nothing else, and every message is a call, answered
// with a *shoppb.Reserved or an error.
func inventory(p *grpcproc.Process[*shoppb.Reserve]) error {
	left := map[string]int64{"apple": 3}
	for {
		m, err := p.Receive()
		if err != nil {
			return err // asked to exit, or the node is stopping
		}
		if left[m.Body.Sku] < m.Body.Qty {
			_ = m.Reply(nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
			continue
		}
		left[m.Body.Sku] -= m.Body.Qty
		_ = m.Reply(&shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
	}
}

func main() {
	ctx := context.Background()

	// Two nodes, each on its own gRPC server; see node() below.
	warehouse, shop := node(ctx, "warehouse"), node(ctx, "shop")

	// The process runs on warehouse, under a name.
	_, err := warehouse.Spawn(inventory, grpcproc.WithName("stock"))
	check(err)

	// From shop it is a node name and a process name. The address carries the
	// mailbox type, so the compiler checks what is sent to it.
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	for range 2 {
		r, err := shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 2})
		if err != nil {
			fmt.Println("reserve failed:", err) // the handler's error, as a *grpcproc.RemoteError
			continue
		}
		fmt.Println("reserved, left:", r.Left)
	}

	// A monitor across nodes works as one within a node does. A process on
	// shop monitors stock, then asks it to exit; the Down arrives with the
	// reason, as it would for a crash or a lost node.
	exited := make(chan string)
	_, err = shop.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(stock)
		check(p.Exit(stock, "closing"))
		m, err := p.Receive() // the Down: nothing else is sent to this process
		if err != nil {
			return err
		}
		exited <- m.Down.Reason
		return nil
	})
	check(err)
	fmt.Println("stock exited:", <-exited)

	_, err = shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 1})
	fmt.Println("no such process:", errors.Is(err, grpcproc.ErrNoProc))

	check(shop.Stop(ctx))
	check(warehouse.Stop(ctx))
}
```

[embedmd]:# (examples/quickstart/output.txt)
```txt
reserved, left: 1
reserve failed: only 1 apple left
stock exited: closing
no such process: true
```

[examples/quickstart](examples/quickstart/main.go) is the whole program,
`node()` included: a `grpcproc.Node` is a value you construct, register on
your `*grpc.Server` next to your other services, start and stop.

## What is in the box

- **Processes** with typed mailboxes, `Send` and `Call`, monitors and
  links, exit reasons: the core, which depends on gRPC and protobuf only.
  [Concepts](https://floatdrop.github.io/grpcproc/concepts/actors/)
- **Actors and supervision trees** in `grpcproc/actor`: a struct with a
  method per kind of message, restarted by a supervisor when it fails.
  [Supervision](https://floatdrop.github.io/grpcproc/concepts/supervision/)
- **Pub/sub** in `grpcproc/pubsub`: topics a process publishes to and any
  node subscribes to, with the last few events kept for whoever comes late,
  and each event sent once to each node.
  [Guide](https://floatdrop.github.io/grpcproc/guides/pubsub/)
- **An Inspector** and [`grpcprocctl`](tools/README.md): every process, its
  mailbox and what it says about itself, from a terminal or an AI agent over
  MCP. [Guide](https://floatdrop.github.io/grpcproc/guides/inspector/)
- **OpenTelemetry** in [`grpcproc/otel`](otel/README.md), **etcd** membership
  in [`grpcproc/etcd`](etcd/README.md), and `grpcproctest`, which runs a
  cluster inside one `go test`, partitions and crashes included.
  [Testing](https://floatdrop.github.io/grpcproc/guides/testing/)
- **A tutorial**: a shop whose services are actors, run as one program or as
  three nodes from the same code.
  [The shop](https://floatdrop.github.io/grpcproc/shop/)

[docs/DESIGN.md](docs/DESIGN.md) has the wire protocol and the reasons behind
the choices; [benchmarks](benchmarks/README.md) measures it against GoAkt,
Hollywood, Proto.Actor and Ergo.

## License

MIT, see [LICENSE](LICENSE).
