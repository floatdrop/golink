# examples

A module of its own, built against the working tree, so every program here
compiles with the grpcproc next to it. CI runs each one and compares what it
prints with its `output.txt`; the README and the
[documentation site](../site) embed the code from here, so what they show is
code that runs.

| | |
| --- | --- |
| [`quickstart`](quickstart) | Two nodes on loopback, a typed process on one, a call and a monitor from the other. The README's example. |
| [`actors`](actors) | An actor built on `grpcproc/actor`: a struct with a method per kind of message, a deferred reply, and how it ends. |
| [`supervisor`](supervisor) | A supervisor restarting an actor that crashed, under the same name, from state kept outside it. |
| [`blockingio`](blockingio) | A line server whose listener and connections are processes: one blocks in `Accept`, the others leave their reads to a goroutine that only sends. |
| [`testing`](testing) | A two-node scenario as a plain `go test`, with `grpcproctest`: a partition, a crash, a restart. |
| [`guide`](guide) | The tutorial's application: a shop whose services are actors wired with `golang.yandex/di`, run as one program or as three nodes. |

After editing a program, refresh its pinned output and the README's embeds:

```sh
cd examples && go test ./quickstart ./actors ./supervisor ./blockingio -update
go test ./guide -update
cd .. && gofmt -w examples && go run github.com/campoy/embedmd@v1.0.0 -w README.md
```

The site reads the files directly at build time and needs no step of its own.
