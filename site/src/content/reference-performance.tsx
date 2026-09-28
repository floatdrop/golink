import { Code } from '../code.tsx';
import { C, Ext } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

const results: { name: string; cells: string[] }[] = [
	{ name: 'Local send', cells: ['96 ns, 0 allocs', '95 ns (±15%), 0 allocs', '59 ns, 0 allocs', '200 ns, 0 allocs', '198 ns (±23%), 3 allocs'] },
	{ name: 'Local request', cells: ['761 ns, 2 allocs', '568 ns, 2 allocs', '2362 ns, 12 allocs', '2451 ns, 10 allocs', '3200 ns, 5 allocs'] },
	{ name: 'Remote send', cells: ['382 ns, 6 allocs', '520 ns (±10%), 7 allocs', '197 ns, 7 allocs', '340 ns, 9 allocs', '551 ns (±18%), 10 allocs'] },
	{ name: 'Remote request', cells: ['44.8 µs, 52 allocs', '35.9 µs, 42 allocs', '37.6 µs, 66 allocs', '60.7 µs, 103 allocs', '49.6 µs, 21 allocs'] },
	{ name: 'Remote request, parallel', cells: ['7.5 µs, 27 allocs', '10.1 µs, 40 allocs', '5.0 µs, 54 allocs', '7.0 µs, 54 allocs', '6.5 µs, 19 allocs'] },
	{ name: 'Geometric mean', cells: ['1.56 µs', '1.59 µs', '1.39 µs', '2.35 µs', '2.58 µs'] }
];

/** The column that is fastest in each row, drawn bold. */
const best = [2, 1, 2, 1, 2, 2];

export const referencePerformance: Doc = {
	path: 'reference/performance/',
	title: 'Performance',
	description:
		'grpcproc measured against GoAkt, Hollywood, Proto.Actor and Ergo: local and remote sends and requests, and how to read the numbers.',
	lead: (
		<p>
			<Ext href={file('benchmarks/README.md')}>benchmarks</Ext> is a module of its own, so grpcproc
			does not depend on what it is compared with, and it measures the working tree. The frameworks
			are <Ext href="https://github.com/tochemey/goakt">GoAkt</Ext> v4.5.6,{' '}
			<Ext href="https://github.com/anthdm/hollywood">Hollywood</Ext> v1.0.5,{' '}
			<Ext href="https://github.com/asynkron/protoactor-go">Proto.Actor</Ext> on its development
			branch, which has no Go-style release tags, and <Ext href="https://ergo.services">Ergo</Ext>{' '}
			3.3.0, each used the way it is meant to be.
		</p>
	),
	sections: [
		{
			id: 'method',
			title: 'What is measured',
			body: (
				<>
					<ul>
						<li>
							<strong>The same message</strong>, <C>wrapperspb.Int64Value</C>, because Hollywood and
							Proto.Actor need protobuf to go remote. Ergo's network registers struct values and not
							pointers, so it sends a struct holding the same <C>int64</C>.
						</li>
						<li>
							<strong>Send</strong> is end-to-end throughput: the timer stops when the receiver has
							counted every message, not when the sender has queued them.
						</li>
						<li>
							<strong>Request</strong> is one caller waiting for each reply; <strong>parallel</strong>{' '}
							is <C>RunParallel</C> callers against one echo.
						</li>
						<li>
							<strong>Remote</strong> is two engines, systems or nodes over real TCP on loopback, the
							connection warmed up before timing. grpcproc's own benchmarks in the root module use
							in-memory connections, which would flatter it here. GoAkt's remote actors are found with
							the public <C>PID.RemoteLookup</C> and reached through its remoting client, as an
							application would; Ergo's nodes find each other through its embedded registrar, on a
							port of their own.
						</li>
					</ul>
					<p>
						Each framework is its own package: Hollywood and Proto.Actor both register a protobuf file
						named <C>actor.proto</C>, which cannot share a binary, and separate binaries also keep one
						framework's globals, Proto.Actor replaces gRPC's logger, out of another's numbers.
					</p>
				</>
			)
		},
		{
			id: 'results',
			title: 'Results',
			body: (
				<>
					<p>
						Apple M3 Max, <C>-count=6</C>, medians by <C>benchstat</C>. The fastest in each row is
						bold.
					</p>
					<div className="gp-table-wrap">
						<table className="gp-table">
							<thead>
								<tr>
									<th></th>
									<th>grpcproc</th>
									<th>GoAkt</th>
									<th>Hollywood</th>
									<th>Proto.Actor</th>
									<th>Ergo</th>
								</tr>
							</thead>
							<tbody>
								{results.map((row, i) => (
									<tr key={row.name}>
										<td>{row.name}</td>
										{row.cells.map((cell, j) => (
											<td key={j}>{j === best[i] ? <strong>{cell}</strong> : cell}</td>
										))}
									</tr>
								))}
							</tbody>
						</table>
					</div>
					<p>
						Proto.Actor's local send varies between runs, 94 ns in one run of six and 189 to 200 ns
						in four others, and GoAkt's and Ergo's sends vary within one, as marked; the others hold
						within a few percent. A mailbox that keeps up, and a local send, allocate nothing.
					</p>
				</>
			)
		},
		{
			id: 'reading',
			title: 'How to read it',
			body: (
				<>
					<ul>
						<li>
							<strong>The transport decides sequential remote latency.</strong> GoAkt, with its own
							TCP protocol, and Hollywood, with dRPC, answer a remote request in 36 to 38 µs; the two
							that speak gRPC take longer, grpcproc 45 µs and Proto.Actor 61 µs. gRPC-go's writer
							adds a goroutine hand-off in each direction. On a real network the round trip dwarfs
							the difference; for grpcproc it is the cost of living on the application's gRPC server.
						</li>
						<li>
							<strong>Against the other gRPC library</strong>, grpcproc answers a remote request 26%
							sooner than Proto.Actor and trails it by about 40 ns on remote send, where Proto.Actor's
							writer batches up to a thousand envelopes.
						</li>
						<li>
							<strong>Locally</strong>, a grpcproc call waits on one channel of its own where Hollywood
							and Proto.Actor create a temporary process per request, which puts it three times ahead
							of them. GoAkt is quicker still, with as many allocations per call.
						</li>
						<li>
							<strong>Ergo starts a goroutine per hop.</strong> A sleeping process runs on a new
							goroutine each time a message wakes it, and so does each connection's read queue and
							each write flush. A request finds its echo asleep every time, so a local one takes
							3.2 µs and a remote one 50 µs, over Ergo's own TCP protocol; a stream of sends keeps
							the sink awake. Its remote calls allocate least of all, and it is second only to
							Hollywood on parallel requests.
						</li>
						<li>
							<strong>Hollywood's sends are fastest</strong> everywhere, with a lighter transport and
							vtprotobuf-generated envelopes. grpcproc also pays, on every message, for what its
							Inspector reports: per-process counters, mailbox ages, and a type check on delivery.
						</li>
					</ul>
				</>
			)
		},
		{
			id: 'ergo',
			title: 'A note on Ergo',
			body: (
				<>
					<p>
						Ergo runs with its software keepalive off, the one default changed. With it on, parallel
						remote requests timed out in 8 runs of 16: each connection's flusher arms one{' '}
						<C>time.AfterFunc</C> timer from its writers (500 ns) and from its own callback (15 s, the
						keepalive), and on Go 1.26.0 and 1.27.1 a <C>Reset</C> to 500 ns is now and then lost when
						more than one P runs. The batch of calls then sits unsent until another timer runs, at
						worst the keepalive, past Ergo's 5 s call timeout. The standard library alone reproduces
						it. The keepalive sends nothing while messages flow, so the numbers are still Ergo's.
					</p>
				</>
			)
		},
		{
			id: 'running',
			title: 'Running them',
			body: (
				<>
					<Code lang="sh">{`cd benchmarks
go test -run '^$' -bench . -count=6 ./... | sed 's|pkg: .*/benchmarks/|pkg: |' > new.txt
benchstat -table goos,goarch,cpu -col pkg -row .name new.txt`}</Code>
					<p>
						CI only checks that the benchmarks still build and run briefly; the numbers above are from
						one machine and one afternoon, and a change to grpcproc's hot path is measured again by
						hand.
					</p>
				</>
			)
		}
	]
};
