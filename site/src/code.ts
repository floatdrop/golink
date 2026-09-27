// The guide's code is the application under examples/guide, read at build
// time and highlighted once here, so the page is plain HTML and the code on
// it is code the Go toolchain compiles and tests.
import { codeToHtml } from 'shiki';

import inventoryProto from '../../examples/guide/proto/inventory/v1/inventory.proto?raw';
import inventoryAddress from '../../examples/guide/proto/inventory/v1/address.go?raw';
import inventory from '../../examples/guide/internal/inventory/inventory.go?raw';
import payments from '../../examples/guide/internal/payments/payments.go?raw';
import orders from '../../examples/guide/internal/orders/orders.go?raw';
import web from '../../examples/guide/internal/web/web.go?raw';
import platform from '../../examples/guide/internal/platform/platform.go?raw';
import root from '../../examples/guide/internal/platform/root.go?raw';
import run from '../../examples/guide/internal/platform/run.go?raw';
import config from '../../examples/guide/internal/platform/config.go?raw';
import local from '../../examples/guide/cmd/local/main.go?raw';
import front from '../../examples/guide/cmd/front/main.go?raw';
import warehouse from '../../examples/guide/cmd/warehouse/main.go?raw';
import billing from '../../examples/guide/cmd/billing/main.go?raw';
import ordersTest from '../../examples/guide/internal/orders/orders_test.go?raw';
import clusterTest from '../../examples/guide/cluster_test.go?raw';
import modules from '../../examples/guide/testdata/modules.txt?raw';
import localTree from '../../examples/guide/testdata/local.txt?raw';
import clusterTree from '../../examples/guide/testdata/cluster.txt?raw';

const localShell = `git clone https://github.com/floatdrop/grpcproc && cd grpcproc/examples
go build -o bin/ ./guide/cmd/...
bin/local &
sleep 1
curl -d '{"sku":"apple","qty":2,"card":"4242"}' localhost:8080/orders
kill %1`;

const clusterShell = `export PEERS=front=127.0.0.1:9101,warehouse=127.0.0.1:9102,billing=127.0.0.1:9103
NODE=warehouse LISTEN=127.0.0.1:9102 bin/warehouse &
NODE=billing LISTEN=127.0.0.1:9103 bin/billing &
NODE=front LISTEN=127.0.0.1:9101 PLACEMENT=inventory=warehouse,payments=billing bin/front &
sleep 1
curl -d '{"sku":"pear","qty":3,"card":"4242"}' localhost:8080/orders`;

const inspect = `go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@main
export GRPCPROC_ADDR=127.0.0.1:9101          # the front's Inspector
grpcprocctl --plaintext nodes                # the nodes it is linked to, and their links
grpcprocctl --plaintext ps --node warehouse  # the warehouse's processes, asked there
grpcprocctl --plaintext inspect root         # the front's root: its children, its restarts
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o shop.svg
kill %1 %2 %3                                # the three programs`;

// Both themes are emitted as custom properties on every token; the theme
// class on the html element picks one, so switching re-highlights nothing.
const highlight = (code: string, lang = 'go') =>
	codeToHtml(code.trimEnd(), {
		lang,
		themes: { light: 'github-light', dark: 'github-dark' },
		defaultColor: false
	});

const sources = {
	inventoryProto: [inventoryProto, 'proto'],
	inventoryAddress: [inventoryAddress, 'go'],
	inventory: [inventory, 'go'],
	payments: [payments, 'go'],
	orders: [orders, 'go'],
	web: [web, 'go'],
	platform: [platform, 'go'],
	root: [root, 'go'],
	run: [run, 'go'],
	config: [config, 'go'],
	local: [local, 'go'],
	front: [front, 'go'],
	warehouse: [warehouse, 'go'],
	billing: [billing, 'go'],
	ordersTest: [ordersTest, 'go'],
	clusterTest: [clusterTest, 'go'],
	localShell: [localShell, 'sh'],
	clusterShell: [clusterShell, 'sh'],
	inspect: [inspect, 'sh']
} as const;

export type Highlighted = keyof typeof sources;

export interface Code {
	html: Record<Highlighted, string>;
	/** Output the Go tests pin, shown as it is. */
	modules: string;
	localTree: string;
	clusterTree: string;
}

export async function loadCode(): Promise<Code> {
	const html = {} as Record<Highlighted, string>;
	for (const [name, [source, lang]] of Object.entries(sources) as [Highlighted, readonly [string, string]][]) {
		html[name] = await highlight(source, lang);
	}
	return {
		html,
		modules: modules.trimEnd(),
		localTree: localTree.trimEnd(),
		clusterTree: clusterTree.trimEnd()
	};
}
