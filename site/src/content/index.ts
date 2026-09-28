// Every page of the site, in the order src/nav.ts reads them. A page listed
// in the navigation and missing here, or the other way round, fails the
// render, so the map and the pages cannot drift apart.
import { pagePaths } from '../nav.ts';
import { conceptsActors } from './concepts-actors.tsx';
import { conceptsAddressing } from './concepts-addressing.tsx';
import { conceptsDiscovery } from './concepts-discovery.tsx';
import { conceptsMonitorsAndLinks } from './concepts-monitors-and-links.tsx';
import { conceptsNodes } from './concepts-nodes.tsx';
import { conceptsProcesses } from './concepts-processes.tsx';
import { conceptsSupervision } from './concepts-supervision.tsx';
import { guidesActors } from './guides-actors.tsx';
import { guidesBlockingIO } from './guides-blocking-io.tsx';
import { guidesConfiguration } from './guides-configuration.tsx';
import { guidesCron } from './guides-cron.tsx';
import { guidesEtcd } from './guides-etcd.tsx';
import { guidesGrpcprocctl } from './guides-grpcprocctl.tsx';
import { guidesInspector } from './guides-inspector.tsx';
import { guidesLeader } from './guides-leader.tsx';
import { guidesMcp } from './guides-mcp.tsx';
import { guidesObservability } from './guides-observability.tsx';
import { guidesPubsub } from './guides-pubsub.tsx';
import { guidesSupervisors } from './guides-supervisors.tsx';
import { guidesTesting } from './guides-testing.tsx';
import { landing } from './landing.tsx';
import { referenceErrors } from './reference-errors.tsx';
import { referencePerformance } from './reference-performance.tsx';
import { shop } from './shop.tsx';
import { shopDeployments } from './shop-deployments.tsx';
import { shopPlatform } from './shop-platform.tsx';
import { shopServices } from './shop-services.tsx';
import { shopTesting } from './shop-testing.tsx';
import { start } from './start.tsx';
import type { Doc } from './types.ts';

const all: Doc[] = [
	landing,
	start,
	conceptsActors,
	conceptsProcesses,
	conceptsAddressing,
	conceptsMonitorsAndLinks,
	conceptsSupervision,
	conceptsNodes,
	conceptsDiscovery,
	guidesActors,
	guidesSupervisors,
	guidesBlockingIO,
	guidesPubsub,
	guidesConfiguration,
	guidesTesting,
	guidesObservability,
	guidesInspector,
	guidesGrpcprocctl,
	guidesMcp,
	guidesEtcd,
	guidesCron,
	guidesLeader,
	shop,
	shopServices,
	shopPlatform,
	shopDeployments,
	shopTesting,
	referenceErrors,
	referencePerformance
];

const byPath = new Map(all.map((d) => [d.path, d]));
for (const path of pagePaths) {
	if (!byPath.has(path)) {
		throw new Error(`content: the navigation lists ${JSON.stringify(path)}, which no page has`);
	}
}
for (const doc of all) {
	if (!pagePaths.includes(doc.path)) {
		throw new Error(`content: ${JSON.stringify(doc.path)} is not in the navigation`);
	}
}

/** The pages, in reading order. */
export const docs: Doc[] = pagePaths.map((path) => byPath.get(path)!);
