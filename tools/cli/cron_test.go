package cli_test

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/cron"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestCron(t *testing.T) {
	f := testcluster.Start(t)
	f.Cron(t, "a", "cron")
	pid := f.Cron(t, "b", "billing")

	table := ok(t, run(t, f, "cron"))
	has(t, table, "NODE CRON JOB SPEC ZONE NEXT LAST RUNNING LAST FAILURE ERROR",
		"a cron leap 0 0 29 2 * UTC 2028-02-29T00:00:00Z", "a cron paused 0 * * * * UTC disabled", "b billing yearly @yearly UTC")
	if lines := strings.Count(table, "\n"); lines != 7 {
		t.Errorf("%d lines:\n%s", lines, table)
	}
	if got := ok(t, run(t, f, "cron", "list", "--node", "b")); strings.Contains(got, " cron ") || !strings.Contains(got, "billing") {
		t.Errorf("on b:\n%s", got)
	}
	has(t, ok(t, run(t, f, "cron", "--node", "b", "billing")), "b billing paused")

	var crons []client.CronView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "cron", pid.String()))), &crons); err != nil || len(crons) != 1 || len(crons[0].Jobs) != 3 {
		t.Fatalf("%+v %v", crons, err)
	}

	// Each change prints the cron process as it now is.
	has(t, ok(t, run(t, f, "cron", "enable", "--node", "b", "billing", "paused")), "b billing paused 0 * * * * UTC 20")
	has(t, ok(t, run(t, f, "cron", "disable", pid.String(), "paused")), "b billing paused 0 * * * * UTC disabled")
	if got := ok(t, run(t, f, "cron", "remove", pid.String(), "paused")); strings.Contains(got, "paused") {
		t.Errorf("removed, still listed:\n%s", got)
	}
}

func TestCronErrors(t *testing.T) {
	f := testcluster.Start(t)
	f.Cron(t, "a", "cron")
	for _, args := range [][]string{
		{"cron", "a", "b"},
		{"cron", "enable", "cron"},
		{"cron", "remove", "cron", "x", "y"},
		{"cron", "--bogus"},
	} {
		if r := run(t, f, args...); r.code != 2 {
			t.Errorf("%v: exit %d, %s", args, r.code, r.stderr)
		}
	}
	for args, want := range map[string]string{
		"cron nobody":            "no process",
		"cron remove cron nope":  "no such job",
		"cron --node nowhere":    "node nowhere",
		"cron remove nobody job": "no such process",
	} {
		r := run(t, f, strings.Fields(args)...)
		if r.code != 1 || !strings.Contains(r.stderr, want) {
			t.Errorf("%s: exit %d, %q, want %q", args, r.code, r.stderr, want)
		}
	}
	// With no cron process anywhere, the table is its header; a cron process
	// with no jobs is a row of its own.
	f2 := testcluster.Start(t)
	if r := run(t, f2, "cron"); r.code != 0 || strings.Count(r.stdout, "\n") != 1 {
		t.Errorf("no cron processes: exit %d\n%s", r.code, r.stdout)
	}
	if _, err := cron.Start(f2.C.Node("b"), cron.Spec{}, grpcproc.WithName("idle")); err != nil {
		t.Fatal(err)
	}
	has(t, ok(t, run(t, f2, "cron")), "b idle")
}
