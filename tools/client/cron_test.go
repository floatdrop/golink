package client_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

// jobs is how a cron's jobs read: name, and whether each runs.
func jobs(c client.CronView) string {
	var out []string
	for _, j := range c.Jobs {
		state := "disabled"
		if !j.Disabled {
			state = "next " + j.Next[:min(4, len(j.Next))]
		}
		out = append(out, j.Name+" "+j.Spec+" "+j.Location+" "+state)
	}
	return strings.Join(out, "; ")
}

func TestCrons(t *testing.T) {
	f := testcluster.Start(t)
	f.Cron(t, "a", "cron")
	pid := f.Cron(t, "b", "billing")
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()

	crons, err := c.Crons(ctx, "")
	if err != nil || len(crons) != 2 || crons[0].Node != "a" || crons[0].Process != "cron" || crons[1].Process != "billing" || crons[1].PID != pid.String() {
		t.Fatalf("%+v %v", crons, err)
	}
	if got, want := jobs(crons[0]), "leap 0 0 29 2 * UTC next 2028; paused 0 * * * * UTC disabled; yearly @yearly UTC next 20"; !strings.HasPrefix(got, want) {
		t.Errorf("jobs %q, want %q…", got, want)
	}
	if crons, err := c.Crons(ctx, "b"); err != nil || len(crons) != 1 || crons[0].Process != "billing" {
		t.Errorf("on b: %+v %v", crons, err)
	}
	if _, err := c.Crons(ctx, "nowhere"); err == nil {
		t.Error("crons of a node that is not there")
	}

	// One, by name on its node or by pid; then its jobs changed.
	if one, err := c.Cron(ctx, "b", "billing"); err != nil || one.Node != "b" || len(one.Jobs) != 3 {
		t.Fatalf("%+v %v", one, err)
	}
	if _, err := c.Cron(ctx, "", "nobody"); err == nil {
		t.Error("a cron nobody runs")
	}
	for _, op := range []string{"enable", "disable", "remove"} {
		if err := c.CronJob(ctx, "", pid.String(), op, "paused"); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	if one, _ := c.Cron(ctx, "", pid.String()); !strings.HasPrefix(jobs(one), "leap 0 0 29 2 * UTC next 2028; yearly") {
		t.Errorf("after removing paused: %s", jobs(one))
	}
	for op, want := range map[string]string{"remove": "no such job", "stop": "bad cron op"} {
		if err := c.CronJob(ctx, "", pid.String(), op, "paused"); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", op, err, want)
		}
	}
	if err := c.CronJob(ctx, "", "<bad", "enable", "x"); err == nil {
		t.Error("a bad pid went out")
	}
	if err := c.CronJob(ctx, "", pid.String(), "enable", "\xff"); err == nil {
		t.Error("a job name that is not UTF-8 went out")
	}
}
