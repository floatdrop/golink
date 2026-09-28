package client

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func TestCronJobLines(t *testing.T) {
	for line, want := range map[string]CronJobView{
		"0,30 9-17 * * 1-5 Europe/Berlin, next 2026-09-29T09:00:00+02:00, last 2026-09-28T17:30:00+02:00, running 2, failed: dial b, then c: refused": {
			Spec: "0,30 9-17 * * 1-5", Location: "Europe/Berlin", Next: "2026-09-29T09:00:00+02:00", Last: "2026-09-28T17:30:00+02:00", Running: 2, Failure: "dial b, then c: refused",
		},
		"@daily UTC, disabled, last 2026-09-28T00:00:00Z": {Spec: "@daily", Location: "UTC", Disabled: true, Last: "2026-09-28T00:00:00Z"},
		"0 0 30 2 * UTC": {Spec: "0 0 30 2 *", Location: "UTC"},
	} {
		want.Name = "j"
		if got := cronJobView("j", line); got != want {
			t.Errorf("%q:\n got %+v\nwant %+v", line, got, want)
		}
	}
}

// cronFake is node a with one cron process, which cannot be inspected.
type cronFake struct{ inspectv1.InspectorClient }

func (cronFake) GetNode(context.Context, *inspectv1.GetNodeRequest, ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{Id: &inspectv1.NodeID{Name: "a"}}}, nil
}

func (cronFake) ListProcesses(context.Context, *inspectv1.ListProcessesRequest, ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	return &inspectv1.ListProcessesResponse{Processes: []*inspectv1.ProcessInfo{
		{Pid: &grpcprocv1.PID{Node: "a", Incarnation: 1, Id: 7}, Type: cronType},
	}}, nil
}

func (cronFake) GetProcess(context.Context, *inspectv1.GetProcessRequest, ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	return nil, status.Error(codes.NotFound, "gone")
}

func TestCronThatCannotBeInspected(t *testing.T) {
	c := &Client{rpc: cronFake{}, now: time.Now}
	crons, err := c.Crons(t.Context(), "")
	if err != nil || len(crons) != 1 || crons[0].PID != "<a.1.7>" || crons[0].Error == "" {
		t.Fatalf("%+v %v", crons, err)
	}
	if _, err := (&Client{rpc: unreachable{}, now: time.Now}).Crons(t.Context(), ""); err == nil {
		t.Error("crons of a cluster that cannot be reached")
	}
}
