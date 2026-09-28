package cron_test

import (
	"fmt"
	"time"

	"github.com/floatdrop/grpcproc/cron"
)

func ExampleSchedule_Next() {
	s, err := cron.Parse("30 1 * * *")
	if err != nil {
		panic(err)
	}
	// On November 1, 2026, New York's clocks go back from 02:00 to 01:00:
	// 01:30 comes twice, and the job runs the first time.
	ny, _ := time.LoadLocation("America/New_York")
	t := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
	for range 3 {
		t = s.Next(t)
		fmt.Println(t.Format("Mon Jan 2 15:04 MST"))
	}
	// Output:
	// Sun Nov 1 01:30 EDT
	// Mon Nov 2 01:30 EST
	// Tue Nov 3 01:30 EST
}
