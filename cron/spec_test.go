package cron_test

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the zones below, wherever the tests run

	"github.com/floatdrop/grpcproc/cron"
)

func zone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// nexts is the first n minutes spec names after from, formatted with
// their weekday and zone.
func nexts(t *testing.T, spec string, from time.Time, n int) string {
	t.Helper()
	s, err := cron.Parse(spec)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for range n {
		from = s.Next(from)
		if from.IsZero() {
			out = append(out, "never")
			break
		}
		out = append(out, from.Format("Mon 2006-01-02 15:04 MST"))
	}
	return strings.Join(out, "; ")
}

func TestNext(t *testing.T) {
	at := func(s string) time.Time {
		t.Helper()
		v, err := time.Parse("2006-01-02 15:04", s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, tc := range []struct {
		spec, from string
		n          int
		want       string
	}{
		{"*/15 * * * *", "2026-09-28 10:07", 4, "Mon 2026-09-28 10:15 UTC; Mon 2026-09-28 10:30 UTC; Mon 2026-09-28 10:45 UTC; Mon 2026-09-28 11:00 UTC"},
		{"5/20 * * * *", "2026-09-28 10:07", 3, "Mon 2026-09-28 10:25 UTC; Mon 2026-09-28 10:45 UTC; Mon 2026-09-28 11:05 UTC"},
		{"0,30 9-10 * * *", "2026-09-28 10:07", 3, "Mon 2026-09-28 10:30 UTC; Tue 2026-09-29 09:00 UTC; Tue 2026-09-29 09:30 UTC"},
		{"0 9-17/4 * * MON-FRI", "2026-10-02 16:30", 3, "Fri 2026-10-02 17:00 UTC; Mon 2026-10-05 09:00 UTC; Mon 2026-10-05 13:00 UTC"},
		{"0 0 L * *", "2026-01-15 00:00", 4, "Sat 2026-01-31 00:00 UTC; Sat 2026-02-28 00:00 UTC; Tue 2026-03-31 00:00 UTC; Thu 2026-04-30 00:00 UTC"},
		{"0 0 1,L 2 *", "2027-06-01 00:00", 3, "Tue 2028-02-01 00:00 UTC; Tue 2028-02-29 00:00 UTC; Thu 2029-02-01 00:00 UTC"},
		{"0 0 * * 5L", "2026-09-01 00:00", 3, "Fri 2026-09-25 00:00 UTC; Fri 2026-10-30 00:00 UTC; Fri 2026-11-27 00:00 UTC"},
		{"0 0 * * fri#2", "2026-09-01 00:00", 3, "Fri 2026-09-11 00:00 UTC; Fri 2026-10-09 00:00 UTC; Fri 2026-11-13 00:00 UTC"},
		{"0 0 * * 1#5", "2026-10-01 00:00", 2, "Mon 2026-11-30 00:00 UTC; Mon 2027-03-29 00:00 UTC"},
		{"0 0 * * 7L,1#1", "2026-10-01 00:00", 3, "Mon 2026-10-05 00:00 UTC; Sun 2026-10-25 00:00 UTC; Mon 2026-11-02 00:00 UTC"},
		// Both day fields restricted: either matches.
		{"0 12 1,15 * 1", "2026-09-28 13:00", 4, "Thu 2026-10-01 12:00 UTC; Mon 2026-10-05 12:00 UTC; Mon 2026-10-12 12:00 UTC; Thu 2026-10-15 12:00 UTC"},
		// One starts with *: both must.
		{"0 12 */2 * 1", "2026-10-01 00:00", 4, "Mon 2026-10-05 12:00 UTC; Mon 2026-10-19 12:00 UTC; Mon 2026-11-09 12:00 UTC; Mon 2026-11-23 12:00 UTC"},
		{"0 0 * * 0", "2026-09-28 00:00", 1, "Sun 2026-10-04 00:00 UTC"},
		{"0 0 * * 7", "2026-09-28 00:00", 1, "Sun 2026-10-04 00:00 UTC"},
		{"0 0 * * SUN", "2026-09-28 00:00", 1, "Sun 2026-10-04 00:00 UTC"},
		{"0 0 * * 5-7", "2026-09-28 00:00", 3, "Fri 2026-10-02 00:00 UTC; Sat 2026-10-03 00:00 UTC; Sun 2026-10-04 00:00 UTC"},
		{"0 0 1 JAN,jul *", "2026-09-28 00:00", 2, "Fri 2027-01-01 00:00 UTC; Thu 2027-07-01 00:00 UTC"},
		{"@hourly", "2026-09-28 10:07", 1, "Mon 2026-09-28 11:00 UTC"},
		{"@daily", "2026-09-28 10:07", 1, "Tue 2026-09-29 00:00 UTC"},
		{"@midnight", "2026-09-28 10:07", 1, "Tue 2026-09-29 00:00 UTC"},
		{"@weekly", "2026-09-28 10:07", 1, "Sun 2026-10-04 00:00 UTC"},
		{"@monthly", "2026-09-28 10:07", 1, "Thu 2026-10-01 00:00 UTC"},
		{"@yearly", "2026-09-28 10:07", 1, "Fri 2027-01-01 00:00 UTC"},
		{" @ANNUALLY ", "2026-09-28 10:07", 1, "Fri 2027-01-01 00:00 UTC"},
		// 2100 is not a leap year.
		{"0 0 29 2 *", "2097-03-01 00:00", 1, "Fri 2104-02-29 00:00 UTC"},
		{"0 0 30 2 *", "2026-01-01 00:00", 1, "never"},
	} {
		if got := nexts(t, tc.spec, at(tc.from), tc.n); got != tc.want {
			t.Errorf("%q from %s:\n got %s\nwant %s", tc.spec, tc.from, got, tc.want)
		}
	}
}

func TestNextAcrossClockChanges(t *testing.T) {
	ny, berlin, santiago := zone(t, "America/New_York"), zone(t, "Europe/Berlin"), zone(t, "America/Santiago")
	for _, tc := range []struct {
		spec string
		from time.Time
		n    int
		want string
	}{
		// Clocks go forward: 02:30 does not exist on March 8.
		{"30 2 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny), 2, "Mon 2026-03-09 02:30 EDT; Tue 2026-03-10 02:30 EDT"},
		// Clocks go back: 01:30 comes twice on November 1, and runs once.
		{"30 1 * * *", time.Date(2026, 10, 31, 12, 0, 0, 0, ny), 2, "Sun 2026-11-01 01:30 EDT; Mon 2026-11-02 01:30 EST"},
		{"*/30 * * * *", time.Date(2026, 11, 1, 0, 45, 0, 0, ny), 4, "Sun 2026-11-01 01:00 EDT; Sun 2026-11-01 01:30 EDT; Sun 2026-11-01 02:00 EST; Sun 2026-11-01 02:30 EST"},
		// From the second pass, the first is behind.
		{"*/30 * * * *", time.Date(2026, 11, 1, 1, 10, 0, 0, ny).Add(time.Hour), 1, "Sun 2026-11-01 02:00 EST"},
		// East of UTC time.Date picks the second pass; Next the first.
		{"30 2 * * *", time.Date(2026, 10, 24, 12, 0, 0, 0, berlin), 2, "Sun 2026-10-25 02:30 CEST; Mon 2026-10-26 02:30 CET"},
		{"0 * * * *", time.Date(2026, 10, 25, 0, 30, 0, 0, berlin), 4, "Sun 2026-10-25 01:00 CEST; Sun 2026-10-25 02:00 CEST; Sun 2026-10-25 03:00 CET; Sun 2026-10-25 04:00 CET"},
		{"30 2 * * *", time.Date(2026, 3, 28, 12, 0, 0, 0, berlin), 1, "Mon 2026-03-30 02:30 CEST"},
		// Chile moves its clocks at midnight: September 6 has no 00:00,
		// and April 4 has two 23:00s.
		{"0 0 * * *", time.Date(2026, 9, 5, 12, 0, 0, 0, santiago), 1, "Mon 2026-09-07 00:00 -03"},
		{"0 1 * * *", time.Date(2026, 9, 5, 12, 0, 0, 0, santiago), 1, "Sun 2026-09-06 01:00 -03"},
		{"30 23 * * *", time.Date(2026, 4, 4, 12, 0, 0, 0, santiago), 2, "Sat 2026-04-04 23:30 -03; Sun 2026-04-05 23:30 -04"},
	} {
		if got := nexts(t, tc.spec, tc.from, tc.n); got != tc.want {
			t.Errorf("%q from %s:\n got %s\nwant %s", tc.spec, tc.from.Format(time.RFC3339), got, tc.want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for spec, want := range map[string]string{
		"":                `cron: "": want 5 fields`,
		"* * * *":         "got 4",
		"@reboot":         `cron: "@reboot": unknown macro`,
		"60 * * * *":      "minute: 60 is out of range 0-59",
		"* 24 * * *":      "hour: 24 is out of range 0-23",
		"* * 0 * *":       "day of month: 0 is out of range 1-31",
		"* * L-2 * *":     `day of month: "L" is not a number`,
		"* * * 13 *":      "month: 13 is out of range 1-12",
		"* * * FOO *":     `month: "FOO" is not a number`,
		"* * * * 8":       "day of week: 8 is out of range 0-7",
		"* * * * 9L":      "day of week: 9 is out of range 0-7",
		"* * * * 9#1":     "day of week: 9 is out of range 0-7",
		"* * * * 1#6":     `day of week: "1#6": 6 is out of range 1-5`,
		"* * * * 1-x":     `day of week: "x" is not a number`,
		"*/0 * * * *":     `minute: "*/0": the step must be a positive number`,
		"*/x * * * *":     `minute: "*/x": the step must be a positive number`,
		"5-1 * * * *":     `minute: "5-1": the range runs backwards`,
		"a-5 * * * *":     `minute: "a" is not a number`,
		"1-b * * * *":     `minute: "b" is not a number`,
		"1,,2 * * * *":    `minute: "" is not a number`,
		"* * * * * *":     "got 6",
		"0 0 1 1 * extra": "got 6",
	} {
		_, err := cron.Parse(spec)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) = %v, want %q", spec, err, want)
		}
	}
}

func TestScheduleString(t *testing.T) {
	s, err := cron.Parse("@daily")
	if err != nil {
		t.Fatal(err)
	}
	if s.String() != "@daily" {
		t.Errorf("String() = %q", s)
	}
}
