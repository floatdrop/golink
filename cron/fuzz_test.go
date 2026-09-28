package cron_test

import (
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/floatdrop/grpcproc/cron"
)

// Zones where a day is not always 24 hours: clocks go forward and back by an
// hour, by half an hour (Lord Howe), and at a quarter-hour offset.
var fuzzZones = []string{"UTC", "America/New_York", "Europe/London", "Australia/Lord_Howe", "Asia/Kathmandu", "America/Santiago"}

// A spec comes from a person, or a file, and a time from anywhere: Parse
// must refuse what it cannot read and never panic, and what Next finds must
// be a minute the schedule names, after the time asked about, with none
// that it names in between.
func FuzzParse(f *testing.F) {
	for _, spec := range []string{
		"* * * * *", "*/15 * * * *", "5/20 * * * *", "0,30 9-10 * * *", "0 9-17/4 * * MON-FRI",
		"0 0 L * *", "0 0 1,L 2 *", "0 12 * * 5L", "30 2 * * 0#2", "0 0 29 2 *", "0 0 30 2 *",
		"@yearly", "@hourly", "@reboot", "", "* * * *", "60 * * * *", "1-0 * * * *", "*/0 * * * *",
		"0 0 * * 7", "0 0 * JAN-DEC SUN-SAT", "59 23 31 12 *", "0 2 * * *",
		// Steps near the largest int, which once wrapped around and panicked.
		"1/9223372036854775807 * * * *", "* 1/9223372036854775807 * * *", "* * 2/9223372036854775807 * *",
		"* * * 3/9223372036854775807 *", "* * * * 1/9223372036854775807", "59/9223372036854775806 * * * *",
	} {
		f.Add(spec, int64(0), uint8(0))
		f.Add(spec, int64(1_000_000), uint8(1))
	}
	f.Fuzz(func(t *testing.T, spec string, minutes int64, zone uint8) {
		s, err := cron.Parse(spec)
		if err != nil {
			if s != nil {
				t.Fatal("Parse returned a schedule and an error")
			}
			return
		}
		if s.String() != spec {
			t.Fatalf("String %q, parsed from %q", s.String(), spec)
		}
		loc, err := time.LoadLocation(fuzzZones[int(zone)%len(fuzzZones)])
		if err != nil {
			t.Fatal(err)
		}
		// Anywhere in the four centuries from 1900, to the second.
		const span = 400 * 365 * 24 * 60
		minutes %= span
		if minutes < 0 {
			minutes += span
		}
		from := time.Date(1900, 1, 1, 0, 0, 0, 0, loc).Add(time.Duration(minutes)*time.Minute + time.Duration(minutes%60)*time.Second)

		next := s.Next(from)
		if next.IsZero() {
			return
		}
		switch {
		case !next.After(from):
			t.Fatalf("%q: Next(%v) = %v, not after it", spec, from, next)
		case next.Second() != 0 || next.Nanosecond() != 0:
			t.Fatalf("%q: Next(%v) = %v, not on a minute", spec, from, next)
		case next.Location() != loc:
			t.Fatalf("%q: Next(%v) = %v, in another location", spec, from, next)
		}
		// Asked again from any instant before it, Next finds it again: it
		// skipped no minute the schedule names. The minute before is the
		// likeliest to show one.
		for _, back := range []time.Duration{time.Nanosecond, time.Minute, time.Hour, 24 * time.Hour} {
			if at := next.Add(-back); !at.Before(from) {
				if again := s.Next(at); !again.Equal(next) {
					t.Fatalf("%q: Next(%v) = %v, but Next(%v) = %v", spec, from, next, at, again)
				}
			}
		}
		// And the one after it is later still.
		if after := s.Next(next); !after.IsZero() && !after.After(next) {
			t.Fatalf("%q: Next(%v) = %v, not after it", spec, next, after)
		}
	})
}
