package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed crontab spec: the minutes it names, in whatever
// location it is asked about.
type Schedule struct {
	spec    string
	minute  uint64   // bit m: minute m, 0-59
	hour    uint64   // bit h: hour h, 0-23
	dom     uint64   // bit d: day of month d, 1-31
	month   uint64   // bit m: month m, 1-12
	dow     uint64   // bit w: weekday w, Sunday 0
	lastDOM bool     // L: the last day of the month
	lastDOW uint64   // bit w: the last weekday w of the month (5L)
	nthDOW  [7]uint8 // bit n of [w]: the nth weekday w of the month (5#2)
	// domStar and dowStar say the field starts with *: then a day must
	// match both day fields, and otherwise either, as in Vixie cron.
	domStar, dowStar bool
}

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var (
	monthNames = map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
	dayNames   = map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
)

// Parse reads a crontab spec: five fields, minute (0-59), hour (0-23), day
// of month (1-31), month (1-12 or JAN-DEC) and day of week (0-7 or SUN-SAT,
// Sunday being 0 and 7). A field is * or a comma-separated list of values,
// ranges (1-5) and steps (*/15, 0-30/10, 5/20, which is 5-59/20). When both
// day fields are restricted a day matching either runs, and when either
// starts with * a day must match both, as in Vixie cron.
//
// Beyond the standard, as in Quartz: L in the day of month is its
// last day; in the day of week, 5L is the month's last Friday and 5#2 its
// second. The macros are @yearly (or @annually), @monthly, @weekly, @daily
// (or @midnight) and @hourly; @reboot is not a schedule.
func Parse(spec string) (*Schedule, error) {
	s, err := parse(spec)
	if err != nil {
		return nil, fmt.Errorf("cron: %w", err)
	}
	return s, nil
}

// parse is Parse, with errors that do not say they are cron's.
func parse(spec string) (*Schedule, error) {
	text := strings.TrimSpace(spec)
	if strings.HasPrefix(text, "@") {
		m, ok := macros[strings.ToLower(text)]
		if !ok {
			return nil, fmt.Errorf("%q: unknown macro", spec)
		}
		text = m
	}
	fields := strings.Fields(text)
	if len(fields) != 5 {
		return nil, fmt.Errorf("%q: want 5 fields, minute hour day-of-month month day-of-week, got %d", spec, len(fields))
	}
	s := &Schedule{spec: spec, domStar: fields[2][0] == '*', dowStar: fields[4][0] == '*'}
	var err error
	if s.minute, err = parseField(fields[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("%q: minute: %w", spec, err)
	}
	if s.hour, err = parseField(fields[1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("%q: hour: %w", spec, err)
	}
	if s.dom, err = s.parseDOM(fields[2]); err != nil {
		return nil, fmt.Errorf("%q: day of month: %w", spec, err)
	}
	if s.month, err = parseField(fields[3], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("%q: month: %w", spec, err)
	}
	if s.dow, err = s.parseDOW(fields[4]); err != nil {
		return nil, fmt.Errorf("%q: day of week: %w", spec, err)
	}
	return s, nil
}

// String is the spec s was parsed from.
func (s *Schedule) String() string { return s.spec }

// parseField reads a field of plain items: values, ranges and steps.
func parseField(field string, lo, hi int, names map[string]int) (uint64, error) {
	var mask uint64
	for item := range strings.SplitSeq(field, ",") {
		m, err := parseItem(item, lo, hi, names)
		if err != nil {
			return 0, err
		}
		mask |= m
	}
	return mask, nil
}

// parseDOM reads the day of month, whose items may also be L.
func (s *Schedule) parseDOM(field string) (uint64, error) {
	var mask uint64
	for item := range strings.SplitSeq(field, ",") {
		if strings.EqualFold(item, "L") {
			s.lastDOM = true
			continue
		}
		m, err := parseItem(item, 1, 31, nil)
		if err != nil {
			return 0, err
		}
		mask |= m
	}
	return mask, nil
}

// parseDOW reads the day of week, whose items may also be 5L or 5#2, and
// where 7 is Sunday as 0 is.
func (s *Schedule) parseDOW(field string) (uint64, error) {
	var mask uint64
	for item := range strings.SplitSeq(field, ",") {
		if day, ok := strings.CutSuffix(strings.ToUpper(item), "L"); ok {
			w, err := value(day, 0, 7, dayNames)
			if err != nil {
				return 0, err
			}
			s.lastDOW |= 1 << (w % 7)
			continue
		}
		if day, nth, ok := strings.Cut(item, "#"); ok {
			w, err := value(day, 0, 7, dayNames)
			if err != nil {
				return 0, err
			}
			n, err := value(nth, 1, 5, nil)
			if err != nil {
				return 0, fmt.Errorf("%q: %w", item, err)
			}
			s.nthDOW[w%7] |= 1 << n
			continue
		}
		m, err := parseItem(item, 0, 7, dayNames)
		if err != nil {
			return 0, err
		}
		mask |= m
	}
	if mask&(1<<7) != 0 {
		mask = mask&^(1<<7) | 1
	}
	return mask, nil
}

// parseItem reads *, a value, a range, or either of those with a step.
func parseItem(item string, lo, hi int, names map[string]int) (uint64, error) {
	rng, stepText, stepped := strings.Cut(item, "/")
	step := 1
	if stepped {
		n, err := strconv.Atoi(stepText)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%q: the step must be a positive number", item)
		}
		step = n
	}
	first, last := lo, hi
	switch from, to, isRange := strings.Cut(rng, "-"); {
	case rng == "*":
	case isRange:
		var err error
		if first, err = value(from, lo, hi, names); err != nil {
			return 0, err
		}
		if last, err = value(to, lo, hi, names); err != nil {
			return 0, err
		}
		if first > last {
			return 0, fmt.Errorf("%q: the range runs backwards", item)
		}
	default:
		var err error
		if first, err = value(rng, lo, hi, names); err != nil {
			return 0, err
		}
		if !stepped {
			last = first
		}
	}
	var mask uint64
	for v := first; v <= last; v += step {
		mask |= 1 << v
	}
	return mask, nil
}

// value reads a number between lo and hi, or a name.
func value(text string, lo, hi int, names map[string]int) (int, error) {
	if v, ok := names[strings.ToLower(text)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", text)
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("%d is out of range %d-%d", v, lo, hi)
	}
	return v, nil
}

// maxYears bounds Next's search: the longest wait between two matches of a
// satisfiable spec is 8 years, for February 29 across a year like 2100.
const maxYears = 10

// Next is the first minute after t that s names, in t's location, or the
// zero time if there is none within the next ten years (as for "0 0 30 2
// *"). A minute that is skipped when clocks go forward does not exist, so
// it never matches; one that repeats when clocks go back matches the first
// time only.
func (s *Schedule) Next(t time.Time) time.Time {
	loc := t.Location()
	m := t.Truncate(time.Minute).Add(time.Minute)
	end := m.AddDate(maxYears, 0, 0)
	for m.Before(end) {
		w := m.In(loc)
		var next time.Time
		switch {
		case s.month&(1<<int(w.Month())) == 0:
			next = earliest(time.Date(w.Year(), w.Month()+1, 1, 0, 0, 0, 0, loc))
		case !s.dayMatches(w):
			next = earliest(time.Date(w.Year(), w.Month(), w.Day()+1, 0, 0, 0, 0, loc))
		case s.hour&(1<<w.Hour()) == 0:
			next = earliest(time.Date(w.Year(), w.Month(), w.Day(), w.Hour()+1, 0, 0, 0, loc))
		case s.minute&(1<<w.Minute()) == 0, repeated(w):
			next = m.Add(time.Minute)
		default:
			return w
		}
		m = later(next, m.Add(time.Minute)) // forward, whatever a zone does
	}
	return time.Time{}
}

// dayMatches reports whether s names the day of w.
func (s *Schedule) dayMatches(w time.Time) bool {
	d, last := w.Day(), daysIn(w)
	dom := s.dom&(1<<d) != 0 || s.lastDOM && d == last
	wd := w.Weekday()
	dow := s.dow&(1<<wd) != 0 ||
		s.lastDOW&(1<<wd) != 0 && d+7 > last ||
		s.nthDOW[wd]&(1<<((d-1)/7+1)) != 0
	if s.domStar || s.dowStar {
		return dom && dow
	}
	return dom || dow
}

// daysIn is the number of days in t's month.
func daysIn(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// earliest is the first instant that has t's wall-clock time in t's
// location: t, or, when t falls in the second pass through an hour clocks
// went back over, the same time on the first pass. time.Date leaves which
// of the two it returns unspecified.
func earliest(t time.Time) time.Time {
	_, now := t.Zone()
	_, before := t.Add(-12 * time.Hour).Zone()
	if before > now {
		if e := t.Add(-time.Duration(before-now) * time.Second); sameWallMinute(e, t) {
			return e
		}
	}
	return t
}

// repeated reports whether t's wall-clock minute already passed once, when
// clocks went back.
func repeated(t time.Time) bool { return earliest(t).Before(t) }

func sameWallMinute(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd && a.Hour() == b.Hour() && a.Minute() == b.Minute()
}
