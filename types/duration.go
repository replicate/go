package types

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidDurationString     = fmt.Errorf("invalid duration string")
	ErrUnsupportedDurationString = fmt.Errorf("unsupported duration string")

	durationDay  = 24 * time.Hour
	durationWeek = 7 * durationDay

	pattYears    = `(?P<years>[\d\.]+Y)?`
	pattMonths   = `(?P<months>[\d\.]+M)?`
	pattWeeks    = `(?P<weeks>[\d\.]+W)?`
	pattDays     = `(?P<days>[\d\.]+D)?`
	pattHours    = `(?P<hours>[\d\.]+H)?`
	pattMinutes  = `(?P<minutes>[\d\.]+M)?`
	pattSeconds  = `(?P<seconds>[\d\.]+S)?`
	pattDuration = regexp.MustCompile(
		`\A-?P` +
			pattYears + pattMonths + pattWeeks + pattDays +
			`(?:T` + pattHours + pattMinutes + pattSeconds + `)?` +
			`\z`,
	)
)

// Duration is a custom duration type which marshals from/to a subset of ISO8601
// duration strings.
//
// Specifically, it supports ISO8601 duration strings which can be mapped
// unambiguously to a Go time.Duration value. This means that strings which
// contain non-zero values in the month or year fields are forbidden, as these
// require a point-in-time reference to be unambiguous.
//
// Technically the same is true for days or weeks, as days can be less or more
// than 24 hours long at daylight savings boundaries. Unfortunately, Django's
// JSON encoder treats a day as exactly 24 hours long and so we have to support
// duration strings containing days. Weeks come along for the ride as exactly
// seven days.
//
// Note that periods of time spanning months or years can be represented and
// marshaled by this type, but they will be marshaled in terms of weeks, days,
// etc.
type Duration time.Duration

// Duration returns the duration as a time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// Abs returns the absolute value of d. As a special case, math.MinInt64 is
// converted to math.MaxInt64.
func (d Duration) Abs() Duration {
	return Duration(d.Duration().Abs())
}

// Hours returns the duration as a floating point number of hours.
func (d Duration) Hours() float64 {
	return d.Duration().Hours()
}

// Microseconds returns the duration as an integer microsecond count.
func (d Duration) Microseconds() int64 {
	return d.Duration().Microseconds()
}

// Milliseconds returns the duration as an integer millisecond count.
func (d Duration) Milliseconds() int64 {
	return d.Duration().Milliseconds()
}

// Minutes returns the duration as a floating point number of minutes.
func (d Duration) Minutes() float64 {
	return d.Duration().Minutes()
}

// Nanoseconds returns the duration as an integer nanosecond count.
func (d Duration) Nanoseconds() int64 {
	return d.Duration().Nanoseconds()
}

// Round returns the result of rounding d to the nearest multiple of m. The
// rounding behavior for halfway values is to round away from zero. If the
// result exceeds the maximum (or minimum) value that can be stored in a
// Duration, Round returns the maximum (or minimum) duration. If m <= 0, Round
// returns d unchanged.
func (d Duration) Round(m Duration) Duration {
	return Duration(d.Duration().Round(m.Duration()))
}

// Seconds returns the duration as a floating point number of seconds.
func (d Duration) Seconds() float64 {
	return d.Duration().Seconds()
}

func (d Duration) String() string {
	if int(d) == 0 {
		return "PT0S"
	}

	var sb strings.Builder

	parts := [4]int{
		int(durationWeek),
		int(durationDay),
		int(time.Hour),
		int(time.Minute),
	}

	val := int(d)
	out := [4]int{0, 0, 0, 0} // W, D, H, M

	if val < 0 {
		_, _ = sb.WriteRune('-')
		val *= -1
	}

	for i := 0; i < 4; i++ {
		div := val / parts[i]
		out[i] += div
		val -= div * parts[i]
	}

	_, _ = sb.WriteRune('P')

	if out[0] > 0 {
		_, _ = sb.WriteString(strconv.Itoa(out[0]))
		_, _ = sb.WriteRune('W')
	}
	if out[1] > 0 {
		_, _ = sb.WriteString(strconv.Itoa(out[1]))
		_, _ = sb.WriteRune('D')
	}
	if out[2]+out[3]+val > 0 {
		_, _ = sb.WriteRune('T')
	}
	if out[2] > 0 {
		_, _ = sb.WriteString(strconv.Itoa(out[2]))
		_, _ = sb.WriteRune('H')
	}
	if out[3] > 0 {
		_, _ = sb.WriteString(strconv.Itoa(out[3]))
		_, _ = sb.WriteRune('M')
	}
	if val > 0 {
		secs := time.Duration(val).Seconds()
		_, _ = sb.WriteString(strconv.FormatFloat(secs, 'f', -1, 64))
		_, _ = sb.WriteRune('S')
	}
	return sb.String()
}

// Truncate returns the result of rounding d toward zero to a multiple of m. If
// m <= 0, Truncate returns d unchanged.
func (d Duration) Truncate(m Duration) Duration {
	return Duration(d.Duration().Truncate(m.Duration()))
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	result, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = result
	return nil
}

func ParseDuration(s string) (Duration, error) {
	d := time.Duration(0)

	match := pattDuration.FindStringSubmatch(s)

	if match == nil {
		return Duration(d), fmt.Errorf("%w: %q", ErrInvalidDurationString, s)
	}

	// if there's a T there must be data after else it is invalid
	if s[len(s)-1] == 'T' {
		return Duration(d), fmt.Errorf("%w: %q", ErrInvalidDurationString, s)
	}

	names := pattDuration.SubexpNames()
	seenAnyValue := false

	for i, v := range match {
		if i == 0 {
			// skip over the whole string match
			continue
		}
		if v == "" {
			continue
		}
		name := names[i]
		if f, err := strconv.ParseFloat(v[:len(v)-1], 64); err == nil {
			switch name {
			case "years", "months":
				if f > 0 {
					return Duration(d), fmt.Errorf("%w: contains %s", ErrUnsupportedDurationString, name)
				}
				continue
			case "weeks":
				d += time.Duration(int(math.Round(f * 7 * 24 * float64(time.Hour))))
			case "days":
				d += time.Duration(int(math.Round(f * 24 * float64(time.Hour))))
			case "hours":
				d += time.Duration(int(math.Round(f * float64(time.Hour))))
			case "minutes":
				d += time.Duration(int(math.Round(f * float64(time.Minute))))
			case "seconds":
				d += time.Duration(int(math.Round(f * float64(time.Second))))
			}
			seenAnyValue = true
		}
	}

	// reject functionally empty strings like "P" or "PT"
	if !seenAnyValue {
		return Duration(d), fmt.Errorf("%w: %q", ErrInvalidDurationString, s)
	}

	if s[0] == '-' {
		d *= -1
	}

	return Duration(d), nil
}
