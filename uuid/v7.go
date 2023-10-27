package uuid

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
)

var ErrBigTime = errors.New("uuid: timestamp overflow, cannot generate")

const maxTime = uint64(0xFFFF_FFFF_FFFF) // maximum 48-bit value

// NewV7 This is an implementation of a UUIDv7-compatible ID generator. The
// reference used was the latest version of the draft IETF RFC4122bis
// specification at
//
//	https://www.ietf.org/archive/id/draft-ietf-uuidrev-rfc4122bis-13.html
//
// The UUIDv7 bit layout is
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                           unix_ts_ms                          |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|          unix_ts_ms           |  ver  |       rand_a          |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|var|                        rand_b                             |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                            rand_b                             |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// The fields are to be filled as follows:
//
//  1. unix_ts_ms: a 48-bit Unix millisecond timestamp.
//  2. ver: the 4-bit literal value 0b0111 (7) denoting the UUID version.
//  3. rand_a: a 12-bit pseudo-random value.
//  4. var: the 2-bit literal value 0b10 (2) denoting the UUID variant.
//  5. rand_b: a 62-bit pseudo-random value.
//
// The specification allows for additional monotonicity guarantees within the
// millisecond by incrementing the value of rand_b by a random integer of any
// desired length for additional UUIDs generated within a single timestamp tick.
// This may become relevant in future. For now, we generate a new 74-bit
// pseudo-random value for every generated UUID.
func NewV7() (UUID, error) {
	var u UUID

	ts := uint64(time.Now().UnixMilli())
	if ts > maxTime {
		return u, ErrBigTime
	}

	// Fill the first 48 bytes with a millisecond timestamp
	u[0] = byte(ts >> 40)
	u[1] = byte(ts >> 32)
	u[2] = byte(ts >> 24)
	u[3] = byte(ts >> 16)
	u[4] = byte(ts >> 8)
	u[5] = byte(ts)

	// Fill the rest of the value with random data
	_, err := io.ReadFull(rand.Reader, u[6:])

	// Set version and variant fields
	u[6] = (u[6] & 0x0F) | (V7 << 4)
	u[8] = (u[8] & 0x3F) | (0x02 << 6)

	return u, err
}

func TimeFromV7(u UUID) (time.Time, error) {
	if u.Version() != 7 {
		return time.UnixMilli(0), fmt.Errorf("uuid: %s is version %d, not version 7", u, u.Version())
	}

	t := 0 |
		uint64(u[5]) |
		uint64(u[4])<<8 |
		uint64(u[3])<<16 |
		uint64(u[2])<<24 |
		uint64(u[1])<<32 |
		uint64(u[0])<<40

	return time.UnixMilli(int64(t)), nil
}
