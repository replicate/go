package uuid

import "encoding/hex"

const (
	Size = 16

	V7 byte = 7
)

const (
	VariantNCS = iota
	VariantRFC4122
	VariantMicrosoft
	VariantFuture
)

type UUID [Size]byte

func (u UUID) Version() byte {
	return u[6] >> 4
}

func (u UUID) Variant() int {
	switch {
	case (u[8] >> 7) == 0x00:
		return VariantNCS
	case (u[8] >> 6) == 0x02:
		return VariantRFC4122
	case (u[8] >> 5) == 0x06:
		return VariantMicrosoft
	case (u[8] >> 5) == 0x07:
		fallthrough
	default:
		return VariantFuture
	}
}

func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	hex.Encode(buf[9:13], u[4:6])
	hex.Encode(buf[14:18], u[6:8])
	hex.Encode(buf[19:23], u[8:10])
	hex.Encode(buf[24:36], u[10:16])
	buf[8] = '-'
	buf[13] = '-'
	buf[18] = '-'
	buf[23] = '-'
	return string(buf[:])
}

func Must(u UUID, err error) UUID {
	if err != nil {
		panic(err)
	}
	return u
}
