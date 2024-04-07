package signing

import "errors"

var (
	ErrInvalidLabel     = errors.New("invalid signature label")
	ErrInvalidComponent = errors.New("invalid signature component")
	ErrSigningFailure   = errors.New("failed to sign request")

	ErrNotImplemented = errors.New("not implemented")
)
