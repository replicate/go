package signing

import (
	"regexp"
	"time"
)

type Option interface {
	apply(*options) error
}

type options struct {
	label string

	alg   string
	keyID string
	ttl   time.Duration
}

type optionFunc func(*options) error

func (fn optionFunc) apply(opts *options) error {
	return fn(opts)
}

// WithExpiry sets the time-to-live for the signature. This is used to calculate
// the "expires" parameter.
//
// Values of expiry <= 0 will be ignored.
func WithExpiry(expiry time.Duration) Option {
	return optionFunc(func(opts *options) error {
		opts.ttl = expiry
		return nil
	})
}

// WithKeyID sets the "keyid" parameter of the signature.
func WithKeyID(id string) Option {
	return optionFunc(func(opts *options) error {
		opts.keyID = id
		return nil
	})
}

// WithLabel sets the label of the signature as used in the Signature-Input and
// Signature headers.
func WithLabel(label string) Option {
	return optionFunc(func(opts *options) error {
		// name must be usable as a "param-key" from [RFC 8941]
		//
		// [RFC 8941]: https://www.rfc-editor.org/rfc/rfc8941#section-3.1.2
		if !regexp.MustCompile(`\A[a-z*][a-z0-9_.*-]*\z`).MatchString(label) {
			return ErrInvalidLabel
		}
		opts.label = label
		return nil
	})
}

func makeOptions(opts ...Option) (*options, error) {
	options := &options{}
	for _, o := range opts {
		if err := o.apply(options); err != nil {
			return nil, err
		}
	}
	return options, nil
}
