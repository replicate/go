// Package signing implements support for HTTP Message Signatures as defined in
// [RFC 9241].
//
// [RFC 9241]: https://www.rfc-editor.org/rfc/rfc9421.html
package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const AlgEd25519 = "ed25519"

type Signer interface {
	Sign(req *http.Request) (*Signature, error)
}

type Signature struct {
	Input     string
	Signature string
}

type signer struct {
	components ValidatedComponents
	options    *options
	signFn     func(payload []byte) (signature []byte, err error)
}

func (s *signer) Sign(req *http.Request) (*Signature, error) {
	params, payload, err := s.prepare(req)
	if err != nil {
		return nil, err
	}

	if s.signFn == nil {
		return nil, fmt.Errorf("%w: improperly configured signer", ErrSigningFailure)
	}
	data, err := s.signFn(payload)
	if err != nil {
		return nil, err
	}

	return s.makesig(params, data), nil
}

func (s *signer) params(ts time.Time) string {
	var b strings.Builder
	b.WriteRune('(')
	b.WriteString(strings.Join(s.components.Identifiers(), " "))
	b.WriteRune(')')

	b.WriteString(`;created=`)
	b.WriteString(strconv.FormatInt(ts.Unix(), 10))

	if s.options.ttl > 0 {
		b.WriteString(`;expires=`)
		b.WriteString(strconv.FormatInt(ts.Add(s.options.ttl).Unix(), 10))
	}

	if s.options.keyID != "" {
		b.WriteString(`;keyid="`)
		b.WriteString(url.QueryEscape(s.options.keyID))
		b.WriteString(`"`)
	}

	if s.options.alg != "" {
		b.WriteString(`;alg="`)
		b.WriteString(s.options.alg)
		b.WriteRune('"')
	}

	return b.String()
}

func (s *signer) prepare(req *http.Request) (params string, payload []byte, err error) {
	var b strings.Builder

	params = s.params(time.Now())

	base, err := s.components.Base(req)
	if err != nil {
		return "", []byte{}, err
	}
	b.WriteString(base)
	b.WriteString(`"@signature-params": `)
	b.WriteString(params)

	return params, []byte(b.String()), nil
}

func (s *signer) makesig(params string, data []byte) *Signature {
	sig := base64.StdEncoding.EncodeToString(data)

	label := s.options.label
	if label == "" {
		label = "sig"
	}

	return &Signature{
		Input:     label + "=" + params,
		Signature: label + "=:" + sig + ":",
	}
}

// NewEd25519Signer creates a new Signer that signs requests with the provided
// private key.
func NewEd25519Signer(key ed25519.PrivateKey, components ValidatedComponents, opts ...Option) (Signer, error) {
	options, err := makeOptions(opts...)
	if err != nil {
		return nil, err
	}
	options.alg = AlgEd25519

	return &signer{
		components: components,
		options:    options,

		signFn: func(data []byte) ([]byte, error) {
			return key.Sign(nil, data, &ed25519.Options{})
		},
	}, nil
}
