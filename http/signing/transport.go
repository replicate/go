package signing

import "net/http"

// Transport is an implementation of http.RoundTripper that adds request signing
// headers as defined in RFC 9241.
type Transport struct {
	http.RoundTripper
	Signer
	SignatureName string
}

func NewTransport(t http.RoundTripper, s Signer, signame string) *Transport {
	return &Transport{
		RoundTripper:  t,
		Signer:        s,
		SignatureName: signame,
	}
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTrip must not modify the original request.
	req = req.Clone(req.Context())

	sig, err := t.Sign(req)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Signature-Input", sig.Input)
	req.Header.Add("Signature", sig.Signature)

	return t.RoundTripper.RoundTrip(req)
}
