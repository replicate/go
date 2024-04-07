package signing

import (
	"fmt"
	"net/http"
	"net/textproto"
	"regexp"
	"strings"
)

const (
	// sf-string from [RFC 8941]
	//
	//     sf-string = DQUOTE *chr DQUOTE
	//     chr       = unescaped / escaped
	//     unescaped = %x20-21 / %x23-5B / %x5D-7E
	//     escaped   = "\" ( DQUOTE / "\" )
	//
	// [RFC 8941]: https://www.rfc-editor.org/rfc/rfc8941
	reSFString = `"(?:[\x20-\x21\x23-\x5B\x5D-\x7E]|\\"|\\\\)*"`

	// We don't need to implement a full scanner for parameters, as only a small
	// subset of parameters are currently permitted by [RFC 9421 Section 6.5.2].
	//
	// [RFC 9421 Section 6.5.2]: https://www.rfc-editor.org/rfc/rfc9421#section-6.5.2
	reComponentParameter = `(?:(?:sf|bs|tr|req|key=` + reSFString + `|name=` + reSFString + `))`
)

var (
	// The ABNF for component identifiers is as follows
	//
	//     component-identifier = component-name parameters
	//     component-name = sf-string
	//
	pattComponentIdentifier = regexp.MustCompile(
		`\A` +
			// component-name = sf-string
			`(` + reSFString + `)` +
			// parameters = *( ";" parameter )
			`((?:;` + reComponentParameter + `)*)` +
			`\z`,
	)
	pattComponentParameter = regexp.MustCompile(`;` + reComponentParameter)

	// Obsolete line folding from [RFC 7230]
	//
	// [RFC 7230]: https://www.rfc-editor.org/rfc/rfc7230
	pattObsFold = regexp.MustCompile(`\r\n[ \t]+`)
)

var derivedComponents = map[string]bool{
	"@method":         true,
	"@target-uri":     true,
	"@authority":      true,
	"@scheme":         true,
	"@request-target": true,
	"@path":           true,
	"@query":          true,
	"@query-param":    true,
	"@status":         true,
}

type ValidatedComponents []component

func (cs ValidatedComponents) Base(req *http.Request) (string, error) {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.Identifier())
		b.WriteRune(':')
		b.WriteRune(' ')
		v, err := c.Value(req)
		if err != nil {
			return "", err
		}
		b.WriteString(v)
		b.WriteRune('\n')
	}
	return b.String(), nil
}

func (cs ValidatedComponents) Identifiers() []string {
	ids := make([]string, len(cs))
	for i, c := range cs {
		ids[i] = c.Identifier()
	}
	return ids
}

type component interface {
	Identifier() string
	Value(req *http.Request) (string, error)
}

type param struct {
	Key   string
	Value string
}

func Components(spec []string) (ValidatedComponents, error) {
	cs := make([]component, len(spec))
	for i, s := range spec {
		c, err := validateComponent(s)
		if err != nil {
			return nil, err
		}
		cs[i] = c
	}
	return cs, nil
}

func MustComponents(spec []string) ValidatedComponents {
	cs, err := Components(spec)
	if err != nil {
		panic(err)
	}
	return cs
}

func validateComponent(s string) (component, error) {
	matches := pattComponentIdentifier.FindStringSubmatch(s)
	if len(matches) != 3 {
		return nil, fmt.Errorf("%w: malformed identifier %q", ErrInvalidComponent, s)
	}

	nameStr := matches[1]
	paramStr := matches[2]

	var params []param

	// Validate parameters
	if paramStr != "" {
		paramMatches := pattComponentParameter.FindAllString(paramStr, -1)

		paramKeys := make(map[string]bool)
		params = make([]param, len(paramMatches))

		for i, p := range paramMatches {
			pk, pv, _ := strings.Cut(p[1:], "=")
			if _, ok := paramKeys[pk]; ok {
				return nil, fmt.Errorf("%w: repeated parameter %s for %s is not permitted", ErrInvalidComponent, pk, nameStr)
			}

			paramKeys[pk] = true
			params[i] = param{Key: pk, Value: pv}
		}

		// TODO: validate cross-compatibility of parameters
		// TODO: validate that `req` parameter is not supplied
	}

	// It's not clear whether this is actually required by the spec, but it's hard
	// to see a valid case for providing a blank component name.
	if nameStr == `""` {
		return nil, fmt.Errorf("%w: component names may not be blank", ErrInvalidComponent)
	}

	// Remove outer quotes
	name := nameStr[1 : len(nameStr)-1]

	if name[0] == '@' {
		if _, ok := derivedComponents[name]; !ok {
			return nil, fmt.Errorf("%w: unknown derived component name %s", ErrInvalidComponent, name)
		}
		return derivedComponent{
			Name:   name,
			Params: params,
		}, nil
	}

	return fieldComponent{
		Name:   name,
		Params: params,
	}, nil
}

type derivedComponent struct {
	Name   string
	Params []param
}

func (c derivedComponent) Identifier() string {
	return makeIdentifier(c.Name, c.Params)
}

func (c derivedComponent) Value(req *http.Request) (string, error) {
	// For now, treat any parameters as ErrNotImplemented.
	if len(c.Params) > 0 {
		return "", fmt.Errorf("%w: parameters are not yet supported (field %s)", ErrNotImplemented, c.Name)
	}

	switch c.Name {
	case "@method":
		return req.Method, nil
	case "@target-uri":
		return req.URL.String(), nil
	case "@authority":
		return req.Host, nil
	case "@scheme":
		return req.URL.Scheme, nil
	case "@request-target":
		return req.URL.RequestURI(), nil
	case "@path":
		result := req.URL.EscapedPath()
		if result == "" {
			result = "/"
		}
		return result, nil
	case "@query":
		return req.URL.RawQuery, nil
	case "@query-param":
		return "", fmt.Errorf("%w: @query-param is not yet implemented", ErrNotImplemented)
	default:
		return "", fmt.Errorf("%w: unknown derived component %s", ErrSigningFailure, c.Name)
	}
}

type fieldComponent struct {
	Name   string
	Params []param
}

func (c fieldComponent) Identifier() string {
	return makeIdentifier(c.Name, c.Params)
}

func (c fieldComponent) Value(req *http.Request) (string, error) {
	key := textproto.CanonicalMIMEHeaderKey(c.Name)
	vals := req.Header[key]

	// For now, treat any parameters as ErrNotImplemented.
	if len(c.Params) > 0 {
		return "", fmt.Errorf("%w: parameters are not yet supported (field %s)", ErrNotImplemented, c.Name)
	}

	// If the field has been requested for signing and there are no values
	// available, signing must fail.
	if len(vals) == 0 {
		return "", fmt.Errorf("%w: request lacks requested field %s", ErrSigningFailure, c.Name)
	}

	canonicalVals := make([]string, len(vals))
	for i, v := range vals {
		// Strip leading and trailing whitespace from each item in the list.
		s := strings.TrimSpace(v)
		// Remove any obsolete line folding within the line, and replace it with a
		// single space (" "), as discussed in [Section 5.2 of HTTP/1.1].
		//
		// [Section 5.2 of HTTP/1.1]: https://rfc-editor.org/rfc/rfc9112#section-5.2
		s = pattObsFold.ReplaceAllString(v, " ")

		canonicalVals[i] = s
	}
	// Concatenate the list of values with a single comma (",") and a single space
	// (" ") between each item.
	return strings.Join(canonicalVals, ", "), nil
}

func makeIdentifier(name string, params []param) string {
	var b strings.Builder
	b.WriteRune('"')
	b.WriteString(name)
	b.WriteRune('"')
	for _, p := range params {
		b.WriteRune(';')
		b.WriteString(p.Key)
		if p.Value != "" {
			b.WriteRune('=')
			b.WriteString(p.Value)
		}
	}
	return b.String()
}
