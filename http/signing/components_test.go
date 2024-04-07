package signing

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComponents(t *testing.T) {
	testcases := []struct {
		Name       string
		Spec       []string
		Components ValidatedComponents
		Err        error
	}{
		{
			Name:       "empty spec",
			Spec:       []string{},
			Components: ValidatedComponents{},
			Err:        nil,
		},
		{
			Name: "single field component",
			Spec: []string{`"content-type"`},
			Components: ValidatedComponents{
				fieldComponent{Name: "content-type"},
			},
			Err: nil,
		},
		{
			Name: "single field component with params",
			Spec: []string{`"content-type";sf`},
			Components: ValidatedComponents{
				fieldComponent{
					Name:   "content-type",
					Params: []param{{Key: "sf"}},
				},
			},
			Err: nil,
		},
		{
			Name: "multiple field components",
			Spec: []string{`"content-type";sf`, `"content-encoding"`},
			Components: ValidatedComponents{
				fieldComponent{
					Name:   "content-type",
					Params: []param{{Key: "sf"}},
				},
				fieldComponent{
					Name: "content-encoding",
				},
			},
			Err: nil,
		},
		{
			Name: "derived component",
			Spec: []string{`"@method"`},
			Components: ValidatedComponents{
				derivedComponent{
					Name: "@method",
				},
			},
			Err: nil,
		},
		{
			Name: "mixed components",
			Spec: []string{
				`"@authority"`,
				`"content-type"`,
				`"@method"`,
			},
			Components: ValidatedComponents{
				derivedComponent{
					Name: "@authority",
				},
				fieldComponent{
					Name: "content-type",
				},
				derivedComponent{
					Name: "@method",
				},
			},
			Err: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			components, err := Components(tc.Spec)
			if tc.Err != nil {
				assert.ErrorIs(t, err, tc.Err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.Components, components)
			}
		})
	}
}

func TestValidatedComponentsBase(t *testing.T) {
	testcases := []struct {
		Name       string
		Components ValidatedComponents
		RequestFn  func() *http.Request
		Expected   string
		Err        error
	}{
		{
			Name:       "empty components",
			Components: MustComponents([]string{}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
				return r
			},
			Expected: "",
		},
		{
			Name: "single field component (valid)",
			Components: MustComponents([]string{
				`"content-type"`,
			}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
				r.Header.Set("Content-Type", "application/json")
				return r
			},
			Expected: `"content-type": application/json` + "\n",
		},
		{
			Name: "single derived component",
			Components: MustComponents([]string{
				`"@method"`,
			}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodPost, "http://example.com", nil)
				return r
			},
			Expected: `"@method": POST` + "\n",
		},
		{
			Name: "mixed components",
			Components: MustComponents([]string{
				`"@method"`,
				`"content-type"`,
				`"@path"`,
			}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodPost, "http://example.com/animals/giraffes?age=123", nil)
				r.Header.Set("Content-Type", "application/json")
				return r
			},
			Expected: `"@method": POST` + "\n" +
				`"content-type": application/json` + "\n" +
				`"@path": /animals/giraffes` + "\n",
		},
		{
			Name: "unhandled derived component",
			Components: MustComponents([]string{
				`"@status"`,
			}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodPost, "/envelope/opener", nil)
				return r
			},
			Err: ErrSigningFailure,
		},
		{
			Name: "missing field component",
			Components: MustComponents([]string{
				`"content-type"`,
			}),
			RequestFn: func() *http.Request {
				r, _ := http.NewRequest(http.MethodPost, "/envelope/opener", nil)
				return r
			},
			Err: ErrSigningFailure,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			base, err := tc.Components.Base(tc.RequestFn())
			if tc.Err != nil {
				assert.ErrorIs(t, err, tc.Err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.Expected, base)
			}
		})
	}
}

func TestValidatedComponentsIdentifiers(t *testing.T) {
	testcases := []struct {
		Name       string
		Components ValidatedComponents
		Expected   []string
	}{
		{
			Name:       "empty components",
			Components: MustComponents([]string{}),
			Expected:   []string{},
		},
		{
			Name: "single field component",
			Components: MustComponents([]string{
				`"content-type"`,
			}),
			Expected: []string{`"content-type"`},
		},
		{
			Name: "single derived component",
			Components: MustComponents([]string{
				`"@method"`,
			}),
			Expected: []string{`"@method"`},
		},
		{
			Name: "mixed components",
			Components: MustComponents([]string{
				`"@method"`,
				`"content-type"`,
				`"@path"`,
			}),
			Expected: []string{`"@method"`, `"content-type"`, `"@path"`},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			ids := tc.Components.Identifiers()
			assert.Equal(t, tc.Expected, ids)
		})
	}
}
