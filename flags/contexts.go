package flags

import (
	"context"
	"net/http"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/speps/go-hashids/v2"
)

type goContextKey string

const (
	versionKind     = "version"
	savedContextKey = goContextKey("savedContext")
)

var userHashID *hashids.HashID
var systemUser = ldcontext.New("__system__")
var unknownUser = ldcontext.New("__unknown__")

func init() {
	hd := hashids.NewData()
	hd.Salt = "replicate:launchdarkly" // This must match the salt used in replicate_web.flags
	userHashID, _ = hashids.NewWithData(hd)
}

// WithFlagContext returns a child context that has had the provided
// LaunchDarkly context stored on it.
func WithFlagContext(ctx context.Context, c ldcontext.Context) context.Context {
	return context.WithValue(ctx, savedContextKey, c)
}

// FlagContextFromContext returns any LaunchDarkly context stored on the passed
// Go context, or the "unknown" user context if none is found.
func FlagContextFromContext(ctx context.Context) ldcontext.Context {
	v := ctx.Value(savedContextKey)
	if v == nil {
		return unknownUser
	}

	c, ok := v.(ldcontext.Context)
	if !ok {
		return unknownUser
	}

	return c
}

func GetUser(id int, r *http.Request) ldcontext.Context {
	if id == 0 {
		return unknownUser
	}

	key, err := userHashID.Encode([]int{id})
	if err != nil {
		return unknownUser
	}

	userBuilder := ldcontext.NewBuilder(key)

	// For now we rely on web to have populated other fields (name, is_staff,
	// etc.) in LaunchDarkly.
	if ip := r.Header.Get("CF-Connecting-IP"); ip != "" {
		userBuilder.SetString("ip", ip)
	}

	return userBuilder.Build()
}

func GetVersion(id string) ldcontext.Context {
	// We rely on web to have populated other fields (hardware, model.owner,
	// etc.) in LaunchDarkly.
	return ldcontext.NewWithKind(versionKind, id)
}
