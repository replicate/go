package flags

import (
	"net/http"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/speps/go-hashids/v2"
)

var userHashID *hashids.HashID
var systemUser = ldcontext.New("__system__")
var unknownUser = ldcontext.New("__unknown__")

func init() {
	hd := hashids.NewData()
	hd.Salt = "replicate:launchdarkly" // This must match the salt used in replicate_web.flags
	userHashID, _ = hashids.NewWithData(hd)
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
