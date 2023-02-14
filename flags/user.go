package flags

import (
	"net/http"

	"github.com/speps/go-hashids/v2"
	"gopkg.in/launchdarkly/go-sdk-common.v2/lduser"
)

var userHashID *hashids.HashID
var systemUser = lduser.NewUserBuilder("__system__").Build()
var unknownUser = lduser.NewUserBuilder("__unknown__").Build()

func init() {
	hd := hashids.NewData()
	hd.Salt = "replicate:launchdarkly" // This must match the salt used in replicate_web.flags
	userHashID, _ = hashids.NewWithData(hd)
}

func GetUser(id int, r *http.Request) lduser.User {
	if id == 0 {
		return unknownUser
	}

	key, err := userHashID.Encode([]int{id})
	if err != nil {
		return unknownUser
	}

	userBuilder := lduser.NewUserBuilder(key)

	// For now we rely on web to have populated other fields (name, is_staff,
	// etc.) in LaunchDarkly.
	if ip := r.Header.Get("CF-Connecting-IP"); ip != "" {
		userBuilder.IP(ip)
	}

	return userBuilder.Build()
}
