package accounts

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// BasicCredential describes a username and password for use with Funnel's basic auth.
type BasicCredential struct {
	User     string
	Password string
}

type BasicAuth []BasicCredential

func (ba BasicAuth) Validate(md MetaData) (string, error) {
	var auth []string
	var ok bool

	fmt.Printf("Running BasicAuth: %#v\n", md)

	if auth, ok = md["Authorization"]; !ok {
		if auth, ok = md["authorization"]; !ok {
			return "", fmt.Errorf("no authorization") // no basic auth found
		}
	}

	if len(auth) > 0 {
		user, password, ok := parseBasicAuth(auth[0])
		fmt.Printf("User: %s Password: %s OK: %s\n", user, password, ok)
		for _, c := range ba {
			if c.User == user && c.Password == password {
				return user, nil
			}
		}
	}
	return "", fmt.Errorf("no authorization")
}

/*

// Check the context's metadata for the configured server/API password.
func (bc *) authorize(ctx context.Context, user, password string) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md["authorization"]) > 0 {
			raw := md["authorization"][0]
			requser, reqpass, ok := parseBasicAuth(raw)
			if ok {
				if requser == user && reqpass == password {
					return nil
				}
				return fmt.Errorf("Permission denied")
			}
		}
	}

	return fmt.Errorf("Permission denied")
}

*/

// parseBasicAuth parses an HTTP Basic Authentication string.
// "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==" returns ("Aladdin", "open sesame", true).
//
// Taken from Go core: https://golang.org/src/net/http/request.go?s=27379:27445#L828
func parseBasicAuth(auth string) (username, password string, ok bool) {
	const prefix = "Basic "

	if !strings.HasPrefix(auth, prefix) {
		return
	}

	c, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		return
	}

	cs := string(c)
	s := strings.IndexByte(cs, ':')
	if s < 0 {
		return
	}

	return cs[:s], cs[s+1:], true
}
