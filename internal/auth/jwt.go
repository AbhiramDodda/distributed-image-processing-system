// Package auth provides request authentication (HS256 JWT) and authorization
// (role-based access control) for the platform API (Level 6). JWTs are validated
// locally with a shared secret -- no per-request call to an auth server -- so the
// coordinator can authenticate a token on the hot path without a network round
// trip. OIDC/OAuth2 (which mints these tokens from an institutional IdP) is a
// later increment; this layer only verifies and reads them.
//
// HS256 (HMAC-SHA256) is used rather than RS256 because issuer and verifier are
// the same platform sharing one secret; asymmetric signing buys nothing here and
// costs a keypair to manage. The verifier hard-rejects every other algorithm --
// see Parse -- to close the classic JWT alg-confusion attack.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Claims is the subset of registered + platform-specific claims the platform
// cares about. Tenant and Roles drive quota lookup and RBAC respectively.
type Claims struct {
	Subject string `json:"sub"`
	Issuer string `json:"iss"`
	Tenant string `json:"tenant"`
	Roles []string `json:"roles"`
	IssuedAt int64 `json:"iat"`
	NotBefore int64 `json:"nbf"`
	ExpiresAt int64 `json:"exp"`
}

type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Verifier validates and mints HS256 tokens against a shared secret. Leeway
// absorbs small clock skew between the issuer and this verifier when checking
// exp/nbf.
type Verifier struct {
	secret []byte
	leeway time.Duration
	now func() time.Time
}

// NewVerifier returns a Verifier for the given secret. A zero leeway is fine for
// tests; production should allow a few seconds for clock skew.
func NewVerifier(secret []byte, leeway time.Duration) *Verifier {
	return &Verifier{secret: secret, leeway: leeway, now: time.Now}
}

var b64 = base64.RawURLEncoding // JWT uses base64url with no padding.

// Sign encodes claims into a signed compact JWT. It exists so the platform (and
// tests) can mint tokens without a second library; the real token source in
// production is the OIDC provider, which signs the same way.
func (v *Verifier) Sign(c Claims) (string, error) {
	hb, err := json.Marshal(header{Alg: "HS256", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("auth: marshal header: %w", err)
	}
	pb, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("auth: marshal claims: %w", err)
	}
	signingInput := b64.EncodeToString(hb) + "." + b64.EncodeToString(pb)
	sig := v.mac(signingInput)
	return signingInput + "." + b64.EncodeToString(sig), nil
}

// Parse verifies a compact JWT's signature and time bounds and returns its
// claims. It rejects any token whose header alg is not exactly "HS256" -- this is
// the defense against the alg-confusion attack, where an attacker submits an
// unsigned ("none") token or swaps in an algorithm the verifier will
// misinterpret. The signature is compared in constant time.
func (v *Verifier) Parse(token string) (*Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("auth: token has %d segments, want 3", len(parts))
	}

	hb, err := b64.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("auth: decode header: %w", err)
	}
	var h header
	if err := json.Unmarshal(hb, &h); err != nil {
		return nil, fmt.Errorf("auth: parse header: %w", err)
	}
	if h.Alg != "HS256" {
		return nil, fmt.Errorf("auth: unsupported alg %q, only HS256 accepted", h.Alg)
	}

	expected := v.mac(parts[0] + "." + parts[1])
	got, err := b64.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("auth: decode signature: %w", err)
	}
	if !hmac.Equal(expected, got) {
		return nil, fmt.Errorf("auth: signature mismatch")
	}

	pb, err := b64.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("auth: decode claims: %w", err)
	}
	var c Claims
	if err := json.Unmarshal(pb, &c); err != nil {
		return nil, fmt.Errorf("auth: parse claims: %w", err)
	}

	now := v.now().Unix()
	skew := int64(v.leeway.Seconds())
	if c.ExpiresAt != 0 && now > c.ExpiresAt+skew {
		return nil, fmt.Errorf("auth: token expired at %d (now %d)", c.ExpiresAt, now)
	}
	if c.NotBefore != 0 && now < c.NotBefore-skew {
		return nil, fmt.Errorf("auth: token not valid before %d (now %d)", c.NotBefore, now)
	}
	return &c, nil
}

func (v *Verifier) mac(input string) []byte {
	m := hmac.New(sha256.New, v.secret)
	m.Write([]byte(input))
	return m.Sum(nil)
}
