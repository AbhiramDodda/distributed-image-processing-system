package auth

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fixedNow(sec int64) func() time.Time {
	return func() time.Time { return time.Unix(sec, 0) }
}

func TestJWT_signParseRoundTrip(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	v.now = fixedNow(1000)
	in := Claims{Subject: "u1", Tenant: "acme", Roles: []string{"operator"}, ExpiresAt: 2000}
	tok, err := v.Sign(in)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	got, err := v.Parse(tok)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Subject != "u1" || got.Tenant != "acme" || len(got.Roles) != 1 || got.Roles[0] != "operator" {
		t.Fatalf("claims round-tripped wrong: %+v", got)
	}
}

func TestJWT_rejectsExpired(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	v.now = fixedNow(1000)
	tok, _ := v.Sign(Claims{Subject: "u1", ExpiresAt: 999})
	if _, err := v.Parse(tok); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestJWT_leewayAbsorbsSkew(t *testing.T) {
	v := NewVerifier([]byte("secret"), 5*time.Second)
	v.now = fixedNow(1003)
	tok, _ := v.Sign(Claims{Subject: "u1", ExpiresAt: 1000}) // 3s past exp, within 5s leeway
	if _, err := v.Parse(tok); err != nil {
		t.Fatalf("token within leeway should pass: %v", err)
	}
}

func TestJWT_rejectsNotYetValid(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	v.now = fixedNow(1000)
	tok, _ := v.Sign(Claims{Subject: "u1", NotBefore: 2000})
	if _, err := v.Parse(tok); err == nil {
		t.Fatal("expected not-yet-valid token to be rejected")
	}
}

func TestJWT_rejectsTamperedPayload(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	v.now = fixedNow(1000)
	tok, _ := v.Sign(Claims{Subject: "u1", Roles: []string{"viewer"}, ExpiresAt: 2000})

	parts := strings.Split(tok, ".")
	forged, _ := json.Marshal(Claims{Subject: "u1", Roles: []string{"admin"}, ExpiresAt: 2000})
	parts[1] = b64.EncodeToString(forged) // keep old signature
	if _, err := v.Parse(strings.Join(parts, ".")); err == nil {
		t.Fatal("expected signature mismatch on tampered payload")
	}
}

func TestJWT_rejectsWrongSecret(t *testing.T) {
	signer := NewVerifier([]byte("real-secret"), 0)
	signer.now = fixedNow(1000)
	tok, _ := signer.Sign(Claims{Subject: "u1", ExpiresAt: 2000})

	attacker := NewVerifier([]byte("guessed-secret"), 0)
	attacker.now = fixedNow(1000)
	if _, err := attacker.Parse(tok); err == nil {
		t.Fatal("token verified under the wrong secret")
	}
}

// The alg-confusion attack: an attacker crafts a header claiming "none" (or any
// non-HS256 alg) hoping the verifier skips signature checking. Parse must reject
// on the alg field before it ever trusts the token.
func TestJWT_rejectsAlgNone(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	v.now = fixedNow(1000)
	hb, _ := json.Marshal(header{Alg: "none", Typ: "JWT"})
	pb, _ := json.Marshal(Claims{Subject: "attacker", Roles: []string{"admin"}, ExpiresAt: 2000})
	forged := b64.EncodeToString(hb) + "." + b64.EncodeToString(pb) + "."
	if _, err := v.Parse(forged); err == nil {
		t.Fatal("expected alg=none token to be rejected")
	}
}

func TestJWT_rejectsMalformed(t *testing.T) {
	v := NewVerifier([]byte("secret"), 0)
	for _, tok := range []string{"", "onlyonepart", "two.parts", "a.b.c.d"} {
		if _, err := v.Parse(tok); err == nil {
			t.Fatalf("expected malformed token %q to be rejected", tok)
		}
	}
}
