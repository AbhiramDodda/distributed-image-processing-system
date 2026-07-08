package auth

import "testing"

func TestRBAC_adminWildcard(t *testing.T) {
	p := DefaultPolicy()
	for _, perm := range []Permission{PermJobSubmit, PermQuotaAdmin, PermAlgoRegister, "anything:new"} {
		if !p.Allow([]string{"admin"}, perm) {
			t.Fatalf("admin should hold %q via wildcard", perm)
		}
	}
}

func TestRBAC_operatorScope(t *testing.T) {
	p := DefaultPolicy()
	if !p.Allow([]string{"operator"}, PermJobSubmit) {
		t.Fatal("operator should be able to submit jobs")
	}
	if p.Allow([]string{"operator"}, PermQuotaAdmin) {
		t.Fatal("operator must not hold quota:admin")
	}
}

func TestRBAC_viewerReadOnly(t *testing.T) {
	p := DefaultPolicy()
	if !p.Allow([]string{"viewer"}, PermJobRead) {
		t.Fatal("viewer should read jobs")
	}
	if p.Allow([]string{"viewer"}, PermJobSubmit) {
		t.Fatal("viewer must not submit jobs")
	}
}

func TestRBAC_unknownRoleDenies(t *testing.T) {
	p := DefaultPolicy()
	if p.Allow([]string{"gremlin"}, PermJobRead) {
		t.Fatal("unknown role should grant nothing")
	}
}

func TestRBAC_rolesUnion(t *testing.T) {
	p := DefaultPolicy()
	roles := []string{"viewer", "operator"}
	if !p.Allow(roles, PermJobRead) || !p.Allow(roles, PermJobSubmit) {
		t.Fatal("caller with multiple roles should get the union of permissions")
	}
}

func TestRBAC_authorizeFromClaims(t *testing.T) {
	p := DefaultPolicy()
	if !p.Authorize(&Claims{Roles: []string{"operator"}}, PermJobCancel) {
		t.Fatal("operator claims should authorize job:cancel")
	}
	if p.Authorize(nil, PermJobRead) {
		t.Fatal("nil claims must never authorize")
	}
}
