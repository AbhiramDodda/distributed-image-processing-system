package auth

// Permission is a coarse capability a caller may hold, named resource:verb (for
// example "job:submit"). Keeping permissions coarse -- rather than per-object
// ACLs -- matches the platform's needs: authorization here answers "may this
// role submit jobs?", while per-tenant data isolation is enforced separately by
// the tenant claim, so RBAC does not also have to encode ownership.
type Permission string

const (
	PermJobSubmit Permission = "job:submit"
	PermJobRead Permission = "job:read"
	PermJobCancel Permission = "job:cancel"
	PermAlgoRegister Permission = "algorithm:register"
	PermQuotaAdmin Permission = "quota:admin"
	// Worker-facing capabilities: leasing a task (poll/start) and reporting its
	// result. Held by the "worker" role, not by human roles, so a leaked user
	// token cannot masquerade as a worker and drain the task queue.
	PermTaskLease Permission = "task:lease"
	PermTaskReport Permission = "task:report"
)

// wildcard grants every permission; it is how the admin role is expressed
// without enumerating each capability (and without silently missing new ones
// added later).
const wildcard Permission = "*"

// Policy maps roles to the permissions they grant. It is built once at startup
// and only read thereafter, so it needs no locking.
type Policy struct {
	grants map[string]map[Permission]bool
}

// NewPolicy returns an empty policy that denies everything until roles are
// granted.
func NewPolicy() *Policy {
	return &Policy{grants: make(map[string]map[Permission]bool)}
}

// Grant adds permissions to a role, creating the role if needed. Granting
// wildcard makes the role an admin.
func (p *Policy) Grant(role string, perms ...Permission) *Policy {
	set, ok := p.grants[role]
	if !ok {
		set = make(map[Permission]bool)
		p.grants[role] = set
	}
	for _, perm := range perms {
		set[perm] = true
	}
	return p
}

// Allow reports whether any of the caller's roles grants perm. Roles are unioned
// (a caller with both "viewer" and "operator" gets the sum of their
// permissions), and an unknown role simply contributes nothing rather than
// erroring -- so a stale role in a token degrades to fewer permissions, never
// more.
func (p *Policy) Allow(roles []string, perm Permission) bool {
	for _, role := range roles {
		set, ok := p.grants[role]
		if !ok {
			continue
		}
		if set[wildcard] || set[perm] {
			return true
		}
	}
	return false
}

// Authorize is the convenience path from an authenticated token to a decision:
// it reads the roles straight off the verified claims.
func (p *Policy) Authorize(c *Claims, perm Permission) bool {
	if c == nil {
		return false
	}
	return p.Allow(c.Roles, perm)
}

// DefaultPolicy is the platform's baseline role model: admin does everything,
// operator runs and manages jobs, viewer only reads. Callers may extend it or
// build their own with NewPolicy.
func DefaultPolicy() *Policy {
	return NewPolicy().
		Grant("admin", wildcard).
		Grant("operator", PermJobSubmit, PermJobRead, PermJobCancel, PermAlgoRegister).
		Grant("viewer", PermJobRead).
		Grant("worker", PermTaskLease, PermTaskReport, PermJobRead)
}
