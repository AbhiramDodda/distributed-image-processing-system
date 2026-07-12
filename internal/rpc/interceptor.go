package rpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/auth"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/ratelimit"
)

// AuthInterceptor enforces, in order, authentication (verify the bearer JWT),
// authorization (RBAC per method), and per-tenant rate limiting. Wiring these as
// gRPC interceptors keeps every handler in server.go free of cross-cutting
// concerns -- a handler only runs once the caller is known, permitted, and under
// budget. The three collaborators are exactly the stdlib pieces built earlier in
// Level 6 (auth.Verifier, auth.Policy, ratelimit.Limiter).
type AuthInterceptor struct {
	verifier *auth.Verifier
	policy *auth.Policy
	limiter *ratelimit.Limiter
	perms map[string]auth.Permission
}

// NewAuthInterceptor maps every RPC to the permission it requires. A method
// absent from this map is denied by default, so adding an RPC without granting
// it a permission fails closed rather than shipping unprotected.
func NewAuthInterceptor(v *auth.Verifier, p *auth.Policy, l *ratelimit.Limiter) *AuthInterceptor {
	const svc = "/coordinator.v1.Coordinator/"
	return &AuthInterceptor{
		verifier: v,
		policy: p,
		limiter: l,
		perms: map[string]auth.Permission{
			svc + "SubmitJob": auth.PermJobSubmit,
			svc + "GetJob": auth.PermJobRead,
			svc + "ListJobs": auth.PermJobRead,
			svc + "WatchJob": auth.PermJobRead,
			svc + "PollTasks": auth.PermTaskLease,
			svc + "StartTask": auth.PermTaskLease,
			svc + "ReportResult": auth.PermTaskReport,
		},
	}
}

type ctxKey struct{}

// ClaimsFromContext returns the verified claims a handler was invoked with, if
// the request passed through the interceptor.
func ClaimsFromContext(ctx context.Context) (*auth.Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*auth.Claims)
	return c, ok
}

// Unary is the grpc.UnaryServerInterceptor.
func (i *AuthInterceptor) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	claims, err := i.gate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(context.WithValue(ctx, ctxKey{}, claims), req)
}

// Stream is the grpc.StreamServerInterceptor (covers WatchJob).
func (i *AuthInterceptor) Stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	claims, err := i.gate(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, &wrappedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), ctxKey{}, claims)})
}

// gate runs the authenticate -> authorize -> rate-limit pipeline shared by unary
// and streaming calls.
func (i *AuthInterceptor) gate(ctx context.Context, fullMethod string) (*auth.Claims, error) {
	claims, err := i.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	perm, ok := i.perms[fullMethod]
	if !ok {
		return nil, status.Errorf(codes.PermissionDenied, "no policy for method %s", fullMethod)
	}
	if !i.policy.Authorize(claims, perm) {
		return nil, status.Errorf(codes.PermissionDenied, "tenant %q not permitted: %s", claims.Tenant, perm)
	}
	if i.limiter != nil && !i.limiter.Allow(claims.Tenant) {
		return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded for tenant %q", claims.Tenant)
	}
	return claims, nil
}

func (i *AuthInterceptor) authenticate(ctx context.Context) (*auth.Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization")
	}
	token, ok := strings.CutPrefix(vals[0], "Bearer ")
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "authorization must be a Bearer token")
	}
	claims, err := i.verifier.Parse(token)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}
	return claims, nil
}

// wrappedStream overrides Context so downstream handlers see the claims-carrying
// context instead of the original.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
