package rpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/auth"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/ratelimit"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/rpc/coordinatorpb"
)

func secured(t *testing.T, svc JobService, limiter *ratelimit.Limiter) (coordinatorpb.CoordinatorClient, *auth.Verifier) {
	t.Helper()
	v := auth.NewVerifier([]byte("test-secret"), 0)
	ic := NewAuthInterceptor(v, auth.DefaultPolicy(), limiter)
	cli := dialServer(t, svc,
		grpc.UnaryInterceptor(ic.Unary),
		grpc.StreamInterceptor(ic.Stream),
	)
	return cli, v
}

func bearer(ctx context.Context, v *auth.Verifier, tenant string, roles ...string) context.Context {
	tok, _ := v.Sign(auth.Claims{Subject: "u", Tenant: tenant, Roles: roles, ExpiresAt: time.Now().Add(time.Hour).Unix()})
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func TestInterceptor_rejectsMissingToken(t *testing.T) {
	cli, _ := secured(t, newFakeService(), nil)
	_, err := cli.SubmitJob(context.Background(), &coordinatorpb.SubmitJobRequest{Dataset: "d"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestInterceptor_rejectsBadToken(t *testing.T) {
	cli, _ := secured(t, newFakeService(), nil)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer garbage.token.here")
	_, err := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{Dataset: "d"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestInterceptor_deniesInsufficientRole(t *testing.T) {
	cli, v := secured(t, newFakeService(), nil)
	ctx := bearer(context.Background(), v, "acme", "viewer") // viewer cannot submit
	_, err := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{Dataset: "d"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestInterceptor_allowsPermittedRole(t *testing.T) {
	cli, v := secured(t, newFakeService(), nil)
	ctx := bearer(context.Background(), v, "acme", "operator")
	if _, err := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{Dataset: "laion", Shards: []string{"a3"}}); err != nil {
		t.Fatalf("operator submit should succeed: %v", err)
	}
}

func TestInterceptor_workerCanPoll(t *testing.T) {
	cli, v := secured(t, newFakeService(), nil)
	ctx := bearer(context.Background(), v, "acme", "worker")
	if _, err := cli.PollTasks(ctx, &coordinatorpb.PollTasksRequest{WorkerId: "w1"}); err != nil {
		t.Fatalf("worker poll should succeed: %v", err)
	}
	// A worker must not be able to submit jobs.
	if _, err := cli.SubmitJob(ctx, &coordinatorpb.SubmitJobRequest{Dataset: "d"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("worker submit code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestInterceptor_rateLimited(t *testing.T) {
	// burst of 2: third call in the same instant is throttled.
	limiter := ratelimit.New(1, 2)
	cli, v := secured(t, newFakeService(), limiter)
	ctx := bearer(context.Background(), v, "acme", "operator")

	req := &coordinatorpb.SubmitJobRequest{Dataset: "laion", Shards: []string{"a3"}}
	if _, err := cli.SubmitJob(ctx, req); err != nil {
		t.Fatalf("call 1: %v", err)
	}
	if _, err := cli.SubmitJob(ctx, req); err != nil {
		t.Fatalf("call 2: %v", err)
	}
	if _, err := cli.SubmitJob(ctx, req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("call 3 code = %v, want ResourceExhausted", status.Code(err))
	}
}

func TestInterceptor_guardsStream(t *testing.T) {
	cli, _ := secured(t, newFakeService(), nil)
	stream, err := cli.WatchJob(context.Background(), &coordinatorpb.WatchJobRequest{JobId: "j1"})
	if err == nil {
		// The error may surface on first Recv rather than at call time.
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stream code = %v, want Unauthenticated", status.Code(err))
	}
}
