package grpcapi

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
)

func authzCtx(t *testing.T, authz string) context.Context {
	if authz == "" {
		return t.Context()
	}
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", authz)
}

// wantStatus fails unless err has code and exactly message msg.
func wantStatus(t *testing.T, err error, code codes.Code, msg string) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code || st.Message() != msg {
		t.Fatalf("status = %v %q, want %v %q", st.Code(), st.Message(), code, msg)
	}
}

// callAll runs a unary call and a stream with ctx and checks both.
func callAll(t *testing.T, e *env, ctx context.Context, code codes.Code, msg string) {
	t.Helper()
	_, err := e.client.Status(ctx, &uploadv1.StatusRequest{UploadId: uuid.NewString()})
	wantStatus(t, err, code, msg)
	s, err := e.client.Upload(ctx)
	if err == nil {
		_ = s.Send(&uploadv1.Chunk{UploadId: uuid.NewString(), Data: []byte("x")})
		_, err = s.Recv()
	}
	wantStatus(t, err, code, msg)
}

func TestTokenAuthGRPC(t *testing.T) {
	e := newEnv(t)
	k := keys(t)
	otherIss, err := auth.NewIssuer(auth.IssuerConfig{Key: k.Key, Issuer: "someone-else", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	wrongIss, _, err := otherIss.Issue(device)
	if err != nil {
		t.Fatal(err)
	}
	valid := token(t, device)

	t.Run("valid", func(t *testing.T) {
		callAll(t, e, authzCtx(t, "Bearer "+valid), codes.NotFound, "upload not found")
	})
	for _, tc := range []struct {
		name, authz, msg string
	}{
		{"missing", "", "invalid_token"},
		{"not bearer", "Basic " + valid, "invalid_token"},
		{"garbage", "Bearer junk", "invalid_token"},
		{"other signing key", authtest.New(t).Bearer(t, device), "invalid_token"},
		{"tampered signature", "Bearer " + valid[:len(valid)-4] + "AAAA", "invalid_token"},
		{"wrong issuer", "Bearer " + wrongIss, "invalid_token"},
		{"expired", "Bearer " + k.Expired(t, device), "token_expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callAll(t, e, authzCtx(t, tc.authz), codes.Unauthenticated, tc.msg)
		})
	}
	t.Run("new token", func(t *testing.T) {
		// Tokens are stateless: any unexpired token for the device works.
		f := newFile(t, 10)
		if _, err := e.client.Init(authzCtx(t, k.Bearer(t, device)), f.req); err != nil {
			t.Fatalf("Init with a fresh token: %v", err)
		}
	})
	t.Run("metadata device mismatch", func(t *testing.T) {
		f := newFile(t, 10)
		_, err := e.client.Init(ctxFor(t, other), f.req)
		wantStatus(t, err, codes.PermissionDenied, "device_not_active")
	})
	t.Run("other device's upload", func(t *testing.T) {
		// NOT_FOUND, not PERMISSION_DENIED: another device's upload is not
		// revealed, on unary calls and streams alike.
		f := newFile(t, 10)
		res, err := e.client.Init(ctxFor(t, device), f.req)
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.client.Status(ctxFor(t, other), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
		wantStatus(t, err, codes.NotFound, "upload not found")
		s, err := e.client.Upload(ctxFor(t, other))
		if err == nil {
			_ = s.Send(&uploadv1.Chunk{UploadId: res.GetUploadId(), Data: f.data})
			_, err = s.Recv()
		}
		wantStatus(t, err, codes.NotFound, "upload not found")
	})
}
