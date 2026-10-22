package grpcapi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

func bearerCtx(t *testing.T, tok string) context.Context {
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+tok)
}

// callAll runs a unary call and a stream with ctx and checks both codes.
func callAll(t *testing.T, e *env, ctx context.Context, want codes.Code) {
	t.Helper()
	_, err := e.client.Status(ctx, &uploadv1.StatusRequest{UploadId: uuid.NewString()})
	wantCode(t, err, want)
	s, err := e.client.Upload(ctx)
	if err == nil {
		_ = s.Send(&uploadv1.Chunk{UploadId: uuid.NewString(), Data: []byte("x")})
		_, err = s.Recv()
	}
	wantCode(t, err, want)
}

func TestDeviceKeyAuthGRPC(t *testing.T) {
	keys := auth.NewMemoryStore()
	e := startEnvWith(t, keys, newFakeRepo(), t.TempDir())
	tok, err := keys.Issue(device)
	if err != nil {
		t.Fatal(err)
	}
	_, key, _ := auth.ParseToken(tok)

	t.Run("valid", func(t *testing.T) { callAll(t, e, bearerCtx(t, tok), codes.NotFound) })
	for name, bad := range map[string]string{
		"wrong key":      auth.FormatToken(device, strings.Repeat("a", auth.KeyHexLen)),
		"unknown device": auth.FormatToken("dev-9999", key),
		"malformed":      device + key,
	} {
		t.Run(name, func(t *testing.T) { callAll(t, e, bearerCtx(t, bad), codes.Unauthenticated) })
	}
	t.Run("rotated", func(t *testing.T) {
		rotated, err := keys.Issue(device)
		if err != nil {
			t.Fatal(err)
		}
		callAll(t, e, bearerCtx(t, tok), codes.Unauthenticated)
		callAll(t, e, bearerCtx(t, rotated), codes.NotFound)
		f := newFile(t, 10)
		if _, err := e.client.Init(bearerCtx(t, rotated), f.req); err != nil {
			t.Fatalf("Init with the rotated key: %v", err)
		}
	})
	t.Run("store down", func(t *testing.T) {
		cur, _ := keys.Token(device)
		keys.SetErr(errors.New("connection refused"))
		defer keys.SetErr(nil)
		callAll(t, e, bearerCtx(t, cur), codes.Unavailable)
		// Malformed tokens never reach the store.
		callAll(t, e, bearerCtx(t, "junk"), codes.Unauthenticated)
	})
}
