package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOpenRejectsBadURL(t *testing.T) {
	_, err := Open(context.Background(), "postgres://%zz")
	if err == nil || !strings.Contains(err.Error(), "parse DATABASE_URL") {
		t.Fatalf("Open = %v, want parse error", err)
	}
}

func TestOpenGivesUpWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	// Port 1 on localhost: connection refused immediately, so Open retries.
	_, err := Open(ctx, "postgres://u:p@127.0.0.1:1/db?connect_timeout=1")
	if err == nil || !strings.Contains(err.Error(), "connect to database") {
		t.Fatalf("Open = %v, want connect error", err)
	}
}
