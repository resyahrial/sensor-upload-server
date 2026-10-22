package grpcapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/stats"
)

// framedBody is an endless request body of gRPC messages with payload
// bytes each, returning at most perRead bytes per Read (like small HTTP/2
// DATA frames).
type framedBody struct {
	payload, perRead int
	pos              int // offset in the current message
	closed           atomic.Bool
}

func (f *framedBody) wire() int { return grpcHeaderLen + f.payload }

func (f *framedBody) Read(p []byte) (int, error) {
	n := min(len(p), f.perRead)
	for i := range n {
		if f.pos < grpcHeaderLen {
			var hdr [grpcHeaderLen]byte
			binary.BigEndian.PutUint32(hdr[1:], uint32(f.payload))
			p[i] = hdr[f.pos]
		} else {
			p[i] = 'x'
		}
		f.pos = (f.pos + 1) % f.wire()
	}
	return n, nil
}

func (f *framedBody) Close() error { f.closed.Store(true); return nil }

type readResult struct {
	n   int
	err error
}

// readAsync starts one Read with a buffer of size n.
func readAsync(b *readAhead, n int) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		k, err := b.Read(make([]byte, n))
		ch <- readResult{k, err}
	}()
	return ch
}

func mustBlock(t *testing.T, ch <-chan readResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("Read returned %d, %v; want it to block", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}
}

func mustReturn(t *testing.T, ch <-chan readResult) readResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("Read still blocked")
		return readResult{}
	}
}

func mustRead(t *testing.T, b *readAhead, size int) int {
	t.Helper()
	n, err := b.Read(make([]byte, size))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return n
}

// Each Read is charged for its whole buffer: four 100-byte reads of 10-byte
// messages fill a budget of 400.
func TestReadAheadChargesBufferSizeAndResumesOnConsume(t *testing.T) {
	body := &framedBody{payload: 5, perRead: 10} // one 10-byte message per read
	b := newReadAhead(t.Context(), body, 400, 1600)
	for range 4 {
		if n := mustRead(t, b, 100); n != 10 {
			t.Fatalf("Read = %d, want 10", n)
		}
	}
	ch := readAsync(b, 100)
	mustBlock(t, ch)

	b.consume(10) // first message handed to the handler: its buffer is free
	if r := mustReturn(t, ch); r.n != 10 || r.err != nil {
		t.Fatalf("Read after consume = %d, %v", r.n, r.err)
	}
	mustBlock(t, readAsync(b, 100))
	b.stop()
}

// While no complete message is buffered a Read goes ahead even above the
// budget, so a message larger than the budget is never stuck; past four
// times the budget the body fails.
func TestReadAheadAllowsIncompleteMessageUpToHardLimit(t *testing.T) {
	body := &framedBody{payload: 1 << 20, perRead: 10}
	b := newReadAhead(t.Context(), body, 100, 400)
	for i := range 4 {
		if n := mustRead(t, b, 100); n != 10 {
			t.Fatalf("Read %d = %d, want 10", i, n)
		}
	}
	if _, err := b.Read(make([]byte, 100)); !errors.Is(err, errReadAheadExceeded) {
		t.Fatalf("Read past the hard limit: %v, want errReadAheadExceeded", err)
	}
}

func TestReadAheadFramingAcrossReads(t *testing.T) {
	// 13-byte messages read 4 bytes at a time: headers and payloads split.
	body := &framedBody{payload: 8, perRead: 4}
	b := newReadAhead(t.Context(), body, 5, 20)
	for range 3 { // 12 bytes: still inside the first message
		mustRead(t, b, 4)
	}
	if b.lastEnd != 0 {
		t.Fatalf("lastEnd = %d before the first message is complete", b.lastEnd)
	}
	mustRead(t, b, 4) // bytes 12..15: completes message 1
	if b.lastEnd != 13 {
		t.Fatalf("lastEnd = %d, want 13", b.lastEnd)
	}
	mustBlock(t, readAsync(b, 4)) // over budget with a complete message buffered
	b.stop()
}

func TestReadAheadConsumeNeverExceedsRead(t *testing.T) {
	b := newReadAhead(t.Context(), &framedBody{payload: 5, perRead: 10}, 200, 800)
	b.consume(1000) // nothing read yet: must not grant anything later
	mustRead(t, b, 100)
	mustRead(t, b, 100)
	mustBlock(t, readAsync(b, 100))
	b.stop()
}

func TestReadAheadUnblocksOnContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	b := newReadAhead(ctx, &framedBody{payload: 5, perRead: 10}, 10, 40)
	mustRead(t, b, 10)
	ch := readAsync(b, 10)
	mustBlock(t, ch)
	cancel()
	if r := mustReturn(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("Read = %d, %v; want context.Canceled", r.n, r.err)
	}
}

func TestReadAheadUnblocksOnClose(t *testing.T) {
	body := &framedBody{payload: 5, perRead: 10}
	b := newReadAhead(t.Context(), body, 10, 40)
	mustRead(t, b, 10)
	ch := readAsync(b, 10)
	mustBlock(t, ch)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if r := mustReturn(t, ch); !errors.Is(r.err, http.ErrBodyReadAfterClose) {
		t.Fatalf("Read = %d, %v; want ErrBodyReadAfterClose", r.n, r.err)
	}
	if !body.closed.Load() {
		t.Error("underlying body not closed")
	}
}

// When the handler returns, a reader still blocked on the budget (e.g. a
// goroutine the handler left behind) is released.
func TestLimitReadAheadReleasesOnHandlerReturn(t *testing.T) {
	blocked := make(chan (<-chan readResult), 1)
	h := LimitReadAhead(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b := r.Context().Value(readAheadKey{}).(*readAhead)
		if r.Body != io.ReadCloser(b) {
			t.Error("request body is not the read-ahead reader")
		}
		// One Read charged the whole budget, with complete messages in it.
		if _, err := r.Body.Read(make([]byte, readAheadBudget)); err != nil {
			t.Error(err)
		}
		ch := readAsync(b, 8)
		mustBlock(t, ch)
		blocked <- ch
	}), 3)
	msg := []byte{0, 0, 0, 0, 3, 'a', 'b', 'c'} // one complete 8-byte message
	req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(bytes.NewReader(bytes.Repeat(msg, 8))))
	h.ServeHTTP(httptest.NewRecorder(), req)
	if r := mustReturn(t, <-blocked); r.err == nil {
		t.Fatalf("Read after handler return = %d, nil error", r.n)
	}
}

func TestReadAheadStatsConsumesWireLength(t *testing.T) {
	b := newReadAhead(t.Context(), &framedBody{payload: 40, perRead: 45}, 90, 360)
	mustRead(t, b, 45)
	mustRead(t, b, 45)
	ch := readAsync(b, 45)
	mustBlock(t, ch)
	ctx := context.WithValue(t.Context(), readAheadKey{}, b)
	var h readAheadStats
	h.HandleRPC(ctx, &stats.InHeader{})                        // ignored
	h.HandleRPC(t.Context(), &stats.InPayload{WireLength: 45}) // no reader: ignored
	mustBlock(t, ch)
	h.HandleRPC(ctx, &stats.InPayload{Length: 1000, WireLength: 45}) // wire bytes count
	if r := mustReturn(t, ch); r.n != 45 || r.err != nil {
		t.Fatalf("Read = %d, %v; want 45", r.n, r.err)
	}
	b.stop()
}
