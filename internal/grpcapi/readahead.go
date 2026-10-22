package grpcapi

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sync"

	"google.golang.org/grpc/stats"
)

// On the TLS listener gRPC runs through grpc.Server.ServeHTTP. grpc-go's
// handler transport (internal/transport/handler_server.go, v1.84.0) reads
// the request body in its own goroutine, one Read per pooled 16 KiB buffer,
// and queues every buffer in an unbounded receive buffer. net/http therefore
// returns the HTTP/2 flow-control window to the client as fast as it sends,
// and a client that pipelines chunks makes the server hold everything the
// service has not committed yet. LimitReadAhead restores backpressure: the
// body reader blocks once the buffers it has filled and gRPC has not yet
// handed to the handler reach the budget, so net/http stops returning
// window and the client's sends block. The native listener uses grpc-go's
// own HTTP/2 transport, which enforces flow control itself.
//
// The budget counts each Read as len(p), not the bytes it returned: grpc-go
// keeps the whole buffer alive for as few as one byte of data, so with
// small chunks (4 KiB) counting bytes would let 4x the budget pile up.

// errReadAheadExceeded ends a stream whose next message does not fit the
// hard limit (a client splitting a message into very many tiny frames).
var errReadAheadExceeded = errors.New("grpcapi: request body read-ahead limit exceeded")

// grpcHeaderLen is the length of a gRPC message frame header (compressed
// flag + 4-byte big-endian length).
const grpcHeaderLen = 5

// readAheadBudget is the soft read-ahead budget per request: the same as
// the native listener's stream window. Above it, reads continue only while
// no complete message is buffered, so a stream holds at most about one
// message (or 64 KiB of small ones) beyond what the handler has received.
const readAheadBudget = nativeStreamWindow

// MaxMessageBytes returns the largest request message the server accepts
// (grpc.MaxRecvMsgSize), the argument for LimitReadAhead.
func (s *Service) MaxMessageBytes() int64 {
	return int64(s.cfg.MaxChunkBytes) + msgOverhead
}

// LimitReadAhead wraps h (a grpc.Server, or a handler in front of one) so
// that the request-body buffers read ahead of the messages gRPC has handed
// to the handler stay within a 64 KiB budget. A read is still allowed above
// the budget while no complete message is buffered, so a message of any
// accepted size (up to maxMsg) is always received and nothing deadlocks; a
// message that needs buffers beyond 4 x (maxMsg + frame header) + budget
// (a client splitting it into very many tiny frames) fails the body. The
// grpc.Server must have been built by NewServer, whose stats handler
// reports each received message. Body reads also stop when the request
// context is done or when h returns.
func LimitReadAhead(h http.Handler, maxMsg int64) http.Handler {
	limit := 4*(maxMsg+grpcHeaderLen) + readAheadBudget
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := newReadAhead(r.Context(), r.Body, readAheadBudget, limit)
		defer b.stop()
		r = r.WithContext(context.WithValue(r.Context(), readAheadKey{}, b))
		r.Body = b
		h.ServeHTTP(w, r)
	})
}

type readAheadKey struct{}

// span is one Read: the stream offset it ended at and the buffer size it
// is charged for.
type span struct {
	end, cost int64
}

// readAhead is a request body whose Read blocks while the cost of the
// buffers not yet consumed has reached the budget and a complete message is
// buffered. consume is called (through readAheadStats) with the wire size
// of each message gRPC hands to the handler.
type readAhead struct {
	body   io.ReadCloser
	budget int64
	limit  int64           // hard limit on the cost ahead
	done   <-chan struct{} // request context

	mu       sync.Mutex
	read     int64  // body bytes read
	consumed int64  // body bytes of messages handed to the handler
	cost     int64  // sum of spans[i].cost
	spans    []span // reads not yet fully consumed, in order
	stopped  bool
	wake     chan struct{} // closed (and replaced) on consume and stop

	// gRPC framing, to know whether a complete message is buffered.
	hdr      [grpcHeaderLen]byte
	hdrN     int   // header bytes seen of the current message
	bodyLeft int64 // payload bytes still to come of the current message
	lastEnd  int64 // stream offset after the last complete message read
}

func newReadAhead(ctx context.Context, body io.ReadCloser, budget, limit int64) *readAhead {
	return &readAhead{body: body, budget: budget, limit: limit, done: ctx.Done(), wake: make(chan struct{})}
}

// Read reads from the body once the read-ahead allows it.
func (b *readAhead) Read(p []byte) (int, error) {
	if err := b.wait(); err != nil {
		return 0, err
	}
	n, err := b.body.Read(p)
	if n > 0 {
		b.mu.Lock()
		b.frame(p[:n])
		b.read += int64(n)
		b.spans = append(b.spans, span{end: b.read, cost: int64(len(p))})
		b.cost += int64(len(p))
		b.mu.Unlock()
	}
	return n, err
}

// wait blocks until a Read may proceed.
func (b *readAhead) wait() error {
	for {
		b.mu.Lock()
		stopped, cost, complete, wake := b.stopped, b.cost, b.lastEnd > b.consumed, b.wake
		b.mu.Unlock()
		switch {
		case stopped:
			return http.ErrBodyReadAfterClose
		case cost < b.budget:
			return nil
		case !complete && cost >= b.limit:
			return errReadAheadExceeded
		case !complete:
			return nil // the handler may be waiting for this message
		}
		select {
		case <-wake:
		case <-b.done:
			return context.Canceled
		}
	}
}

// frame advances the gRPC framing over p, which starts at offset b.read;
// b.mu must be held.
func (b *readAhead) frame(p []byte) {
	off := b.read
	for len(p) > 0 {
		if b.hdrN < grpcHeaderLen {
			k := copy(b.hdr[b.hdrN:], p)
			b.hdrN += k
			p, off = p[k:], off+int64(k)
			if b.hdrN < grpcHeaderLen {
				return
			}
			b.bodyLeft = int64(binary.BigEndian.Uint32(b.hdr[1:]))
		}
		k := min(int64(len(p)), b.bodyLeft)
		b.bodyLeft -= k
		p, off = p[k:], off+k
		if b.bodyLeft == 0 {
			b.lastEnd, b.hdrN = off, 0
		}
	}
}

// Close stops the reader (waking a blocked Read) and closes the body.
// grpc-go closes the body when the stream is over.
func (b *readAhead) Close() error {
	b.stop()
	return b.body.Close()
}

// consume releases the buffers of n more consumed bytes. It never counts
// more than has been read, so the accounting cannot drift.
func (b *readAhead) consume(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumed = min(b.consumed+n, b.read)
	i := 0
	for i < len(b.spans) && b.spans[i].end <= b.consumed {
		b.cost -= b.spans[i].cost
		i++
	}
	b.spans = b.spans[i:]
	b.signal()
}

// stop makes every current and later Read return an error.
func (b *readAhead) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.stopped {
		b.stopped = true
		b.signal()
	}
}

// signal wakes a waiting Read; b.mu must be held.
func (b *readAhead) signal() {
	close(b.wake)
	b.wake = make(chan struct{})
}

// readAheadStats is the grpc stats.Handler that releases read-ahead budget:
// for every message gRPC has taken out of the transport's buffer it reports
// InPayload with the message's exact wire length (5-byte frame header plus
// the possibly compressed payload). Streams without a readAhead in their
// context (the native listener) are ignored.
type readAheadStats struct{}

func (readAheadStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (readAheadStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	p, ok := s.(*stats.InPayload)
	if !ok {
		return
	}
	if b, ok := ctx.Value(readAheadKey{}).(*readAhead); ok {
		b.consume(int64(p.WireLength))
	}
}

func (readAheadStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (readAheadStats) HandleConn(context.Context, stats.ConnStats) {}
