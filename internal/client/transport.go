package client

import (
	"context"
	"io"
	"sync"
)

// session is the server's view of an upload at the start of an attempt.
type session struct {
	Ref      string // tus upload URL or gRPC upload_id
	Offset   int64  // server-committed offset; the only trusted progress value
	MaxChunk int    // server limit on chunk size; 0 = none
	Complete bool   // server reports the upload as complete
}

// transport is one wire protocol (tus over TCP or QUIC, or gRPC).
type transport interface {
	// begin asks the server where to resume r.UploadRef, or creates a new
	// upload when the ref is empty or unusable.
	begin(ctx context.Context, r Row) (session, error)
	// send uploads src from s.Offset to the end. It returns nil only after
	// the server confirmed completion.
	send(ctx context.Context, r Row, s session, src io.ReaderAt, chunk int, p *progress) error
	close() error
}

// progress counts payload bytes on the wire for one file. Methods are safe
// for concurrent use (HTTP transports read request bodies on their own
// goroutines).
type progress struct {
	mu        sync.Mutex
	sent      int64 // bytes_sent_total
	resent    int64 // bytes_resent: bytes at offsets already sent once
	hwm       int64 // highest offset sent so far
	committed int64 // last server-confirmed offset seen
	claim     func(end int64) error
}

// claimChunk durably records that bytes up to end are about to be sent, so a
// killed process counts them as re-sent when it resumes.
func (p *progress) claimChunk(end int64) error {
	if p.claim == nil {
		return nil
	}
	return p.claim(end)
}

// add records n bytes sent starting at file offset off.
func (p *progress) add(off, n int64) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent += n
	end := off + n
	if overlap := min(end, p.hwm) - off; overlap > 0 {
		p.resent += overlap
	}
	p.hwm = max(p.hwm, end)
}

// commit records a server-confirmed offset.
func (p *progress) commit(off int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.committed = max(p.committed, off)
}

// ackedOffset returns the last server-confirmed offset.
func (p *progress) ackedOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.committed
}

func (p *progress) snapshot() (sent, resent, hwm, committed int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent, p.resent, p.hwm, p.committed
}

// countingReader reports bytes read from a file section to a progress.
type countingReader struct {
	r   io.Reader
	off int64
	p   *progress
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.p.add(c.off, int64(n))
	c.off += int64(n)
	return n, err
}
