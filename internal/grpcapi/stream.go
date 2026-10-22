package grpcapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/staging"
)

const (
	// commitTimeout bounds the per-chunk UpdateCommitted, so a hung DB
	// ends the stream instead of holding it (and the upload) forever.
	commitTimeout = 30 * time.Second
	// finalizeTimeout bounds one finalizer run; the same value as tusapi's.
	finalizeTimeout = 10 * time.Minute
)

type recvResult struct {
	chunk *uploadv1.Chunk
	err   error
}

// Upload receives chunks for one upload (FR-6). Each chunk is written at the
// committed offset and fsynced, then the committed offset is stored in the
// DB, and only then acked. The staging info file is written once at Init:
// the DB offset is authoritative and the .bin is truncated to it when a
// stream opens the upload (FR-7), so a per-chunk info update would add an
// fsync'd rename per chunk without being read by anything. Once the last byte is
// committed the finalizer runs and the stream ends with Ack{complete}.
func (s *Service) Upload(stream grpc.BidiStreamingServer[uploadv1.Chunk, uploadv1.Ack]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Recv runs in its own goroutine so the loop can also watch the idle
	// timer and shutdown. It exits when ctx is done or Recv fails, which
	// happens at the latest when this handler returns.
	chunks := make(chan recvResult, 1)
	go func() {
		for {
			c, err := stream.Recv()
			select {
			case chunks <- recvResult{c, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	u := &upstream{s: s, stream: stream, ctx: ctx, start: time.Now(), log: s.log.With("method", "Upload", "device_id", deviceFrom(ctx))}
	err := u.run(chunks)
	u.finish(err)
	return err
}

// upstream is the state of one Upload stream.
type upstream struct {
	s      *Service
	stream grpc.BidiStreamingServer[uploadv1.Chunk, uploadv1.Ack]
	ctx    context.Context
	start  time.Time
	log    *slog.Logger

	// Set once the first chunk has opened the upload.
	id        string
	upload    Upload
	file      *staging.File
	release   func()
	before    int64 // committed offset when the stream opened the upload
	committed int64
	acked     int64
	chunks    int
	bytes     int64
	complete  bool
}

func (u *upstream) run(chunks <-chan recvResult) error {
	idle := time.NewTimer(u.s.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case r := <-chunks:
			if errors.Is(r.err, io.EOF) {
				// Client half-closed: ack any unacked progress and end.
				if u.file != nil && u.acked < u.committed {
					return u.send(&uploadv1.Ack{CommittedOffset: u.committed})
				}
				return nil
			}
			if r.err != nil {
				return r.err
			}
			done, err := u.handle(r.chunk)
			if err != nil || done {
				return err
			}
			idle.Reset(u.s.cfg.IdleTimeout)
		case <-idle.C:
			return status.Errorf(codes.DeadlineExceeded, "no chunk received for %s", u.s.cfg.IdleTimeout)
		case <-u.s.closing:
			return errUnavailable
		case <-u.ctx.Done():
			return status.FromContextError(u.ctx.Err()).Err()
		}
	}
}

// handle applies one chunk. It reports done once the upload is complete.
func (u *upstream) handle(c *uploadv1.Chunk) (bool, error) {
	if u.file == nil {
		if err := u.open(c.GetUploadId()); err != nil {
			return false, err
		}
		if u.committed == u.upload.Meta.Size {
			// All bytes were committed before (crash before finalize).
			return true, u.finalize()
		}
	}
	if c.GetUploadId() != u.id {
		return false, status.Error(codes.InvalidArgument, "upload_id: all chunks in a stream must have the same upload_id")
	}
	if err := u.check(c); err != nil {
		return false, err
	}

	data := c.GetData()
	if _, err := u.file.WriteAt(data, c.GetOffset()); err != nil { // fsyncs
		return false, u.s.internal(err)
	}
	next := u.committed + int64(len(data))
	if err := u.commit(next); err != nil {
		return false, u.s.internal(err)
	}
	u.committed = next
	u.chunks++
	u.bytes += int64(len(data))
	u.s.metrics.BytesReceived.WithLabelValues(Protocol).Add(float64(len(data)))
	u.s.metrics.ChunkBytes.WithLabelValues(Protocol).Observe(float64(len(data)))
	u.log.Debug("chunk committed", "offset_after", next, "bytes", len(data))

	if u.committed == u.upload.Meta.Size {
		return true, u.finalize()
	}
	if u.chunks%u.s.cfg.AckEveryN == 0 {
		return false, u.send(&uploadv1.Ack{CommittedOffset: u.committed})
	}
	return false, nil
}

// commit stores the committed offset in the DB. The bytes are durable, so
// they are recorded even if the client goes away now; commitTimeout bounds
// a hung DB.
func (u *upstream) commit(next int64) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(u.ctx), commitTimeout)
	defer cancel()
	return u.s.repo.UpdateCommitted(ctx, u.id, next)
}

// check validates a chunk against the committed state (FR-6).
func (u *upstream) check(c *uploadv1.Chunk) error {
	n := int64(len(c.GetData()))
	switch {
	case c.GetOffset() != u.committed:
		return status.Errorf(codes.FailedPrecondition, "offset %d does not match the committed offset; expected offset %d", c.GetOffset(), u.committed)
	case n == 0:
		return status.Error(codes.InvalidArgument, "data: must not be empty")
	case n > int64(u.s.cfg.MaxChunkBytes):
		return status.Errorf(codes.InvalidArgument, "data: chunk of %d bytes exceeds max_chunk_bytes %d", n, u.s.cfg.MaxChunkBytes)
	case c.GetOffset()+n > u.upload.Meta.Size:
		return status.Errorf(codes.InvalidArgument, "data: chunk ends at %d, beyond the upload size %d", c.GetOffset()+n, u.upload.Meta.Size)
	}
	return nil
}

// open claims the upload for this stream and prepares the staged file,
// truncating it to the DB committed offset (the DB is authoritative, FR-7).
func (u *upstream) open(id string) error {
	if _, err := u.load(id); err != nil {
		return err
	}
	release, ok := u.s.active.acquire(id)
	if !ok {
		return errBusy
	}
	u.release = release
	u.log = u.log.With("upload_ref", id)
	// Reload: the previous holder may have advanced or finished the upload.
	up, err := u.load(id)
	if err != nil {
		return err
	}
	if _, err := u.s.store.LoadInfo(id); err != nil {
		return u.stagingErr(err)
	}
	f, err := u.s.store.Open(id)
	if err != nil {
		return u.stagingErr(err)
	}
	if err := f.Truncate(up.Committed); err != nil {
		if cerr := f.Close(); cerr != nil {
			u.log.Warn("close staged file", "error", cerr)
		}
		return u.s.internal(err)
	}
	u.id, u.upload, u.file = id, up, f
	u.before, u.committed, u.acked = up.Committed, up.Committed, up.Committed
	u.s.metrics.InProgress.WithLabelValues(Protocol).Inc()
	return nil
}

// load looks the upload up for the stream's device. A completed upload is
// ALREADY_EXISTS; anything not 'uploading' otherwise is NOT_FOUND.
func (u *upstream) load(id string) (Upload, error) {
	up, err := u.s.lookup(u.ctx, id)
	if err != nil {
		return Upload{}, err
	}
	if up.Status == StatusCompleted {
		return Upload{}, errCompleted
	}
	return up, nil
}

func (u *upstream) stagingErr(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		u.log.Warn("staging data missing", "error", err)
		return errNotFound
	}
	return u.s.internal(err)
}

func (u *upstream) finalize() error {
	if err := u.s.finalize(u.ctx, u.upload); err != nil {
		return err
	}
	u.complete = true
	return u.send(&uploadv1.Ack{CommittedOffset: u.upload.Meta.Size, Complete: true})
}

func (u *upstream) send(a *uploadv1.Ack) error {
	if err := u.stream.Send(a); err != nil {
		return err
	}
	u.acked = a.GetCommittedOffset()
	return nil
}

// finish releases the upload and writes the per-stream log line (§10).
func (u *upstream) finish(err error) {
	if u.file != nil {
		if cerr := u.file.Close(); cerr != nil {
			u.log.Warn("close staged file", "error", cerr)
		}
		u.s.metrics.InProgress.WithLabelValues(Protocol).Dec()
	}
	if u.release != nil {
		u.release()
	}
	if err != nil && !u.complete {
		u.s.metrics.GRPCStreamResets.WithLabelValues(Protocol).Inc()
	}
	attrs := []any{
		"offset_before", u.before, "offset_after", u.committed, "bytes", u.bytes,
		"chunks", u.chunks, "complete", u.complete,
		"status", status.Code(err).String(), "duration_ms", time.Since(u.start).Milliseconds(),
	}
	if err != nil {
		attrs = append(attrs, "error", status.Convert(err).Message())
	}
	u.log.Info("grpc stream", attrs...)
}
