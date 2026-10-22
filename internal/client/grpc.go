package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
)

// grpcClient implements contract §4/§5.4 on a connection that already carries
// the /grpc prefix interceptors.
type grpcClient struct {
	conn       *grpc.ClientConn
	c          uploadv1.UploadServiceClient
	token      string
	device     string
	reqTimeout time.Duration
	window     int           // max chunks sent but not acked (≥ 1)
	ackTimeout time.Duration // stall guard while blocked on the window
}

func (g *grpcClient) close() error { return g.conn.Close() }

func (g *grpcClient) auth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+g.token)
}

// grpcErr maps a gRPC status to an outcome error (contract §4 error table).
func grpcErr(op string, err error) error {
	switch status.Code(err) {
	case codes.AlreadyExists:
		return fmt.Errorf("%s: %w: %w", op, ErrAlreadyExists, err)
	case codes.NotFound:
		return fmt.Errorf("%s: %w: %w", op, ErrExpired, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("%s: %w: %w", op, ErrOffsetMismatch, err)
	case codes.DataLoss:
		return fmt.Errorf("%s: %w: %w", op, ErrChecksum, err)
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied:
		return fmt.Errorf("%s: %w: %w", op, ErrPermanent, err)
	default:
		// UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED, INTERNAL, …: transient.
		return fmt.Errorf("%s: %w", op, err)
	}
}

// begin calls Init, which is idempotent per sha256 and returns the server's
// committed offset (contract §5.4: "Init resumes").
func (g *grpcClient) begin(ctx context.Context, r Row) (session, error) {
	ctx, cancel := context.WithTimeout(g.auth(ctx), g.reqTimeout)
	defer cancel()
	res, err := g.c.Init(ctx, &uploadv1.InitRequest{
		DeviceId:    g.device,
		Sha256:      r.SHA256,
		Size:        r.Size,
		Filename:    r.Filename,
		ContentType: r.ContentType,
		CapturedAt:  r.CapturedAt,
		Priority:    int32(r.Priority),
	})
	if err != nil {
		return session{}, grpcErr("init", err)
	}
	s := session{Ref: res.GetUploadId(), Offset: res.GetCommittedOffset(), MaxChunk: int(res.GetMaxChunkBytes())}
	if s.Offset < r.Size {
		return s, nil
	}
	// Every byte is committed but no complete ack was seen: ask Status.
	st, err := g.c.Status(ctx, &uploadv1.StatusRequest{UploadId: s.Ref})
	if err != nil {
		return session{}, grpcErr("status", err)
	}
	if !st.GetComplete() {
		return session{}, errors.New("status: all bytes committed, upload not yet complete")
	}
	s.Complete = true
	return s, nil
}

// send streams chunks from the committed offset and waits for Ack{complete}.
// At most g.window chunks are in flight (sent, not yet acked): the sender
// blocks until an ack opens the window. If no ack arrives for g.ackTimeout
// while blocked, the stream is abandoned with a transient error, so the
// retry/backoff loop runs instead of hanging (a window smaller than the
// server's GRPC_ACK_EVERY_N can never be satisfied).
func (g *grpcClient) send(ctx context.Context, r Row, s session, src io.ReaderAt, chunk int, p *progress) error {
	ctx, cancel := context.WithCancel(g.auth(ctx))
	defer cancel()
	stream, err := g.c.Upload(ctx)
	if err != nil {
		return grpcErr("upload", err)
	}

	done := make(chan error, 1)
	acked := make(chan struct{}, 1) // wakes a sender blocked on the window
	go func() {
		for {
			ack, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				done <- errors.New("upload: stream closed before completion")
				return
			}
			if err != nil {
				done <- grpcErr("upload", err)
				return
			}
			p.commit(ack.GetCommittedOffset())
			if ack.GetComplete() {
				done <- nil
				return
			}
			select {
			case acked <- struct{}{}:
			default:
			}
		}
	}()

	if s.MaxChunk > 0 {
		chunk = min(chunk, s.MaxChunk)
	}
	window := int64(max(g.window, 1)) * int64(chunk)
	buf := make([]byte, chunk)
	off := s.Offset
send:
	for off < r.Size {
		if err := g.waitWindow(ctx, p, off, window, acked, done); err != nil {
			if errors.Is(err, errComplete) {
				return nil
			}
			return err
		}
		n := int(min(int64(chunk), r.Size-off))
		if m, err := src.ReadAt(buf[:n], off); m < n {
			return fmt.Errorf("upload: read file at %d: %w", off, err)
		}
		if err := p.claimChunk(off + int64(n)); err != nil {
			return err
		}
		if err := stream.Send(&uploadv1.Chunk{UploadId: s.Ref, Offset: off, Data: buf[:n]}); err != nil {
			break send // the real error arrives through Recv
		}
		p.add(off, int64(n))
		off += int64(n)
	}
	if err := stream.CloseSend(); err != nil {
		return grpcErr("upload", err)
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("upload: %w", ctx.Err())
	}
}

// errComplete is waitWindow's signal that the server acked completion.
var errComplete = errors.New("upload complete")

// waitWindow returns nil once off-acked < window. It returns errComplete if
// the server acked completion, the stream's error if it ended, and a
// transient error if no ack arrives for g.ackTimeout while blocked.
func (g *grpcClient) waitWindow(ctx context.Context, p *progress, off, window int64, acked <-chan struct{}, done <-chan error) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	ended := func(err error) error {
		if err == nil {
			return errComplete
		}
		return err
	}
	for {
		select {
		case err := <-done:
			return ended(err)
		default:
		}
		if off-p.ackedOffset() < window {
			return nil
		}
		if timer == nil {
			timer = time.NewTimer(g.ackTimeout)
		} else {
			timer.Reset(g.ackTimeout)
		}
		select {
		case <-acked:
		case err := <-done:
			return ended(err)
		case <-ctx.Done():
			return fmt.Errorf("upload: %w", ctx.Err())
		case <-timer.C:
			return fmt.Errorf("upload: no ack for %v with %d bytes unacked: -grpc-window (%d chunks) must be >= the server's GRPC_ACK_EVERY_N",
				g.ackTimeout, off-p.ackedOffset(), g.window)
		}
	}
}
