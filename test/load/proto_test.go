//go:build load

package load

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// Protocols under test.
const (
	protoREST       = "rest"
	protoRESTHTTP3  = "rest-http3"
	protoGRPC       = "grpc"
	protoGRPCNative = "grpc-native" // GRPC_NATIVE_ADDR, grpc-go's own HTTP/2 transport
)

// dbProtocol is the uploads.protocol value and metric label of protocol p:
// both gRPC listeners serve the same "grpc" service.
func dbProtocol(p string) string {
	if p == protoGRPCNative {
		return protoGRPC
	}
	return p
}

const (
	contentType = "video/mp4"
	filename    = "load.mp4"
	capturedAt  = "2026-10-21T05:30:00+07:00"
)

// errUnimplemented means the endpoint is not mounted on this server build.
var errUnimplemented = errors.New("endpoint not implemented by the server")

// runNonce makes content (and so sha256) unique per process run.
var runNonce = rand.Uint64()

// file is generated upload content: a ChaCha8 stream seeded by (runNonce,
// seed), so it can be produced twice (hash, then send) without storing it.
type file struct {
	Device string
	Seed   uint64
	Size   int64
	SHA256 string
}

// newFile hashes the generated content of size bytes.
func newFile(device string, seed uint64, size int64) (file, error) {
	f := file{Device: device, Seed: seed, Size: size}
	h := sha256.New()
	if _, err := io.Copy(h, f.reader()); err != nil {
		return file{}, fmt.Errorf("hash %s: %w", device, err)
	}
	f.SHA256 = hex.EncodeToString(h.Sum(nil))
	return f, nil
}

// reader returns the content from offset 0.
func (f file) reader() io.Reader {
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[0:], runNonce)
	binary.LittleEndian.PutUint64(seed[8:], f.Seed)
	return io.LimitReader(rand.NewChaCha8(seed), f.Size)
}

func (f file) tusMetadata() string {
	pairs := [][2]string{
		{"device_id", f.Device}, {"sha256", f.SHA256}, {"filename", filename},
		{"content_type", contentType}, {"captured_at", capturedAt}, {"priority", "2"},
	}
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p[0] + " " + base64.StdEncoding.EncodeToString([]byte(p[1]))
	}
	return strings.Join(out, ",")
}

// uploader is one device's client for one protocol. Each uploader has its
// own connection, like a separate device.
type uploader interface {
	// create registers the upload (tus POST / gRPC Init).
	create(ctx context.Context) error
	// send streams bytes [from, to) read from src in pieces of piece bytes,
	// waiting pace between pieces, and returns once the server confirmed
	// offset to (and completion when to is the file size). tus sends one
	// PATCH per piece when pace is 0, else one paced PATCH; gRPC uses one
	// stream with one Chunk message per piece.
	send(ctx context.Context, src io.Reader, from, to int64, piece int, pace time.Duration) error
	// ref is the upload_ref of the DB row.
	ref() string
	close()
}

// newUploader returns an uploader for f over protocol p.
func newUploader(p string, s *server, f file) (uploader, error) {
	tok, err := s.token(context.Background(), f.Device)
	if err != nil {
		return nil, fmt.Errorf("device token: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	switch p {
	case protoREST, protoRESTHTTP3:
		var rt http.RoundTripper
		var closeFn func()
		if p == protoRESTHTTP3 {
			tr := &http3.Transport{TLSClientConfig: tlsCfg}
			rt, closeFn = tr, func() { _ = tr.Close() }
		} else {
			tr := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
			rt, closeFn = tr, tr.CloseIdleConnections
		}
		return &tusUploader{
			hc: &http.Client{Transport: rt}, closeFn: closeFn,
			base: s.URL + "/" + p + "/files/", token: tok, f: f,
		}, nil
	case protoGRPC, protoGRPCNative:
		host := strings.TrimPrefix(s.URL, "https://")
		opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))}
		if p == protoGRPCNative {
			host = s.Native
		} else {
			opts = append(opts, grpcprefix.DialOptions("/grpc")...)
		}
		conn, err := grpc.NewClient(host, opts...)
		if err != nil {
			return nil, fmt.Errorf("grpc client: %w", err)
		}
		return &grpcUploader{conn: conn, c: uploadv1.NewUploadServiceClient(conn), token: tok, f: f}, nil
	default:
		return nil, fmt.Errorf("unknown protocol %q", p)
	}
}

// tusUploader speaks tus 1.0 over HTTP/2 (TCP) or HTTP/3.
type tusUploader struct {
	hc      *http.Client
	closeFn func()
	base    string
	token   string
	f       file
	url     string
}

func (u *tusUploader) ref() string { return u.url[strings.LastIndex(u.url, "/")+1:] }
func (u *tusUploader) close()      { u.closeFn() }

func (u *tusUploader) do(ctx context.Context, method, url string, body io.Reader, n int64, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = n
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Authorization", "Bearer "+u.token)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := u.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	return res, nil
}

func (u *tusUploader) create(ctx context.Context) error {
	res, err := u.do(ctx, http.MethodPost, u.base, http.NoBody, 0, map[string]string{
		"Upload-Length":   strconv.FormatInt(u.f.Size, 10),
		"Upload-Metadata": u.f.tusMetadata(),
	})
	if err != nil {
		return err
	}
	body := drain(res)
	switch res.StatusCode {
	case http.StatusCreated:
		u.url = res.Header.Get("Location")
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("create: %w (404)", errUnimplemented)
	default:
		return fmt.Errorf("create: status %d %q", res.StatusCode, body)
	}
}

func (u *tusUploader) send(ctx context.Context, src io.Reader, from, to int64, piece int, pace time.Duration) error {
	if pace > 0 {
		return u.patch(ctx, &pacedReader{r: io.LimitReader(src, to-from), piece: piece, pace: pace}, from, to)
	}
	buf := make([]byte, piece)
	for off := from; off < to; {
		n := int(min(int64(piece), to-off))
		if _, err := io.ReadFull(src, buf[:n]); err != nil {
			return fmt.Errorf("generate content: %w", err)
		}
		if err := u.patch(ctx, bytes.NewReader(buf[:n]), off, off+int64(n)); err != nil {
			return err
		}
		off += int64(n)
	}
	return nil
}

// patch sends body as bytes [from, to) and checks the new offset.
func (u *tusUploader) patch(ctx context.Context, body io.Reader, from, to int64) error {
	res, err := u.do(ctx, http.MethodPatch, u.url, body, to-from, map[string]string{
		"Upload-Offset": strconv.FormatInt(from, 10),
		"Content-Type":  "application/offset+octet-stream",
	})
	if err != nil {
		return err
	}
	msg := drain(res)
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("patch at %d: status %d %q", from, res.StatusCode, msg)
	}
	if got := res.Header.Get("Upload-Offset"); got != strconv.FormatInt(to, 10) {
		return fmt.Errorf("patch at %d: Upload-Offset %q, want %d", from, got, to)
	}
	return nil
}

func drain(res *http.Response) string {
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
	return string(b)
}

// pacedReader returns at most piece bytes per Read and sleeps pace before
// every Read but the first, so a request body trickles in.
type pacedReader struct {
	r       io.Reader
	piece   int
	pace    time.Duration
	started bool
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if p.started {
		time.Sleep(p.pace)
	}
	p.started = true
	return p.r.Read(b[:min(len(b), p.piece)])
}

// grpcUploader speaks the gRPC contract on the /grpc prefix of the TCP
// listener (grpc) or on the native gRPC listener (grpc-native).
type grpcUploader struct {
	conn     *grpc.ClientConn
	c        uploadv1.UploadServiceClient
	token    string
	f        file
	id       string
	maxChunk int
}

func (u *grpcUploader) ref() string { return u.id }
func (u *grpcUploader) close()      { _ = u.conn.Close() }

func (u *grpcUploader) auth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+u.token)
}

func (u *grpcUploader) create(ctx context.Context) error {
	res, err := u.c.Init(u.auth(ctx), &uploadv1.InitRequest{
		DeviceId: u.f.Device, Sha256: u.f.SHA256, Size: u.f.Size, Filename: filename,
		ContentType: contentType, CapturedAt: capturedAt, Priority: 2,
	})
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("init: %w: %w", errUnimplemented, err)
	}
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	u.id, u.maxChunk = res.GetUploadId(), int(res.GetMaxChunkBytes())
	return nil
}

func (u *grpcUploader) send(ctx context.Context, src io.Reader, from, to int64, piece int, pace time.Duration) error {
	ctx, cancel := context.WithCancel(u.auth(ctx))
	defer cancel()
	stream, err := u.c.Upload(ctx)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	if u.maxChunk > 0 {
		piece = min(piece, u.maxChunk)
	}
	buf := make([]byte, piece)
	for off := from; off < to; {
		if off > from && pace > 0 {
			time.Sleep(pace)
		}
		n := int(min(int64(piece), to-off))
		if _, err := io.ReadFull(src, buf[:n]); err != nil {
			return fmt.Errorf("generate content: %w", err)
		}
		if err := stream.Send(&uploadv1.Chunk{UploadId: u.id, Offset: off, Data: buf[:n]}); err != nil {
			break // the real error arrives through Recv
		}
		off += int64(n)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("upload: close send: %w", err)
	}
	var committed int64
	var complete bool
	for {
		ack, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("upload: %w", err)
		}
		committed, complete = max(committed, ack.GetCommittedOffset()), complete || ack.GetComplete()
	}
	if committed != to {
		return fmt.Errorf("upload: committed %d, want %d", committed, to)
	}
	if to == u.f.Size && !complete {
		return errors.New("upload: stream ended without complete ack")
	}
	return nil
}
