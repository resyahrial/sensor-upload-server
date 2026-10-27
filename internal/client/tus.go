package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// tusClient implements contract §2/§3 (tus 1.0 core + creation) over any
// http.Client; /rest and /rest-http3 differ only in base path and transport.
type tusClient struct {
	base       string // e.g. "https://host:8443/rest/files/"
	tokens     TokenSource
	device     string
	hc         *http.Client
	reqTimeout time.Duration
	closeFn    func() error
}

func (t *tusClient) close() error {
	t.hc.CloseIdleConnections()
	if t.closeFn != nil {
		return t.closeFn()
	}
	return nil
}

// do sends one tus request with token; n is the body length (0 for no body).
func (t *tusClient) do(ctx context.Context, token, method, url string, body io.Reader, n int64, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", method, err)
	}
	req.ContentLength = n
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := t.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %w", method, errConn, err)
	}
	return res, nil
}

// drain discards a little of the body so the connection can be reused.
func drain(res *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	_ = res.Body.Close()
}

func encodeMetadata(r Row, device string) string {
	pairs := []struct{ k, v string }{
		{"device_id", device},
		{"sha256", r.SHA256},
		{"filename", r.Filename},
		{"content_type", r.ContentType},
		{"captured_at", r.CapturedAt},
		{"priority", strconv.Itoa(r.Priority)},
	}
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.k + " " + base64.StdEncoding.EncodeToString([]byte(p.v))
	}
	return strings.Join(out, ",")
}

func (t *tusClient) create(ctx context.Context, r Row) (string, error) {
	var ref string
	err := withToken(ctx, t.tokens, func(token string) error {
		ctx, cancel := context.WithTimeout(ctx, t.reqTimeout)
		defer cancel()
		res, err := t.do(ctx, token, http.MethodPost, t.base, http.NoBody, 0, map[string]string{
			"Upload-Length":   strconv.FormatInt(r.Size, 10),
			"Upload-Metadata": encodeMetadata(r, t.device),
		})
		if err != nil {
			return err
		}
		defer drain(res)
		if res.StatusCode != http.StatusCreated {
			return responseErr("create", res)
		}
		loc, err := res.Location() // resolves relative Location against the request URL
		if err != nil {
			return fmt.Errorf("create: bad Location: %w", err)
		}
		ref = loc.String()
		return nil
	})
	return ref, err
}

func (t *tusClient) head(ctx context.Context, url string) (int64, error) {
	var off int64
	err := withToken(ctx, t.tokens, func(token string) error {
		ctx, cancel := context.WithTimeout(ctx, t.reqTimeout)
		defer cancel()
		res, err := t.do(ctx, token, http.MethodHead, url, http.NoBody, 0, nil)
		if err != nil {
			return err
		}
		defer drain(res)
		if res.StatusCode != http.StatusOK {
			return responseErr("head", res)
		}
		if off, err = strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64); err != nil {
			return fmt.Errorf("head: bad Upload-Offset: %w", err)
		}
		return nil
	})
	return off, err
}

// begin HEADs a known upload (contract: always ask the server before
// resuming) or creates a new one.
func (t *tusClient) begin(ctx context.Context, r Row) (session, error) {
	if r.UploadRef != "" {
		off, err := t.head(ctx, r.UploadRef)
		if err != nil {
			return session{}, err
		}
		if off < r.Size {
			return session{Ref: r.UploadRef, Offset: off}, nil
		}
		// All bytes are staged but no completion was seen (the final 204
		// was lost). Creating again yields 461 once the server has finalized.
	}
	ref, err := t.create(ctx, r)
	if err != nil {
		return session{}, err
	}
	return session{Ref: ref}, nil
}

// send PATCHes chunks from the server offset until Upload-Offset == size.
func (t *tusClient) send(ctx context.Context, r Row, s session, src io.ReaderAt, chunk int, p *progress) error {
	off := s.Offset
	for off < r.Size {
		n := min(int64(chunk), r.Size-off)
		if err := p.claimChunk(off + n); err != nil {
			return err
		}
		next, err := t.patch(ctx, s.Ref, off, n, func() io.Reader {
			return &countingReader{r: io.NewSectionReader(src, off, n), off: off, p: p}
		})
		if err != nil {
			return err
		}
		if next <= off || next > r.Size {
			return fmt.Errorf("patch: server offset %d after sending at %d: %w", next, off, ErrOffsetMismatch)
		}
		p.commit(next)
		off = next
	}
	return nil
}

// patch sends one chunk; body builds the request body (again, if the
// request is retried after a token refresh).
func (t *tusClient) patch(ctx context.Context, url string, off, n int64, body func() io.Reader) (int64, error) {
	var next int64
	err := withToken(ctx, t.tokens, func(token string) error {
		ctx, cancel := context.WithTimeout(ctx, t.reqTimeout)
		defer cancel()
		res, err := t.do(ctx, token, http.MethodPatch, url, body(), n, map[string]string{
			"Upload-Offset": strconv.FormatInt(off, 10),
			"Content-Type":  "application/offset+octet-stream",
		})
		if err != nil {
			return err
		}
		defer drain(res)
		if res.StatusCode != http.StatusNoContent {
			return responseErr("patch", res)
		}
		if next, err = strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64); err != nil {
			return fmt.Errorf("patch: bad Upload-Offset: %w", err)
		}
		return nil
	})
	return next, err
}
