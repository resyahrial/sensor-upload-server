package tusapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/tus/tusd/v2/pkg/handler"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// reqState is shared between the middleware and the tusd hooks of one
// request (hooks run synchronously on the request goroutine and see the
// request context's values).
type reqState struct {
	device       string
	ref          string
	offsetBefore int64
	bytes        atomic.Int64 // request body bytes read by tusd
	expires      time.Time    // Upload-Expires to add to a 2xx response; zero = none
	completed    bool         // finalized during this request
	err          error        // internal cause, for the log line only
}

func (s *reqState) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

type stateKey struct{}

// stateFrom returns the request state; hooks always run behind the
// middleware, but a fresh state keeps a misconfigured chain from panicking.
func stateFrom(ctx context.Context) *reqState {
	if st, ok := ctx.Value(stateKey{}).(*reqState); ok {
		return st
	}
	return &reqState{}
}

// refFromID maps a tus upload ID to the DB upload_ref: the UUID before an
// optional "+<multipart id>" suffix (s3store).
func refFromID(id string) string {
	ref, _, _ := strings.Cut(id, "+")
	return ref
}

// validRef reports whether ref is a canonical UUID string (all our refs
// are UUIDv7), which also keeps arbitrary input out of staging paths.
func validRef(ref string) bool {
	u, err := uuid.Parse(ref)
	return err == nil && u.String() == ref
}

// ServeHTTP authenticates every request, enforces upload ownership and
// state, guards tusd's completed-upload short-circuit and writes the
// per-request log line. The request path has the base path stripped.
func (h *endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	st := &reqState{offsetBefore: -1}
	rw := &responseWriter{ResponseWriter: w, head: r.Method == http.MethodHead, beforeHeader: func(hdr http.Header, status int) {
		h.editHeaders(r.Method, st, hdr, status)
	}}
	r = r.WithContext(context.WithValue(r.Context(), stateKey{}, st))
	// tusd would let a POST become a PATCH or DELETE through this header,
	// bypassing the per-method checks below.
	r.Header.Del("X-HTTP-Method-Override")
	defer func() { h.logRequest(r, rw, st, start) }()

	device, err := h.cfg.Verifier.VerifyHeader(r.Header.Get("Authorization"))
	if err != nil {
		st.fail(err)
		writeError(rw, r, errUnauthenticated)
		return
	}
	st.device = device

	switch r.Method {
	case http.MethodHead, http.MethodPatch, http.MethodDelete:
		h.serveUpload(rw, r, st)
	case http.MethodPost:
		st.offsetBefore = 0
		h.serveCounted(rw, r, st)
	default: // OPTIONS; everything else gets tusd's 405
		h.tusd.ServeHTTP(rw, r)
	}
}

// serveUpload handles HEAD, PATCH and DELETE on an existing upload.
func (h *endpoint) serveUpload(w http.ResponseWriter, r *http.Request, st *reqState) {
	ctx := r.Context()
	id := strings.Trim(r.URL.Path, "/")
	ref := refFromID(id)
	if !validRef(ref) {
		writeError(w, r, errNotFound)
		return
	}
	st.ref = ref
	leave, err := h.cfg.Staging.use.enter(ctx, ref)
	if err != nil {
		st.fail(err)
		writeError(w, r, internalError(ctx))
		return
	}
	defer leave()
	row, err := h.cfg.Repo.Get(ctx, h.cfg.Protocol, ref)
	switch {
	case errors.Is(err, db.ErrNotFound):
		writeError(w, r, errNotFound) // includes uploads of the other base path
		return
	case err != nil:
		st.fail(err)
		writeError(w, r, internalError(ctx))
		return
	}
	if row.DeviceID != st.device {
		// 404, not 403: do not reveal another device's upload.
		st.fail(errors.New("upload belongs to another device"))
		writeError(w, r, errNotFound)
		return
	}
	if row.Status != db.StatusUploading {
		h.serveFinished(w, r, st, row)
		return
	}

	info, err := h.cfg.Staging.info(ctx, id)
	switch {
	case errors.Is(err, handler.ErrNotFound):
		h.tusd.ServeHTTP(w, r) // nothing staged: tusd answers 404
		return
	case err != nil:
		st.fail(err)
		writeError(w, r, internalError(ctx))
		return
	}
	st.offsetBefore = info.Offset

	if r.Method != http.MethodDelete && info.Offset == info.Size {
		// tusd answers a HEAD/PATCH on a fully staged upload itself (PATCH
		// with 204, without calling PreFinish), even if finalization failed
		// or the process crashed before it. Finalize here instead.
		h.finishStaged(w, r, st, id)
		return
	}

	switch r.Method {
	case http.MethodHead:
		st.expires = row.UpdatedAt.Add(h.cfg.IncompleteTTL)
		if info.Offset > 0 {
			if err := h.cfg.Repo.IncrResume(ctx, h.cfg.Protocol, ref); err != nil {
				h.cfg.Log.Warn("increment resume count", "protocol", h.cfg.Protocol, "upload_ref", ref, "error", err.Error())
			} else {
				h.cfg.Metrics.Resumes.WithLabelValues(h.cfg.Protocol).Inc()
				st.expires = h.cfg.Now().Add(h.cfg.IncompleteTTL)
			}
		}
		h.serveGuarded(w, r, st, id, info.Size, h.tusd.ServeHTTP)
	case http.MethodPatch:
		st.expires = h.cfg.Now().Add(h.cfg.IncompleteTTL)
		h.cfg.Metrics.InProgress.WithLabelValues(h.cfg.Protocol).Inc()
		h.serveGuarded(w, r, st, id, info.Size, func(w http.ResponseWriter, r *http.Request) {
			h.serveCounted(w, r, st)
		})
		h.cfg.Metrics.InProgress.WithLabelValues(h.cfg.Protocol).Dec()
		n := st.bytes.Load()
		if n > 0 && !st.completed {
			// Activity keeps the upload from expiring (FR-9 uses updated_at).
			if err := h.cfg.Repo.Touch(context.WithoutCancel(ctx), h.cfg.Protocol, ref); err != nil && !errors.Is(err, db.ErrNotUploading) {
				h.cfg.Log.Warn("touch upload", "protocol", h.cfg.Protocol, "upload_ref", ref, "error", err.Error())
			}
		}
	default: // DELETE
		h.tusd.ServeHTTP(w, r)
	}
}

// serveGuarded runs serve (tusd) on a buffered response and passes it on
// unless it reports a complete upload the request did not finalize. The
// offset check above read the info before tusd took the upload lock, so
// a final PATCH may have staged the last bytes (and failed to finalize)
// in between; tusd then answers HEAD 200 / PATCH 204 with
// Upload-Offset == Upload-Length although the row is still 'uploading',
// and the client would take the upload as done. Finalize instead.
func (h *endpoint) serveGuarded(w http.ResponseWriter, r *http.Request, st *reqState, id string, size int64, serve http.HandlerFunc) {
	rec := newRecorder(w)
	serve(rec, r)
	if !st.completed && rec.reportsComplete(size) {
		h.finishStaged(w, r, st, id)
		return
	}
	rec.replay()
}

// serveCounted passes the request to tusd, counting the body bytes it
// reads (upload_bytes_received_total, upload_chunk_bytes).
func (h *endpoint) serveCounted(w http.ResponseWriter, r *http.Request, st *reqState) {
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &countingBody{ReadCloser: r.Body, n: &st.bytes}
	}
	h.tusd.ServeHTTP(w, r)
	if n := st.bytes.Load(); n > 0 {
		h.cfg.Metrics.BytesReceived.WithLabelValues(h.cfg.Protocol).Add(float64(n))
		h.cfg.Metrics.ChunkBytes.WithLabelValues(h.cfg.Protocol).Observe(float64(n))
	}
}

// finishStaged finalizes a fully staged upload whose row is still
// 'uploading' (under the upload lock) and answers from the outcome.
func (h *endpoint) finishStaged(w http.ResponseWriter, r *http.Request, st *reqState, id string) {
	lockCtx, cancel := context.WithTimeout(r.Context(), lockTimeout)
	defer cancel()
	lock, err := h.cfg.Staging.lock(lockCtx, id)
	if err != nil {
		st.fail(err)
		writeError(w, r, lockError(r.Context(), err))
		return
	}
	err = h.finalize(r.Context(), id)
	if uerr := lock.Unlock(); uerr != nil {
		h.cfg.Log.Warn("unlock upload", "protocol", h.cfg.Protocol, "upload_ref", st.ref, "error", uerr.Error())
	}
	if err != nil {
		st.fail(err)
		writeError(w, r, finalizeError(err))
		return
	}
	st.completed = true
	row, err := h.cfg.Repo.Get(r.Context(), h.cfg.Protocol, st.ref)
	if err != nil {
		st.fail(err)
		writeError(w, r, internalError(r.Context()))
		return
	}
	h.serveFinished(w, r, st, row)
}

// serveFinished answers for a row that is no longer 'uploading'. The
// staged data of a completed upload is already deleted, so tusd cannot
// answer; the responses mirror what tusd sends for a full upload.
func (h *endpoint) serveFinished(w http.ResponseWriter, r *http.Request, st *reqState, row db.Upload) {
	switch row.Status {
	case db.StatusExpired:
		writeError(w, r, errExpired)
		return
	case db.StatusFailedChecksum:
		writeError(w, r, errChecksum)
		return
	case db.StatusCompleted:
	default: // cancelled
		writeError(w, r, errNotFound)
		return
	}
	size := strconv.FormatInt(row.Size, 10)
	hdr := w.Header()
	hdr.Set("Tus-Resumable", "1.0.0")
	switch r.Method {
	case http.MethodHead:
		hdr.Set("Cache-Control", "no-store")
		hdr.Set("Upload-Offset", size)
		hdr.Set("Upload-Length", size)
		w.WriteHeader(http.StatusOK)
	case http.MethodPatch:
		off, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
		if err != nil || off != row.Size {
			writeError(w, r, handler.ErrMismatchOffset)
			return
		}
		hdr.Set("Upload-Offset", size)
		w.WriteHeader(http.StatusNoContent)
	default: // DELETE: the upload resource no longer exists
		writeError(w, r, errNotFound)
	}
}

// editHeaders adjusts tusd's response headers just before they are sent.
func (h *endpoint) editHeaders(method string, st *reqState, hdr http.Header, status int) {
	if method == http.MethodOptions {
		// tusd advertises creation-with-upload etc. and does not implement
		// expiration; Upload-Expires is added below.
		hdr.Set("Tus-Extension", Extensions)
		return
	}
	if status >= 200 && status < 300 && !st.expires.IsZero() && !st.completed {
		hdr.Set("Upload-Expires", st.expires.UTC().Format(http.TimeFormat))
	}
}

// writeError writes a tus error response from the middleware.
func writeError(w http.ResponseWriter, r *http.Request, e handler.Error) {
	hdr := w.Header()
	hdr.Set("Tus-Resumable", "1.0.0")
	for k, v := range e.HTTPResponse.Header {
		hdr.Set(k, v)
	}
	body := e.HTTPResponse.Body
	if r.Method == http.MethodHead {
		body = ""
	}
	if body != "" {
		hdr.Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(e.HTTPResponse.StatusCode)
	_, _ = io.WriteString(w, body) // client may be gone; nothing to do
}

// logRequest writes the §10 request log line (never the token).
func (h *endpoint) logRequest(r *http.Request, rw *responseWriter, st *reqState, start time.Time) {
	status := rw.status
	if status == 0 {
		status = http.StatusOK
	}
	n := st.bytes.Load()
	after := st.offsetBefore
	if v, err := strconv.ParseInt(rw.Header().Get("Upload-Offset"), 10, 64); err == nil {
		after = v
	} else if r.Method == http.MethodPatch && after >= 0 {
		after += n // tusd keeps the bytes of an interrupted PATCH
	}
	attrs := []slog.Attr{
		slog.String("protocol", h.cfg.Protocol),
		slog.String("upload_ref", st.ref),
		slog.String("device_id", st.device),
		slog.String("method", r.Method),
		slog.Int64("offset_before", st.offsetBefore),
		slog.Int64("offset_after", after),
		slog.Int64("bytes", n),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
		slog.Int("status", status),
	}
	level := slog.LevelInfo
	if status >= 400 {
		msg := strings.TrimSpace(string(rw.errBody))
		if st.err != nil {
			msg = st.err.Error()
		}
		attrs = append(attrs, slog.String("error", msg))
		if status >= 500 {
			level = slog.LevelError
		}
	}
	h.cfg.Log.LogAttrs(r.Context(), level, "tus request", attrs...)
}

// responseWriter records the status and the start of an error body, and
// lets the middleware edit headers that tusd sets right before writing.
// It implements Unwrap so http.ResponseController (tusd's read/write
// deadlines) reaches the underlying writer.
type responseWriter struct {
	http.ResponseWriter
	beforeHeader func(http.Header, int)
	head         bool // HEAD request: never a body
	status       int
	errBody      []byte
	generic      bool // 5xx: the body written is replaced
}

const maxErrBody = 256

func (w *responseWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	if w.beforeHeader != nil {
		w.beforeHeader(w.Header(), code)
	}
	// tusd writes err.Error() as the body of any error that is not a
	// handler.Error (store errors with staging paths, bucket names, ...).
	// Every 5xx gets a fixed body instead; the original goes to the log.
	var body string
	if code >= http.StatusInternalServerError {
		w.generic = true
		body = genericBody(code)
		hdr := w.Header()
		hdr.Set("Content-Type", "text/plain; charset=utf-8")
		hdr.Set("Content-Length", strconv.Itoa(len(body)))
		if w.head {
			hdr.Del("Content-Length")
			body = ""
		}
	}
	w.ResponseWriter.WriteHeader(code)
	if body != "" {
		_, _ = io.WriteString(w.ResponseWriter, body) // client may be gone; nothing to do
	}
}

// genericBody is the body sent for a 5xx status.
func genericBody(code int) string {
	if code == http.StatusServiceUnavailable {
		return errShuttingDown.HTTPResponse.Body
	}
	return errInternal.HTTPResponse.Body
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 && len(w.errBody) < maxErrBody {
		w.errBody = append(w.errBody, b[:min(len(b), maxErrBody-len(w.errBody))]...)
	}
	if w.generic {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap returns the underlying ResponseWriter.
func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// recorder buffers a response (headers, status, body) so the middleware
// can inspect it before anything is sent. tusd's HEAD and PATCH answers
// are small: headers plus at most a short error body. Unwrap reaches the
// destination, so tusd's read/write deadlines still apply.
type recorder struct {
	dst    http.ResponseWriter
	header http.Header
	status int
	body   []byte
}

func newRecorder(dst http.ResponseWriter) *recorder {
	return &recorder{dst: dst, header: http.Header{}}
}

func (rc *recorder) Header() http.Header { return rc.header }

func (rc *recorder) WriteHeader(code int) {
	if rc.status == 0 {
		rc.status = code
	}
}

func (rc *recorder) Write(b []byte) (int, error) {
	rc.WriteHeader(http.StatusOK)
	rc.body = append(rc.body, b...)
	return len(b), nil
}

// Unwrap returns the destination writer.
func (rc *recorder) Unwrap() http.ResponseWriter { return rc.dst }

// reportsComplete reports whether the response is a 2xx whose
// Upload-Offset equals size.
func (rc *recorder) reportsComplete(size int64) bool {
	if rc.status < 200 || rc.status >= 300 {
		return false
	}
	off, err := strconv.ParseInt(rc.header.Get("Upload-Offset"), 10, 64)
	return err == nil && off == size
}

// replay sends the buffered response to the destination.
func (rc *recorder) replay() {
	hdr := rc.dst.Header()
	for k, v := range rc.header {
		hdr[k] = v
	}
	if rc.status == 0 {
		rc.status = http.StatusOK
	}
	rc.dst.WriteHeader(rc.status)
	if len(rc.body) > 0 {
		_, _ = rc.dst.Write(rc.body) // client may be gone; nothing to do
	}
}

// countingBody counts bytes read from a request body. Errors pass through
// unchanged (tusd compares them with ==).
type countingBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n.Add(int64(n))
	return n, err
}
