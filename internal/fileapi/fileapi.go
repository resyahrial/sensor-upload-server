// Package fileapi serves the public, unauthenticated "is this file saved?"
// lookup: GET /files/sha256/{sha256}. It discloses only whether a completed
// upload with that sha256 exists (plus its size and completion time), never
// the device, object key, metadata or in-progress uploads.
package fileapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// Prefix is the route prefix the handler is mounted on.
const Prefix = "/files/sha256/"

// Response statuses.
const (
	StatusCompleted = "completed"
	StatusNotFound  = "not_found"
)

// Finder looks up a completed upload by sha256 (*db.Repo implements it).
type Finder interface {
	FindCompletedBySHA(ctx context.Context, sha string) (db.Upload, error)
}

// Config configures the handler.
type Config struct {
	Repo    Finder
	Metrics *obs.Metrics // nil = no metrics
	Log     *slog.Logger // nil = discard
}

// Response is the JSON body of a 200 or 404 answer.
type Response struct {
	SHA256      string `json:"sha256"`
	Status      string `json:"status"`
	Size        int64  `json:"size,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
}

type errorBody struct {
	Error string `json:"error"`
}

// lookupTimeout bounds the database query of one lookup.
const lookupTimeout = 5 * time.Second

// NewHandler returns the handler for GET (and HEAD) /files/sha256/{sha256}.
// Other methods get 405, other paths under Prefix 404.
func NewHandler(cfg Config) http.Handler {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	mux.Handle("GET "+Prefix+"{sha256}", &handler{cfg: cfg})
	return mux
}

type handler struct{ cfg Config }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sha := r.PathValue("sha256")
	code, result, err := h.serve(w, r, sha)
	if h.cfg.Metrics != nil {
		h.cfg.Metrics.FileLookups.WithLabelValues(result).Inc()
	}
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("proto", r.Proto),
		slog.String("result", result),
		slog.Int("status", code),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	}
	if result == obs.LookupInvalid {
		attrs = append(attrs, slog.Int("sha256_len", len(sha))) // never log arbitrary input
	} else {
		attrs = append(attrs, slog.String("sha256", sha))
	}
	level := slog.LevelInfo
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		level = slog.LevelError
	}
	h.cfg.Log.LogAttrs(r.Context(), level, "file lookup", attrs...)
}

func (h *handler) serve(w http.ResponseWriter, r *http.Request, sha string) (int, string, error) {
	if !ValidSHA(sha) {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "sha256 must be 64 lowercase hex characters"})
		return http.StatusBadRequest, obs.LookupInvalid, nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	u, err := h.cfg.Repo.FindCompletedBySHA(ctx, sha)
	switch {
	case errors.Is(err, db.ErrNotFound):
		writeJSON(w, http.StatusNotFound, Response{SHA256: sha, Status: StatusNotFound})
		return http.StatusNotFound, obs.LookupNotFound, nil
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error"})
		return http.StatusInternalServerError, obs.LookupError, err
	}
	res := Response{SHA256: sha, Status: StatusCompleted, Size: u.Size}
	if u.CompletedAt != nil {
		res.CompletedAt = u.CompletedAt.UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, res)
	return http.StatusOK, obs.LookupFound, nil
}

// ValidSHA reports whether s is 64 lowercase hex characters.
func ValidSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v) // client may have gone away; nothing to do
}
