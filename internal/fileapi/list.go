package fileapi

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// PathList is the route of the completed upload listing.
const PathList = "/files"

// Listing limits.
const (
	DefaultListLimit = 50
	MaxListLimit     = 200
	maxContentType   = 255
)

// Lister lists completed uploads (*db.Repo implements it).
type Lister interface {
	ListCompletedPage(ctx context.Context, f db.ListFilter) ([]db.Upload, error)
}

// ListConfig configures the listing handler.
type ListConfig struct {
	Repo    Lister
	Metrics *obs.Metrics // nil = no metrics
	Log     *slog.Logger // nil = discard
}

// ListResponse is the JSON body of a 200 listing.
type ListResponse struct {
	Files []File `json:"files"`
	// NextCursor, when set, is passed as ?cursor= to get the next page.
	NextCursor string `json:"next_cursor,omitempty"`
}

// File is one completed upload in a listing. It never includes the
// storage key or upload ref.
type File struct {
	DeviceID    string `json:"device_id"`
	SHA256      string `json:"sha256"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	CapturedAt  string `json:"captured_at"`
	CompletedAt string `json:"completed_at"`
}

// NewListHandler returns the public, unauthenticated handler for
//
//	GET /files?device_id=&content_type=&limit=&cursor=
//
// which lists completed uploads newest first. device_id is normalized like
// everywhere else; content_type is an exact type or "type/*" for a prefix
// (e.g. image/*); limit is 1..MaxListLimit (default DefaultListLimit).
func NewListHandler(cfg ListConfig) http.Handler {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	return &listHandler{cfg: cfg}
}

type listHandler struct{ cfg ListConfig }

func (h *listHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	code, result, n, err := h.serve(w, r)
	if h.cfg.Metrics != nil {
		h.cfg.Metrics.FileLists.WithLabelValues(result).Inc()
	}
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("proto", r.Proto),
		slog.String("result", result),
		slog.Int("status", code),
		slog.Int("files", n),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	}
	level := slog.LevelInfo
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		level = slog.LevelError
	}
	h.cfg.Log.LogAttrs(r.Context(), level, "file list", attrs...)
}

func (h *listHandler) serve(w http.ResponseWriter, r *http.Request) (int, string, int, error) {
	f, msg := parseListQuery(r.URL.Query())
	if msg != "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: msg})
		return http.StatusBadRequest, obs.ListInvalid, 0, nil
	}
	limit := f.Limit
	f.Limit++ // one extra row tells whether there is a next page
	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	us, err := h.cfg.Repo.ListCompletedPage(ctx, f)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error"})
		return http.StatusInternalServerError, obs.ListError, 0, err
	}
	res := ListResponse{Files: make([]File, 0, min(len(us), limit))}
	if len(us) > limit {
		us = us[:limit]
		last := us[limit-1]
		res.NextCursor = encodeCursor(db.ListCursor{CompletedAt: *last.CompletedAt, ID: last.ID})
	}
	for _, u := range us {
		res.Files = append(res.Files, File{
			DeviceID:    u.DeviceID,
			SHA256:      u.SHA256,
			Filename:    u.Filename,
			ContentType: u.ContentType,
			Size:        u.Size,
			CapturedAt:  u.CapturedAt.UTC().Format(time.RFC3339),
			CompletedAt: u.CompletedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, res)
	return http.StatusOK, obs.ListOK, len(res.Files), nil
}

// parseListQuery returns the filter of a listing request, or a client
// error message.
func parseListQuery(q url.Values) (db.ListFilter, string) {
	f := db.ListFilter{Limit: DefaultListLimit}
	if v := q.Get("device_id"); v != "" {
		if f.DeviceID = meta.NormalizeDeviceID(v); !meta.ValidDeviceID(f.DeviceID) {
			return f, "device_id must be " + meta.DeviceIDRule
		}
	}
	if v := q.Get("content_type"); v != "" {
		if len(v) > maxContentType || strings.ContainsFunc(v, func(r rune) bool { return r <= ' ' || r > '~' }) {
			return f, "content_type must be a media type such as image/jpeg or image/*"
		}
		if prefix, ok := strings.CutSuffix(v, "*"); ok {
			f.ContentTypePrefix = prefix
		} else {
			f.ContentType = v
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > MaxListLimit {
			return f, "limit must be an integer from 1 to " + strconv.Itoa(MaxListLimit)
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			return f, "cursor is invalid"
		}
		f.After = &c
	}
	return f, ""
}

// A cursor is base64url("<completed_at unix microseconds>.<uuid>"):
// opaque to clients, exact at the database's microsecond precision.
func encodeCursor(c db.ListCursor) string {
	raw := strconv.FormatInt(c.CompletedAt.UnixMicro(), 10) + "." + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (db.ListCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return db.ListCursor{}, err
	}
	at, id, ok := strings.Cut(string(raw), ".")
	if !ok {
		return db.ListCursor{}, errors.New("cursor: missing separator")
	}
	us, err := strconv.ParseInt(at, 10, 64)
	if err != nil {
		return db.ListCursor{}, err
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return db.ListCursor{}, err
	}
	return db.ListCursor{CompletedAt: time.UnixMicro(us), ID: uid}, nil
}
