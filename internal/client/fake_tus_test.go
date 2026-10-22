package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// fakeTus is a minimal in-memory tus 1.0 server (core + creation) with
// failure injection, mounted at base.
type fakeTus struct {
	base string

	mu        sync.Mutex
	nextID    int
	uploads   map[string]*fakeUpload
	completed map[string]bool // sha256 → stored
	calls     []string        // "METHOD status" in order
	patches   int

	// onPatch runs before a PATCH is applied (n = 1-based PATCH count).
	// Returning true means the hook wrote the response itself.
	onPatch func(n int, u *fakeUpload, w http.ResponseWriter, r *http.Request) bool
	// onCreate can reply instead of the default handler.
	onCreate func(w http.ResponseWriter, r *http.Request) bool
	// corruptOnce makes the next completion fail the checksum (460).
	corruptOnce bool
}

type fakeUpload struct {
	id   string
	size int64
	meta map[string]string
	data []byte
}

func newFakeTus(base string) *fakeTus {
	return &fakeTus{base: base, uploads: map[string]*fakeUpload{}, completed: map[string]bool{}}
}

func (f *fakeTus) record(r *http.Request, code int) {
	f.calls = append(f.calls, fmt.Sprintf("%s %d", r.Method, code))
}

func (f *fakeTus) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func parseMeta(h string) map[string]string {
	m := map[string]string{}
	for _, kv := range strings.Split(h, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(kv), " ")
		b, _ := base64.StdEncoding.DecodeString(v)
		m[k] = string(b)
	}
	return m
}

func (f *fakeTus) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Tus-Resumable") != "1.0.0" || r.Header.Get("Authorization") != "Bearer tok" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == f.base:
		f.create(w, r)
	case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, f.base):
		f.head(w, r)
	case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, f.base):
		f.patch(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeTus) create(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onCreate != nil && f.onCreate(w, r) {
		f.record(r, -1)
		return
	}
	size, err := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
	meta := parseMeta(r.Header.Get("Upload-Metadata"))
	if err != nil || len(meta["sha256"]) != 64 || meta["device_id"] != "dev-1" {
		f.record(r, 400)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if f.completed[meta["sha256"]] {
		f.record(r, 461)
		w.WriteHeader(461)
		return
	}
	f.nextID++
	id := strconv.Itoa(f.nextID)
	f.uploads[id] = &fakeUpload{id: id, size: size, meta: meta}
	w.Header().Set("Location", f.base+id) // relative, like the contract example
	f.record(r, 201)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeTus) lookup(r *http.Request) *fakeUpload {
	return f.uploads[strings.TrimPrefix(r.URL.Path, f.base)]
}

func (f *fakeTus) head(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.lookup(r)
	if u == nil {
		f.record(r, 404)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Upload-Offset", strconv.Itoa(len(u.data)))
	w.Header().Set("Upload-Length", strconv.FormatInt(u.size, 10))
	f.record(r, 200)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeTus) patch(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.lookup(r)
	if u == nil {
		f.record(r, 404)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	f.patches++
	if f.onPatch != nil && f.onPatch(f.patches, u, w, r) {
		f.record(r, -1)
		return
	}
	off, _ := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if off != int64(len(u.data)) || r.Header.Get("Content-Type") != "application/offset+octet-stream" {
		f.record(r, 409)
		w.WriteHeader(http.StatusConflict)
		return
	}
	b, err := io.ReadAll(r.Body)
	u.data = append(u.data, b...)
	if err != nil {
		f.record(r, 500)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if int64(len(u.data)) == u.size {
		sum := sha256.Sum256(u.data)
		if f.corruptOnce || hex.EncodeToString(sum[:]) != u.meta["sha256"] {
			f.corruptOnce = false
			delete(f.uploads, u.id)
			f.record(r, 460)
			w.WriteHeader(460)
			return
		}
		f.completed[u.meta["sha256"]] = true
	}
	w.Header().Set("Upload-Offset", strconv.Itoa(len(u.data)))
	f.record(r, 204)
	w.WriteHeader(http.StatusNoContent)
}

// stored returns the bytes of the completed upload with sha, if any.
func (f *fakeTus) stored(sha string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.uploads {
		if u.meta["sha256"] == sha && int64(len(u.data)) == u.size && f.completed[sha] {
			return bytes.Clone(u.data), true
		}
	}
	return nil, false
}

// abortMidBody stores half of the chunk, then resets the connection/stream.
func abortMidBody(u *fakeUpload, r *http.Request) {
	half := r.ContentLength / 2
	b, _ := io.ReadAll(io.LimitReader(r.Body, half))
	u.data = append(u.data, b...)
	panic(http.ErrAbortHandler)
}
