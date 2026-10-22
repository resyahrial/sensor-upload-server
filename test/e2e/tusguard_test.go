//go:build integration

package e2e

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// TestHeadRacingFailedFinalPatch reproduces the race behind the HEAD
// guard: a HEAD reads the staged offset while the final PATCH still
// misses its last byte, then waits for tusd's upload lock. The PATCH
// stages the last byte, finalization fails (object store down) and the
// lock passes to the HEAD, for which tusd itself would report
// Upload-Offset == Upload-Length (the contract client's "done") although
// the row is still 'uploading'. The HEAD must not report completion
// until the row is completed; a later HEAD completes it.
func TestHeadRacingFailedFinalPatch(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, chunk)
			size := len(f.Data)
			// The PATCH's lock holder polls for release requests every
			// second; if that poll lands between the HEAD's request and
			// the last byte, the PATCH is interrupted instead (400) and
			// the race did not happen. Retry with a fresh upload then.
			for attempt := 1; ; attempt++ {
				loc := c.MustCreate(f)
				s.Store.FailPuts(2) // the final PATCH and the HEAD's finalize
				p := c.StartPatch(loc, 0, f.Data, size-1)
				WaitStaged(t, s, loc, size-1)

				head := make(chan *http.Response, 1)
				go func() {
					res, err := c.Do(context.Background(), http.MethodHead, loc, http.NoBody, nil)
					if err != nil {
						t.Errorf("HEAD: %v", err)
						head <- nil
						return
					}
					res.Body.Close()
					head <- res
				}()
				waitFile(t, filepath.Join(s.TusDir(), refOf(loc)+".stop")) // HEAD is waiting for the lock
				go func() {
					_, _ = p.pw.Write(f.Data[size-1:])
					_ = p.pw.Close()
				}()
				pr := p.wait(t)
				hr := <-head
				if hr == nil {
					t.FailNow()
				}
				if pr.status == http.StatusBadRequest && attempt < 3 {
					t.Logf("attempt %d: PATCH interrupted before its last byte; retrying", attempt)
					s.Store.FailPuts(0)
					continue
				}
				if pr.err != nil || pr.status != http.StatusInternalServerError {
					t.Fatalf("final PATCH: %d %q %v, want 500", pr.status, pr.body, pr.err)
				}
				if hr.StatusCode == http.StatusOK && hr.Header.Get("Upload-Offset") == strconv.Itoa(size) {
					t.Fatalf("HEAD reported completion (offset %s) while finalization fails", hr.Header.Get("Upload-Offset"))
				}
				if hr.StatusCode != http.StatusInternalServerError {
					t.Errorf("HEAD while finalization fails: %d, want 500", hr.StatusCode)
				}
				if u := Row(t, s, ep.protocol, loc); u.Status != db.StatusUploading {
					t.Fatalf("status after failures = %s", u.Status)
				}

				// The fault has cleared: the next HEAD finalizes.
				if off := c.Head(loc); off != size {
					t.Fatalf("HEAD after recovery = %d, want %d", off, size)
				}
				AssertStored(t, s, ep.protocol, loc, f)
				return
			}
		})
	}
}

// TestStoreErrorHidesPaths: a staging store error reaches tusd's generic
// error path, which would put err.Error() (with the staging path) in the
// body. The client gets a fixed body instead.
func TestStoreErrorHidesPaths(t *testing.T) {
	s := StartServer(t, nil)
	if err := os.Chmod(s.TusDir(), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(s.TusDir(), 0o750) })
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			res, body := c.Create(NewFile(t, dev, 1000), "")
			if res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("create on unwritable staging: %d %q", res.StatusCode, body)
			}
			if body != "internal error\n" || strings.Contains(body, "/") || strings.Contains(body, "staging") {
				t.Errorf("body %q, want the generic internal error", body)
			}
		})
	}
}

// waitFile waits until path exists.
func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", path)
}
