package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// DefaultOrphanAge is Config.OrphanAge when unset.
const DefaultOrphanAge = time.Hour

// Orphan sweep: a crash between recording an upload's outcome and deleting
// its staged data (after Complete, after SetStatus(failed_checksum),
// between tus pre-terminate and tusd's terminate, or mid-SaveInfo) leaves
// staged files that no 'uploading' row points to. Each pass deletes the
// staged files of uploads whose row is terminal or missing, once every
// file of the upload is older than OrphanAge (so an upload being created
// right now, whose row may not be visible yet, is never touched).

// orphanArea is one staging directory and how its file names map to refs.
type orphanArea struct {
	label     string   // metric label and log value: "grpc" or "tus"
	dir       string   // "" disables the area
	protocols []string // DB protocols a ref may belong to
	// suffix reports whether the part of a file name after "{ref}" belongs
	// to the staged upload.
	suffix func(s string) bool
}

// grpcSuffix matches staging.Store's {id}.bin, {id}.json and SaveInfo's
// temp files {id}.json.tmp-*.
func grpcSuffix(s string) bool {
	return s == ".bin" || s == ".json" || strings.HasPrefix(s, ".json.tmp-")
}

// tusSuffix matches filestore's {id} and {id}.info and filelocker's
// {id}.lock and {id}.stop.
func tusSuffix(s string) bool {
	return s == "" || s == ".info" || s == ".lock" || s == ".stop"
}

// stagedGroup is the files of one ref in an area.
type stagedGroup struct {
	ref    string
	names  []string
	newest time.Time
}

func (c *Cleaner) orphanAreas() []orphanArea {
	var tus []string
	for p := range c.staging {
		if p != GRPCProtocol {
			tus = append(tus, p)
		}
	}
	slices.Sort(tus)
	areas := []orphanArea{{label: "grpc", dir: c.grpcDir, protocols: []string{GRPCProtocol}, suffix: grpcSuffix}}
	if len(tus) > 0 {
		areas = append(areas, orphanArea{label: "tus", dir: c.cfg.TusDir, protocols: tus, suffix: tusSuffix})
	}
	return areas
}

// sweepOrphans deletes orphaned staged files in every area.
func (c *Cleaner) sweepOrphans(ctx context.Context, p *pass) {
	cutoff := time.Now().Add(-c.cfg.OrphanAge)
	for _, a := range c.orphanAreas() {
		if a.dir == "" {
			continue
		}
		groups, err := scanStaging(a, cutoff)
		if err != nil {
			p.fail(fmt.Errorf("cleanup: orphan sweep %s: %w", a.label, err))
			continue
		}
		for _, g := range groups {
			if err := ctx.Err(); err != nil {
				p.fail(err)
				return
			}
			if err := c.sweepOne(ctx, a, g); err != nil {
				p.fail(fmt.Errorf("cleanup: orphan sweep %s/%s: %w", a.label, g.ref, err))
			}
		}
	}
}

// scanStaging groups the files of a.dir by ref and returns the groups
// whose newest file is older than cutoff, sorted by ref. Names that are
// not "{canonical uuid}{suffix}" are ignored. A missing dir is empty.
func scanStaging(a orphanArea, cutoff time.Time) ([]stagedGroup, error) {
	entries, err := os.ReadDir(a.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	byRef := map[string]*stagedGroup{}
	for _, e := range entries {
		name := e.Name()
		ref, _, _ := strings.Cut(name, ".") // UUIDs contain no dot
		if !e.Type().IsRegular() || !a.suffix(name[len(ref):]) || !validRef(ref) {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted meanwhile
		}
		if err != nil {
			return nil, err
		}
		g := byRef[ref]
		if g == nil {
			g = &stagedGroup{ref: ref}
			byRef[ref] = g
		}
		g.names = append(g.names, name)
		if mt := info.ModTime(); mt.After(g.newest) {
			g.newest = mt
		}
	}
	var out []stagedGroup
	for _, g := range byRef {
		if g.newest.Before(cutoff) {
			out = append(out, *g)
		}
	}
	slices.SortFunc(out, func(x, y stagedGroup) int { return strings.Compare(x.ref, y.ref) })
	return out, nil
}

// sweepOne deletes the staged files of g if its upload is terminal or has
// no row, while holding the upload's lock. In-use uploads are skipped.
func (c *Cleaner) sweepOne(ctx context.Context, a orphanArea, g stagedGroup) error {
	row, found, err := c.lookup(ctx, a.protocols, g.ref)
	if err != nil {
		return err
	}
	if found && row.Status == db.StatusUploading {
		return nil // live (or stale: expire's business)
	}
	protocol, state := a.protocols[0], "missing"
	if found {
		protocol, state = row.Protocol, string(row.Status)
	}
	st := c.staging[protocol]
	unlock, ok, err := st.TryLock(ctx, g.ref)
	if err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	if !ok {
		return nil
	}
	defer unlock()
	if err := st.Delete(ctx, g.ref); err != nil {
		return fmt.Errorf("delete staging: %w", err)
	}
	// Delete leaves what it does not know about (temp and lock files, data
	// without info); remove the rest by name.
	var errs []error
	for _, name := range g.names {
		if err := os.Remove(filepath.Join(a.dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("remove staged files: %w", err)
	}
	c.metrics.StagingOrphans.WithLabelValues(a.label).Inc()
	c.log.Info("orphaned staging deleted", "staging", a.label, "protocol", protocol, "upload_ref", g.ref,
		"status", state, "files", strings.Join(g.names, ","), "modified_at", g.newest)
	return nil
}

// lookup returns ref's row from the first of protocols that has it.
func (c *Cleaner) lookup(ctx context.Context, protocols []string, ref string) (db.Upload, bool, error) {
	for _, p := range protocols {
		u, err := c.repo.Get(ctx, p, ref)
		if err == nil {
			return u, true, nil
		}
		if !errors.Is(err, db.ErrNotFound) {
			return db.Upload{}, false, err
		}
	}
	return db.Upload{}, false, nil
}

// validRef reports whether ref is a canonical UUID (every upload_ref is a
// UUIDv7), which also keeps other files out of the sweep.
func validRef(ref string) bool {
	u, err := uuid.Parse(ref)
	return err == nil && u.String() == ref
}
