package main

import (
	"cmp"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

// runRecord is one row of a per-run CSV.
type runRecord struct {
	key         cellKey
	durationMS  float64
	attempts    float64
	resumes     float64
	bytesSent   float64
	bytesResent float64
	result      string
}

// cellKey identifies one cell of the experiment matrix.
type cellKey struct {
	protocol string
	profile  string
	size     int64
	chunk    int64
}

// stat is the median and p95 of one metric over a cell's successful runs.
type stat struct{ median, p95 float64 }

// cell is one summary row.
type cell struct {
	key                  cellKey
	n, ok                int
	duration, attempts   stat
	resumes, bytesResent stat
	overhead             stat
}

// successResults are the uploader results that mean the file is on the
// server ("fallback": delivered over /rest after HTTP/3 failed).
var successResults = map[string]bool{"ok": true, "duplicate": true, "fallback": true}

// requiredColumns are the per-run CSV columns summarize reads: "profile" is
// added by the driver, the rest are the uploader's §11 report columns.
var requiredColumns = []string{
	"protocol", "profile", "size", "chunk", "duration_ms", "attempts",
	"resumes", "bytes_sent_total", "bytes_resent", "result",
}

var summaryHeader = []string{
	"protocol", "profile", "size", "chunk", "n", "n_success", "success_rate",
	"duration_ms_median", "duration_ms_p95", "attempts_median", "attempts_p95",
	"resumes_median", "resumes_p95", "bytes_resent_median", "bytes_resent_p95",
	"overhead_median", "overhead_p95",
}

// readRuns parses every *.csv file under dir and returns its rows and the
// number of files read.
func readRuns(dir string) ([]runRecord, int, error) {
	var runs []runRecord
	files := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".csv" {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		recs, err := parseRuns(f)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		runs = append(runs, recs...)
		files++
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("read runs: %w", err)
	}
	return runs, files, nil
}

// parseRuns reads one per-run CSV; columns are located by header name.
func parseRuns(r io.Reader) ([]runRecord, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	col := make(map[string]int, len(header))
	for i, h := range header {
		col[h] = i
	}
	for _, c := range requiredColumns {
		if _, ok := col[c]; !ok {
			return nil, fmt.Errorf("missing column %q", c)
		}
	}

	var recs []runRecord
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return recs, nil
		}
		if err != nil {
			return nil, err
		}
		line, _ := cr.FieldPos(0)
		var nums [7]float64
		for i, c := range []string{"size", "chunk", "duration_ms", "attempts", "resumes", "bytes_sent_total", "bytes_resent"} {
			if nums[i], err = strconv.ParseFloat(row[col[c]], 64); err != nil {
				return nil, fmt.Errorf("line %d: column %s: %w", line, c, err)
			}
		}
		recs = append(recs, runRecord{
			key: cellKey{
				protocol: row[col["protocol"]],
				profile:  row[col["profile"]],
				size:     int64(nums[0]),
				chunk:    int64(nums[1]),
			},
			durationMS:  nums[2],
			attempts:    nums[3],
			resumes:     nums[4],
			bytesSent:   nums[5],
			bytesResent: nums[6],
			result:      row[col["result"]],
		})
	}
}

// summarize groups runs by cell. Metric statistics are taken over the
// successful runs only (a timeout's duration is just the timeout); n and the
// success rate count every run. Cells are sorted by protocol, profile, size,
// chunk.
func summarize(runs []runRecord) []cell {
	groups := make(map[cellKey][]runRecord)
	for _, r := range runs {
		groups[r.key] = append(groups[r.key], r)
	}
	cells := make([]cell, 0, len(groups))
	for k, rs := range groups {
		c := cell{key: k, n: len(rs)}
		var dur, att, res, resent, over []float64
		for _, r := range rs {
			if !successResults[r.result] {
				continue
			}
			c.ok++
			dur = append(dur, r.durationMS)
			att = append(att, r.attempts)
			res = append(res, r.resumes)
			resent = append(resent, r.bytesResent)
			if k.size > 0 {
				over = append(over, r.bytesSent/float64(k.size))
			}
		}
		c.duration, c.attempts, c.resumes = statOf(dur), statOf(att), statOf(res)
		c.bytesResent, c.overhead = statOf(resent), statOf(over)
		cells = append(cells, c)
	}
	slices.SortFunc(cells, func(a, b cell) int {
		return cmp.Or(
			cmp.Compare(a.key.protocol, b.key.protocol),
			cmp.Compare(a.key.profile, b.key.profile),
			cmp.Compare(a.key.size, b.key.size),
			cmp.Compare(a.key.chunk, b.key.chunk),
		)
	})
	return cells
}

// statOf returns the median (mean of the two middle values for an even
// count) and the nearest-rank p95. Empty input yields NaN for both.
func statOf(v []float64) stat {
	if len(v) == 0 {
		return stat{math.NaN(), math.NaN()}
	}
	s := slices.Clone(v)
	slices.Sort(s)
	n := len(s)
	median := s[n/2]
	if n%2 == 0 {
		median = (s[n/2-1] + s[n/2]) / 2
	}
	rank := int(math.Ceil(0.95 * float64(n)))
	return stat{median: median, p95: s[rank-1]}
}

// formatNum prints v with at most 4 decimals; NaN (no successful runs) is
// an empty field.
func formatNum(v float64) string {
	if math.IsNaN(v) {
		return ""
	}
	return strconv.FormatFloat(math.Round(v*1e4)/1e4, 'f', -1, 64)
}

// writeSummary writes the summary CSV with a header.
func writeSummary(w io.Writer, cells []cell) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(summaryHeader); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	for _, c := range cells {
		row := []string{
			c.key.protocol, c.key.profile,
			strconv.FormatInt(c.key.size, 10), strconv.FormatInt(c.key.chunk, 10),
			strconv.Itoa(c.n), strconv.Itoa(c.ok),
			formatNum(float64(c.ok) / float64(c.n)),
		}
		for _, s := range []stat{c.duration, c.attempts, c.resumes, c.bytesResent, c.overhead} {
			row = append(row, formatNum(s.median), formatNum(s.p95))
		}
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write summary: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}
