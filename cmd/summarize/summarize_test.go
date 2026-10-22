package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const runHeader = "run_id,profile,rep,exit_code,protocol,file,size,chunk,duration_ms,attempts,resumes,bytes_sent_total,bytes_resent,result\n"

// writeRun writes one per-run CSV like experiment.sh does.
func writeRun(t *testing.T, dir, name, profile, proto string, size, chunk, dur, attempts, resumes, sent, resent int64, result string) {
	t.Helper()
	row := fmt.Sprintf("%s,%s,1,0,%s,/results/work/%s.flac,%d,%d,%d,%d,%d,%d,%d,%s\n",
		name, profile, proto, name, size, chunk, dur, attempts, resumes, sent, resent, result)
	if err := os.WriteFile(filepath.Join(dir, name+".csv"), []byte(runHeader+row), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readSummary(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]string
	for _, r := range recs[1:] {
		m := map[string]string{}
		for i, h := range recs[0] {
			m[h] = r[i]
		}
		out = append(out, m)
	}
	return out
}

func TestRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	runs := filepath.Join(dir, "runs")
	if err := os.MkdirAll(filepath.Join(runs, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Cell A: grpc/very-poor/2 MiB/64 KiB: 4 ok + 1 timeout.
	for i, d := range []int64{400, 100, 300, 200} {
		writeRun(t, runs, fmt.Sprintf("a%d", i), "very-poor", "grpc", 2097152, 65536, d, int64(i+1), int64(i), 2097152+int64(i)*65536, int64(i)*65536, "ok")
	}
	writeRun(t, runs, "a-timeout", "very-poor", "grpc", 2097152, 65536, 7200000, 99, 98, 1, 1, "timeout")
	// Cell B: rest/good-4g/50 KiB/64 KiB: one fallback (success), one failed; in a subdirectory.
	writeRun(t, filepath.Join(runs, "nested"), "b1", "good-4g", "rest", 51200, 65536, 1000, 1, 0, 51200, 0, "fallback")
	writeRun(t, runs, "b2", "good-4g", "rest", 51200, 65536, 5, 51, 0, 0, 0, "failed")
	// Cell C: only failures → empty stats.
	writeRun(t, runs, "c1", "leo-sat", "rest-http3", 51200, 262144, 5, 3, 0, 0, 0, "interrupted")
	// Non-CSV files are ignored.
	if err := os.WriteFile(filepath.Join(runs, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "summary.csv")
	var stderr bytes.Buffer
	if err := run([]string{"-runs", runs, "-out", out}, &stderr); err != nil {
		t.Fatalf("run: %v (%s)", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), "8 runs from 8 files → 3 cells") {
		t.Errorf("stderr = %q", stderr.String())
	}

	got := readSummary(t, out)
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3: %v", len(got), got)
	}
	// Sorted by protocol: grpc, rest, rest-http3.
	a, b, c := got[0], got[1], got[2]
	wantA := map[string]string{
		"protocol": "grpc", "profile": "very-poor", "size": "2097152", "chunk": "65536",
		"n": "5", "n_success": "4", "success_rate": "0.8",
		"duration_ms_median": "250", "duration_ms_p95": "400",
		"attempts_median": "2.5", "attempts_p95": "4",
		"resumes_median": "1.5", "resumes_p95": "3",
		"bytes_resent_median": "98304", "bytes_resent_p95": "196608",
		"overhead_median": "1.0469", "overhead_p95": "1.0938",
	}
	for k, v := range wantA {
		if a[k] != v {
			t.Errorf("cell A %s = %q, want %q", k, a[k], v)
		}
	}
	if b["protocol"] != "rest" || b["n"] != "2" || b["success_rate"] != "0.5" || b["duration_ms_median"] != "1000" || b["overhead_p95"] != "1" {
		t.Errorf("cell B = %v", b)
	}
	if c["protocol"] != "rest-http3" || c["success_rate"] != "0" || c["duration_ms_median"] != "" || c["overhead_p95"] != "" {
		t.Errorf("cell C = %v", c)
	}
}

func TestStatOf(t *testing.T) {
	tests := []struct {
		in          []float64
		median, p95 float64
	}{
		{[]float64{7}, 7, 7},
		{[]float64{3, 1, 2}, 2, 3},
		{[]float64{4, 1, 3, 2}, 2.5, 4},
		// 20 values: nearest-rank p95 = 19th value.
		{[]float64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, 10.5, 19},
	}
	for _, tt := range tests {
		got := statOf(tt.in)
		if got.median != tt.median || got.p95 != tt.p95 {
			t.Errorf("statOf(%v) = %+v, want median %v p95 %v", tt.in, got, tt.median, tt.p95)
		}
	}
	if s := statOf(nil); !math.IsNaN(s.median) || !math.IsNaN(s.p95) {
		t.Errorf("statOf(nil) = %+v, want NaN", s)
	}
}

func TestParseRunsErrors(t *testing.T) {
	tests := map[string]string{
		"missing column": "protocol,size\nrest,1\n",
		"bad number":     runHeader + "x,good-4g,1,0,rest,f,abc,1,1,1,1,1,1,ok\n",
		"short row":      runHeader + "x,good-4g\n",
	}
	for name, in := range tests {
		if _, err := parseRuns(strings.NewReader(in)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	recs, err := parseRuns(strings.NewReader(""))
	if err != nil || recs != nil {
		t.Errorf("empty file: %v, %v", recs, err)
	}
}

func TestRunErrors(t *testing.T) {
	dir := t.TempDir()
	var stderr bytes.Buffer
	if err := run([]string{"-h"}, &stderr); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: %v", err)
	}
	if err := run([]string{"extra"}, &stderr); err == nil {
		t.Error("extra arg: want error")
	}
	if err := run([]string{"-runs", filepath.Join(dir, "nope"), "-out", filepath.Join(dir, "s.csv")}, &stderr); err == nil {
		t.Error("missing runs dir: want error")
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.csv"), []byte("a,b\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-runs", dir, "-out", filepath.Join(dir, "s.out")}, &stderr); err == nil || !strings.Contains(err.Error(), "bad.csv") {
		t.Errorf("bad csv: %v", err)
	}
	empty := t.TempDir()
	if err := run([]string{"-runs", empty, "-out", filepath.Join(dir, "no", "such", "s.csv")}, &stderr); err == nil {
		t.Error("unwritable out: want error")
	}
}
