// Command summarize merges the per-run CSVs of the protocol experiment
// (deploy/netem/experiment.sh) into one summary row per
// (protocol, profile, size, chunk): run count, success rate, and the median
// and p95 of duration, attempts, resumes, re-sent bytes and the overhead
// ratio bytes_sent_total/size.
//
//	summarize -runs results/runs -out results/summary.csv
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	err := run(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "summarize:", err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("summarize", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runsDir := fs.String("runs", "results/runs", "directory with per-run CSVs (searched recursively)")
	out := fs.String("out", "results/summary.csv", "summary CSV to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	runs, files, err := readRuns(*runsDir)
	if err != nil {
		return err
	}
	cells := summarize(runs)

	f, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("create summary: %w", err)
	}
	if err := writeSummary(f, cells); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close summary: %w", err)
	}
	fmt.Fprintf(stderr, "summarize: %d runs from %d files → %d cells in %s\n", len(runs), files, len(cells), *out)
	return nil
}
