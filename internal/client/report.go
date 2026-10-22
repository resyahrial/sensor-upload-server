package client

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

// ReportHeader is the CSV header from requirements §11.
var ReportHeader = []string{
	"protocol", "file", "size", "chunk", "duration_ms", "attempts",
	"resumes", "bytes_sent_total", "bytes_resent", "result",
}

// WriteReport writes one CSV row per result, with a header.
func WriteReport(w io.Writer, results []Result) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(ReportHeader); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	for _, r := range results {
		rec := []string{
			r.Protocol,
			r.File,
			strconv.FormatInt(r.Size, 10),
			strconv.Itoa(r.Chunk),
			strconv.FormatInt(r.Duration.Milliseconds(), 10),
			strconv.Itoa(r.Attempts),
			strconv.Itoa(r.Resumes),
			strconv.FormatInt(r.BytesSent, 10),
			strconv.FormatInt(r.BytesResent, 10),
			r.Result,
		}
		if err := cw.Write(rec); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
