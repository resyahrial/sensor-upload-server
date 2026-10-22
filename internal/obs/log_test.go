package obs

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewLoggerJSONAndLevel(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, slog.LevelInfo)
	l.Debug("hidden")
	l.Info("shown", "protocol", "rest")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not a single JSON line: %q: %v", buf.String(), err)
	}
	if rec["msg"] != "shown" || rec["protocol"] != "rest" {
		t.Errorf("unexpected record %v", rec)
	}
}
