package meta

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

var (
	now   = time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	rules = Rules{MaxFileBytes: 1000, ContentTypes: []string{"audio/flac", "image/jpeg"}}
	sha   = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

func valid() Metadata {
	return Metadata{
		DeviceID:    "bioac-0142",
		SHA256:      sha,
		Filename:    "20261021_0530.flac",
		ContentType: "audio/flac",
		CapturedAt:  time.Date(2026, 10, 21, 5, 30, 0, 0, time.FixedZone("+07", 7*3600)),
		Priority:    2,
		Size:        10,
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*Metadata)
		wantField string // "" = valid
		wantKind  error
	}{
		{"valid", func(*Metadata) {}, "", nil},
		{"device min length 3", func(m *Metadata) { m.DeviceID = "ab1" }, "", nil},
		{"device max length 64", func(m *Metadata) { m.DeviceID = "a" + strings.Repeat("b", 63) }, "", nil},
		{"device too short", func(m *Metadata) { m.DeviceID = "ab" }, KeyDeviceID, ErrInvalid},
		{"device too long", func(m *Metadata) { m.DeviceID = "a" + strings.Repeat("b", 64) }, KeyDeviceID, ErrInvalid},
		{"device uppercase", func(m *Metadata) { m.DeviceID = "Bioac-0142" }, KeyDeviceID, ErrInvalid},
		{"device leading dash", func(m *Metadata) { m.DeviceID = "-bioac" }, KeyDeviceID, ErrInvalid},
		{"device underscore", func(m *Metadata) { m.DeviceID = "bio_ac" }, KeyDeviceID, ErrInvalid},
		{"device empty", func(m *Metadata) { m.DeviceID = "" }, KeyDeviceID, ErrInvalid},
		{"sha uppercase", func(m *Metadata) { m.SHA256 = strings.ToUpper(sha) }, KeySHA256, ErrInvalid},
		{"sha short", func(m *Metadata) { m.SHA256 = sha[:63] }, KeySHA256, ErrInvalid},
		{"sha long", func(m *Metadata) { m.SHA256 = sha + "0" }, KeySHA256, ErrInvalid},
		{"sha non-hex", func(m *Metadata) { m.SHA256 = "g" + sha[1:] }, KeySHA256, ErrInvalid},
		{"filename empty", func(m *Metadata) { m.Filename = "" }, KeyFilename, ErrInvalid},
		{"filename 255 chars", func(m *Metadata) { m.Filename = strings.Repeat("a", 255) }, "", nil},
		{"filename 255 multibyte chars", func(m *Metadata) { m.Filename = strings.Repeat("é", 255) }, "", nil},
		{"filename 256 chars", func(m *Metadata) { m.Filename = strings.Repeat("a", 256) }, KeyFilename, ErrInvalid},
		{"filename slash", func(m *Metadata) { m.Filename = "../etc/passwd" }, KeyFilename, ErrInvalid},
		{"filename backslash", func(m *Metadata) { m.Filename = `a\b.flac` }, KeyFilename, ErrInvalid},
		{"filename NUL", func(m *Metadata) { m.Filename = "a\x00b" }, KeyFilename, ErrInvalid},
		{"filename newline", func(m *Metadata) { m.Filename = "a\nb" }, KeyFilename, ErrInvalid},
		{"filename invalid utf8", func(m *Metadata) { m.Filename = "a\xffb" }, KeyFilename, ErrInvalid},
		{"filename dots only ok", func(m *Metadata) { m.Filename = ".." }, "", nil},
		{"content type not allowed", func(m *Metadata) { m.ContentType = "application/x-sh" }, KeyContentType, ErrInvalid},
		{"content type case sensitive", func(m *Metadata) { m.ContentType = "Audio/FLAC" }, KeyContentType, ErrInvalid},
		{"captured_at zero", func(m *Metadata) { m.CapturedAt = time.Time{} }, KeyCapturedAt, ErrInvalid},
		{"captured_at exactly +24h", func(m *Metadata) { m.CapturedAt = now.Add(24 * time.Hour) }, "", nil},
		{"captured_at +24h+1s", func(m *Metadata) { m.CapturedAt = now.Add(24*time.Hour + time.Second) }, KeyCapturedAt, ErrInvalid},
		{"captured_at old ok", func(m *Metadata) { m.CapturedAt = now.AddDate(-5, 0, 0) }, "", nil},
		{"priority 0", func(m *Metadata) { m.Priority = 0 }, KeyPriority, ErrInvalid},
		{"priority 1", func(m *Metadata) { m.Priority = 1 }, "", nil},
		{"priority 4", func(m *Metadata) { m.Priority = 4 }, "", nil},
		{"priority 5", func(m *Metadata) { m.Priority = 5 }, KeyPriority, ErrInvalid},
		{"size 0", func(m *Metadata) { m.Size = 0 }, FieldSize, ErrInvalid},
		{"size 1", func(m *Metadata) { m.Size = 1 }, "", nil},
		{"size max", func(m *Metadata) { m.Size = 1000 }, "", nil},
		{"size max+1", func(m *Metadata) { m.Size = 1001 }, FieldSize, ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := valid()
			tt.mutate(&m)
			err := m.Validate(now, rules)
			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				return
			}
			var fe *FieldError
			if !errors.As(err, &fe) || fe.Field != tt.wantField {
				t.Fatalf("Validate = %v, want field %s", err, tt.wantField)
			}
			if !errors.Is(err, tt.wantKind) {
				t.Errorf("Validate = %v, want kind %v", err, tt.wantKind)
			}
			if !strings.HasPrefix(err.Error(), tt.wantField+": ") {
				t.Errorf("message %q does not name the field", err)
			}
		})
	}
}

func TestParseCapturedAt(t *testing.T) {
	for _, s := range []string{"2026-10-21T05:30:00+07:00", "2026-10-21T05:30:00Z", "2026-10-21T05:30:00.123-03:30"} {
		if _, err := ParseCapturedAt(s); err != nil {
			t.Errorf("ParseCapturedAt(%q) = %v", s, err)
		}
	}
	for _, s := range []string{"", "2026-10-21T05:30:00", "2026-10-21", "21/10/2026 05:30", "2026-10-21 05:30:00+07:00"} {
		if _, err := ParseCapturedAt(s); !errors.Is(err, ErrInvalid) {
			t.Errorf("ParseCapturedAt(%q) = %v, want ErrInvalid", s, err)
		}
	}
}

func TestParsePriority(t *testing.T) {
	if p, err := ParsePriority("3"); err != nil || p != 3 {
		t.Errorf("ParsePriority(3) = %d, %v", p, err)
	}
	for _, s := range []string{"", "two", "2.0", " 2"} {
		if _, err := ParsePriority(s); !errors.Is(err, ErrInvalid) {
			t.Errorf("ParsePriority(%q) = %v", s, err)
		}
	}
}

func tusPairs() map[string]string {
	return map[string]string{
		KeyDeviceID: "bioac-0142", KeySHA256: sha, KeyFilename: "20261021_0530.flac",
		KeyContentType: "audio/flac", KeyCapturedAt: "2026-10-21T05:30:00+07:00", KeyPriority: "2",
	}
}

func TestFromTus(t *testing.T) {
	m, err := FromTus(tusPairs(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(now, rules); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if m.Priority != 2 || m.Size != 10 || !m.CapturedAt.Equal(valid().CapturedAt) {
		t.Errorf("unexpected metadata %+v", m)
	}

	for _, key := range []string{KeyDeviceID, KeySHA256, KeyFilename, KeyContentType, KeyCapturedAt, KeyPriority} {
		p := tusPairs()
		delete(p, key)
		var fe *FieldError
		if _, err := FromTus(p, 10); !errors.As(err, &fe) || fe.Field != key {
			t.Errorf("missing %s: %v", key, err)
		}
	}
	for key, bad := range map[string]string{KeyCapturedAt: "yesterday", KeyPriority: "high"} {
		p := tusPairs()
		p[key] = bad
		var fe *FieldError
		if _, err := FromTus(p, 10); !errors.As(err, &fe) || fe.Field != key {
			t.Errorf("bad %s: %v", key, err)
		}
	}
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestParseUploadMetadata(t *testing.T) {
	// The example header from the contract §2.
	header := "device_id YmlvYWMtMDE0Mg==,sha256 " + b64(sha) + ",filename MjAyNjEwMjFfMDUzMC5mbGFj," +
		"content_type YXVkaW8vZmxhYw==,captured_at MjAyNi0xMC0yMVQwNTozMDowMCswNzowMA==,priority Mg=="
	got, err := ParseUploadMetadata(header)
	if err != nil {
		t.Fatal(err)
	}
	want := tusPairs()
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}

	ok := map[string]map[string]string{
		"":                        {},
		"   ":                     {},
		"flag":                    {"flag": ""},
		" a " + b64("x") + " , b": {"a": "x", "b": ""},
	}
	for h, want := range ok {
		got, err := ParseUploadMetadata(h)
		if err != nil {
			t.Errorf("ParseUploadMetadata(%q) = %v", h, err)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("ParseUploadMetadata(%q) = %v, want %v", h, got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("ParseUploadMetadata(%q)[%s] = %q, want %q", h, k, got[k], v)
			}
		}
	}

	for _, h := range []string{
		"a " + b64("x") + ",,b " + b64("y"), // empty pair
		"a not-base64!",
		"a " + b64("x") + " extra",
		"a " + b64("x") + ",a " + b64("y"), // duplicate
		",",
	} {
		var fe *FieldError
		if _, err := ParseUploadMetadata(h); !errors.As(err, &fe) || fe.Field != HeaderUploadMetadata || !errors.Is(err, ErrInvalid) {
			t.Errorf("ParseUploadMetadata(%q) = %v, want Upload-Metadata error", h, err)
		}
	}
}

func TestParseUploadMetadataSizeCap(t *testing.T) {
	// "key <base64>" of n bytes (n-4 must be a multiple of 4).
	header := func(n int) string { return "key " + strings.Repeat("A", n-4) }
	at := header(MaxUploadMetadataBytes)
	if _, err := ParseUploadMetadata(at); err != nil {
		t.Errorf("%d-byte header rejected: %v", len(at), err)
	}
	over := header(MaxUploadMetadataBytes + 4)
	var fe *FieldError
	if _, err := ParseUploadMetadata(over); !errors.As(err, &fe) || fe.Field != HeaderUploadMetadata || !errors.Is(err, ErrInvalid) {
		t.Errorf("%d-byte header = %v, want Upload-Metadata error", len(over), err)
	}
	// Unknown keys (tus-js-client's filetype/name) are accepted.
	if got, err := ParseUploadMetadata("filetype " + b64("audio/flac") + ",name " + b64("a.flac")); err != nil || got["filetype"] != "audio/flac" {
		t.Errorf("unknown keys: %v %v", got, err)
	}
}

func TestExtensionAndFinalKey(t *testing.T) {
	want := map[string]string{
		"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/avif": ".avif",
		"audio/wav": ".wav", "audio/x-wav": ".wav", "audio/flac": ".flac", "audio/ogg": ".ogg",
		"audio/opus": ".opus", "video/mp4": ".mp4", "video/quicktime": ".mov", "application/json": ".json",
		"application/x-custom": ".bin",
	}
	for ct, ext := range want {
		if got := Extension(ct); got != ext {
			t.Errorf("Extension(%q) = %q, want %q", ct, got, ext)
		}
	}

	m := valid() // 2026-10-21T05:30+07:00 = 2026-10-20T22:30Z
	if got, want := FinalKey(m), "bioac-0142/2026/10/20/"+sha+".flac"; got != want {
		t.Errorf("FinalKey = %q, want %q", got, want)
	}
	m.CapturedAt = time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	m.ContentType = "video/quicktime"
	if got, want := FinalKey(m), "bioac-0142/2027/01/02/"+sha+".mov"; got != want {
		t.Errorf("FinalKey = %q, want %q", got, want)
	}
}
