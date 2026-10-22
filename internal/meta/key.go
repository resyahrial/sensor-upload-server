package meta

import "fmt"

// extensions maps each FR-1a content type to its file extension.
var extensions = map[string]string{
	"image/jpeg":       ".jpg",
	"image/png":        ".png",
	"image/webp":       ".webp",
	"image/avif":       ".avif",
	"audio/wav":        ".wav",
	"audio/x-wav":      ".wav",
	"audio/flac":       ".flac",
	"audio/ogg":        ".ogg",
	"audio/opus":       ".opus",
	"video/mp4":        ".mp4",
	"video/quicktime":  ".mov",
	"application/json": ".json",
}

// Extension returns the file extension for contentType. Types added via
// CONTENT_TYPES that have no known mapping get ".bin".
func Extension(contentType string) string {
	if ext, ok := extensions[contentType]; ok {
		return ext
	}
	return ".bin"
}

// FinalKey returns the object store key
// {device_id}/{yyyy}/{mm}/{dd}/{sha256}{ext}, dated by captured_at in UTC.
// m must have passed Validate, so the key contains no client-controlled
// path separators.
func FinalKey(m Metadata) string {
	t := m.CapturedAt.UTC()
	return fmt.Sprintf("%s/%04d/%02d/%02d/%s%s", m.DeviceID, t.Year(), int(t.Month()), t.Day(), m.SHA256, Extension(m.ContentType))
}
