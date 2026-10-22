package meta

import (
	"encoding/base64"
	"strings"
)

// HeaderUploadMetadata names the tus header in errors.
const HeaderUploadMetadata = "Upload-Metadata"

// MaxUploadMetadataBytes caps the raw Upload-Metadata header. The six
// contract keys need well under 1 KiB; tusd stores every pair in the
// upload's info, so without a cap a client could persist up to the
// server's header limit (~1 MiB) per upload.
const MaxUploadMetadataBytes = 4 << 10

// ParseUploadMetadata strictly parses a tus Upload-Metadata header:
// comma-separated "key base64(value)" pairs, where the value is optional.
// Unlike tusd's lenient parser it rejects empty or space-containing keys,
// invalid base64, duplicate keys and headers longer than
// MaxUploadMetadataBytes, so malformed headers get a 400. Unknown keys
// are kept (tus clients commonly add filetype/name).
func ParseUploadMetadata(header string) (map[string]string, error) {
	if len(header) > MaxUploadMetadataBytes {
		return nil, invalid(HeaderUploadMetadata, "must be at most %d bytes", MaxUploadMetadataBytes)
	}
	out := map[string]string{}
	if strings.TrimSpace(header) == "" {
		return out, nil
	}
	for pair := range strings.SplitSeq(header, ",") {
		pair = strings.TrimSpace(pair)
		key, enc, _ := strings.Cut(pair, " ")
		if key == "" {
			return nil, invalid(HeaderUploadMetadata, "contains an empty key")
		}
		if strings.ContainsAny(enc, " ") {
			return nil, invalid(HeaderUploadMetadata, "value for %q must be a single base64 token", key)
		}
		if _, dup := out[key]; dup {
			return nil, invalid(HeaderUploadMetadata, "duplicate key %q", key)
		}
		val, err := base64.StdEncoding.DecodeString(enc)
		if err != nil {
			return nil, invalid(HeaderUploadMetadata, "value for %q is not valid base64", key)
		}
		out[key] = string(val)
	}
	return out, nil
}
