package deviceapi

import (
	"net/http/httptest"
	"testing"
)

func TestClientIP(t *testing.T) {
	tests := []struct {
		name        string
		xff         []string
		behindProxy bool
		want        string
	}{
		{"direct ignores XFF", []string{"198.51.100.7"}, false, "192.0.2.1"},
		{"proxy without XFF", nil, true, "192.0.2.1"},
		{"proxy single hop", []string{"198.51.100.7"}, true, "198.51.100.7"},
		{"proxy takes last entry", []string{"10.9.9.9, 198.51.100.7"}, true, "198.51.100.7"},
		{"proxy takes last header", []string{"10.9.9.9", "198.51.100.7"}, true, "198.51.100.7"},
		{"proxy empty entry", []string{" "}, true, "192.0.2.1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", PathRegister, nil)
			r.RemoteAddr = "192.0.2.1:4321"
			for _, v := range tc.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			if got := clientIP(r, tc.behindProxy); got != tc.want {
				t.Errorf("clientIP = %q, want %q", got, tc.want)
			}
		})
	}
}
