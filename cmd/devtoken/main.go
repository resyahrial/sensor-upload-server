// Command devtoken issues device JWTs for development and testing.
//
//	devtoken -device bioac-0142 -alg ES256 -key certs/jwt.key -ttl 24h
//	devtoken -device bioac-0142 -alg HS256 -secret "$JWT_HMAC_SECRET"
//
// The token is printed to stdout. Not for production use.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Getenv, time.Now()); err != nil {
		fmt.Fprintln(os.Stderr, "devtoken:", err)
		os.Exit(2)
	}
}

func run(args []string, out io.Writer, getenv func(string) string, now time.Time) error {
	fs := flag.NewFlagSet("devtoken", flag.ContinueOnError)
	alg := fs.String("alg", auth.ES256, "signing algorithm: ES256 or HS256")
	keyFile := fs.String("key", "certs/jwt.key", "PEM ECDSA P-256 private key (ES256)")
	secret := fs.String("secret", getenv("JWT_HMAC_SECRET"), "HMAC secret (HS256); defaults to $JWT_HMAC_SECRET")
	device := fs.String("device", "", "device_id claim (required)")
	ttl := fs.Duration("ttl", 24*time.Hour, "token lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *device == "" {
		return errors.New("-device is required")
	}

	var key any
	switch *alg {
	case auth.ES256:
		pem, err := os.ReadFile(*keyFile)
		if err != nil {
			return fmt.Errorf("read key: %w", err)
		}
		k, err := auth.ParseES256PrivateKey(pem)
		if err != nil {
			return err
		}
		key = k
	case auth.HS256:
		if *secret == "" {
			return errors.New("-secret (or $JWT_HMAC_SECRET) is required for HS256")
		}
		key = []byte(*secret)
	default:
		return fmt.Errorf("unsupported -alg %q (ES256 or HS256)", *alg)
	}

	token, err := auth.Sign(*alg, key, *device, now, *ttl)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, token)
	return err
}
