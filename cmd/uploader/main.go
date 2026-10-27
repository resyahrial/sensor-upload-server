// Command uploader is the sensor test client: it uploads files over tus
// (/rest, /rest-http3) or gRPC (/grpc, or the native listener) following
// sensor-upload-integration.md §5, and writes a CSV report.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/client"
)

func main() {
	err := run(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "uploader:", err)
		os.Exit(1)
	}
}

type options struct {
	cfg         client.Config
	refreshFile string
	authURL     string
	file        string
	dir         string
	report      string
	state       string
	ca          string
	insecure    bool
	verbose     bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("uploader", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.cfg.Protocol, "protocol", client.ProtoREST, "rest | rest-http3 | grpc | grpc-native")
	fs.StringVar(&o.cfg.URL, "url", "https://localhost:8443", "server base URL (grpc-native: the native listener address)")
	fs.StringVar(&o.cfg.Token, "token", os.Getenv("UPLOADER_TOKEN"),
		"static access `JWT` (devicectl access-token); never refreshed, so an expired token stops the run (default $UPLOADER_TOKEN)")
	fs.StringVar(&o.refreshFile, "refresh-token-file", "",
		"token pair JSON `file` (devicectl issue output); access tokens are refreshed and the file is rewritten (0600) on every rotation")
	fs.StringVar(&o.authURL, "auth-url", "", "server base `URL` for "+client.RefreshPath+" (default: -url; set it for grpc-native)")
	fs.StringVar(&o.cfg.Device, "device", "", "device_id (must match the token; default: the token's sub)")
	fs.IntVar(&o.cfg.Priority, "priority", 3, "1 alert · 2 thumbnail · 3 clip · 4 full file")
	fs.IntVar(&o.cfg.Chunk, "chunk", 262144, "chunk size in bytes")
	fs.IntVar(&o.cfg.Retries, "retries", 50, "retries per file after the first attempt")
	fs.DurationVar(&o.cfg.BackoffMax, "backoff-max", 5*time.Minute, "maximum backoff between retries")
	fs.IntVar(&o.cfg.Concurrency, "concurrency", 1, "files uploaded in parallel")
	fs.BoolVar(&o.cfg.H3Fallback, "h3-fallback", false, "rest-http3: fall back to /rest after 3 consecutive QUIC failures")
	fs.IntVar(&o.cfg.GRPCWindow, "grpc-window", 1,
		"grpc: max chunks sent but not yet acked; must be >= the server's GRPC_ACK_EVERY_N "+
			"(otherwise each stream stalls until the ack timeout, 60 s per 256 KiB of chunk, and retries)")
	fs.StringVar(&o.file, "file", "", "file to upload")
	fs.StringVar(&o.dir, "dir", "", "upload every regular file in this directory")
	fs.StringVar(&o.report, "report", "", "write a CSV report to this path")
	fs.StringVar(&o.state, "state", "uploader.db", "SQLite outbox path (keeps resume state across runs)")
	fs.StringVar(&o.ca, "ca", "", "PEM CA certificate for the server")
	fs.BoolVar(&o.insecure, "insecure", false, "skip TLS verification (dev only)")
	fs.BoolVar(&o.verbose, "v", false, "debug logging")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.refreshFile != "" {
		// An explicit -token conflicts; a $UPLOADER_TOKEN default is ignored.
		tokenFlag := false
		fs.Visit(func(f *flag.Flag) { tokenFlag = tokenFlag || f.Name == "token" })
		if tokenFlag {
			return o, errors.New("use either -token or -refresh-token-file, not both")
		}
		o.cfg.Token = ""
	} else if o.cfg.Token == "" {
		return o, errors.New("-token (or $UPLOADER_TOKEN) or -refresh-token-file is required")
	}
	if o.cfg.Device == "" && o.cfg.Token != "" {
		d, err := auth.DeviceFromToken(o.cfg.Token)
		if err != nil {
			return o, fmt.Errorf("cannot take the device_id from -token (%w); pass -device", err)
		}
		o.cfg.Device = d
	}
	if o.authURL == "" {
		o.authURL = o.cfg.URL
	}
	if (o.file == "") == (o.dir == "") {
		return o, errors.New("exactly one of -file or -dir is required")
	}
	if o.cfg.GRPCWindow < 1 {
		return o, fmt.Errorf("-grpc-window must be >= 1, got %d", o.cfg.GRPCWindow)
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return o, nil
}

func tlsConfig(ca string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // opt-in dev flag
	if ca == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", ca)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func listFiles(o options) ([]string, error) {
	if o.file != "" {
		return []string{o.file}, nil
	}
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, filepath.Join(o.dir, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no files in %s", o.dir)
	}
	return files, nil
}

func run(args []string, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if o.verbose {
		level = slog.LevelDebug
	}
	o.cfg.Logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	if o.cfg.TLS, err = tlsConfig(o.ca, o.insecure); err != nil {
		return err
	}
	files, err := listFiles(o)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if o.refreshFile != "" {
		src, err := client.NewRefreshingSource(client.RefreshConfig{
			File: o.refreshFile, URL: o.authURL, TLS: o.cfg.TLS, Logger: o.cfg.Logger,
		})
		if err != nil {
			return err
		}
		defer src.Close()
		o.cfg.Tokens = src
		if o.cfg.Device == "" {
			o.cfg.Device = src.Device()
		}
	}

	ob, err := client.OpenOutbox(ctx, o.state)
	if err != nil {
		return err
	}
	defer func() { _ = ob.Close() }()
	r, err := client.New(o.cfg, ob)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	results, err := r.Run(ctx, files)
	if err != nil {
		return err
	}
	if o.report != "" {
		if err := writeReport(o.report, results); err != nil {
			return err
		}
	}
	var failed int
	for _, res := range results {
		if res.Result == client.ResultFailed || res.Result == client.ResultInterrupted {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d files not uploaded", failed, len(results))
	}
	return nil
}

func writeReport(path string, results []client.Result) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	if err := client.WriteReport(f, results); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}
