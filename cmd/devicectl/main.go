// Command devicectl administers device registration (tables device_batch,
// devices, refresh_tokens; docs/decisions.md "Decisions (JWT + refresh)"):
//
//	devicectl batch-import -file ids.txt       # allow-list; "-" reads stdin
//	devicectl batch-import -device netem-0001
//	devicectl list [-status pending]
//	devicectl approve -device netem-0001       # also reject, revoke
//	devicectl issue -device netem-0001         # dev/ops: token pair JSON on stdout
//	devicectl access-token -device netem-0001 -ttl 24h   # also activates
//
// The database is $DATABASE_URL (pending migrations are applied first).
// issue and access-token sign with $JWT_SIGNING_KEY_FILE ($JWT_ISSUER,
// default sensor-upload-server; $ACCESS_TOKEN_TTL, default 1h; issue also
// reads $REFRESH_IDLE_TTL and $REFRESH_MAX_AGE, defaults 2160h and 8760h).
// In the server image it runs as `docker compose exec server /devicectl ...`.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/deviceapi"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// timeout bounds connecting (db.Open retries until then) plus the command.
const timeout = 30 * time.Second

// Defaults of the token environment (same as internal/config).
const (
	defaultIssuer         = "sensor-upload-server"
	defaultAccessTokenTTL = time.Hour
	defaultRefreshIdleTTL = 90 * 24 * time.Hour
	defaultRefreshMaxAge  = 365 * 24 * time.Hour
)

// deviceStore is the subset of *db.Devices the commands use.
type deviceStore interface {
	deviceapi.Store
	ImportBatch(ctx context.Context, ids []string) (int64, error)
	List(ctx context.Context, status db.DeviceStatus) ([]db.Device, error)
	SetStatus(ctx context.Context, id string, status db.DeviceStatus, now time.Time) error
}

// opener connects to the store; close releases it.
type opener func(ctx context.Context) (store deviceStore, close func(), err error)

// deps is what run needs from the outside world.
type deps struct {
	open   opener
	getenv func(string) string
	now    func() time.Time
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

const usage = `usage:
  devicectl batch-import [-file PATH] [-device ID]...
                                  add device ids to the registration allow-list
                                  (file: one id per line, # comments; - = stdin)
  devicectl list [-status pending|active|rejected|revoked]
                                  print devices (tab-separated: device_id, status,
                                  lat, lon, firmware, registered_at, last_seen_at)
  devicectl approve -device ID    make a registered device active (its next
                                  register call receives tokens)
  devicectl reject -device ID     reject a registration (deletes its refresh tokens)
  devicectl revoke -device ID     revoke a device (deletes its refresh tokens)
  devicectl issue -device ID      dev/ops: allow-list and activate ID, start a new
                                  refresh token family (replacing any) and print
                                  the token pair JSON
  devicectl access-token -device ID [-ttl DURATION]
                                  dev/experiments: allow-list and activate ID
                                  (not if rejected/revoked; refresh tokens are
                                  untouched) and print a signed access token
                                  (default ttl $ACCESS_TOKEN_TTL)
Uses $DATABASE_URL; issue and access-token also $JWT_SIGNING_KEY_FILE,
$JWT_ISSUER, $ACCESS_TOKEN_TTL, $REFRESH_IDLE_TTL, $REFRESH_MAX_AGE.`

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := run(ctx, os.Args[1:], deps{
		open:   openDB(os.Getenv("DATABASE_URL")),
		getenv: os.Getenv,
		now:    time.Now,
		stdin:  os.Stdin,
		stdout: os.Stdout,
		stderr: os.Stderr,
	})
	cancel()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "devicectl:", err)
		os.Exit(1)
	}
}

func openDB(url string) opener {
	return func(ctx context.Context) (deviceStore, func(), error) {
		if url == "" {
			return nil, nil, errors.New("DATABASE_URL is not set")
		}
		pool, err := db.Open(ctx, url)
		if err != nil {
			return nil, nil, err
		}
		if err := db.Migrate(ctx, pool); err != nil {
			pool.Close()
			return nil, nil, err
		}
		return db.NewDevices(pool), pool.Close, nil
	}
}

// stringList is a repeatable string flag.
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func run(ctx context.Context, args []string, d deps) error {
	if len(args) == 0 {
		fmt.Fprintln(d.stderr, usage)
		return errors.New("missing command")
	}
	cmd, args := args[0], args[1:]
	fs := flag.NewFlagSet("devicectl "+cmd, flag.ContinueOnError)
	fs.SetOutput(d.stderr)
	fs.Usage = func() { fmt.Fprintln(d.stderr, usage) }
	var (
		device  string
		devices stringList
		file    string
		status  string
		ttl     time.Duration
	)
	switch cmd {
	case "batch-import":
		fs.StringVar(&file, "file", "", "file with one device id per line (# comments; - = stdin)")
		fs.Var(&devices, "device", "device id to import (repeatable)")
	case "list":
		fs.StringVar(&status, "status", "", "only devices with this status")
	case "approve", "reject", "revoke", "issue":
		fs.StringVar(&device, "device", "", "device_id")
	case "access-token":
		fs.StringVar(&device, "device", "", "device_id")
		fs.DurationVar(&ttl, "ttl", 0, "token lifetime (default $ACCESS_TOKEN_TTL)")
	case "-h", "-help", "--help", "help":
		fmt.Fprintln(d.stderr, usage)
		return flag.ErrHelp
	default:
		fmt.Fprintln(d.stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	switch cmd {
	case "batch-import":
		ids, err := batchIDs(file, devices, d.stdin)
		if err != nil {
			return err
		}
		return withStore(ctx, d, func(s deviceStore) error {
			n, err := s.ImportBatch(ctx, ids)
			if err != nil {
				return err
			}
			fmt.Fprintf(d.stderr, "devicectl: %d device ids imported (%d new)\n", len(ids), n)
			return nil
		})
	case "list":
		if status != "" && !db.ValidDeviceStatus(db.DeviceStatus(status)) {
			return fmt.Errorf("-status %q: must be pending, active, rejected or revoked", status)
		}
		return withStore(ctx, d, func(s deviceStore) error {
			devs, err := s.List(ctx, db.DeviceStatus(status))
			if err != nil {
				return err
			}
			return printDevices(d.stdout, devs)
		})
	}
	if !meta.ValidDeviceID(device) {
		return fmt.Errorf("-device %q is invalid: must match ^[a-z0-9][a-z0-9-]{2,63}$", device)
	}
	switch cmd {
	case "approve", "reject", "revoke":
		st := map[string]db.DeviceStatus{"approve": db.DeviceActive, "reject": db.DeviceRejected, "revoke": db.DeviceRevoked}[cmd]
		return withStore(ctx, d, func(s deviceStore) error {
			err := s.SetStatus(ctx, device, st, d.now())
			if errors.Is(err, db.ErrDeviceNotFound) {
				return fmt.Errorf("device %s has not registered (no devices row)", device)
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(d.stderr, "devicectl: %s is now %s\n", device, st)
			return nil
		})
	case "issue":
		return issue(ctx, d, device)
	default: // access-token
		return accessToken(ctx, d, device, ttl)
	}
}

func withStore(ctx context.Context, d deps, fn func(deviceStore) error) error {
	s, closeStore, err := d.open(ctx)
	if err != nil {
		return err
	}
	defer closeStore()
	return fn(s)
}

// batchIDs collects the ids of batch-import: the -device values and the
// lines of file ("-" = stdin), without blanks and # comments.
func batchIDs(file string, devices []string, stdin io.Reader) ([]string, error) {
	if file == "" && len(devices) == 0 {
		return nil, errors.New("batch-import needs -file or -device")
	}
	var ids []string
	for _, id := range devices {
		if !meta.ValidDeviceID(id) {
			return nil, fmt.Errorf("-device %q is invalid: must match ^[a-z0-9][a-z0-9-]{2,63}$", id)
		}
		ids = append(ids, id)
	}
	if file == "" {
		return ids, nil
	}
	r := stdin
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text, _, _ := strings.Cut(sc.Text(), "#")
		id := strings.TrimSpace(text)
		if id == "" {
			continue
		}
		if !meta.ValidDeviceID(id) {
			return nil, fmt.Errorf("%s:%d: device id %q is invalid: must match ^[a-z0-9][a-z0-9-]{2,63}$", file, line, id)
		}
		ids = append(ids, id)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("%s: no device ids", file)
	}
	return ids, nil
}

func printDevices(w io.Writer, devs []db.Device) error {
	bw := bufio.NewWriter(w)
	fmt.Fprintln(bw, "DEVICE_ID\tSTATUS\tLAT\tLON\tFIRMWARE\tREGISTERED_AT\tLAST_SEEN_AT")
	for _, d := range devs {
		fmt.Fprintf(bw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", d.DeviceID, d.Status, float(d.Lat), float(d.Lon),
			firmware(d.Firmware), d.RegisteredAt.UTC().Format(time.RFC3339), d.LastSeenAt.UTC().Format(time.RFC3339))
	}
	return bw.Flush()
}

func float(f *float64) string {
	if f == nil {
		return "-"
	}
	return strconv.FormatFloat(*f, 'f', -1, 64)
}

// firmware prints the reported firmware ("-" if none) without tabs or
// newlines, which would break the table.
func firmware(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}
	return strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, *s)
}

// tokenEnv is the signing configuration read from the environment.
type tokenEnv struct {
	issuer         *auth.Issuer
	idleTTL        time.Duration
	maxAge         time.Duration
	accessTokenTTL time.Duration
}

func loadTokenEnv(d deps) (tokenEnv, error) {
	var errs []error
	dur := func(key string, def time.Duration) time.Duration {
		v := d.getenv(key)
		if v == "" {
			return def
		}
		t, err := time.ParseDuration(v)
		if err != nil || t <= 0 {
			errs = append(errs, fmt.Errorf("%s: %q is not a positive duration (e.g. 1h)", key, v))
			return def
		}
		return t
	}
	env := tokenEnv{
		accessTokenTTL: dur("ACCESS_TOKEN_TTL", defaultAccessTokenTTL),
		idleTTL:        dur("REFRESH_IDLE_TTL", defaultRefreshIdleTTL),
		maxAge:         dur("REFRESH_MAX_AGE", defaultRefreshMaxAge),
	}
	keyFile := d.getenv("JWT_SIGNING_KEY_FILE")
	if keyFile == "" {
		errs = append(errs, errors.New("JWT_SIGNING_KEY_FILE is not set"))
	}
	if err := errors.Join(errs...); err != nil {
		return tokenEnv{}, err
	}
	key, err := auth.LoadSigningKey(keyFile)
	if err != nil {
		return tokenEnv{}, err
	}
	iss := d.getenv("JWT_ISSUER")
	if iss == "" {
		iss = defaultIssuer
	}
	env.issuer, err = auth.NewIssuer(auth.IssuerConfig{Key: key, Issuer: iss, TTL: env.accessTokenTTL, Now: d.now})
	if err != nil {
		return tokenEnv{}, err
	}
	return env, nil
}

func issue(ctx context.Context, d deps, device string) error {
	env, err := loadTokenEnv(d)
	if err != nil {
		return err
	}
	return withStore(ctx, d, func(s deviceStore) error {
		if _, err := s.ImportBatch(ctx, []string{device}); err != nil {
			return err
		}
		res, err := deviceapi.Issue(ctx, deviceapi.IssueConfig{
			Store: s, Issuer: env.issuer, IdleTTL: env.idleTTL, MaxAge: env.maxAge, Now: d.now(),
		}, device)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(d.stdout).Encode(res); err != nil {
			return err
		}
		fmt.Fprintf(d.stderr, "devicectl: %s is active with a new refresh token family "+
			"(any previous refresh token no longer works)\n", device)
		return nil
	})
}

func accessToken(ctx context.Context, d deps, device string, ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("-ttl must be positive")
	}
	env, err := loadTokenEnv(d)
	if err != nil {
		return err
	}
	if ttl == 0 {
		ttl = env.accessTokenTTL
	}
	return withStore(ctx, d, func(s deviceStore) error {
		// Like issue: allow-list and activate, so a fresh experiment device
		// needs no registration (rejected/revoked devices are refused).
		if _, err := s.ImportBatch(ctx, []string{device}); err != nil {
			return err
		}
		if err := deviceapi.Activate(ctx, s, device, d.now()); err != nil {
			return err
		}
		tok, exp, err := env.issuer.IssueTTL(device, ttl)
		if err != nil {
			return err
		}
		fmt.Fprintln(d.stdout, tok)
		fmt.Fprintf(d.stderr, "devicectl: access token for %s expires %s\n", device, exp.UTC().Format(time.RFC3339))
		return nil
	})
}
