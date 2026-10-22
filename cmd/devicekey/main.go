// Command devicekey manages the per-device upload keys in Postgres
// (table device_keys; README "Authentication").
//
//	devicekey set -device netem-0001      # new key; prints the bearer token once
//	devicekey delete -device netem-0001
//	devicekey list                        # device ids and created_at, never keys
//
// The database is $DATABASE_URL (pending migrations are applied first). In
// the server image it runs as `docker compose exec server /devicekey ...`.
// set replaces the device's previous key immediately.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// timeout bounds connecting (db.Open retries until then) plus the command.
const timeout = 30 * time.Second

// keyStore is the subset of *db.DeviceKeys the commands use.
type keyStore interface {
	Set(ctx context.Context, device string, hash []byte) error
	Delete(ctx context.Context, device string) (bool, error)
	List(ctx context.Context) ([]db.DeviceKey, error)
}

// opener connects to the key store; close releases it.
type opener func(ctx context.Context) (store keyStore, close func(), err error)

const usage = `usage:
  devicekey set -device ID      generate a key for ID (replacing any previous one)
                                and print the bearer token ID_key (shown only once)
  devicekey delete -device ID   delete ID's key (its uploads are rejected with 401)
  devicekey list                list devices with a key (device_id, created_at)
Uses $DATABASE_URL.`

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, openDB(os.Getenv("DATABASE_URL")))
	cancel()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "devicekey:", err)
		os.Exit(1)
	}
}

func openDB(url string) opener {
	return func(ctx context.Context) (keyStore, func(), error) {
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
		return db.NewDeviceKeys(pool), pool.Close, nil
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, open opener) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return errors.New("missing command")
	}
	cmd, args := args[0], args[1:]
	fs := flag.NewFlagSet("devicekey "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintln(stderr, usage) }
	var device string
	switch cmd {
	case "set", "delete":
		fs.StringVar(&device, "device", "", "device_id (same rules as upload metadata device_id)")
	case "list":
	case "-h", "-help", "--help", "help":
		fmt.Fprintln(stderr, usage)
		return flag.ErrHelp
	default:
		fmt.Fprintln(stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if cmd != "list" && !meta.ValidDeviceID(device) {
		return fmt.Errorf("-device %q is invalid: must match ^[a-z0-9][a-z0-9-]{2,63}$", device)
	}

	store, closeStore, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeStore()

	switch cmd {
	case "set":
		key, err := auth.GenerateKey()
		if err != nil {
			return err
		}
		if err := store.Set(ctx, device, auth.HashKey(key)); err != nil {
			return err
		}
		fmt.Fprintln(stdout, auth.FormatToken(device, key))
		fmt.Fprintf(stderr, "devicekey: key for %s set (any previous key no longer works); "+
			"the token above is shown only once: Authorization: Bearer <token>\n", device)
	case "delete":
		ok, err := store.Delete(ctx, device)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("device %s has no key", device)
		}
		fmt.Fprintf(stderr, "devicekey: key for %s deleted\n", device)
	case "list":
		keys, err := store.List(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "DEVICE_ID\tCREATED_AT")
		for _, k := range keys {
			fmt.Fprintf(tw, "%s\t%s\n", k.DeviceID, k.CreatedAt.UTC().Format(time.RFC3339))
		}
		return tw.Flush()
	}
	return nil
}
