// Command server runs the sensor upload server.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/resyahrial/sensor-upload-server/internal/app"
	"github.com/resyahrial/sensor-upload-server/internal/config"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "upload-server:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	log := obs.NewLogger(os.Stdout, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	a, err := app.New(ctx, app.Options{Config: cfg, Log: log})
	if err != nil {
		return err
	}
	return a.Run(ctx)
}
