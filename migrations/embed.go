// Package migrations embeds the goose SQL migrations so the server binary
// can apply them at startup.
package migrations

import "embed"

// FS holds every *.sql migration at its root.
//
//go:embed *.sql
var FS embed.FS
