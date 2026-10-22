package tusapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/tus/tusd/v2/pkg/handler"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// RecoveryRepository lists the rows crash recovery looks at (*db.Repo).
type RecoveryRepository interface {
	ListStale(ctx context.Context, ttl time.Duration) ([]db.Upload, error)
}

// Recover finalizes tus uploads that are fully staged but still
// 'uploading' (the process stopped between the last byte and the end of
// finalization). Run it once at startup, before serving. Failures are
// logged, not returned: the HEAD/PATCH guard finalizes such uploads on the
// next request anyway. Only the filestore can map a row to its staged
// upload without a request (s3store IDs carry the multipart ID).
func Recover(ctx context.Context, repo RecoveryRepository, fin Finalizer, st *Staging, log *slog.Logger) error {
	if !st.idIsRef {
		return nil
	}
	// ttl 0 = every 'uploading' row.
	rows, err := repo.ListStale(ctx, 0)
	if err != nil {
		return fmt.Errorf("tus recovery: %w", err)
	}
	protocols := []string{ProtocolREST, ProtocolRESTHTTP3}
	for _, row := range rows {
		if !slices.Contains(protocols, row.Protocol) || !validRef(row.Ref) {
			continue
		}
		info, err := st.info(ctx, row.Ref)
		if errors.Is(err, handler.ErrNotFound) {
			continue
		}
		if err != nil {
			log.Warn("tus recovery: read staged upload", "protocol", row.Protocol, "upload_ref", row.Ref, "error", err.Error())
			continue
		}
		if info.Offset != info.Size {
			continue
		}
		key, err := fin.Run(ctx, st.input(row.Protocol, row.Ref, row.Ref))
		if err != nil {
			log.Warn("tus recovery: finalize", "protocol", row.Protocol, "upload_ref", row.Ref, "error", err.Error())
			continue
		}
		log.Info("tus recovery: finalized", "protocol", row.Protocol, "upload_ref", row.Ref, "storage_key", key)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("tus recovery: %w", err)
		}
	}
	return nil
}
