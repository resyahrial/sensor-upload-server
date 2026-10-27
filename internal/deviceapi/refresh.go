package deviceapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// RefreshRequest is the body of POST /v1/devices/token/refresh.
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (h *handler) refresh(w http.ResponseWriter, r *http.Request) result {
	var req RefreshRequest
	if err := decode(w, r, &req); err != nil || !auth.ValidSecret(req.RefreshToken) {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest)
		return result{code: http.StatusBadRequest, metric: obs.RefreshInvalid}
	}
	hash := auth.HashSecret(req.RefreshToken)
	now := h.cfg.Now()
	ctx := r.Context()
	// The lookup takes no lock; if the family changed before Update locked
	// it (a concurrent refresh promoted next), look the hash up again.
	for range refreshAttempts {
		m, err := h.cfg.Store.FindRefreshHash(ctx, hash)
		switch {
		case errors.Is(err, db.ErrTokenNotFound):
			return invalidGrant(w, obs.RefreshInvalidGrant, "")
		case err != nil:
			return unavailable(w, obs.RefreshError, "", err)
		case m.Retired:
			return h.retired(ctx, w, m)
		}
		var (
			newToken string
			fam      db.RefreshFamily
		)
		err = h.cfg.Store.Update(ctx, m.DeviceID, func(s *db.DeviceState) error {
			var err error
			newToken, err = h.rotate(s, hash, now)
			if err == nil {
				fam = *s.Family
			}
			return err
		})
		switch {
		case errors.Is(err, errRaced):
			continue
		case errors.Is(err, errNotActive):
			writeError(w, http.StatusForbidden, CodeDeviceNotActive)
			return result{code: http.StatusForbidden, metric: obs.RefreshNotActive, deviceID: m.DeviceID}
		case errors.Is(err, errExpired):
			return invalidGrant(w, obs.RefreshExpired, m.DeviceID)
		case err != nil:
			return unavailable(w, obs.RefreshError, m.DeviceID, err)
		}
		res, err := tokenResponse(h.cfg.Issuer, m.DeviceID, newToken, &fam, now, h.cfg.IdleTTL, h.cfg.MaxAge)
		if err != nil {
			// The rotation is saved; the presented token still works (it
			// is current now, or still current), so a retry succeeds.
			return unavailable(w, obs.RefreshError, m.DeviceID, err)
		}
		h.maybePrune(ctx, now)
		writeJSON(w, http.StatusOK, res)
		return result{code: http.StatusOK, metric: obs.RefreshOK, deviceID: m.DeviceID}
	}
	return invalidGrant(w, obs.RefreshInvalidGrant, "")
}

// rotate is the refresh state machine on the locked state; hash is the
// presented token's hash and the returned token the new refresh token.
//
//   - hash is current: the new token replaces next (an unused next is
//     retired); current keeps working, so a lost response is harmless
//   - hash is next: next is promoted to current (the old current is
//     retired) and the new token becomes next
//
// Either way the family is marked used, refreshed_at is now and the
// device's poll secret is cleared.
func (h *handler) rotate(s *db.DeviceState, hash []byte, now time.Time) (string, error) {
	f := s.Family
	if f == nil {
		return "", errRaced
	}
	isCurrent, isNext := equalHash(f.CurrentHash, hash), equalHash(f.NextHash, hash)
	if !isCurrent && !isNext {
		return "", errRaced
	}
	if s.Device.Status != db.DeviceActive {
		return "", errNotActive
	}
	if expired(f, now, h.cfg.IdleTTL, h.cfg.MaxAge) {
		return "", errExpired
	}
	tok, err := auth.GenerateSecret()
	if err != nil {
		return "", err
	}
	switch {
	case isNext:
		s.Retire = append(s.Retire, db.RetiredHash{Hash: f.CurrentHash, Reason: db.RetireRotated, At: now})
		f.CurrentHash = f.NextHash
	case f.NextHash != nil:
		s.Retire = append(s.Retire, db.RetiredHash{Hash: f.NextHash, Reason: db.RetireRotated, At: now})
	}
	f.NextHash = auth.HashSecret(tok)
	f.Used = true
	f.RefreshedAt = now
	s.Device.PollSecretHash = nil
	s.Device.LastSeenAt = now
	return tok, nil
}

// retired answers a token that no longer works: 403 if the device is not
// active, else 401; a rotated-out token presented again is logged as a
// security event (the device is not revoked).
func (h *handler) retired(ctx context.Context, w http.ResponseWriter, m db.TokenMatch) result {
	d, err := h.cfg.Store.Get(ctx, m.DeviceID)
	switch {
	case errors.Is(err, db.ErrDeviceNotFound):
		return invalidGrant(w, obs.RefreshInvalidGrant, "")
	case err != nil:
		return unavailable(w, obs.RefreshError, m.DeviceID, err)
	case d.Status != db.DeviceActive:
		writeError(w, http.StatusForbidden, CodeDeviceNotActive)
		return result{code: http.StatusForbidden, metric: obs.RefreshNotActive, deviceID: m.DeviceID}
	case m.Reason == db.RetireRotated:
		h.cfg.Log.LogAttrs(ctx, slog.LevelWarn, "refresh token reuse", slog.String("device_id", m.DeviceID))
		return invalidGrant(w, obs.RefreshReuse, m.DeviceID)
	}
	return invalidGrant(w, obs.RefreshInvalidGrant, m.DeviceID)
}

func invalidGrant(w http.ResponseWriter, metric, device string) result {
	writeError(w, http.StatusUnauthorized, CodeInvalidGrant)
	return result{code: http.StatusUnauthorized, metric: metric, deviceID: device}
}

// maybePrune deletes retired hashes older than MaxAge (no token they
// belonged to could still be valid) at most once per pruneInterval.
func (h *handler) maybePrune(ctx context.Context, now time.Time) {
	last := h.lastPrune.Load()
	if now.Unix()-last < int64(pruneInterval/time.Second) || !h.lastPrune.CompareAndSwap(last, now.Unix()) {
		return
	}
	n, err := h.cfg.Store.PruneRetired(ctx, now.Add(-h.cfg.MaxAge))
	if err != nil {
		h.cfg.Log.Warn("prune retired refresh tokens", "error", err.Error())
		return
	}
	if n > 0 {
		h.cfg.Log.Info("pruned retired refresh tokens", "count", n)
	}
}
