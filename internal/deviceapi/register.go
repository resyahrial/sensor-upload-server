package deviceapi

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// RegisterRequest is the body of POST /v1/devices/register.
type RegisterRequest struct {
	DeviceID   string   `json:"device_id"`
	PollSecret string   `json:"poll_secret"`
	Lat        *float64 `json:"lat,omitempty"`
	Lon        *float64 `json:"lon,omitempty"`
	Firmware   *string  `json:"firmware,omitempty"`
}

func (q RegisterRequest) validate() error {
	switch {
	case !meta.ValidDeviceID(q.DeviceID):
		return fmt.Errorf("%w: device_id", errInvalidInput)
	case !auth.ValidSecret(q.PollSecret):
		return fmt.Errorf("%w: poll_secret", errInvalidInput)
	case q.Lat != nil && (math.IsNaN(*q.Lat) || *q.Lat < -90 || *q.Lat > 90):
		return fmt.Errorf("%w: lat", errInvalidInput)
	case q.Lon != nil && (math.IsNaN(*q.Lon) || *q.Lon < -180 || *q.Lon > 180):
		return fmt.Errorf("%w: lon", errInvalidInput)
	case q.Firmware != nil && len(*q.Firmware) > maxFirmwareLen:
		return fmt.Errorf("%w: firmware", errInvalidInput)
	}
	return nil
}

// applyTo copies the reported location/firmware (when present) to d and
// marks it seen at now.
func (q RegisterRequest) applyTo(d *db.Device, now time.Time) {
	if q.Lat != nil {
		d.Lat = q.Lat
	}
	if q.Lon != nil {
		d.Lon = q.Lon
	}
	if q.Firmware != nil {
		d.Firmware = q.Firmware
	}
	d.LastSeenAt = now
}

// registerOutcome is what the register state machine decided.
type registerOutcome struct {
	metric  string // obs.Register*
	refresh string // set when metric is RegisterIssued
	family  *db.RefreshFamily
}

func (h *handler) register(w http.ResponseWriter, r *http.Request) result {
	if ok, wait := h.limiter.allow(clientIP(r, h.cfg.BehindProxy)); !ok {
		setRetryAfter(w, wait)
		writeError(w, http.StatusTooManyRequests, CodeRateLimited)
		return result{code: http.StatusTooManyRequests, metric: obs.RegisterRateLimited}
	}
	var req RegisterRequest
	err := decode(w, r, &req)
	if err == nil {
		err = req.validate()
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest)
		return result{code: http.StatusBadRequest, metric: obs.RegisterInvalid}
	}
	now := h.cfg.Now()
	var out registerOutcome
	err = h.cfg.Store.Update(r.Context(), req.DeviceID, func(s *db.DeviceState) error {
		var err error
		out, err = h.decideRegister(s, req, now)
		return err
	})
	dev := req.DeviceID
	switch {
	case errors.Is(err, errNotAllowed):
		writeError(w, http.StatusForbidden, CodeDeviceNotAllowed)
		return result{code: http.StatusForbidden, metric: obs.RegisterNotAllowed, deviceID: dev}
	case errors.Is(err, errNotActive):
		writeError(w, http.StatusForbidden, CodeDeviceNotActive)
		return result{code: http.StatusForbidden, metric: obs.RegisterNotActive, deviceID: dev}
	case errors.Is(err, errMismatch):
		writeError(w, http.StatusConflict, CodePollSecretMismatch)
		return result{code: http.StatusConflict, metric: obs.RegisterSecretMismatch, deviceID: dev}
	case err != nil:
		return unavailable(w, obs.RegisterError, dev, err)
	}
	if out.metric != obs.RegisterIssued {
		setRetryAfter(w, h.cfg.RetryAfter)
		writeJSON(w, http.StatusAccepted, pendingBody{Status: StatusPending})
		return result{code: http.StatusAccepted, metric: out.metric, deviceID: dev}
	}
	res, err := tokenResponse(h.cfg.Issuer, dev, out.refresh, out.family, now, h.cfg.IdleTTL, h.cfg.MaxAge)
	if err != nil {
		// The family is saved but unusable by the device; a retry with
		// the same poll secret replaces it.
		return unavailable(w, obs.RegisterError, dev, err)
	}
	writeJSON(w, http.StatusOK, res)
	return result{code: http.StatusOK, metric: obs.RegisterIssued, deviceID: dev}
}

// decideRegister is the registration state machine on the locked state.
//
//   - not in the batch → errNotAllowed
//   - no device row → create it pending
//   - active, poll secret matches and the family is unused or missing →
//     start a new family (a lost 200 can be collected again)
//   - active and the family expired, or there is no family and no poll
//     secret → re-registration: back to pending with the new secret
//   - poll secret differs (or was cleared by a refresh) → errMismatch
//   - pending → refresh location/firmware, stay pending
//   - rejected or revoked → errNotActive
func (h *handler) decideRegister(s *db.DeviceState, req RegisterRequest, now time.Time) (registerOutcome, error) {
	if !s.InBatch {
		return registerOutcome{}, errNotAllowed
	}
	hash := auth.HashSecret(req.PollSecret)
	d := s.Device
	if d == nil {
		d = &db.Device{DeviceID: req.DeviceID, Status: db.DevicePending, PollSecretHash: hash, RegisteredAt: now}
		req.applyTo(d, now)
		s.Device = d
		return registerOutcome{metric: obs.RegisterPending}, nil
	}
	match := equalHash(d.PollSecretHash, hash)
	if d.Status == db.DeviceActive {
		f := s.Family
		if match && (f == nil || !f.Used) {
			req.applyTo(d, now)
			s.RetireFamily(db.RetireReset, now)
			tok, fam, err := newFamily(now)
			if err != nil {
				return registerOutcome{}, err
			}
			s.Family = fam
			return registerOutcome{metric: obs.RegisterIssued, refresh: tok, family: fam}, nil
		}
		if (f != nil && expired(f, now, h.cfg.IdleTTL, h.cfg.MaxAge)) || (f == nil && d.PollSecretHash == nil) {
			d.Status = db.DevicePending
			d.PollSecretHash = hash
			d.RegisteredAt = now
			d.ApprovedAt = nil
			req.applyTo(d, now)
			s.RetireFamily(db.RetireReset, now)
			return registerOutcome{metric: obs.RegisterReregistered}, nil
		}
	}
	if !match {
		return registerOutcome{}, errMismatch
	}
	switch d.Status {
	case db.DevicePending:
		req.applyTo(d, now)
		return registerOutcome{metric: obs.RegisterPending}, nil
	case db.DeviceRejected, db.DeviceRevoked:
		return registerOutcome{}, errNotActive
	}
	// Active with a matching secret but a used family: not reachable (a
	// refresh clears the secret), answered like a cleared secret.
	return registerOutcome{}, errMismatch
}

// clientIP is the host of r.RemoteAddr (the whole value if it has no
// port). Behind a reverse proxy it is the last X-Forwarded-For entry: the
// address the proxy itself appended, which the client cannot forge.
func clientIP(r *http.Request, behindProxy bool) string {
	if xff := r.Header.Values("X-Forwarded-For"); behindProxy && len(xff) > 0 {
		last := xff[len(xff)-1]
		if i := strings.LastIndexByte(last, ','); i >= 0 {
			last = last[i+1:]
		}
		if ip := strings.TrimSpace(last); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
