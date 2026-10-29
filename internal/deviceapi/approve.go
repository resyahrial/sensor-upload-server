package deviceapi

import (
	"errors"
	"net/http"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// ApproveRequest is the body of POST /v1/devices/approve.
type ApproveRequest struct {
	DeviceID string `json:"device_id"`
}

// ApproveResponse is the 200 body of POST /v1/devices/approve.
type ApproveResponse struct {
	DeviceID string `json:"device_id"`
	Status   string `json:"status"`
}

// approve makes a pending device active, so its next register call with
// the same poll secret collects the token pair. It is unauthenticated (an
// owner decision, docs/decisions.md), so it is limited to what is safe to
// expose: an active device is a no-op, and a rejected or revoked device
// stays as it is (re-activating one takes devicectl approve).
func (h *handler) approve(w http.ResponseWriter, r *http.Request) result {
	if ok, wait := h.limiter.allow(clientIP(r, h.cfg.BehindProxy)); !ok {
		setRetryAfter(w, wait)
		writeError(w, http.StatusTooManyRequests, CodeRateLimited)
		return result{code: http.StatusTooManyRequests, metric: obs.ApproveRateLimited}
	}
	var req ApproveRequest
	err := decode(w, r, &req)
	if err == nil {
		if req.DeviceID = meta.NormalizeDeviceID(req.DeviceID); !meta.ValidDeviceID(req.DeviceID) {
			err = errInvalidInput
		}
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest)
		return result{code: http.StatusBadRequest, metric: obs.ApproveInvalid}
	}
	dev := req.DeviceID
	metric := obs.ApproveApproved
	err = h.cfg.Store.Update(r.Context(), dev, func(s *db.DeviceState) error {
		if s.Device == nil {
			return errNotRegistered
		}
		switch s.Device.Status {
		case db.DeviceActive:
			metric = obs.ApproveAlreadyActive
			return nil
		case db.DeviceRejected, db.DeviceRevoked:
			return errNotActive
		}
		return db.ApplyStatus(s, db.DeviceActive, h.cfg.Now())
	})
	switch {
	case errors.Is(err, errNotRegistered):
		writeError(w, http.StatusNotFound, CodeDeviceNotFound)
		return result{code: http.StatusNotFound, metric: obs.ApproveNotFound, deviceID: dev}
	case errors.Is(err, errNotActive):
		writeError(w, http.StatusForbidden, CodeDeviceNotActive)
		return result{code: http.StatusForbidden, metric: obs.ApproveNotActive, deviceID: dev}
	case err != nil:
		return unavailable(w, obs.ApproveError, dev, err)
	}
	writeJSON(w, http.StatusOK, ApproveResponse{DeviceID: dev, Status: string(db.DeviceActive)})
	return result{code: http.StatusOK, metric: metric, deviceID: dev}
}
