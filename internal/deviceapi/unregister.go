package deviceapi

import (
	"errors"
	"net/http"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// UnregisterRequest is the body of POST /v1/devices/unregister.
type UnregisterRequest struct {
	DeviceID string `json:"device_id"`
}

// UnregisterResponse is the 200 body of POST /v1/devices/unregister.
type UnregisterResponse struct {
	DeviceID string `json:"device_id"`
	Status   string `json:"status"`
}

// StatusUnregistered is the "status" of a 200 unregister response.
const StatusUnregistered = "unregistered"

// unregister deletes a device's row in any state, so the id can register
// again with a new poll secret (and lands pending). Its refresh tokens
// are retired as reset; access tokens already issued stay valid until
// they expire, as with revoke. It is unauthenticated (an owner decision,
// docs/decisions.md). Uploaded files are kept.
func (h *handler) unregister(w http.ResponseWriter, r *http.Request) result {
	if ok, wait := h.limiter.allow(clientIP(r, h.cfg.BehindProxy)); !ok {
		setRetryAfter(w, wait)
		writeError(w, http.StatusTooManyRequests, CodeRateLimited)
		return result{code: http.StatusTooManyRequests, metric: obs.UnregisterRateLimited}
	}
	var req UnregisterRequest
	err := decode(w, r, &req)
	if err == nil {
		if req.DeviceID = meta.NormalizeDeviceID(req.DeviceID); !meta.ValidDeviceID(req.DeviceID) {
			err = errInvalidInput
		}
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest)
		return result{code: http.StatusBadRequest, metric: obs.UnregisterInvalid}
	}
	dev := req.DeviceID
	err = h.cfg.Store.Update(r.Context(), dev, func(s *db.DeviceState) error {
		if s.Device == nil {
			return errNotRegistered
		}
		s.RetireFamily(db.RetireReset, h.cfg.Now())
		s.Device = nil
		return nil
	})
	switch {
	case errors.Is(err, errNotRegistered):
		writeError(w, http.StatusNotFound, CodeDeviceNotFound)
		return result{code: http.StatusNotFound, metric: obs.UnregisterNotFound, deviceID: dev}
	case err != nil:
		return unavailable(w, obs.UnregisterError, dev, err)
	}
	writeJSON(w, http.StatusOK, UnregisterResponse{DeviceID: dev, Status: StatusUnregistered})
	return result{code: http.StatusOK, metric: obs.UnregisterDeleted, deviceID: dev}
}
