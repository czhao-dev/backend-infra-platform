package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/czhao-dev/control-plane/internal/scheduler"
	"github.com/czhao-dev/control-plane/internal/state"
)

// Handlers holds the dependencies shared by every HTTP handler.
type Handlers struct {
	store     state.Store
	scheduler *scheduler.Scheduler // nil until the scheduler is wired in (milestone 3)
}

func NewHandlers(st state.Store, sched *scheduler.Scheduler) *Handlers {
	return &Handlers{store: st, scheduler: sched}
}

func generateID(prefix string) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
