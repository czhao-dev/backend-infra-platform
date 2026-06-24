package api

import (
	"encoding/json"
	"net/http"

	"github.com/czhao-dev/control-plane/internal/model"
	"github.com/czhao-dev/control-plane/internal/state"
)

// ListRoutes handles GET /api/v1/routes.
func (h *Handlers) ListRoutes(w http.ResponseWriter, r *http.Request) {
	routes, err := h.store.ListRoutes(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": routes, "total": len(routes)})
}

// CreateRoute handles POST /api/v1/routes.
func (h *Handlers) CreateRoute(w http.ResponseWriter, r *http.Request) {
	var route model.Route
	if err := json.NewDecoder(r.Body).Decode(&route); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if route.PathPrefix == "" {
		writeJSONError(w, http.StatusBadRequest, "path_prefix is required")
		return
	}
	if route.ID == "" {
		genID, err := generateID("route")
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate route id")
			return
		}
		route.ID = genID
	}
	if route.Strategy == "" {
		route.Strategy = model.StrategyRoundRobin
	}
	if err := h.store.UpsertRoute(r.Context(), &route); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	created, _ := h.store.GetRoute(r.Context(), route.ID)
	w.Header().Set("Location", "/api/v1/routes/"+route.ID)
	writeJSON(w, http.StatusCreated, created)
}

// GetRoute handles GET /api/v1/routes/{id}.
func (h *Handlers) GetRoute(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	route, err := h.store.GetRoute(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "route not found")
		return
	}
	writeJSON(w, http.StatusOK, route)
}

// UpdateRoute handles PUT /api/v1/routes/{id}.
func (h *Handlers) UpdateRoute(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetRoute(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusNotFound, "route not found")
		return
	}
	var route model.Route
	if err := json.NewDecoder(r.Body).Decode(&route); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	route.ID = id
	if err := h.store.UpsertRoute(r.Context(), &route); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	updated, _ := h.store.GetRoute(r.Context(), id)
	writeJSON(w, http.StatusOK, updated)
}

// DeleteRoute handles DELETE /api/v1/routes/{id}.
func (h *Handlers) DeleteRoute(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.store.DeleteRoute(r.Context(), id); err != nil {
		if err == state.ErrNotFound {
			writeJSONError(w, http.StatusNotFound, "route not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ProxyConfig handles GET /api/v1/proxy/config -- the full set of routes the
// reverse proxy should be aware of.
func (h *Handlers) ProxyConfig(w http.ResponseWriter, r *http.Request) {
	routes, err := h.store.ListRoutes(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": routes})
}

// ProxyBackends handles GET /api/v1/proxy/backends. It synthesizes the
// dynamic-discovery proxy's backend list live from currently HEALTHY
// workers -- this is the literal "proxy routes to worker nodes" story: the
// reverse proxy load-balances directly across the worker fleet the control
// plane is managing, rather than a separately-configured static set.
func (h *Handlers) ProxyBackends(w http.ResponseWriter, r *http.Request) {
	workers, err := h.store.ListWorkers(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	backends := make([]model.BackendSpec, 0, len(workers))
	for _, wk := range workers {
		if wk.Status != model.WorkerHealthy {
			continue
		}
		backends = append(backends, model.BackendSpec{Name: wk.ID, URL: wk.Address, Weight: 1})
	}
	writeJSON(w, http.StatusOK, map[string]any{"backends": backends})
}
