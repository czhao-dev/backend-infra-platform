package api

import (
	"context"
	"net/http"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(addr string, h *Handlers) *Server {
	return &Server{httpServer: &http.Server{Addr: addr, Handler: NewRouter(h)}}
}

// Start blocks until the server stops; it returns http.ErrServerClosed on a
// graceful Shutdown, which callers should treat as a normal exit.
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
