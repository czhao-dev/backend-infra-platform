package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/czhao-dev/control-plane/config"
	"github.com/czhao-dev/control-plane/internal/api"
	"github.com/czhao-dev/control-plane/internal/logging"
	"github.com/czhao-dev/control-plane/internal/reconciler"
	"github.com/czhao-dev/control-plane/internal/scheduler"
	"github.com/czhao-dev/control-plane/internal/state"
)

func main() {
	cfg := config.Load()
	logger := logging.New(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)

	store := state.NewMemoryStore()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sched := scheduler.New(store, cfg.SchedulerInterval, logger)
	go sched.Run(ctx)

	rec := reconciler.New(store, cfg.ReconcileInterval, cfg.HeartbeatTimeout, logger)
	go rec.Run(ctx)

	handlers := api.NewHandlers(store, sched)
	srv := api.NewServer(fmt.Sprintf(":%d", cfg.Port), handlers)

	go func() {
		logger.Info("control-plane starting",
			"port", cfg.Port,
			"scheduler_interval", cfg.SchedulerInterval,
			"reconcile_interval", cfg.ReconcileInterval,
			"heartbeat_timeout", cfg.HeartbeatTimeout,
		)
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}
	logger.Info("control-plane stopped")
}
