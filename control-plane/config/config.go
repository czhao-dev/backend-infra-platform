package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds cmd/control-plane's runtime configuration, loaded from
// CTRLPLANE_* environment variables with sensible defaults.
type Config struct {
	Port              int
	LogLevel          string
	LogFormat         string
	SchedulerInterval time.Duration
	ReconcileInterval time.Duration
	HeartbeatTimeout  time.Duration
	ShutdownTimeout   time.Duration
}

func Load() Config {
	return Config{
		Port:              envInt("CTRLPLANE_PORT", 7070),
		LogLevel:          envString("CTRLPLANE_LOG_LEVEL", "info"),
		LogFormat:         envString("CTRLPLANE_LOG_FORMAT", "json"),
		SchedulerInterval: envDuration("CTRLPLANE_SCHEDULER_INTERVAL", 500*time.Millisecond),
		ReconcileInterval: envDuration("CTRLPLANE_RECONCILE_INTERVAL", 2*time.Second),
		HeartbeatTimeout:  envDuration("CTRLPLANE_HEARTBEAT_TIMEOUT", 15*time.Second),
		ShutdownTimeout:   envDuration("CTRLPLANE_SHUTDOWN_TIMEOUT", 10*time.Second),
	}
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
