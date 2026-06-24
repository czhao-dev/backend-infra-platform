package reconciler

import (
	"math"
	"time"
)

const maxBackoff = 60 * time.Second

// backoff returns the exponential-backoff delay for the given (post-
// increment) attempt count: 2^attempt seconds, capped at 60s. Same formula
// as ml-job-orchestrator/internal/retry/retry.go's ScheduleRetry,
// reimplemented here rather than imported to keep the two modules
// independent.
func backoff(attempt int) time.Duration {
	d := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
