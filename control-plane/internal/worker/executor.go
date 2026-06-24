package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/czhao-dev/control-plane/internal/agentmetrics"
	"github.com/czhao-dev/control-plane/internal/model"
)

// runJob executes job as a subprocess and reports its outcome back to the
// control plane. Pattern-matches (does not import)
// ml-job-orchestrator/internal/executor/executor.go's exec.CommandContext +
// bytes.Buffer approach, freshly written here since the two modules stay
// independent.
func (a *Agent) runJob(ctx context.Context, job model.Job) {
	a.reportStatus(job.ID, model.JobRunning, nil, "", "")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var buf bytes.Buffer
	cmd := exec.CommandContext(runCtx, job.Command, job.Args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	runErr := cmd.Run()
	output := buf.String()

	switch {
	case ctx.Err() != nil:
		a.reportStatus(job.ID, model.JobCancelled, nil, "worker shutting down", output)
	case runErr != nil:
		exitCode := exitCodeOf(runErr)
		a.reportStatus(job.ID, model.JobFailed, exitCode, runErr.Error(), output)
		agentmetrics.WorkerFailedJobsTotal.Inc()
	default:
		zero := 0
		a.reportStatus(job.ID, model.JobSucceeded, &zero, "", output)
		agentmetrics.WorkerCompletedJobsTotal.Inc()
	}
}

func exitCodeOf(err error) *int {
	if exitErr, ok := err.(*exec.ExitError); ok {
		code := exitErr.ExitCode()
		return &code
	}
	return nil
}

type jobStatusRequest struct {
	Status   model.JobStatus `json:"status"`
	ExitCode *int            `json:"exit_code,omitempty"`
	Error    string          `json:"error,omitempty"`
	Output   string          `json:"output,omitempty"`
}

func (a *Agent) reportStatus(jobID string, status model.JobStatus, exitCode *int, errMsg, output string) {
	body, _ := json.Marshal(jobStatusRequest{Status: status, ExitCode: exitCode, Error: errMsg, Output: output})
	path := "/api/v1/workers/" + a.id + "/jobs/" + jobID + "/status"
	if err := a.post(context.Background(), path, body, 200, nil); err != nil {
		a.logger.Warn("report job status failed", "job_id", jobID, "status", status, "error", err)
	}
}
