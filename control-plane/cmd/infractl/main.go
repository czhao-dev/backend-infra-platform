// infractl is a stdlib-net/http-only CLI client for the control-plane REST
// API, modeled on ml-job-orchestrator's mlctl but with a two-level
// noun/verb command tree (workload/worker/route/cluster/scheduler).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/czhao-dev/control-plane/internal/model"
	"gopkg.in/yaml.v3"
)

func serverURL() string {
	if v := os.Getenv("INFRACTL_SERVER"); v != "" {
		return v
	}
	return "http://localhost:7070"
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}
	noun, verb, rest := os.Args[1], os.Args[2], os.Args[3:]

	switch noun {
	case "workload":
		dispatchWorkload(verb, rest)
	case "worker":
		dispatchWorker(verb, rest)
	case "route":
		dispatchRoute(verb, rest)
	case "cluster":
		dispatchCluster(verb, rest)
	case "scheduler":
		dispatchScheduler(verb, rest)
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `infractl <noun> <verb> [args]

  workload submit <file.yaml>
  workload list
  workload status <id>
  workload cancel <id>

  worker list
  worker status <id>
  worker drain <id>

  route list
  route add <file.yaml>
  route status <id>

  cluster status
  scheduler stats

Set INFRACTL_SERVER (default http://localhost:7070) to point at a different control plane.`)
}

// --- workload ---

func dispatchWorkload(verb string, args []string) {
	switch verb {
	case "submit":
		cmdWorkloadSubmit(args)
	case "list":
		cmdWorkloadList()
	case "status":
		cmdWorkloadStatus(args)
	case "cancel":
		cmdWorkloadCancel(args)
	default:
		usage()
		os.Exit(1)
	}
}

func cmdWorkloadSubmit(args []string) {
	if len(args) < 1 {
		fatalf("workload submit: file path required")
	}
	body := readYAMLAsJSON(args[0])

	resp, err := http.Post(serverURL()+"/api/v1/workloads", "application/json", bytes.NewReader(body))
	if err != nil {
		fatalf("submit failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		fatalf("submit failed: %s", readErrorBody(resp))
	}
	var w model.Workload
	if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
		fatalf("submit: failed to decode response: %v", err)
	}
	fmt.Printf("Submitted %s (%s)\n", w.ID, w.Name)
}

func cmdWorkloadList() {
	var result struct {
		Workloads []model.Workload `json:"workloads"`
		Total     int              `json:"total"`
	}
	getJSON("/api/v1/workloads", &result)

	fmt.Printf("%-20s %-16s %-10s %-10s %-10s\n", "ID", "NAME", "TYPE", "STATUS", "REPLICAS")
	for _, w := range result.Workloads {
		fmt.Printf("%-20s %-16s %-10s %-10s %-10d\n", w.ID, w.Name, w.Type, w.Status, w.Replicas)
	}
	fmt.Printf("\n%d total\n", result.Total)
}

func cmdWorkloadStatus(args []string) {
	if len(args) < 1 {
		fatalf("workload status: id required")
	}
	var w model.Workload
	getJSON("/api/v1/workloads/"+args[0], &w)

	fmt.Printf("ID:       %s\n", w.ID)
	fmt.Printf("Name:     %s\n", w.Name)
	fmt.Printf("Type:     %s\n", w.Type)
	fmt.Printf("Status:   %s\n", w.Status)
	fmt.Printf("Replicas: %d\n", w.Replicas)

	var jobsResult struct {
		Jobs  []model.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	getJSON("/api/v1/workloads/"+args[0]+"/jobs", &jobsResult)

	fmt.Printf("\nJobs (%d):\n", jobsResult.Total)
	fmt.Printf("%-16s %-16s %-12s %-8s\n", "ID", "WORKER", "STATUS", "ATTEMPT")
	for _, j := range jobsResult.Jobs {
		fmt.Printf("%-16s %-16s %-12s %-8d\n", j.ID, j.WorkerID, j.Status, j.Attempt)
	}
}

func cmdWorkloadCancel(args []string) {
	if len(args) < 1 {
		fatalf("workload cancel: id required")
	}
	doRequest(http.MethodDelete, "/api/v1/workloads/"+args[0], nil, http.StatusOK)
	fmt.Printf("Cancelled %s\n", args[0])
}

// --- worker ---

func dispatchWorker(verb string, args []string) {
	switch verb {
	case "list":
		cmdWorkerList()
	case "status":
		cmdWorkerStatus(args)
	case "drain":
		cmdWorkerDrain(args)
	default:
		usage()
		os.Exit(1)
	}
}

func cmdWorkerList() {
	var result struct {
		Workers []model.Worker `json:"workers"`
		Total   int            `json:"total"`
	}
	getJSON("/api/v1/workers", &result)

	fmt.Printf("%-16s %-24s %-10s %-8s %-8s\n", "ID", "ADDRESS", "STATUS", "RUNNING", "MAX")
	for _, w := range result.Workers {
		fmt.Printf("%-16s %-24s %-10s %-8d %-8d\n", w.ID, w.Address, w.Status, w.RunningJobs, w.MaxConcurrent)
	}
	fmt.Printf("\n%d total\n", result.Total)
}

func cmdWorkerStatus(args []string) {
	if len(args) < 1 {
		fatalf("worker status: id required")
	}
	var w model.Worker
	getJSON("/api/v1/workers/"+args[0], &w)

	fmt.Printf("ID:            %s\n", w.ID)
	fmt.Printf("Hostname:      %s\n", w.Hostname)
	fmt.Printf("Address:       %s\n", w.Address)
	fmt.Printf("Status:        %s\n", w.Status)
	fmt.Printf("Running Jobs:  %d / %d\n", w.RunningJobs, w.MaxConcurrent)
	fmt.Printf("Capacity:      cpu=%.2f memory_mb=%d\n", w.Capacity.CPU, w.Capacity.MemoryMB)
	fmt.Printf("Available:     cpu=%.2f memory_mb=%d\n", w.Available.CPU, w.Available.MemoryMB)
	fmt.Printf("Last Heartbeat: %s\n", w.LastHeartbeatAt.Format(time.RFC3339))
}

func cmdWorkerDrain(args []string) {
	if len(args) < 1 {
		fatalf("worker drain: id required")
	}
	doRequest(http.MethodPost, "/api/v1/workers/"+args[0]+"/drain", nil, http.StatusOK)
	fmt.Printf("Draining %s\n", args[0])
}

// --- route ---

func dispatchRoute(verb string, args []string) {
	switch verb {
	case "list":
		cmdRouteList()
	case "add":
		cmdRouteAdd(args)
	case "status":
		cmdRouteStatus(args)
	default:
		usage()
		os.Exit(1)
	}
}

func cmdRouteList() {
	var result struct {
		Routes []model.Route `json:"routes"`
		Total  int           `json:"total"`
	}
	getJSON("/api/v1/routes", &result)

	fmt.Printf("%-16s %-16s %-16s %-16s\n", "ID", "NAME", "PATH_PREFIX", "STRATEGY")
	for _, r := range result.Routes {
		fmt.Printf("%-16s %-16s %-16s %-16s\n", r.ID, r.Name, r.PathPrefix, r.Strategy)
	}
	fmt.Printf("\n%d total\n", result.Total)
}

func cmdRouteAdd(args []string) {
	if len(args) < 1 {
		fatalf("route add: file path required")
	}
	body := readYAMLAsJSON(args[0])
	resp, err := http.Post(serverURL()+"/api/v1/routes", "application/json", bytes.NewReader(body))
	if err != nil {
		fatalf("route add failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		fatalf("route add failed: %s", readErrorBody(resp))
	}
	var r model.Route
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		fatalf("route add: failed to decode response: %v", err)
	}
	fmt.Printf("Added route %s (%s)\n", r.ID, r.Name)
}

func cmdRouteStatus(args []string) {
	if len(args) < 1 {
		fatalf("route status: id required")
	}
	var r model.Route
	getJSON("/api/v1/routes/"+args[0], &r)
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
}

// --- cluster / scheduler ---

func dispatchCluster(verb string, _ []string) {
	if verb != "status" {
		usage()
		os.Exit(1)
	}
	cmdClusterStatus()
}

func cmdClusterStatus() {
	var workloads struct {
		Total int `json:"total"`
	}
	getJSON("/api/v1/workloads", &workloads)

	var workers struct {
		Workers []model.Worker `json:"workers"`
		Total   int            `json:"total"`
	}
	getJSON("/api/v1/workers", &workers)

	var routes struct {
		Total int `json:"total"`
	}
	getJSON("/api/v1/routes", &routes)

	healthy := 0
	for _, w := range workers.Workers {
		if w.Status == model.WorkerHealthy {
			healthy++
		}
	}

	fmt.Printf("Control plane: %s\n", serverURL())
	fmt.Printf("Workloads: %d\n", workloads.Total)
	fmt.Printf("Workers:   %d (%d healthy)\n", workers.Total, healthy)
	fmt.Printf("Routes:    %d\n", routes.Total)
}

func dispatchScheduler(verb string, _ []string) {
	if verb != "stats" {
		usage()
		os.Exit(1)
	}
	var raw map[string]any
	getJSON("/api/v1/scheduler/stats", &raw)
	b, _ := json.MarshalIndent(raw, "", "  ")
	fmt.Println(string(b))
}

// --- shared helpers ---

func readYAMLAsJSON(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		fatalf("failed to read %s: %v", path, err)
	}
	var generic map[string]any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		fatalf("failed to parse YAML %s: %v", path, err)
	}
	body, err := json.Marshal(generic)
	if err != nil {
		fatalf("failed to re-encode %s as JSON: %v", path, err)
	}
	return body
}

func getJSON(path string, out any) {
	resp, err := http.Get(serverURL() + path)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fatalf("request failed: %s", readErrorBody(resp))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		fatalf("failed to decode response: %v", err)
	}
}

func doRequest(method, path string, body []byte, wantStatus int) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, serverURL()+path, reader)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		fatalf("request failed: %s", readErrorBody(resp))
	}
}

func readErrorBody(resp *http.Response) string {
	b, _ := io.ReadAll(resp.Body)
	return fmt.Sprintf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
