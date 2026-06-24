package model

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransitionWorkload(t *testing.T) {
	allStatuses := []WorkloadStatus{WorkloadPending, WorkloadActive, WorkloadDegraded, WorkloadCancelled}

	valid := map[WorkloadStatus]map[WorkloadStatus]bool{
		WorkloadPending:   {WorkloadActive: true, WorkloadCancelled: true},
		WorkloadActive:    {WorkloadDegraded: true, WorkloadCancelled: true},
		WorkloadDegraded:  {WorkloadActive: true, WorkloadCancelled: true},
		WorkloadCancelled: {},
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			from, to := from, to
			want := valid[from][to]
			t.Run(fmt.Sprintf("%s_to_%s", from, to), func(t *testing.T) {
				assert.Equal(t, want, TransitionWorkload(from, to))
			})
		}
	}
}

func TestWorkloadClone(t *testing.T) {
	w := Workload{ID: "w1", Args: []string{"a"}}
	clone := w.Clone()
	clone.Args[0] = "mutated"
	assert.Equal(t, "a", w.Args[0])
}
