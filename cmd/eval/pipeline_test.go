package main

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestAhead(t *testing.T) {
	var jobs []*job
	for _, settle := range []string{"2m", "6m", "4m", "10m", "90s", "90s", "2m", "10m"} {
		jobs = append(jobs, &job{s: Scenario{Settle: settle}})
	}
	p := &pipeline{jobs: jobs, slot: 90 * time.Second, prepare: func(context.Context, Scenario, int) prepared { return prepared{} }}
	started := func() (n []int) {
		for i, j := range jobs {
			if j.ready != nil {
				n = append(n, i)
			}
		}
		return n
	}
	// Each is started once the runs ahead of it, at 90s each, take no longer
	// than its settle time and a minute to set up.
	p.ahead(context.Background(), 0, 0)
	if got := started(); !slices.Equal(got, []int{1, 2, 3, 7}) {
		t.Errorf("started %v", got)
	}
	// The short ones start as their turn nears.
	p.ahead(context.Background(), 2, 30*time.Second)
	if jobs[4].ready == nil || jobs[5].ready != nil {
		t.Errorf("then started %v", started())
	}
	p.sequential = true
	jobs[5].ready = nil
	p.ahead(context.Background(), 4, time.Hour)
	if jobs[5].ready != nil {
		t.Error("sequential started ahead")
	}
}
