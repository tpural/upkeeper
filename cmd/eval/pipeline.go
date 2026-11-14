package main

import (
	"context"
	"sync"
	"time"
)

// job is one run of a scenario.
type job struct {
	s     Scenario
	n     int
	ready chan prepared // its scenario, once broken; nil until started
}

// pipeline breaks each scenario ahead of its turn, so it's done settling
// about when upkeeper is free for it: the model never waits on a scenario's
// settle time, and no scenario waits long past it.
type pipeline struct {
	e          *evaluator
	prepare    func(ctx context.Context, s Scenario, n int) prepared
	jobs       []*job
	slot       time.Duration // how long a run takes upkeeper, learnt as it goes
	sequential bool
	cleanup    sync.WaitGroup
	took       []time.Duration
}

// work runs job i, starting the scenarios of later jobs as their turn nears.
func (p *pipeline) work(ctx context.Context, i int) Result {
	// A run's slot is its whole turn: waiting on its scenario, upkeeper
	// picking it up and the run itself.
	began := time.Now()
	j := p.jobs[i]
	p.start(ctx, j)
	p.ahead(ctx, i, 0)
	pr := <-j.ready
	r := p.e.run(ctx, j.s, j.n, pr, func() { p.ahead(ctx, i, time.Since(began)) })
	if r.Investigation != 0 {
		p.took = append(p.took, time.Since(began))
	}
	// Namespaces go in the background: the next run needn't wait for them.
	p.cleanup.Go(func() {
		_ = sh(context.WithoutCancel(ctx), pr.env, "kubectl delete namespace "+pr.ns+" --ignore-not-found --wait --timeout=5m")
	})
	return r
}

// ahead starts each later job whose scenario needs to settle for longer than
// the runs before it will take, given job i has been running for elapsed.
func (p *pipeline) ahead(ctx context.Context, i int, elapsed time.Duration) {
	if p.sequential {
		return
	}
	slot := p.average()
	for k := i + 1; k < len(p.jobs); k++ {
		j := p.jobs[k]
		if j.ready != nil {
			continue
		}
		d, _ := settle(j.s)
		// Setting up takes a while too; a scenario that's ready a little early
		// is better than the model idling.
		until := time.Duration(k-i)*slot - elapsed - time.Minute
		if d >= until {
			p.start(ctx, j)
		}
	}
}

func (p *pipeline) start(ctx context.Context, j *job) {
	if j.ready != nil {
		return
	}
	j.ready = make(chan prepared, 1)
	prepare := p.prepare
	if prepare == nil {
		prepare = p.e.prepare
	}
	go func() { j.ready <- prepare(ctx, j.s, j.n) }()
}

// average is the mean time of the last 8 runs, or the first guess.
func (p *pipeline) average() time.Duration {
	if len(p.took) == 0 {
		return p.slot
	}
	last := p.took[max(0, len(p.took)-8):]
	var sum time.Duration
	for _, d := range last {
		sum += d
	}
	return sum / time.Duration(len(last))
}
