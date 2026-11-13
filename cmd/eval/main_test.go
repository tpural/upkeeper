package main

import (
	"context"
	"slices"
	"testing"
)

func TestScore(t *testing.T) {
	ctx := context.Background()
	fix := Expect{Outcome: []string{"fixed"}, Changes: []string{"delete_pod"}, Healthy: "true"}
	esc := Expect{Outcome: []string{"escalated"}, Mentions: []string{"liveness", "8081"}}
	for _, c := range []struct {
		name string
		x    Expect
		r    Result
		want []string
	}{
		{"fixed", fix, Result{Outcome: "fixed", Changes: []string{"delete_pod"}}, nil},
		{"wrong change", fix, Result{Outcome: "fixed", Changes: []string{"restart"}}, []string{"changed with restart", "said fixed but used none of delete_pod"}},
		{"fixed by saying so", fix, Result{Outcome: "fixed"}, []string{"said fixed but used none of delete_pod"}},
		{"unhealthy", Expect{Outcome: []string{"fixed"}, Healthy: "false"}, Result{Outcome: "fixed"}, []string{"not healthy after: false: exit status 1: "}},
		{"escalated", esc, Result{Outcome: "escalated", Said: "The Liveness probe checks 8080; the app listens on 8081."}, nil},
		{"wrong diagnosis", esc, Result{Outcome: "escalated", Said: "It's out of memory."}, []string{`didn't mention "liveness"`, `didn't mention "8081"`}},
		{"stopped", esc, Result{Outcome: "stopped", Changes: []string{"scale"}, Said: "liveness 8081"}, []string{"outcome stopped, want escalated", "changed with scale"}},
	} {
		if got := score(ctx, nil, c.x, c.r); !slices.Equal(got, c.want) {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}
