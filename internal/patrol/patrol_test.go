package patrol

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tpural/upkeeper/internal/tremolo"
)

func TestCheck(t *testing.T) {
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	owned := []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "gridtie-7d9f8"}}
	pod := func(name string, phase corev1.PodPhase, restarts int32, created time.Time) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "gridtie", OwnerReferences: owned, CreationTimestamp: metav1.NewTime(created)},
			Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gridtie", RestartCount: restarts,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}},
			}}},
		}
	}
	k := fake.NewClientset(
		pod("gridtie-7d9f8-a", corev1.PodRunning, 1, now.Add(-24*time.Hour)),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "angelorooms", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
			Status:     corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Message: "0/12 nodes: insufficient memory"}}},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "pi-1"}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "possum.dev/asleep"}}},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "mm02"},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeDiskPressure, Status: corev1.ConditionTrue, Message: "ephemeral storage low"}}},
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "e1", Namespace: "gridtie"}, Type: "Warning", Reason: "Unhealthy", Count: 12, Message: "Readiness probe failed",
			LastTimestamp: metav1.NewTime(now.Add(-time.Minute)), InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "gridtie", Name: "gridtie-7d9f8-a"},
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "e3", Namespace: "gridtie"}, Type: "Warning", Reason: "BackOff", Count: 40, Message: "Back-off restarting",
			LastTimestamp: metav1.NewTime(now.Add(-time.Minute)), InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "gridtie", Name: "deleted-7d9f8-b"},
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "e2", Namespace: "gridtie"}, Type: "Warning", Reason: "Unhealthy", Count: 50,
			LastTimestamp: metav1.NewTime(now.Add(-3 * time.Hour)), InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "gridtie", Name: "old-1-x"},
		},
	)
	c := &Checker{
		K: k, AsleepTaints: []string{"possum.dev/asleep"}, Now: func() time.Time { return now },
		Coverage: func(context.Context) ([]tremolo.Gap, error) {
			return []tremolo.Gap{{Kind: "logs", Subject: "namespace nfs", Detail: "no logs"}, {Kind: "check", Subject: "x", Detail: "failed"}}, nil
		},
	}
	titles := func(fs []Finding) []string {
		var out []string
		for _, f := range fs {
			out = append(out, f.Title)
		}
		slices.Sort(out)
		return out
	}
	got := titles(c.Check(context.Background()))
	want := []string{"Coverage: logs namespace nfs", "Node mm02: DiskPressure", "Pending: angelorooms/pod/web-1", "Warnings: Unhealthy gridtie/pod/gridtie"}
	if !slices.Equal(got, want) {
		t.Errorf("first check %v,\nwant %v", got, want)
	}

	// Restarts count from the last check: 1 → 5 is a finding, with the last exit.
	p, _ := k.CoreV1().Pods("gridtie").Get(context.Background(), "gridtie-7d9f8-a", metav1.GetOptions{})
	p.Status.ContainerStatuses[0].RestartCount = 5
	_, _ = k.CoreV1().Pods("gridtie").UpdateStatus(context.Background(), p, metav1.UpdateOptions{})
	var restarting *Finding
	for _, f := range c.Check(context.Background()) {
		if f.Title == "Restarting: gridtie/deployment/gridtie gridtie" {
			restarting = &f
		}
	}
	if restarting == nil || restarting.Text != "Container gridtie of pod gridtie-7d9f8-a restarted 4 times since the last check. Its last exit: 137 (OOMKilled)." {
		t.Errorf("restarting = %+v", restarting)
	}
}
