package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCluster(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	one := int32(1)
	k := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "ok-1", Namespace: "a", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "c", Ready: true}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-1", Namespace: "a", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "bad-5f6"}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "c", RestartCount: 7, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}}},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "llama-swap", Namespace: "llama-swap"}, Spec: appsv1.DeploymentSpec{Replicas: &one}},
	)
	c := &Cluster{K: k, Now: func() time.Time { return now }}

	out, err := c.Pods(ctx, "a")
	if err != nil || !strings.HasPrefix(out, "bad-1 Running ready 0/1 restarts 7") || !strings.Contains(out, "c CrashLoopBackOff") {
		t.Errorf("pods = %q, %v", out, err)
	}
	out, _ = c.Pod(ctx, "a", "bad-1")
	if !strings.Contains(out, "owner deployment/bad") || !strings.Contains(out, "last exit 1 (Error)") {
		t.Errorf("pod = %q", out)
	}
	if awake, _ := c.Awake(ctx, "llama-swap/llama-swap"); awake {
		t.Error("awake with no ready replicas")
	}
	if err := c.DeletePod(ctx, "a", "ok-1"); err == nil || !strings.Contains(err.Error(), "no controller") {
		t.Errorf("deleted an ownerless pod: %v", err)
	}
	if err := c.DeletePod(ctx, "a", "bad-1"); err != nil {
		t.Error(err)
	}
	if err := c.Scale(ctx, "llama-swap", "deployment", "llama-swap", 11); err == nil {
		t.Error("scaled to 11")
	}
	if err := c.Scale(ctx, "llama-swap", "deployment", "llama-swap", 0); err != nil {
		t.Error(err)
	}
}

func TestSpecAndNodes(t *testing.T) {
	ctx := context.Background()
	k := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "billing", Namespace: "a"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"disktype": "nvme"},
			Tolerations:  []corev1.Toleration{{Key: "possum.dev/asleep", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			Containers: []corev1.Container{{
				Name: "billing", Image: "busybox:1.37.0", Command: []string{"sh", "-c", "run"},
				Env: []corev1.EnvVar{
					{Name: "LOG_LEVEL", Value: "debug"},
					{Name: "DB_PASSWORD", Value: "hunter2"},
					{Name: "LEDGER_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "ledger"}, Key: "token"}}},
				},
			}},
		}}}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "pi-1", Labels: map[string]string{"kubernetes.io/arch": "arm64", "kubernetes.io/hostname": "pi-1", "node-role.kubernetes.io/control-plane": "", "zone": "rack"}},
			Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule}}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
	)
	c := &Cluster{K: k, Now: time.Now}
	out, err := c.Spec(ctx, "a", "deployment", "billing")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"image busybox:1.37.0", "runs: sh -c run", "LOG_LEVEL=debug", "DB_PASSWORD=(set, hidden)",
		"LEDGER_TOKEN from secret ledger key token", "nodeSelector: disktype=nvme", "tolerations: possum.dev/asleep Exists  NoSchedule",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("spec lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hunter2") {
		t.Error("spec shows a secret-looking value")
	}
	nodes, _ := c.Nodes(ctx)
	if want := "pi-1 Ready labels: kubernetes.io/arch=arm64 node-role.kubernetes.io/control-plane= zone=rack taints: node-role.kubernetes.io/control-plane:NoSchedule\n"; nodes != want {
		t.Errorf("nodes = %q, want %q", nodes, want)
	}
}
