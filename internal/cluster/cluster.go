// Package cluster is what upkeeper can see and do in Kubernetes: a few reads,
// and a short list of changes. There's no shell; its ServiceAccount's RBAC is
// the real limit.
package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Cluster wraps a Kubernetes client.
type Cluster struct {
	K   kubernetes.Interface
	Now func() time.Time
}

// New returns a Cluster.
func New(k kubernetes.Interface) *Cluster { return &Cluster{K: k, Now: time.Now} }

// Awake reports whether the deployment ns/name has a ready replica. Reading it
// doesn't wake anything.
func (c *Cluster) Awake(ctx context.Context, deployment string) (bool, error) {
	ns, name, _ := strings.Cut(deployment, "/")
	d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return d.Status.ReadyReplicas > 0, nil
}

// Pods lists a namespace's pods, problems first.
func (c *Cluster) Pods(ctx context.Context, ns string) (string, error) {
	list, err := c.K.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	if len(list.Items) == 0 {
		return "No pods in " + ns + ".", nil
	}
	type row struct {
		bad  bool
		text string
	}
	rows := make([]row, 0, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		ready, total, restarts, why := containers(p)
		bad := p.Status.Phase != corev1.PodRunning && p.Status.Phase != corev1.PodSucceeded || ready < total
		t := fmt.Sprintf("%s %s ready %d/%d restarts %d age %s node %s", p.Name, p.Status.Phase, ready, total, restarts, age(c.Now(), p.CreationTimestamp.Time), p.Spec.NodeName)
		if why != "" {
			t += " — " + why
		}
		rows = append(rows, row{bad, t})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].bad && !rows[j].bad })
	var b strings.Builder
	for _, r := range rows {
		b.WriteString(r.text + "\n")
	}
	return b.String(), nil
}

// Pod describes one pod: its containers' states, conditions and recent warnings.
func (c *Cluster) Pod(ctx context.Context, ns, name string) (string, error) {
	p, err := c.K.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%s %s on %s, started %s, owner %s\n", ns, p.Name, p.Status.Phase, p.Spec.NodeName, age(c.Now(), p.CreationTimestamp.Time), Workload(p))
	if p.Status.Reason != "" || p.Status.Message != "" {
		fmt.Fprintf(&b, "status: %s %s\n", p.Status.Reason, p.Status.Message)
	}
	for _, s := range append(slices.Clone(p.Status.InitContainerStatuses), p.Status.ContainerStatuses...) {
		fmt.Fprintf(&b, "container %s: ready %v, restarts %d, %s", s.Name, s.Ready, s.RestartCount, state(s.State))
		if t := s.LastTerminationState.Terminated; t != nil {
			fmt.Fprintf(&b, "; last exit %d (%s) %s ago %s", t.ExitCode, t.Reason, age(c.Now(), t.FinishedAt.Time), strings.TrimSpace(t.Message))
		}
		b.WriteString("\n")
	}
	for _, ct := range p.Spec.Containers {
		var lim []string
		for k, v := range ct.Resources.Limits {
			lim = append(lim, string(k)+"="+v.String())
		}
		sort.Strings(lim)
		if len(lim) > 0 {
			fmt.Fprintf(&b, "container %s limits: %s\n", ct.Name, strings.Join(lim, " "))
		}
	}
	for _, cd := range p.Status.Conditions {
		if cd.Status != corev1.ConditionTrue {
			fmt.Fprintf(&b, "condition %s=%s %s %s\n", cd.Type, cd.Status, cd.Reason, cd.Message)
		}
	}
	ev, err := c.events(ctx, ns, "involvedObject.name="+name)
	if err == nil && ev != "" {
		b.WriteString("warnings:\n" + ev)
	}
	return b.String(), nil
}

// Logs returns a container's last lines; previous is the run before its last restart.
func (c *Cluster) Logs(ctx context.Context, ns, pod, container string, previous bool, lines int64) (string, error) {
	if lines <= 0 || lines > 200 {
		lines = 80
	}
	rc, err := c.K.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: container, Previous: previous, TailLines: &lines}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var b strings.Builder
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		l := sc.Text()
		if len(l) > 400 {
			l = l[:400] + "…"
		}
		b.WriteString(l + "\n")
	}
	if b.Len() == 0 {
		return "No log lines.", sc.Err()
	}
	return b.String(), sc.Err()
}

// Events lists a namespace's warnings of the last hour, grouped.
func (c *Cluster) Events(ctx context.Context, ns string) (string, error) {
	out, err := c.events(ctx, ns, "type=Warning")
	if out == "" && err == nil {
		return "No warnings in the last hour in " + ns + ".", nil
	}
	return out, err
}

func (c *Cluster) events(ctx context.Context, ns, selector string) (string, error) {
	list, err := c.K.CoreV1().Events(ns).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err != nil {
		return "", err
	}
	since := c.Now().Add(-time.Hour)
	type group struct {
		n    int32
		last time.Time
		text string
	}
	groups := map[string]*group{}
	for i := range list.Items {
		e := &list.Items[i]
		if e.Type != corev1.EventTypeWarning {
			continue
		}
		at := lastSeen(e)
		if at.Before(since) {
			continue
		}
		k := e.InvolvedObject.Kind + "/" + e.InvolvedObject.Name + " " + e.Reason
		g := groups[k]
		if g == nil {
			g = &group{}
			groups[k] = g
		}
		g.n += max(e.Count, 1)
		if at.After(g.last) {
			g.last, g.text = at, e.Message
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return groups[keys[i]].last.After(groups[keys[j]].last) })
	var b strings.Builder
	for i, k := range keys {
		if i == 30 {
			fmt.Fprintf(&b, "… %d more\n", len(keys)-30)
			break
		}
		g := groups[k]
		fmt.Fprintf(&b, "%s ×%d, last %s ago: %s\n", k, g.n, age(c.Now(), g.last), clip(g.text, 250))
	}
	return b.String(), nil
}

// Rollout says how a deployment, statefulset or daemonset is doing.
func (c *Cluster) Rollout(ctx context.Context, ns, kind, name string) (string, error) {
	switch kind {
	case "deployment":
		d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		s := d.Status
		out := fmt.Sprintf("deployment %s/%s: want %d, ready %d, up to date %d, available %d, unavailable %d", ns, name, ptr(d.Spec.Replicas, 1), s.ReadyReplicas, s.UpdatedReplicas, s.AvailableReplicas, s.UnavailableReplicas)
		for _, cd := range s.Conditions {
			if cd.Status != corev1.ConditionTrue || cd.Type == appsv1.DeploymentProgressing && cd.Reason != "NewReplicaSetAvailable" {
				out += fmt.Sprintf("\n%s=%s %s %s", cd.Type, cd.Status, cd.Reason, cd.Message)
			}
		}
		return out, nil
	case "statefulset":
		s, err := c.K.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("statefulset %s/%s: want %d, ready %d, current %d, updated %d", ns, name, ptr(s.Spec.Replicas, 1), s.Status.ReadyReplicas, s.Status.CurrentReplicas, s.Status.UpdatedReplicas), nil
	case "daemonset":
		d, err := c.K.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		s := d.Status
		return fmt.Sprintf("daemonset %s/%s: want %d, ready %d, updated %d, unavailable %d", ns, name, s.DesiredNumberScheduled, s.NumberReady, s.UpdatedNumberScheduled, s.NumberUnavailable), nil
	}
	return "", fmt.Errorf("kind must be deployment, statefulset or daemonset")
}

// Restart rolls a workload's pods, as kubectl rollout restart does.
func (c *Cluster) Restart(ctx context.Context, ns, kind, name string) error {
	patch := fmt.Appendf(nil, `{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, c.Now().Format(time.RFC3339))
	var err error
	switch kind {
	case "deployment":
		_, err = c.K.AppsV1().Deployments(ns).Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	case "statefulset":
		_, err = c.K.AppsV1().StatefulSets(ns).Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	case "daemonset":
		_, err = c.K.AppsV1().DaemonSets(ns).Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	default:
		err = errors.New("kind must be deployment, statefulset or daemonset")
	}
	return err
}

// DeletePod deletes a pod so its controller makes a new one.
func (c *Cluster) DeletePod(ctx context.Context, ns, name string) error {
	p, err := c.K.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if len(p.OwnerReferences) == 0 {
		return errors.New("this pod has no controller, so it wouldn't come back; not deleting it")
	}
	return c.K.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
}

// Scale sets a deployment's or statefulset's replicas.
func (c *Cluster) Scale(ctx context.Context, ns, kind, name string, replicas int32) error {
	if replicas < 0 || replicas > 10 {
		return errors.New("replicas must be 0 to 10")
	}
	patch := fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, replicas)
	var err error
	switch kind {
	case "deployment":
		_, err = c.K.AppsV1().Deployments(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	case "statefulset":
		_, err = c.K.AppsV1().StatefulSets(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	default:
		err = errors.New("kind must be deployment or statefulset")
	}
	return err
}

// Cordon marks a node (un)schedulable. It doesn't evict anything.
func (c *Cluster) Cordon(ctx context.Context, node string, cordon bool) error {
	patch := fmt.Appendf(nil, `{"spec":{"unschedulable":%v}}`, cordon)
	_, err := c.K.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// Workload names what owns a pod, e.g. deployment/gridtie.
func Workload(p *corev1.Pod) string {
	for _, o := range p.OwnerReferences {
		switch o.Kind {
		case "ReplicaSet":
			if i := strings.LastIndex(o.Name, "-"); i > 0 {
				return "deployment/" + o.Name[:i]
			}
			return "replicaset/" + o.Name
		case "Job":
			if i := strings.LastIndex(o.Name, "-"); i > 0 {
				return "cronjob/" + o.Name[:i]
			}
			return "job/" + o.Name
		default:
			return strings.ToLower(o.Kind) + "/" + o.Name
		}
	}
	return "pod/" + p.Name
}

func containers(p *corev1.Pod) (ready, total int, restarts int32, why string) {
	for _, s := range p.Status.ContainerStatuses {
		total++
		if s.Ready {
			ready++
		}
		restarts += s.RestartCount
		if w := s.State.Waiting; w != nil && why == "" {
			why = s.Name + " " + w.Reason
		} else if t := s.LastTerminationState.Terminated; t != nil && why == "" && s.RestartCount > 0 {
			why = fmt.Sprintf("%s last exit %d %s", s.Name, t.ExitCode, t.Reason)
		}
	}
	if total == 0 {
		total = len(p.Spec.Containers)
	}
	if why == "" && p.Status.Phase == corev1.PodPending {
		for _, cd := range p.Status.Conditions {
			if cd.Type == corev1.PodScheduled && cd.Status != corev1.ConditionTrue {
				why = "unschedulable: " + clip(cd.Message, 200)
			}
		}
	}
	return ready, total, restarts, why
}

func state(s corev1.ContainerState) string {
	switch {
	case s.Waiting != nil:
		return "waiting " + s.Waiting.Reason + " " + clip(s.Waiting.Message, 200)
	case s.Terminated != nil:
		return fmt.Sprintf("terminated exit %d %s", s.Terminated.ExitCode, s.Terminated.Reason)
	case s.Running != nil:
		return "running since " + s.Running.StartedAt.UTC().Format(time.RFC3339)
	}
	return "unknown"
}

func lastSeen(e *corev1.Event) time.Time {
	switch {
	case !e.LastTimestamp.IsZero():
		return e.LastTimestamp.Time
	case e.Series != nil:
		return e.Series.LastObservedTime.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	}
	return e.CreationTimestamp.Time
}

func age(now, t time.Time) string {
	if t.IsZero() {
		return "?"
	}
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func ptr(p *int32, def int32) int32 {
	if p == nil {
		return def
	}
	return *p
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
