// Package cluster is what upkeeper can see and do in Kubernetes: a few reads,
// and a short list of changes. There's no shell; its ServiceAccount's RBAC is
// the real limit.
package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
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
		ready, total, restarts, why := containers(p, c.Now())
		bad := p.Status.Phase != corev1.PodRunning && p.Status.Phase != corev1.PodSucceeded || ready < total
		count := fmt.Sprint(restarts)
		if last := lastRestart(p); !last.IsZero() {
			// A count is over the pod's life; whether it's still happening is when.
			count += " (last " + age(c.Now(), last) + " ago)"
		}
		t := fmt.Sprintf("%s %s ready %d/%d restarts %s age %s node %s", p.Name, p.Status.Phase, ready, total, count, age(c.Now(), p.CreationTimestamp.Time), p.Spec.NodeName)
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
	// Each object and reason, with its distinct messages: a generic one, like
	// "Error: ImagePullBackOff", mustn't hide the one that says why.
	type message struct {
		n    int32
		last time.Time
	}
	type group struct {
		n    int32
		last time.Time
		msgs map[string]*message
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
			g = &group{msgs: map[string]*message{}}
			groups[k] = g
		}
		n := max(e.Count, 1)
		g.n += n
		g.last = maxTime(g.last, at)
		m := g.msgs[e.Message]
		if m == nil {
			m = &message{}
			g.msgs[e.Message] = m
		}
		m.n += n
		m.last = maxTime(m.last, at)
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
		texts := make([]string, 0, len(g.msgs))
		for t := range g.msgs {
			texts = append(texts, t)
		}
		sort.Slice(texts, func(i, j int) bool { return g.msgs[texts[i]].last.After(g.msgs[texts[j]].last) })
		if len(texts) == 1 {
			fmt.Fprintf(&b, "%s ×%d, last %s ago: %s\n", k, g.n, age(c.Now(), g.last), clip(texts[0], 300))
			continue
		}
		fmt.Fprintf(&b, "%s ×%d, last %s ago:\n", k, g.n, age(c.Now(), g.last))
		for _, t := range texts[:min(len(texts), 3)] {
			fmt.Fprintf(&b, "  - ×%d: %s\n", g.msgs[t].n, clip(t, 300))
		}
	}
	return b.String(), nil
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
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

// lastRestart is when a container of p last stopped, or zero.
func lastRestart(p *corev1.Pod) time.Time {
	var last time.Time
	for _, s := range p.Status.ContainerStatuses {
		if t := s.LastTerminationState.Terminated; t != nil && s.RestartCount > 0 && t.FinishedAt.After(last) {
			last = t.FinishedAt.Time
		}
	}
	return last
}

func containers(p *corev1.Pod, now time.Time) (ready, total int, restarts int32, why string) {
	for _, s := range p.Status.ContainerStatuses {
		total++
		if s.Ready {
			ready++
		}
		restarts += s.RestartCount
		if w := s.State.Waiting; w != nil && why == "" {
			why = s.Name + " " + w.Reason
		} else if t := s.LastTerminationState.Terminated; t != nil && why == "" && s.RestartCount > 0 {
			// When matters as much as why: restarts count over the pod's life.
			why = fmt.Sprintf("%s last exit %d %s %s ago", s.Name, t.ExitCode, t.Reason, age(now, t.FinishedAt.Time))
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

// reSecretName is an env var whose literal value isn't shown.
var reSecretName = regexp.MustCompile(`(?i)pass|secret|token|key|cred|auth|private|cert`)

// Spec describes what a deployment, statefulset, daemonset or pod asks for:
// containers (image, command, env names and where each comes from, resources),
// scheduling (nodeSelector, affinity, tolerations) and volumes. Secret values
// are never read; a literal value with a secret-like name is hidden.
// notes are a workload's annotations that people wrote, not the ones tools
// keep: they often say why it's set up the way it is.
func notes(ann map[string]string) string {
	var out []string
	for k, v := range ann {
		domain, _, ok := strings.Cut(k, "/")
		if ok && slices.ContainsFunc([]string{"kubernetes.io", "k8s.io", "fluxcd.io", "helm.sh"}, func(d string) bool {
			return domain == d || strings.HasSuffix(domain, "."+d)
		}) {
			continue
		}
		out = append(out, fmt.Sprintf("%s=%q", k, clip(v, 200)))
	}
	if len(out) == 0 {
		return ""
	}
	slices.Sort(out)
	return "annotations: " + strings.Join(out, " ") + "\n"
}

// autoscaled says whether an HPA sets the workload's replicas.
func (c *Cluster) autoscaled(ctx context.Context, ns, kind, name string) string {
	hpas, err := c.K.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	for _, h := range hpas.Items {
		if r := h.Spec.ScaleTargetRef; r.Kind == kind && r.Name == name {
			return fmt.Sprintf("autoscaled by HPA %s: %d to %d replicas, now wants %d\n", h.Name, ptr(h.Spec.MinReplicas, 1), h.Spec.MaxReplicas, h.Status.DesiredReplicas)
		}
	}
	return "no HPA scales it\n"
}

func (c *Cluster) Spec(ctx context.Context, ns, kind, name string) (string, error) {
	var spec corev1.PodSpec
	var b strings.Builder
	switch kind {
	case "deployment":
		d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		spec = d.Spec.Template.Spec
		fmt.Fprintf(&b, "deployment %s/%s, replicas %d\n", ns, name, ptr(d.Spec.Replicas, 1))
		b.WriteString(notes(d.Annotations))
		b.WriteString(c.autoscaled(ctx, ns, "Deployment", name))
	case "statefulset":
		s, err := c.K.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		spec = s.Spec.Template.Spec
		fmt.Fprintf(&b, "statefulset %s/%s, replicas %d\n", ns, name, ptr(s.Spec.Replicas, 1))
		b.WriteString(notes(s.Annotations))
		b.WriteString(c.autoscaled(ctx, ns, "StatefulSet", name))
	case "daemonset":
		d, err := c.K.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		spec = d.Spec.Template.Spec
		fmt.Fprintf(&b, "daemonset %s/%s\n", ns, name)
		b.WriteString(notes(d.Annotations))
	case "pod":
		p, err := c.K.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		spec = p.Spec
		fmt.Fprintf(&b, "pod %s/%s\n", ns, name)
	default:
		return "", errors.New("kind must be deployment, statefulset, daemonset or pod")
	}
	for _, ct := range append(slices.Clone(spec.InitContainers), spec.Containers...) {
		fmt.Fprintf(&b, "container %s: image %s\n", ct.Name, ct.Image)
		if len(ct.Command)+len(ct.Args) > 0 {
			fmt.Fprintf(&b, "  runs: %s\n", clip(strings.Join(append(slices.Clone(ct.Command), ct.Args...), " "), 300))
		}
		var env []string
		for _, e := range ct.Env {
			switch v := e.ValueFrom; {
			case v == nil && reSecretName.MatchString(e.Name):
				env = append(env, e.Name+"=(set, hidden)")
			case v == nil:
				env = append(env, e.Name+"="+clip(e.Value, 80))
			case v.SecretKeyRef != nil:
				env = append(env, fmt.Sprintf("%s from secret %s key %s", e.Name, v.SecretKeyRef.Name, v.SecretKeyRef.Key))
			case v.ConfigMapKeyRef != nil:
				env = append(env, fmt.Sprintf("%s from configmap %s key %s", e.Name, v.ConfigMapKeyRef.Name, v.ConfigMapKeyRef.Key))
			case v.FieldRef != nil:
				env = append(env, e.Name+" from field "+v.FieldRef.FieldPath)
			default:
				env = append(env, e.Name+" from elsewhere")
			}
		}
		for _, f := range ct.EnvFrom {
			switch {
			case f.SecretRef != nil:
				env = append(env, "every key of secret "+f.SecretRef.Name)
			case f.ConfigMapRef != nil:
				env = append(env, "every key of configmap "+f.ConfigMapRef.Name)
			}
		}
		if len(env) == 0 {
			env = []string{"none"}
		}
		fmt.Fprintf(&b, "  env: %s\n", strings.Join(env, "; "))
		var res []string
		for k, v := range ct.Resources.Requests {
			res = append(res, "request "+string(k)+"="+v.String())
		}
		for k, v := range ct.Resources.Limits {
			res = append(res, "limit "+string(k)+"="+v.String())
		}
		sort.Strings(res)
		if len(res) > 0 {
			fmt.Fprintf(&b, "  resources: %s\n", strings.Join(res, ", "))
		}
		if ct.ReadinessProbe != nil || ct.LivenessProbe != nil {
			fmt.Fprintf(&b, "  probes: %s\n", probes(ct))
		}
	}
	if spec.ServiceAccountName != "" && spec.ServiceAccountName != "default" {
		fmt.Fprintf(&b, "serviceAccount: %s\n", spec.ServiceAccountName)
	}
	if len(spec.ImagePullSecrets) > 0 {
		var names []string
		for _, s := range spec.ImagePullSecrets {
			names = append(names, s.Name)
		}
		fmt.Fprintf(&b, "imagePullSecrets: %s\n", strings.Join(names, ", "))
	} else {
		b.WriteString("imagePullSecrets: none (a private registry would refuse the pull)\n")
	}
	if len(spec.NodeSelector) > 0 {
		var sel []string
		for k, v := range spec.NodeSelector {
			sel = append(sel, k+"="+v)
		}
		sort.Strings(sel)
		fmt.Fprintf(&b, "nodeSelector: %s\n", strings.Join(sel, ", "))
	}
	if a := spec.Affinity; a != nil && a.NodeAffinity != nil && a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		var terms []string
		for _, t := range a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			for _, e := range t.MatchExpressions {
				terms = append(terms, fmt.Sprintf("%s %s %s", e.Key, e.Operator, strings.Join(e.Values, ",")))
			}
		}
		fmt.Fprintf(&b, "node affinity (required): %s\n", strings.Join(terms, "; "))
	}
	if len(spec.Tolerations) > 0 {
		var tol []string
		for _, t := range spec.Tolerations {
			tol = append(tol, strings.TrimSpace(fmt.Sprintf("%s %s %s %s", t.Key, t.Operator, t.Value, t.Effect)))
		}
		fmt.Fprintf(&b, "tolerations: %s\n", strings.Join(tol, "; "))
	}
	if len(spec.Volumes) > 0 {
		var vols []string
		for _, v := range spec.Volumes {
			switch {
			case v.PersistentVolumeClaim != nil:
				vols = append(vols, v.Name+": pvc "+v.PersistentVolumeClaim.ClaimName)
			case v.Secret != nil:
				vols = append(vols, v.Name+": secret "+v.Secret.SecretName)
			case v.ConfigMap != nil:
				vols = append(vols, v.Name+": configmap "+v.ConfigMap.Name)
			case v.HostPath != nil:
				vols = append(vols, v.Name+": hostPath "+v.HostPath.Path)
			case v.EmptyDir != nil:
				vols = append(vols, v.Name+": emptyDir")
			default:
				vols = append(vols, v.Name)
			}
		}
		fmt.Fprintf(&b, "volumes: %s\n", strings.Join(vols, "; "))
	}
	return b.String(), nil
}

func probes(ct corev1.Container) string {
	var out []string
	for _, p := range []struct {
		name string
		p    *corev1.Probe
	}{{"readiness", ct.ReadinessProbe}, {"liveness", ct.LivenessProbe}} {
		if p.p == nil {
			continue
		}
		what := "probe"
		switch h := p.p.ProbeHandler; {
		case h.HTTPGet != nil:
			what = "GET " + h.HTTPGet.Path + " on " + h.HTTPGet.Port.String()
		case h.TCPSocket != nil:
			what = "TCP " + h.TCPSocket.Port.String()
		case h.Exec != nil:
			what = "exec " + clip(strings.Join(h.Exec.Command, " "), 100)
		}
		out = append(out, fmt.Sprintf("%s %s every %ds, fails after %d", p.name, what, max(p.p.PeriodSeconds, 10), max(p.p.FailureThreshold, 3)))
	}
	return strings.Join(out, "; ")
}

// Nodes lists nodes with what scheduling looks at: ready, schedulable,
// architecture, the labels people set, and taints.
func (c *Cluster) Nodes(ctx context.Context) (string, error) {
	list, err := c.K.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, n := range list.Items {
		ready := "NotReady"
		for _, cd := range n.Status.Conditions {
			if cd.Type == corev1.NodeReady && cd.Status == corev1.ConditionTrue {
				ready = "Ready"
			}
		}
		if n.Spec.Unschedulable {
			ready += ",cordoned"
		}
		var labels []string
		for k, v := range n.Labels {
			if strings.Contains(k, "kubernetes.io/") && k != "kubernetes.io/arch" && !strings.HasPrefix(k, "node-role.") ||
				strings.HasPrefix(k, "feature.node.") || strings.HasPrefix(k, "nvidia.com/") || strings.HasPrefix(k, "beta.") {
				continue
			}
			labels = append(labels, k+"="+v)
		}
		sort.Strings(labels)
		var taints []string
		for _, t := range n.Spec.Taints {
			taints = append(taints, strings.TrimSuffix(t.Key+"="+t.Value, "=")+":"+string(t.Effect))
		}
		fmt.Fprintf(&b, "%s %s labels: %s", n.Name, ready, strings.Join(labels, " "))
		if len(taints) > 0 {
			fmt.Fprintf(&b, " taints: %s", strings.Join(taints, " "))
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}

// Hopeless says why replacing the pods of a workload, or one pod, can't fix
// them: their image or config is wrong, they're killed for memory by their
// own limit, or they can't be scheduled. A new pod would have the same
// problem. It's empty when a fresh pod may help, e.g. a wedged process.
func (c *Cluster) Hopeless(ctx context.Context, ns, kind, name string) (string, error) {
	var pods []corev1.Pod
	if kind == "pod" {
		p, err := c.K.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		pods = []corev1.Pod{*p}
	} else {
		var sel *metav1.LabelSelector
		switch kind {
		case "deployment":
			d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			sel = d.Spec.Selector
		case "statefulset":
			s, err := c.K.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			sel = s.Spec.Selector
		case "daemonset":
			d, err := c.K.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			sel = d.Spec.Selector
		default:
			return "", fmt.Errorf("kind must be deployment, statefulset or daemonset")
		}
		s, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return "", err
		}
		list, err := c.K.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: s.String()})
		if err != nil {
			return "", err
		}
		pods = list.Items
	}
	var why []string
	for i := range pods {
		w := hopeless(&pods[i])
		if w == "" {
			w = c.crashedOn(ctx, &pods[i])
		}
		if w == "" {
			w = c.probeKilled(ctx, &pods[i])
		}
		if w == "" && !ready(&pods[i]) {
			return "", nil // a pod that's failing for a reason a fresh one may not have
		}
		if w != "" {
			why = append(why, pods[i].Name+": "+w)
		}
	}
	if len(why) == 0 {
		return "", nil
	}
	return strings.Join(why, "; "), nil
}

var configReasons = []string{"ImagePullBackOff", "ErrImagePull", "InvalidImageName", "ErrImageNeverPull", "CreateContainerConfigError", "CreateContainerError"}

func hopeless(p *corev1.Pod) string {
	for _, cd := range p.Status.Conditions {
		if cd.Type == corev1.PodScheduled && cd.Status == corev1.ConditionFalse {
			return "it can't be scheduled (" + clip(cd.Message, 160) + "), and a new pod can't either"
		}
	}
	for _, s := range p.Status.ContainerStatuses {
		if w := s.State.Waiting; w != nil && slices.Contains(configReasons, w.Reason) {
			return fmt.Sprintf("%s is %s (%s): its image or config is wrong, and a new pod gets the same", s.Name, w.Reason, clip(w.Message, 160))
		}
		if t := s.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" {
			return s.Name + " is OOMKilled by its own memory limit, which a new pod has too"
		}
		if t := s.LastTerminationState.Terminated; t != nil && strings.Contains(t.Message, "exec format error") {
			return s.Name + "'s image is built for another CPU architecture"
		}
	}
	return ""
}

// crashedOn says why a crashing container can't be fixed by a new pod, from
// the last lines it wrote before it died.
func (c *Cluster) crashedOn(ctx context.Context, p *corev1.Pod) string {
	for _, s := range p.Status.ContainerStatuses {
		if s.RestartCount == 0 {
			continue
		}
		lines := int64(10)
		rc, err := c.K.CoreV1().Pods(p.Namespace).GetLogs(p.Name, &corev1.PodLogOptions{Container: s.Name, Previous: true, TailLines: &lines}).Stream(ctx)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(rc, 64<<10))
		_ = rc.Close()
		if strings.Contains(string(b), "exec format error") {
			return s.Name + "'s image is built for another CPU architecture (exec format error)"
		}
	}
	return ""
}

// probeKilled says whether the kubelet keeps restarting a container for
// failing its liveness probe: it has already done what a restart would, and a
// new pod gets the same probe.
func (c *Cluster) probeKilled(ctx context.Context, p *corev1.Pod) string {
	var restarts int32
	for _, s := range p.Status.ContainerStatuses {
		restarts = max(restarts, s.RestartCount)
	}
	if restarts < 3 {
		return ""
	}
	evs, err := c.K.CoreV1().Events(p.Namespace).List(ctx, metav1.ListOptions{FieldSelector: "involvedObject.name=" + p.Name})
	if err != nil {
		return ""
	}
	since := c.Now().Add(-time.Hour)
	for _, e := range evs.Items {
		if e.InvolvedObject.Name == p.Name && e.Reason == "Unhealthy" && strings.HasPrefix(e.Message, "Liveness probe failed") && !lastSeen(&e).Before(since) {
			return fmt.Sprintf("the kubelet has restarted it %d times for failing its liveness probe (%s), and a new pod gets the same probe", restarts, clip(e.Message, 160))
		}
	}
	return ""
}

func ready(p *corev1.Pod) bool {
	for _, cd := range p.Status.Conditions {
		if cd.Type == corev1.PodReady {
			return cd.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Settle waits up to timeout for a deployment, statefulset or daemonset to
// have every replica updated and ready, and says how it went.
func (c *Cluster) Settle(ctx context.Context, ns, kind, name string, timeout time.Duration) string {
	start := c.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var want, ready, updated int32
	for {
		switch kind {
		case "deployment":
			if d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
				want, ready, updated = ptr(d.Spec.Replicas, 1), d.Status.ReadyReplicas, d.Status.UpdatedReplicas
			}
		case "statefulset":
			if s, err := c.K.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
				want, ready, updated = ptr(s.Spec.Replicas, 1), s.Status.ReadyReplicas, s.Status.UpdatedReplicas
			}
		case "daemonset":
			if d, err := c.K.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
				want, ready, updated = d.Status.DesiredNumberScheduled, d.Status.NumberReady, d.Status.UpdatedNumberScheduled
			}
		default:
			return ""
		}
		if want > 0 && ready >= want && updated >= want {
			return fmt.Sprintf("%s %s/%s has %d/%d ready after %s.", kind, ns, name, ready, want, c.Now().Sub(start).Round(time.Second))
		}
		select {
		case <-ctx.Done():
			return fmt.Sprintf("%s %s/%s has %d/%d ready after %s: not settled yet; look at its pods.", kind, ns, name, ready, want, timeout)
		case <-time.After(3 * time.Second):
		}
	}
}

// Owner is a pod's workload, as kind and name, when it's a deployment,
// statefulset or daemonset.
func (c *Cluster) Owner(ctx context.Context, ns, pod string) (string, string) {
	p, err := c.K.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return "", ""
	}
	kind, name, _ := strings.Cut(Workload(p), "/")
	switch kind {
	case "deployment", "statefulset", "daemonset":
		return kind, name
	}
	return "", ""
}

// Replicas is how many replicas a deployment or statefulset wants.
func (c *Cluster) Replicas(ctx context.Context, ns, kind, name string) (int32, error) {
	switch kind {
	case "deployment":
		d, err := c.K.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return ptr(d.Spec.Replicas, 1), nil
	case "statefulset":
		s, err := c.K.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return ptr(s.Spec.Replicas, 1), nil
	}
	return 0, errors.New("kind must be deployment or statefulset")
}
