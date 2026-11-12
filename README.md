# upkeeper

upkeeper keeps a homelab running with a local model. It works [tremolo](https://github.com/tpural/tremolo)'s
investigations: when an alert fires it finds the cause, fixes what it safely can, and escalates the rest to a
person. Between alerts it patrols for trouble that hasn't become an alert yet.

It runs next to tremolo, not inside it. tremolo holds the investigations, the safety rails (pause, action
budgets, protected hosts) and the audit log; upkeeper only does the thinking and the changes.

## How it works

1. Every 30 seconds upkeeper asks tremolo for open investigations, incidents first.
2. It claims one and gives the model the alert, its runbook and what fixed it before, along with tools:
   - tremolo's MCP tools for looking: metrics, logs, hosts, alerts, runbooks, coverage;
   - Kubernetes reads: pods, one pod's detail, container logs, warnings, rollouts;
   - a short list of changes: `restart`, `delete_pod`, `scale`, `cordon`. There's no shell.
3. Before every change upkeeper itself calls tremolo's `begin_action`, so the rails always apply; a refusal is
   shown to the model, and nothing changes.
4. Each tool call is recorded on the investigation's timeline. The run ends with the model's `finish` (fixed, with
   the fix in one line) or `escalate`. A model that repeats itself, stops using tools or runs out of steps or
   time is escalated automatically, with its last steps.

## The GPU

The model runs on a GPU node that sleeps when idle. upkeeper only uses the model when there's work:

- an incident waits `gpu.wakeDelay` (3m) before it wakes the GPU, and is closed untouched if its alert resolves
  first;
- at most `gpu.maxWakesPerDay` (6) wakes; past that, incidents are escalated instead;
- other investigations (gaps, tuning, runbook reviews) are only worked while the GPU is already awake.

It's awake when `gpu.deployment` has a ready replica; reading that doesn't wake it.

## Patrol

- **Hourly, without the model:** pods pending for over 15 minutes, containers restarting 3 or more times since
  the last check, nodes under pressure or not ready (except asleep ones), warnings repeated 10 or more times in an
  hour, and tremolo's coverage gaps. Each opens an investigation, once; a closed one isn't reopened for a week.
- **With the model:** daily at `patrol.at`, and at most hourly while the GPU is awake anyway, with
  `patrol.prompt`. It only looks, and opens investigations for what it finds.

## Configuration

```yaml
name: upkeeper                 # the tremolo token's name
tremolo:
  url: http://tremolo.tremolo.svc
model:
  url: http://llama-swap.llama-swap.svc:8080/v1
  name: qwen3-coder-next
  maxSteps: 30
  timeout: 15m                 # per investigation
gpu:
  deployment: llama-swap/llama-swap
  wakeDelay: 3m
  maxWakesPerDay: 6
patrol:
  every: 1h
  at: "07:30"
  timezone: America/Chicago
  prompt: |
    Nothing is firing. Look around the lab for trouble before it becomes an alert...
# prompt: replaces the built-in system prompt for investigations
```

The tremolo token comes from `$TREMOLO_TOKEN`: an assistant token with read access, `investigate`, and if you want
runbook edits and silences, `write:runbooks` and `write:silences`.

`:9090` serves `/healthz`, `/status` (what it's doing, wakes today, the last patrol) and `/metrics`.

## Kubernetes permissions

upkeeper's ServiceAccount is the real limit on what it can do. It needs: get/list/watch pods, pods/log, events,
nodes, deployments, statefulsets, daemonsets; patch deployments, statefulsets and daemonsets (restart, scale);
delete pods; patch nodes (cordon). Give it nothing on secrets and no RBAC rights.
