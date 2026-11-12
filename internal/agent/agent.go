// Package agent runs the model in a loop over tools until it finishes, gives
// up, or upkeeper stops it. Each model call and tool call is a span, named and
// attributed after OpenTelemetry's GenAI conventions.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tpural/upkeeper/internal/llm"
)

// Tool is something the model can do.
type Tool struct {
	Name        string
	Description string
	// Params is the JSON Schema of the arguments.
	Params json.RawMessage
	// Run does it and says what happened, for the model to read. An error is
	// also shown to the model; only Fatal errors end the run.
	Run func(ctx context.Context, args map[string]any) (string, error)
	// Ends reports that a successful call ends the run, e.g. finish or escalate.
	Ends bool
}

// Fatal wraps an error that should stop the run, not go back to the model.
type Fatal struct{ Err error }

func (f Fatal) Error() string { return f.Err.Error() }
func (f Fatal) Unwrap() error { return f.Err }

// Chatter is the model.
type Chatter interface {
	Chat(ctx context.Context, msgs []llm.Message, tools []llm.Tool) (llm.Message, llm.Usage, error)
}

// Task is one run.
type Task struct {
	System, User string
	Tools        []Tool
	MaxSteps     int
	// OnCall is told of each tool call and its answer, e.g. to record it.
	OnCall func(name string, args map[string]any, answer string)
}

// Result is how a run ended.
type Result struct {
	// Ended is the tool that ended the run; empty when upkeeper stopped it.
	Ended string
	// Answer is what that tool returned.
	Answer string
	// Stopped says why upkeeper stopped it.
	Stopped string
	Steps   int
	Tokens  int
	// Last is the last few calls, for a person reading an escalation.
	Last []string
}

// Agent runs tasks against a model.
type Agent struct {
	Model Chatter
	// ModelName is reported on spans.
	ModelName string
	Log       *slog.Logger
	Tracer    trace.Tracer
	// Budget is roughly how many characters of conversation to keep; older
	// tool answers are dropped past it.
	Budget int
}

// Nudges, and when they're given.
const (
	emptyNudge = 5 // tool answers in a row that found nothing
	emptyStop  = 9
	nudgeEmpty = "Your last %d tool calls found nothing new. You have what you're going to get: decide now. Finish if it's fixed or there's nothing to do; otherwise escalate with what you found."
	nudgeSteps = "You've used %d of your %d steps. Wrap up now: finish, or escalate with what you found."
	nudgeIdle  = "Use a tool to continue. When you're done, call the tool that ends the task."
)

// Run loops until a tool ends the run, the model gets stuck, or steps run out.
func (a *Agent) Run(ctx context.Context, t Task) (Result, error) {
	tracer := a.Tracer
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}
	byName := map[string]Tool{}
	tools := make([]llm.Tool, 0, len(t.Tools))
	for _, tl := range t.Tools {
		byName[tl.Name] = tl
		tools = append(tools, llm.Tool{Name: tl.Name, Description: tl.Description, Parameters: tl.Params})
	}
	msgs := []llm.Message{{Role: "system", Content: t.System}, {Role: "user", Content: t.User}}
	var res Result
	seen := map[string]int{}
	idle, empty := 0, 0
	warnAt := (t.MaxSteps*6 + 9) / 10
	nudge := func(text, why string) {
		msgs = append(msgs, llm.Message{Role: "user", Content: text})
		trace.SpanFromContext(ctx).AddEvent("upkeeper.nudge", trace.WithAttributes(attribute.String("upkeeper.nudge", why), attribute.String("upkeeper.text", text)))
	}
	for res.Steps < t.MaxSteps {
		if err := ctx.Err(); err != nil {
			res.Stopped = "ran out of time"
			return res, nil
		}
		a.trim(msgs)
		m, err := a.chat(ctx, tracer, msgs, tools, &res)
		if err != nil {
			if ctx.Err() != nil {
				res.Stopped = "ran out of time"
				return res, nil
			}
			return res, fmt.Errorf("model: %w", err)
		}
		msgs = append(msgs, m)
		if len(m.ToolCalls) == 0 {
			idle++
			if idle >= 2 {
				res.Stopped = "stopped calling tools: " + clip(m.Content, 300)
				return res, nil
			}
			nudge(nudgeIdle, "idle")
			continue
		}
		idle = 0
		for _, call := range m.ToolCalls {
			name := call.Function.Name
			answer, ended, err := a.call(ctx, tracer, byName, call, seen)
			var fatal Fatal
			if errors.As(err, &fatal) {
				return res, fatal.Err
			}
			res.Last = append(res.Last, name+": "+clip(answer, 160))
			if len(res.Last) > 4 {
				res.Last = res.Last[1:]
			}
			if t.OnCall != nil {
				t.OnCall(name, args(call), answer)
			}
			msgs = append(msgs, llm.Message{Role: "tool", ToolCallID: call.ID, Content: answer})
			if ended {
				res.Ended, res.Answer = name, answer
				return res, nil
			}
			if seen[key(call)] >= 3 {
				res.Stopped = "kept repeating " + name
				return res, nil
			}
			if found(answer) {
				empty = 0
			} else {
				empty++
			}
		}
		switch {
		case empty >= emptyStop:
			res.Stopped = fmt.Sprintf("searched %d times in a row without finding anything", empty)
			return res, nil
		case empty == emptyNudge:
			nudge(fmt.Sprintf(nudgeEmpty, empty), "nothing found")
		case res.Steps == warnAt && warnAt < t.MaxSteps:
			nudge(fmt.Sprintf(nudgeSteps, res.Steps, t.MaxSteps), "steps")
		}
	}
	res.Stopped = fmt.Sprintf("used all %d steps", t.MaxSteps)
	return res, nil
}

// chat is one model call, as a span with what it was asked and what it said.
func (a *Agent) chat(ctx context.Context, tracer trace.Tracer, msgs []llm.Message, tools []llm.Tool, res *Result) (llm.Message, error) {
	ctx, span := tracer.Start(ctx, "chat "+a.ModelName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.request.model", a.ModelName),
		attribute.Int("upkeeper.step", res.Steps+1),
		attribute.Int("upkeeper.messages", len(msgs)),
	))
	defer span.End()
	// The newest input: what the model is answering this step.
	if last := msgs[len(msgs)-1]; last.Role != "system" {
		span.SetAttributes(attribute.String("upkeeper.input", clipRaw(last.Content, 4000)), attribute.String("upkeeper.input.role", last.Role))
	}
	m, u, err := a.Model.Chat(ctx, msgs, tools)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return m, err
	}
	res.Steps++
	res.Tokens += u.Prompt + u.Completion
	finish := "stop"
	if len(m.ToolCalls) > 0 {
		finish = "tool_calls"
	}
	out, _ := json.Marshal([]map[string]any{{"role": "assistant", "content": m.Content, "tool_calls": m.ToolCalls}})
	span.SetAttributes(
		attribute.Int("gen_ai.usage.input_tokens", u.Prompt),
		attribute.Int("gen_ai.usage.output_tokens", u.Completion),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{finish}),
		attribute.String("gen_ai.output.messages", clipRaw(string(out), 8000)),
		attribute.String("upkeeper.said", clipRaw(m.Content, 4000)),
	)
	return m, nil
}

func (a *Agent) call(ctx context.Context, tracer trace.Tracer, tools map[string]Tool, call llm.ToolCall, seen map[string]int) (string, bool, error) {
	ctx, span := tracer.Start(ctx, "execute_tool "+call.Function.Name, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "execute_tool"),
		attribute.String("gen_ai.tool.name", call.Function.Name),
		attribute.String("gen_ai.tool.call.id", call.ID),
		attribute.String("gen_ai.tool.call.arguments", clipRaw(call.Function.Arguments, 2000)),
	))
	defer span.End()
	answer, ended, err := a.run(ctx, tools, call, seen)
	span.SetAttributes(attribute.String("gen_ai.tool.call.result", clipRaw(answer, 4000)), attribute.Bool("upkeeper.found", found(answer)))
	if err != nil || strings.HasPrefix(answer, "Failed: ") {
		span.SetStatus(codes.Error, clip(answer, 200))
	}
	return answer, ended, err
}

func (a *Agent) run(ctx context.Context, tools map[string]Tool, call llm.ToolCall, seen map[string]int) (string, bool, error) {
	tl, ok := tools[call.Function.Name]
	if !ok {
		return fmt.Sprintf("There's no tool %q. Use one of the tools you were given.", call.Function.Name), false, nil
	}
	var in map[string]any
	if s := strings.TrimSpace(call.Function.Arguments); s != "" && s != "null" {
		if err := json.Unmarshal([]byte(s), &in); err != nil {
			return "Your arguments aren't a JSON object: " + err.Error(), false, nil
		}
	}
	k := key(call)
	seen[k]++
	if seen[k] > 1 && !tl.Ends {
		return repeated, false, nil
	}
	out, err := tl.Run(ctx, in)
	if err != nil {
		var fatal Fatal
		if errors.As(err, &fatal) {
			return "", false, err
		}
		a.Log.Debug("tool failed", "tool", tl.Name, "err", err)
		return "Failed: " + err.Error(), false, nil
	}
	return out, tl.Ends, nil
}

const repeated = "You already made this exact call; its answer is above. Try something different, or end the task."

// found reports whether a tool's answer had anything in it.
func found(answer string) bool {
	for _, p := range []string{"No ", "Failed: ", repeated, "There's no tool", "Your arguments"} {
		if strings.HasPrefix(answer, p) {
			return false
		}
	}
	return strings.TrimSpace(answer) != ""
}

// trim drops the oldest tool answers until the conversation fits the budget.
// The system and first user message stay.
func (a *Agent) trim(msgs []llm.Message) {
	if a.Budget <= 0 {
		return
	}
	size := 0
	for _, m := range msgs {
		size += len(m.Content)
	}
	for i := 2; i < len(msgs) && size > a.Budget; i++ {
		if msgs[i].Role == "tool" && len(msgs[i].Content) > 100 {
			size -= len(msgs[i].Content)
			msgs[i].Content = "(older answer dropped to save space)"
			size += len(msgs[i].Content)
		}
	}
}

func args(call llm.ToolCall) map[string]any {
	var in map[string]any
	_ = json.Unmarshal([]byte(call.Function.Arguments), &in)
	return in
}

func key(call llm.ToolCall) string {
	var in any
	if json.Unmarshal([]byte(call.Function.Arguments), &in) == nil {
		b, _ := json.Marshal(in) // sorted keys
		return call.Function.Name + string(b)
	}
	return call.Function.Name + call.Function.Arguments
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// clipRaw shortens s keeping its lines.
func clipRaw(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
