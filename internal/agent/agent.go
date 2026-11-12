// Package agent runs the model in a loop over tools until it finishes, gives
// up, or upkeeper stops it.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

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
	Log   *slog.Logger
	// Budget is roughly how many characters of conversation to keep; older
	// tool answers are dropped past it.
	Budget int
}

// Run loops until a tool ends the run, the model gets stuck, or steps run out.
func (a *Agent) Run(ctx context.Context, t Task) (Result, error) {
	byName := map[string]Tool{}
	tools := make([]llm.Tool, 0, len(t.Tools))
	for _, tl := range t.Tools {
		byName[tl.Name] = tl
		tools = append(tools, llm.Tool{Name: tl.Name, Description: tl.Description, Parameters: tl.Params})
	}
	msgs := []llm.Message{{Role: "system", Content: t.System}, {Role: "user", Content: t.User}}
	var res Result
	seen := map[string]int{}
	idle := 0
	for res.Steps < t.MaxSteps {
		if err := ctx.Err(); err != nil {
			res.Stopped = "ran out of time"
			return res, nil
		}
		a.trim(msgs)
		m, u, err := a.Model.Chat(ctx, msgs, tools)
		if err != nil {
			if ctx.Err() != nil {
				res.Stopped = "ran out of time"
				return res, nil
			}
			return res, fmt.Errorf("model: %w", err)
		}
		res.Steps++
		res.Tokens += u.Prompt + u.Completion
		msgs = append(msgs, m)
		if len(m.ToolCalls) == 0 {
			idle++
			if idle >= 2 {
				res.Stopped = "stopped calling tools: " + clip(m.Content, 300)
				return res, nil
			}
			msgs = append(msgs, llm.Message{Role: "user", Content: "Use a tool to continue. When you're done, call the tool that ends the task."})
			continue
		}
		idle = 0
		for _, call := range m.ToolCalls {
			name := call.Function.Name
			answer, ended, err := a.call(ctx, byName, call, seen)
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
		}
	}
	res.Stopped = fmt.Sprintf("used all %d steps", t.MaxSteps)
	return res, nil
}

func (a *Agent) call(ctx context.Context, tools map[string]Tool, call llm.ToolCall, seen map[string]int) (string, bool, error) {
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
		return "You already made this exact call; its answer is above. Try something different, or end the task.", false, nil
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
