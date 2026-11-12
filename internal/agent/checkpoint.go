package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tpural/upkeeper/internal/llm"
)

// Checkpoint is the model stepping back: what it knows, what it tried, what's
// missing, and what it'll do next.
type Checkpoint struct {
	Known   string `json:"known"`
	Tried   string `json:"tried"`
	Missing string `json:"missing"`
	Next    string `json:"next"`
	LookAt  string `json:"look_at"`
	// Query is what to search the web for, when Next is search.
	Query string `json:"query,omitempty"`
}

// Summary is the checkpoint for a person.
func (c Checkpoint) Summary() string {
	var parts []string
	for _, p := range [][2]string{{"Known", c.Known}, {"Tried", c.Tried}, {"Missing", c.Missing}} {
		if s := strings.TrimSpace(p[1]); s != "" {
			parts = append(parts, p[0]+": "+strings.TrimRight(s, ". ")+".")
		}
	}
	return strings.Join(parts, " ")
}

// Choice is one thing the model may decide at a checkpoint.
type Choice struct {
	Name, Meaning string
	// Continue means the run goes on with the model doing LookAt; otherwise
	// the run ends with this decision.
	Continue bool
}

// Choices are an investigation's.
var Choices = []Choice{
	{"fix", "you know the cause and can fix it with your tools: say the exact change in look_at", true},
	{"look", "one specific check would settle it: say exactly what in look_at", true},
	{"escalate", "the fix needs something you can't do (config, code, a secret, hardware), or you're out of ideas", false},
}

// JSONChatter is a model that can be made to answer with JSON.
type JSONChatter interface {
	ChatJSON(ctx context.Context, msgs []llm.Message, schema json.RawMessage) (llm.Message, llm.Usage, error)
}

func checkpointPrompt(choices []Choice, why string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Stop and step back (%s). Don't call a tool. Answer only with a JSON object:\n", why)
	b.WriteString(`- "known": what you've found so far, with the evidence;` + "\n")
	b.WriteString(`- "tried": what you've already checked;` + "\n")
	b.WriteString(`- "missing": the one thing you still don't know, or "" if nothing;` + "\n")
	b.WriteString(`- "next": one of:` + "\n")
	for _, c := range choices {
		fmt.Fprintf(&b, "    %q: %s;\n", c.Name, c.Meaning)
	}
	b.WriteString(`- "look_at": for a choice that continues, exactly what to do next.` + "\n")
	b.WriteString("Don't repeat a check you've already made. Decide.")
	return b.String()
}

func checkpointSchema(choices []Choice) json.RawMessage {
	names := make([]string, len(choices))
	for i, c := range choices {
		names[i] = c.Name
	}
	enum, _ := json.Marshal(names)
	return json.RawMessage(`{"type":"object","properties":{"known":{"type":"string"},"tried":{"type":"string"},"missing":{"type":"string"},` +
		`"next":{"type":"string","enum":` + string(enum) + `},"look_at":{"type":"string"},"query":{"type":"string"}},"required":["known","tried","missing","next"]}`)
}

// checkpoint asks the model to step back, as a chat span marked as one.
func (a *Agent) checkpoint(ctx context.Context, tracer trace.Tracer, msgs []llm.Message, choices []Choice, why string, res *Result) (Checkpoint, error) {
	ctx, span := tracer.Start(ctx, "chat "+a.ModelName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.request.model", a.ModelName),
		attribute.Bool("upkeeper.checkpoint", true),
		attribute.String("upkeeper.checkpoint.why", why),
		attribute.Int("upkeeper.step", res.Steps+1),
	))
	defer span.End()
	ask := append(append([]llm.Message(nil), msgs...), llm.Message{Role: "user", Content: checkpointPrompt(choices, why)})
	var c Checkpoint
	var last error
	for range 2 {
		var m llm.Message
		var u llm.Usage
		var err error
		if j, ok := a.Model.(JSONChatter); ok {
			m, u, err = j.ChatJSON(ctx, ask, checkpointSchema(choices))
		} else {
			m, u, err = a.Model.Chat(ctx, ask, nil)
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return c, err
		}
		res.Steps++
		res.Tokens += u.Prompt + u.Completion
		span.SetAttributes(attribute.Int("gen_ai.usage.input_tokens", u.Prompt), attribute.Int("gen_ai.usage.output_tokens", u.Completion),
			attribute.String("upkeeper.said", clipRaw(m.Content, 4000)))
		if last = parseCheckpoint(m.Content, choices, &c); last == nil {
			break
		}
	}
	if last != nil {
		span.SetStatus(codes.Error, last.Error())
		return c, errNoDecision{last}
	}
	span.SetAttributes(
		attribute.String("upkeeper.checkpoint.known", c.Known),
		attribute.String("upkeeper.checkpoint.tried", c.Tried),
		attribute.String("upkeeper.checkpoint.missing", c.Missing),
		attribute.String("upkeeper.checkpoint.next", c.Next),
		attribute.String("upkeeper.checkpoint.look_at", c.LookAt),
	)
	return c, nil
}

// errNoDecision is a model that couldn't answer a checkpoint: it's stuck, not deciding.
type errNoDecision struct{ err error }

func (e errNoDecision) Error() string {
	return "couldn't say what to do next when stepping back: " + e.err.Error()
}

func parseCheckpoint(s string, choices []Choice, c *Checkpoint) error {
	// Models sometimes wrap JSON in prose or a code fence.
	i, j := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if i < 0 || j < i {
		return fmt.Errorf("no JSON in %q", clip(s, 120))
	}
	*c = Checkpoint{}
	if err := json.Unmarshal([]byte(s[i:j+1]), c); err != nil {
		return fmt.Errorf("bad JSON: %v", err)
	}
	c.Next = strings.ToLower(strings.TrimSpace(c.Next))
	for _, ch := range choices {
		if ch.Name == c.Next {
			if ch.Continue && strings.TrimSpace(c.LookAt) == "" {
				return fmt.Errorf("next %q without look_at", c.Next)
			}
			return nil
		}
	}
	return fmt.Errorf("next %q isn't one of the choices", c.Next)
}

func continues(choices []Choice, next string) bool {
	for _, c := range choices {
		if c.Name == next {
			return c.Continue
		}
	}
	return false
}
