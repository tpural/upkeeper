// Package llm is a client for OpenAI-compatible chat completions with tools,
// as llama-swap and llama.cpp serve them.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Message is one chat message.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall is the model asking to run a tool.
type ToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// Tool is a function the model may call.
type Tool struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

// Usage is what a request cost.
type Usage struct {
	Prompt     int `json:"prompt_tokens"`
	Completion int `json:"completion_tokens"`
}

// Client calls a chat completions endpoint.
type Client struct {
	URL   string // e.g. http://llama-swap.llama-swap.svc:8080/v1
	Model string
	HTTP  *http.Client
}

// New returns a client. There's no client timeout: the first request after
// the GPU wakes waits for the model to load; callers bound it with ctx.
func New(url, model string) *Client {
	return &Client{URL: strings.TrimRight(url, "/"), Model: model, HTTP: &http.Client{}}
}

// Chat sends the conversation and returns the model's reply.
func (c *Client) Chat(ctx context.Context, msgs []Message, tools []Tool) (Message, Usage, error) {
	type function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	}
	type tool struct {
		Type     string   `json:"type"`
		Function function `json:"function"`
	}
	body := map[string]any{"model": c.Model, "messages": msgs, "temperature": 0.2}
	if len(tools) > 0 {
		ts := make([]tool, len(tools))
		for i, t := range tools {
			ts[i] = tool{"function", function(t)}
		}
		body["tools"] = ts
	}
	b, err := json.Marshal(body)
	if err != nil {
		return Message{}, Usage{}, err
	}
	var last error
	for attempt := range 3 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Message{}, Usage{}, ctx.Err()
			case <-time.After(time.Duration(attempt) * 10 * time.Second):
			}
		}
		m, u, err := c.send(ctx, b)
		if err == nil {
			return m, u, nil
		}
		last = err
		var se statusError
		if errors.As(err, &se) && se.code < 500 {
			break
		}
	}
	return Message{}, Usage{}, last
}

type statusError struct {
	code int
	msg  string
}

func (e statusError) Error() string { return fmt.Sprintf("model: %d: %s", e.code, e.msg) }

func (c *Client) send(ctx context.Context, body []byte) (Message, Usage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Message{}, Usage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return Message{}, Usage{}, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return Message{}, Usage{}, err
	}
	if res.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(b))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return Message{}, Usage{}, statusError{res.StatusCode, msg}
	}
	var out struct {
		Choices []struct {
			Message Message `json:"message"`
		} `json:"choices"`
		Usage Usage `json:"usage"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return Message{}, Usage{}, fmt.Errorf("model: %w", err)
	}
	if len(out.Choices) == 0 {
		return Message{}, Usage{}, errors.New("model: no choices in the answer")
	}
	m := out.Choices[0].Message
	m.Role = "assistant"
	return m, out.Usage, nil
}
