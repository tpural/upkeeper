// Package tremolo talks to tremolo: its investigations API for upkeeper's own
// bookkeeping, and its MCP server for the tools the model uses.
package tremolo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Investigation is the part of tremolo's investigation upkeeper uses.
type Investigation struct {
	ID            int64     `json:"id"`
	Kind          string    `json:"kind"`
	Alert         string    `json:"alert,omitempty"`
	Rule          string    `json:"rule,omitempty"`
	Title         string    `json:"title"`
	Severity      string    `json:"severity,omitempty"`
	Status        string    `json:"status"`
	ClaimedBy     string    `json:"claimedBy,omitempty"`
	ClaimExpires  time.Time `json:"claimExpires,omitzero"`
	Opened        time.Time `json:"opened"`
	Closed        time.Time `json:"closed,omitzero"`
	AlertResolved time.Time `json:"alertResolved,omitzero"`
	Summary       string    `json:"summary,omitempty"`
	Fix           string    `json:"fix,omitempty"`
	Outcome       string    `json:"outcome,omitempty"`
	Previous      int64     `json:"previous,omitempty"`
	Events        []Event   `json:"events,omitempty"`
}

// Claimed reports whether someone other than me holds it at now.
func (i Investigation) Claimed(me string, now time.Time) bool {
	return i.ClaimedBy != "" && i.ClaimedBy != me && i.ClaimExpires.After(now)
}

// Event is an entry in an investigation's timeline.
type Event struct {
	ID   int64     `json:"id"`
	Time time.Time `json:"time"`
	By   string    `json:"by"`
	Kind string    `json:"kind"`
	Text string    `json:"text"`
}

// Action is a change upkeeper asks tremolo about before making it.
type Action struct {
	Action     string `json:"action"`
	Host       string `json:"host,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Target     string `json:"target"`
	Disruptive bool   `json:"disruptive"`
	Why        string `json:"why,omitempty"`
}

// Gap is something tremolo doesn't observe.
type Gap struct {
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Detail  string `json:"detail"`
}

// Error is an answer tremolo refused.
type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("tremolo: %d: %s", e.Code, e.Msg) }

// Refused reports whether err is tremolo saying no (a claim held by someone
// else, an action over budget, agents paused), rather than a failure.
func Refused(err error) bool {
	var e *Error
	return errors.As(err, &e) && (e.Code == http.StatusConflict || e.Code == http.StatusLocked || e.Code == http.StatusForbidden)
}

// Client is a tremolo API client authenticated with an assistant token.
type Client struct {
	URL   *url.URL
	Token string
	HTTP  *http.Client
	// Name is the token's name, which tremolo records as the actor.
	Name string
}

// New returns a client for base with token.
func New(base, token, name string) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("tremolo url %q is not a URL", base)
	}
	return &Client{URL: u, Token: token, Name: name, HTTP: &http.Client{Timeout: time.Minute}}, nil
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body, out any) error {
	u := *c.URL
	u.Path += path
	u.RawQuery = q.Encode()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		var e struct{ Error string }
		_ = json.Unmarshal(b, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(b))
		}
		return &Error{res.StatusCode, e.Error}
	}
	if out == nil || len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

// Paused reports whether a person has paused agents, and who.
func (c *Client) Paused(ctx context.Context) (bool, string, error) {
	var st struct {
		Paused struct {
			Paused bool   `json:"paused"`
			By     string `json:"by"`
		} `json:"paused"`
	}
	err := c.do(ctx, http.MethodGet, "/api/agents", nil, nil, &st)
	return st.Paused.Paused, st.Paused.By, err
}

// List returns investigations with any of statuses, newest first.
func (c *Client) List(ctx context.Context, limit int, statuses ...string) ([]Investigation, error) {
	q := url.Values{"limit": {fmt.Sprint(limit)}}
	if len(statuses) > 0 {
		q.Set("status", strings.Join(statuses, ","))
	}
	var out []Investigation
	err := c.do(ctx, http.MethodGet, "/api/investigations", q, nil, &out)
	return out, err
}

// Past returns the latest closed or escalated investigations of rule, with
// their timelines.
func (c *Client) Past(ctx context.Context, rule string, limit int) ([]Investigation, error) {
	q := url.Values{"rule": {rule}, "limit": {fmt.Sprint(limit)}, "status": {"fixed,escalated,abandoned,resolved"}}
	var list []Investigation
	if err := c.do(ctx, http.MethodGet, "/api/investigations", q, nil, &list); err != nil {
		return nil, err
	}
	for i := range list {
		if full, err := c.Get(ctx, list[i].ID); err == nil {
			list[i] = full
		}
	}
	return list, nil
}

// Get returns an investigation with its timeline.
func (c *Client) Get(ctx context.Context, id int64) (Investigation, error) {
	var out Investigation
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/investigations/%d", id), nil, nil, &out)
	return out, err
}

// Claim takes an investigation, or renews my claim.
func (c *Client) Claim(ctx context.Context, id int64) (Investigation, error) {
	var out Investigation
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/claim", id), nil, struct{}{}, &out)
	return out, err
}

// Open opens an investigation of kind gap, tuning or runbook-review, claimed by me.
func (c *Client) Open(ctx context.Context, kind, title, text string) (Investigation, error) {
	var out Investigation
	err := c.do(ctx, http.MethodPost, "/api/investigations", nil, map[string]string{"kind": kind, "title": title, "text": text}, &out)
	return out, err
}

// Record adds a finding, query, note or change to the timeline.
func (c *Client) Record(ctx context.Context, id int64, kind, text string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/events", id), nil, map[string]string{"kind": kind, "text": text}, nil)
}

// BeginAction asks before a change; a refusal is an *Error that Refused reports.
func (c *Client) BeginAction(ctx context.Context, id int64, a Action) (int64, error) {
	var e Event
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/actions", id), nil, a, &e)
	return e.ID, err
}

// EndAction records how an action went.
func (c *Client) EndAction(ctx context.Context, id, event int64, result string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/actions/%d", id, event), nil, map[string]string{"result": result}, nil)
}

// Close finishes an investigation as fixed (with the fix) or abandoned.
func (c *Client) Close(ctx context.Context, id int64, status, summary, fix, runbook string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/close", id), nil,
		map[string]string{"status": status, "summary": summary, "fix": fix, "runbook": runbook}, nil)
}

// Escalate hands an investigation to a person.
func (c *Client) Escalate(ctx context.Context, id int64, why string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/api/investigations/%d/escalate", id), nil, map[string]string{"why": why}, nil)
}

// Coverage returns what tremolo doesn't observe.
func (c *Client) Coverage(ctx context.Context) ([]Gap, error) {
	var out []Gap
	err := c.do(ctx, http.MethodGet, "/api/coverage", nil, nil, &out)
	return out, err
}

// Tool is one of tremolo's MCP tools.
type Tool struct {
	Name, Description string
	Schema            json.RawMessage
}

// Session is a connection to tremolo's MCP server.
type Session struct{ s *mcp.ClientSession }

// Connect opens an MCP session with the client's token.
func (c *Client) Connect(ctx context.Context, version string) (*Session, error) {
	client := mcp.NewClient(&mcp.Implementation{Name: "upkeeper", Version: version}, nil)
	hc := &http.Client{Timeout: 2 * time.Minute, Transport: bearer{c.Token, http.DefaultTransport}}
	s, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.URL.String() + "/mcp", HTTPClient: hc}, nil)
	if err != nil {
		return nil, fmt.Errorf("tremolo mcp: %w", err)
	}
	return &Session{s}, nil
}

// Close ends the session.
func (s *Session) Close() error { return s.s.Close() }

// Tools lists the server's tools.
func (s *Session) Tools(ctx context.Context) ([]Tool, error) {
	var out []Tool
	for t, err := range s.s.Tools(ctx, nil) {
		if err != nil {
			return nil, err
		}
		schema, err := json.Marshal(t.InputSchema)
		if err != nil {
			return nil, err
		}
		out = append(out, Tool{t.Name, t.Description, schema})
	}
	return out, nil
}

// Call runs a tool. A tool's own failure is returned as text, since the model
// should read it; err is for the call itself failing.
func (s *Session) Call(ctx context.Context, name string, args map[string]any) (string, bool, error) {
	res, err := s.s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return "", false, err
	}
	var parts []string
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, t.Text)
		}
	}
	return strings.Join(parts, "\n"), res.IsError, nil
}

type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

// Runbook is a runbook in tremolo.
type Runbook struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Body    string   `json:"body"`
	Rules   []string `json:"rules"`
	Version int      `json:"version"`
}

// Runbooks returns the runbooks covering rule.
func (c *Client) Runbooks(ctx context.Context, rule string) ([]Runbook, error) {
	var out []Runbook
	err := c.do(ctx, http.MethodGet, "/api/runbooks", url.Values{"rule": {rule}}, nil, &out)
	return out, err
}

// Runbook returns one runbook.
func (c *Client) Runbook(ctx context.Context, id string) (Runbook, error) {
	var out Runbook
	err := c.do(ctx, http.MethodGet, "/api/runbooks/"+url.PathEscape(id), nil, nil, &out)
	return out, err
}

// SaveRunbook updates rb, which must carry the version it was read at, or
// creates it when its version is 0.
func (c *Client) SaveRunbook(ctx context.Context, rb Runbook) (Runbook, error) {
	var out Runbook
	if rb.Version == 0 {
		err := c.do(ctx, http.MethodPost, "/api/runbooks", nil, rb, &out)
		return out, err
	}
	err := c.do(ctx, http.MethodPut, "/api/runbooks/"+url.PathEscape(rb.ID), nil, rb, &out)
	return out, err
}
