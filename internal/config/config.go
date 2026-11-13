// Package config is upkeeper's configuration file.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is upkeeper's configuration.
type Config struct {
	// Name is the tremolo token's name; tremolo records upkeeper's work under it.
	Name    string  `json:"name,omitempty"`
	Tremolo Tremolo `json:"tremolo"`
	Model   Model   `json:"model"`
	GPU     GPU     `json:"gpu"`
	Patrol  Patrol  `json:"patrol"`
	// Web lets a stuck model search the web at a checkpoint; without
	// web.searxng it can't.
	Web Web `json:"web,omitzero"`
	// Traces is where upkeeper sends a trace of each run; empty sends none.
	Traces Traces `json:"traces,omitzero"`
	// Prompt replaces the built-in system prompt for investigations.
	Prompt string `json:"prompt,omitempty"`
	// Every is how often to look for work.
	Every Duration `json:"every,omitempty"`
}

// Tremolo is where upkeeper gets its work. The token comes from $TREMOLO_TOKEN.
type Tremolo struct {
	URL   string `json:"url"`
	Token string `json:"-"`
}

// Model is an OpenAI-compatible chat endpoint.
type Model struct {
	URL      string   `json:"url"`
	Name     string   `json:"name"`
	MaxSteps int      `json:"maxSteps,omitempty"`
	Timeout  Duration `json:"timeout,omitempty"`
	// Temperatures are sampled at before the first checkpoint, after it, and
	// after the second; checkpoints use the last.
	Temperatures []float64 `json:"temperatures,omitempty"`
}

// GPU is what runs the model, and how often upkeeper may wake it.
type GPU struct {
	// Deployment is namespace/name of the model server; it's awake when it has a ready replica.
	Deployment string `json:"deployment,omitempty"`
	// WakeDelay is how long an alert must stay firing before upkeeper wakes the GPU for it.
	WakeDelay Duration `json:"wakeDelay,omitempty"`
	// MaxWakesPerDay caps wakes; past it, alerts are escalated instead. 0, the default, is no limit.
	MaxWakesPerDay int `json:"maxWakesPerDay,omitempty"`
}

// Patrol is the checking upkeeper does when nothing is firing.
type Patrol struct {
	// Every is how often the checks that need no model run.
	Every Duration `json:"every,omitempty"`
	// At is when the model looks around each day, e.g. "07:30"; empty never.
	At       string `json:"at,omitempty"`
	Timezone string `json:"timezone,omitempty"`
	// Prompt is what the model is asked to do on patrol.
	Prompt string `json:"prompt,omitempty"`
}

// Web is search through a SearXNG.
type Web struct {
	SearXNG string `json:"searxng,omitempty"`
	// Allow are the domains whose pages are read from search results.
	Allow []string `json:"allow,omitempty"`
	// Private are words never sent in a query: hostnames, domains, project
	// names. Node names are added on their own.
	Private []string `json:"private,omitempty"`
}

// Traces is an OTLP/HTTP endpoint, e.g. tremolo's ingest. The token comes from $TRACES_TOKEN.
type Traces struct {
	URL   string `json:"url,omitempty"`
	Token string `json:"-"`
}

// Duration is a time.Duration written like "15m".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return []byte(`"` + d.String() + `"`), nil }

// Load reads the file at path, fills in defaults and checks it.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(b)
}

// Parse parses YAML configuration.
func Parse(b []byte) (Config, error) {
	var c Config
	if err := yaml.UnmarshalStrict(b, &c); err != nil {
		return c, err
	}
	c.Tremolo.Token = os.Getenv("TREMOLO_TOKEN")
	c.Traces.Token = os.Getenv("TRACES_TOKEN")
	or := func(d *Duration, v time.Duration) {
		if d.Duration == 0 {
			d.Duration = v
		}
	}
	or(&c.Every, 30*time.Second)
	or(&c.Model.Timeout, 15*time.Minute)
	or(&c.GPU.WakeDelay, 3*time.Minute)
	or(&c.Patrol.Every, time.Hour)
	if c.Name == "" {
		c.Name = "upkeeper"
	}
	if c.Model.MaxSteps == 0 {
		c.Model.MaxSteps = 30
	}
	if c.Model.Temperatures == nil {
		c.Model.Temperatures = []float64{0.7, 0.45, 0.2}
	}
	if c.Patrol.Timezone == "" {
		c.Patrol.Timezone = "Local"
	}
	var errs []error
	if c.Tremolo.URL == "" {
		errs = append(errs, errors.New("tremolo.url is required"))
	}
	if c.Model.URL == "" || c.Model.Name == "" {
		errs = append(errs, errors.New("model.url and model.name are required"))
	}
	if c.GPU.Deployment != "" && !strings.Contains(c.GPU.Deployment, "/") {
		errs = append(errs, errors.New("gpu.deployment must be namespace/name"))
	}
	if c.Patrol.At != "" {
		if _, err := time.Parse("15:04", c.Patrol.At); err != nil {
			errs = append(errs, fmt.Errorf("patrol.at %q must be HH:MM", c.Patrol.At))
		}
	}
	if _, err := time.LoadLocation(c.Patrol.Timezone); err != nil {
		errs = append(errs, fmt.Errorf("patrol.timezone: %w", err))
	}
	return c, errors.Join(errs...)
}
