// Package model holds the ledger's domain types, independent of transport.
package model

import (
	"errors"
	"fmt"
	"time"
)

type Kind string

const (
	KindRun  Kind = "run"
	KindLLM  Kind = "llm"
	KindTool Kind = "tool"
	KindGate Kind = "gate"
)

type Status string

const (
	StatusRunning     Status = "running"
	StatusOK          Status = "ok"
	StatusError       Status = "error"
	StatusInterrupted Status = "interrupted"
)

type Span struct {
	RunID        string            `json:"run_id"`
	SpanID       string            `json:"span_id"`
	ParentID     string            `json:"parent_id,omitempty"`
	Kind         Kind              `json:"kind"`
	Name         string            `json:"name"`
	Agent        string            `json:"agent"`
	Tenant       string            `json:"tenant"`
	Model        string            `json:"model,omitempty"`
	InputTokens  int               `json:"input_tokens"`
	OutputTokens int               `json:"output_tokens"`
	CostMicros   int64             `json:"cost_micros"`
	StartedAt    time.Time         `json:"started_at"`
	EndedAt      time.Time         `json:"ended_at"`
	Status       Status            `json:"status"`
	Error        string            `json:"error,omitempty"`
	Attrs        map[string]string `json:"attrs,omitempty"`
}

func (s Span) Duration() time.Duration { return s.EndedAt.Sub(s.StartedAt) }

var ErrInvalidSpan = errors.New("invalid span")

const maxAttrs = 32

func (s Span) Validate() error {
	bad := func(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidSpan, msg) }
	switch {
	case s.RunID == "" || s.SpanID == "":
		return bad("run id and span id are required")
	case s.Agent == "":
		return bad("agent is required")
	case s.StartedAt.IsZero() || s.EndedAt.IsZero():
		return bad("start and end are required")
	case s.EndedAt.Before(s.StartedAt):
		return bad("span ends before it starts")
	case s.InputTokens < 0 || s.OutputTokens < 0:
		return bad("token counts must not be negative")
	case len(s.Attrs) > maxAttrs:
		return bad(fmt.Sprintf("at most %d attrs", maxAttrs))
	case s.Kind == KindRun && s.ParentID != "":
		return bad("a run span cannot have a parent")
	case s.Kind != KindRun && s.ParentID == "":
		return bad("only the run span may be a root")
	}
	switch s.Kind {
	case KindRun, KindLLM, KindTool, KindGate:
	default:
		return bad(fmt.Sprintf("unknown kind %q", s.Kind))
	}
	switch s.Status {
	case StatusOK, StatusError, StatusInterrupted:
	default:
		return bad(fmt.Sprintf("unknown status %q", s.Status))
	}
	return nil
}

type Run struct {
	ID           string     `json:"id"`
	Agent        string     `json:"agent"`
	Tenant       string     `json:"tenant"`
	Status       Status     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	Spans        int        `json:"spans"`
	LLMCalls     int        `json:"llm_calls"`
	InputTokens  int64      `json:"input_tokens"`
	OutputTokens int64      `json:"output_tokens"`
	CostMicros   int64      `json:"cost_micros"`
}

// Node is a span with its children, used when a run is returned as a tree.
type Node struct {
	Span
	Children []*Node `json:"children,omitempty"`
}
