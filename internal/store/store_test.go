package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/erangamapa/runledger/internal/model"
)

var t0 = time.Date(2026, 9, 12, 18, 0, 0, 0, time.UTC)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func span(run, id, parent string, kind model.Kind, startOff, dur time.Duration) model.Span {
	return model.Span{
		RunID: run, SpanID: id, ParentID: parent, Kind: kind, Name: string(kind) + " " + id,
		Agent: "analyst", Tenant: "pulse-agents", StartedAt: t0.Add(startOff), EndedAt: t0.Add(startOff + dur),
		Status: model.StatusOK,
	}
}

func llm(run, id string, startOff, dur time.Duration, modelName string, in, out int, cost int64) model.Span {
	s := span(run, id, run+"-root", model.KindLLM, startOff, dur)
	s.Model, s.InputTokens, s.OutputTokens, s.CostMicros = modelName, in, out, cost
	return s
}

func TestRunSummaryTracksSpans(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	_, err := s.Append(ctx, []model.Span{
		llm("r1", "a", time.Second, 2*time.Second, "claude-sonnet-4-5", 100, 50, 1050),
		llm("r1", "b", 4*time.Second, time.Second, "claude-sonnet-4-5", 200, 20, 900),
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Run(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != model.StatusRunning || r.EndedAt != nil || r.LLMCalls != 2 || r.CostMicros != 1950 {
		t.Fatalf("in flight run summary wrong %+v", r)
	}

	root := span("r1", "r1-root", "", model.KindRun, 0, 6*time.Second)
	root.Status = model.StatusInterrupted
	if _, err := s.Append(ctx, []model.Span{root}); err != nil {
		t.Fatal(err)
	}
	r, _ = s.Run(ctx, "r1")
	if r.Status != model.StatusInterrupted || r.EndedAt == nil || !r.StartedAt.Equal(t0) || r.Spans != 3 {
		t.Fatalf("closed run summary wrong %+v", r)
	}
}

func TestDuplicatesDoNotDoubleCount(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	batch := []model.Span{llm("r1", "a", 0, time.Second, "gpt-4o", 10, 10, 125)}
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	res, err := s.Append(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := s.Run(ctx, "r1")
	if res.Duplicates != 1 || res.Accepted != 0 || r.CostMicros != 125 || r.Spans != 1 {
		t.Fatalf("retry double counted, res %+v run %+v", res, r)
	}
}

func TestMissingRun(t *testing.T) {
	s := open(t)
	if _, err := s.Run(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestRunsPaging(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	var batch []model.Span
	for i := 0; i < 7; i++ {
		run := fmt.Sprintf("run-%d", i)
		batch = append(batch, span(run, run+"-root", "", model.KindRun, time.Duration(i)*time.Minute, time.Second))
	}
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	var seen []string
	cur := ""
	for page := 0; page < 10; page++ {
		runs, next, err := s.Runs(ctx, RunFilter{Limit: 3, Cursor: cur})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range runs {
			seen = append(seen, r.ID)
		}
		if next == "" {
			break
		}
		cur = next
	}
	want := []string{"run-6", "run-5", "run-4", "run-3", "run-2", "run-1", "run-0"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("paging order %v", seen)
	}
	if _, _, err := s.Runs(ctx, RunFilter{Cursor: "%%%"}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("want bad cursor, got %v", err)
	}
}

func TestRunsFilter(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := span("r1", "r1-root", "", model.KindRun, 0, time.Second)
	b := span("r2", "r2-root", "", model.KindRun, time.Minute, time.Second)
	b.Agent = "vip-concierge"
	b.Status = model.StatusError
	if _, err := s.Append(ctx, []model.Span{a, b}); err != nil {
		t.Fatal(err)
	}
	runs, _, err := s.Runs(ctx, RunFilter{Status: model.StatusError})
	if err != nil || len(runs) != 1 || runs[0].ID != "r2" {
		t.Fatalf("status filter got %+v %v", runs, err)
	}
	runs, _, _ = s.Runs(ctx, RunFilter{Agent: "analyst"})
	if len(runs) != 1 || runs[0].ID != "r1" {
		t.Fatalf("agent filter got %+v", runs)
	}
	runs, _, _ = s.Runs(ctx, RunFilter{Since: t0.Add(30 * time.Second)})
	if len(runs) != 1 || runs[0].ID != "r2" {
		t.Fatalf("since filter got %+v", runs)
	}
}

func TestSpansRoundTrip(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	sp := llm("r1", "a", 0, time.Second, "claude-haiku-4-5", 1, 2, 11)
	sp.Attrs = map[string]string{"tool": "odds_lookup"}
	if _, err := s.Append(ctx, []model.Span{sp}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Spans(ctx, "r1")
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v %v", got, err)
	}
	if got[0].Attrs["tool"] != "odds_lookup" || !got[0].StartedAt.Equal(sp.StartedAt) || got[0].Model != "claude-haiku-4-5" {
		t.Fatalf("round trip lost data %+v", got[0])
	}
}

func TestTree(t *testing.T) {
	spans := []model.Span{
		span("r", "root", "", model.KindRun, 0, 10*time.Second),
		span("r", "plan", "root", model.KindLLM, time.Second, time.Second),
		span("r", "tool", "plan", model.KindTool, 2*time.Second, time.Second),
		span("r", "lost", "never-arrived", model.KindTool, 3*time.Second, time.Second),
	}
	root := Tree(spans)
	if root.SpanID != "root" || len(root.Children) != 2 {
		t.Fatalf("root children %d", len(root.Children))
	}
	if root.Children[0].SpanID != "plan" || root.Children[0].Children[0].SpanID != "tool" {
		t.Fatal("nesting wrong")
	}
	if root.Children[1].SpanID != "lost" {
		t.Fatal("orphan should hang off the root")
	}
}

func TestRollups(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	var batch []model.Span
	for i := 0; i < 20; i++ {
		batch = append(batch, llm("r1", fmt.Sprintf("s%d", i), time.Duration(i)*time.Second,
			time.Duration(i+1)*100*time.Millisecond, "claude-sonnet-4-5", 100, 10, 450))
	}
	cheap := llm("r1", "q", 0, 50*time.Millisecond, "qwen", 500, 1, 0)
	cheap.Status = model.StatusError
	batch = append(batch, cheap, span("r1", "t", "r1-root", model.KindTool, 0, time.Second))
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	rs, err := s.Rollups(ctx, ByModel, t0, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 || rs[0].Key != "claude-sonnet-4-5" {
		t.Fatalf("rollups %+v", rs)
	}
	son := rs[0]
	if son.Calls != 20 || son.CostMicros != 9000 || son.P50Millis != 1000 || son.P95Millis != 1900 {
		t.Fatalf("sonnet rollup %+v", son)
	}
	if rs[1].Errors != 1 {
		t.Fatalf("errors not counted %+v", rs[1])
	}
	if _, err := s.Rollups(ctx, "colour", t0, t0); err == nil {
		t.Fatal("unknown group should fail")
	}
}

func TestPercentile(t *testing.T) {
	if percentile(nil, 95) != 0 || percentile([]int64{7}, 50) != 7 {
		t.Fatal("edge cases")
	}
	if got := percentile([]int64{1, 2, 3, 4}, 50); got != 2 {
		t.Fatalf("p50 of 1..4 want 2 got %d", got)
	}
}
