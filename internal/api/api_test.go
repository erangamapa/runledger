package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/erangamapa/runledger/internal/model"
	"github.com/erangamapa/runledger/internal/store"
)

var now = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st, Options{
		Token: "secret",
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	})
}

func do(t *testing.T, r http.Handler, method, path string, body any, auth bool) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var buf io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		buf = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, buf)
	if auth {
		req.Header.Set("Authorization", "Bearer secret")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w, out
}

func runSpans(run string, at time.Time) []model.Span {
	return []model.Span{
		{RunID: run, SpanID: run + "-llm", ParentID: run + "-root", Kind: model.KindLLM, Name: "draft brief",
			Agent: "analyst", Tenant: "pulse-agents", Model: "claude-sonnet-4-5", InputTokens: 1000, OutputTokens: 200,
			CostMicros: 999_999_999, StartedAt: at.Add(time.Second), EndedAt: at.Add(3 * time.Second), Status: model.StatusOK},
		{RunID: run, SpanID: run + "-root", Kind: model.KindRun, Name: "match preview",
			Agent: "analyst", Tenant: "pulse-agents", StartedAt: at, EndedAt: at.Add(5 * time.Second), Status: model.StatusOK},
	}
}

func TestAuth(t *testing.T) {
	r := setup(t)
	if w, _ := do(t, r, "GET", "/v1/runs", nil, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 got %d", w.Code)
	}
	if w, _ := do(t, r, "GET", "/healthz", nil, false); w.Code != http.StatusOK {
		t.Fatalf("health should be open, got %d", w.Code)
	}
}

func TestIngestThenRead(t *testing.T) {
	r := setup(t)
	w, body := do(t, r, "POST", "/v1/spans", runSpans("r1", now.Add(-time.Hour)), true)
	if w.Code != http.StatusAccepted || body["accepted"].(float64) != 2 {
		t.Fatalf("ingest %d %v", w.Code, body)
	}

	w, body = do(t, r, "GET", "/v1/runs/r1", nil, true)
	if w.Code != http.StatusOK {
		t.Fatalf("get run %d", w.Code)
	}
	run := body["run"].(map[string]any)
	// client sent a silly cost, the ledger priced it from the table instead
	if run["cost_micros"].(float64) != 6000 || run["status"] != "ok" {
		t.Fatalf("run %v", run)
	}
	tree := body["tree"].(map[string]any)
	if tree["span_id"] != "r1-root" || len(tree["children"].([]any)) != 1 {
		t.Fatalf("tree %v", tree)
	}
}

func TestRequestIDEchoed(t *testing.T) {
	r := setup(t)
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("X-Request-Id", "abc123")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Header().Get("X-Request-Id") != "abc123" {
		t.Fatal("request id not echoed")
	}
}

func TestIngestRejectsBadSpans(t *testing.T) {
	r := setup(t)
	bad := runSpans("r1", now)
	bad[0].EndedAt = bad[0].StartedAt.Add(-time.Second)
	bad[1].Agent = ""
	w, body := do(t, r, "POST", "/v1/spans", bad, true)
	if w.Code != http.StatusUnprocessableEntity || len(body["rejected"].([]any)) != 2 {
		t.Fatalf("got %d %v", w.Code, body)
	}
	if w, _ := do(t, r, "POST", "/v1/spans", []model.Span{}, true); w.Code != http.StatusBadRequest {
		t.Fatalf("empty batch want 400 got %d", w.Code)
	}
}

func TestListRunsPaging(t *testing.T) {
	r := setup(t)
	for i := 0; i < 3; i++ {
		do(t, r, "POST", "/v1/spans", runSpans(fmt.Sprintf("r%d", i), now.Add(time.Duration(-i)*time.Hour)), true)
	}
	w, body := do(t, r, "GET", "/v1/runs?limit=2", nil, true)
	if w.Code != http.StatusOK || len(body["runs"].([]any)) != 2 || body["next_cursor"] == "" {
		t.Fatalf("page one %d %v", w.Code, body)
	}
	w, body = do(t, r, "GET", "/v1/runs?limit=2&cursor="+body["next_cursor"].(string), nil, true)
	if len(body["runs"].([]any)) != 1 || body["next_cursor"] != "" {
		t.Fatalf("page two %v", body)
	}
	for _, q := range []string{"limit=0", "status=weird", "since=yesterday", "cursor=%25%25"} {
		if w, _ := do(t, r, "GET", "/v1/runs?"+q, nil, true); w.Code != http.StatusBadRequest {
			t.Errorf("%s want 400 got %d", q, w.Code)
		}
	}
}

func TestRollups(t *testing.T) {
	r := setup(t)
	do(t, r, "POST", "/v1/spans", runSpans("r1", now.Add(-2*time.Hour)), true)
	do(t, r, "POST", "/v1/spans", runSpans("old", now.Add(-48*time.Hour)), true)

	w, body := do(t, r, "GET", "/v1/rollups?group=model", nil, true)
	if w.Code != http.StatusOK {
		t.Fatalf("rollups %d %v", w.Code, body)
	}
	rows := body["rows"].([]any)
	if len(rows) != 1 || body["total_cost_micros"].(float64) != 6000 {
		t.Fatalf("default window should be the last day only, got %v", body)
	}
	row := rows[0].(map[string]any)
	if row["key"] != "claude-sonnet-4-5" || row["p95_ms"].(float64) != 2000 {
		t.Fatalf("row %v", row)
	}
	for _, q := range []string{"group=colour", "since=2026-10-03T12:00:00Z&until=2026-10-01T00:00:00Z", "since=2026-01-01T00:00:00Z"} {
		if w, _ := do(t, r, "GET", "/v1/rollups?"+q, nil, true); w.Code != http.StatusBadRequest {
			t.Errorf("%s want 400 got %d", q, w.Code)
		}
	}
}

func TestMissingRun(t *testing.T) {
	r := setup(t)
	w, body := do(t, r, "GET", "/v1/runs/ghost", nil, true)
	if w.Code != http.StatusNotFound || body["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("got %d %v", w.Code, body)
	}
}
