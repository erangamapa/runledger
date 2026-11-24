// Package store persists spans in sqlite and keeps a per run summary row
// up to date in the same transaction, so listing runs never scans spans.
package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/erangamapa/runledger/internal/model"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrBadCursor = errors.New("bad cursor")
)

const schema = `
CREATE TABLE IF NOT EXISTS spans (
  span_id       TEXT PRIMARY KEY,
  run_id        TEXT NOT NULL,
  parent_id     TEXT NOT NULL DEFAULT '',
  kind          TEXT NOT NULL,
  name          TEXT NOT NULL,
  agent         TEXT NOT NULL,
  tenant        TEXT NOT NULL,
  model         TEXT NOT NULL DEFAULT '',
  input_tokens  INTEGER NOT NULL,
  output_tokens INTEGER NOT NULL,
  cost_micros   INTEGER NOT NULL,
  started_at    INTEGER NOT NULL,
  ended_at      INTEGER NOT NULL,
  status        TEXT NOT NULL,
  error         TEXT NOT NULL DEFAULT '',
  attrs         TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS spans_run ON spans(run_id, started_at);
CREATE INDEX IF NOT EXISTS spans_started ON spans(started_at);

CREATE TABLE IF NOT EXISTS runs (
  id            TEXT PRIMARY KEY,
  agent         TEXT NOT NULL,
  tenant        TEXT NOT NULL,
  status        TEXT NOT NULL,
  started_at    INTEGER NOT NULL,
  ended_at      INTEGER,
  spans         INTEGER NOT NULL,
  llm_calls     INTEGER NOT NULL,
  input_tokens  INTEGER NOT NULL,
  output_tokens INTEGER NOT NULL,
  cost_micros   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_started ON runs(started_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS runs_agent ON runs(agent, started_at DESC);
`

type Store struct {
	db *sql.DB
}

// Open takes a file path, or ":memory:" for tests.
func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	if path == ":memory:" {
		dsn = "file::memory:?_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// sqlite has one writer anyway, a single connection also keeps an
	// in memory database alive and shared across calls
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

type AppendResult struct {
	Accepted   int
	Duplicates int
}

// Append writes spans in one transaction. A span id seen before is skipped,
// agents retry pushes and the summary must not double count.
func (s *Store) Append(ctx context.Context, spans []model.Span) (AppendResult, error) {
	var res AppendResult
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	for _, sp := range spans {
		attrs, err := json.Marshal(sp.Attrs)
		if err != nil {
			return res, err
		}
		r, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO spans
			(span_id, run_id, parent_id, kind, name, agent, tenant, model, input_tokens, output_tokens,
			 cost_micros, started_at, ended_at, status, error, attrs)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			sp.SpanID, sp.RunID, sp.ParentID, sp.Kind, sp.Name, sp.Agent, sp.Tenant, sp.Model,
			sp.InputTokens, sp.OutputTokens, sp.CostMicros, sp.StartedAt.UnixNano(), sp.EndedAt.UnixNano(),
			sp.Status, sp.Error, string(attrs))
		if err != nil {
			return res, err
		}
		if n, _ := r.RowsAffected(); n == 0 {
			res.Duplicates++
			continue
		}
		if err := upsertRun(ctx, tx, sp); err != nil {
			return res, err
		}
		res.Accepted++
	}
	return res, tx.Commit()
}

func upsertRun(ctx context.Context, tx *sql.Tx, sp model.Span) error {
	llm := 0
	if sp.Kind == model.KindLLM {
		llm = 1
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO runs
		(id, agent, tenant, status, started_at, ended_at, spans, llm_calls, input_tokens, output_tokens, cost_micros)
		VALUES (?,?,?,?,?,NULL,1,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
		  spans = spans + 1,
		  llm_calls = llm_calls + excluded.llm_calls,
		  input_tokens = input_tokens + excluded.input_tokens,
		  output_tokens = output_tokens + excluded.output_tokens,
		  cost_micros = cost_micros + excluded.cost_micros,
		  started_at = min(started_at, excluded.started_at)`,
		sp.RunID, sp.Agent, sp.Tenant, model.StatusRunning, sp.StartedAt.UnixNano(),
		llm, sp.InputTokens, sp.OutputTokens, sp.CostMicros)
	if err != nil {
		return err
	}
	if sp.Kind != model.KindRun {
		return nil
	}
	// the run span usually arrives last, it is the one that knows how the run ended
	_, err = tx.ExecContext(ctx, `UPDATE runs SET status = ?, ended_at = ?, agent = ?, tenant = ?,
		started_at = min(started_at, ?) WHERE id = ?`,
		sp.Status, sp.EndedAt.UnixNano(), sp.Agent, sp.Tenant, sp.StartedAt.UnixNano(), sp.RunID)
	return err
}

const runCols = `id, agent, tenant, status, started_at, ended_at, spans, llm_calls, input_tokens, output_tokens, cost_micros`

func scanRun(row interface{ Scan(...any) error }) (model.Run, error) {
	var r model.Run
	var started int64
	var ended sql.NullInt64
	err := row.Scan(&r.ID, &r.Agent, &r.Tenant, &r.Status, &started, &ended, &r.Spans, &r.LLMCalls,
		&r.InputTokens, &r.OutputTokens, &r.CostMicros)
	if err != nil {
		return r, err
	}
	r.StartedAt = time.Unix(0, started).UTC()
	if ended.Valid {
		t := time.Unix(0, ended.Int64).UTC()
		r.EndedAt = &t
	}
	return r, nil
}

func (s *Store) Run(ctx context.Context, id string) (model.Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

type RunFilter struct {
	Agent  string
	Tenant string
	Status model.Status
	Since  time.Time
	Limit  int
	Cursor string
}

type cursor struct {
	started int64
	id      string
}

func encodeCursor(r model.Run) string {
	raw := strconv.FormatInt(r.StartedAt.UnixNano(), 10) + "|" + r.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, ErrBadCursor
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	n, err := strconv.ParseInt(ts, 10, 64)
	if !ok || err != nil || id == "" {
		return cursor{}, ErrBadCursor
	}
	return cursor{started: n, id: id}, nil
}

// Runs pages newest first with a keyset cursor. Offsets fall apart once
// runs keep arriving while someone is paging.
func (s *Store) Runs(ctx context.Context, f RunFilter) ([]model.Run, string, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	var where []string
	var args []any
	if f.Agent != "" {
		where = append(where, "agent = ?")
		args = append(args, f.Agent)
	}
	if f.Tenant != "" {
		where = append(where, "tenant = ?")
		args = append(args, f.Tenant)
	}
	if f.Status != "" {
		where = append(where, "status = ?")
		args = append(args, f.Status)
	}
	if !f.Since.IsZero() {
		where = append(where, "started_at >= ?")
		args = append(args, f.Since.UnixNano())
	}
	if f.Cursor != "" {
		c, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, "(started_at < ? OR (started_at = ? AND id < ?))")
		args = append(args, c.started, c.started, c.id)
	}
	q := `SELECT ` + runCols + ` FROM runs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY started_at DESC, id DESC LIMIT ?"
	args = append(args, f.Limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []model.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > f.Limit {
		out = out[:f.Limit]
		next = encodeCursor(out[len(out)-1])
	}
	return out, next, nil
}

const spanCols = `span_id, run_id, parent_id, kind, name, agent, tenant, model, input_tokens, output_tokens,
	cost_micros, started_at, ended_at, status, error, attrs`

func scanSpan(rows *sql.Rows) (model.Span, error) {
	var sp model.Span
	var started, ended int64
	var attrs string
	err := rows.Scan(&sp.SpanID, &sp.RunID, &sp.ParentID, &sp.Kind, &sp.Name, &sp.Agent, &sp.Tenant, &sp.Model,
		&sp.InputTokens, &sp.OutputTokens, &sp.CostMicros, &started, &ended, &sp.Status, &sp.Error, &attrs)
	if err != nil {
		return sp, err
	}
	sp.StartedAt = time.Unix(0, started).UTC()
	sp.EndedAt = time.Unix(0, ended).UTC()
	if attrs != "" && attrs != "null" {
		if err := json.Unmarshal([]byte(attrs), &sp.Attrs); err != nil {
			return sp, err
		}
	}
	return sp, nil
}

func (s *Store) Spans(ctx context.Context, runID string) ([]model.Span, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+spanCols+` FROM spans WHERE run_id = ? ORDER BY started_at, span_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		sp, err := scanSpan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sp)
	}
	return out, rows.Err()
}

// Tree nests spans under their parents. Spans whose parent never arrived are
// hung off the root so nothing silently disappears from the view.
func Tree(spans []model.Span) *model.Node {
	nodes := make(map[string]*model.Node, len(spans))
	for _, sp := range spans {
		nodes[sp.SpanID] = &model.Node{Span: sp}
	}
	var root *model.Node
	var orphans []*model.Node
	for _, sp := range spans {
		n := nodes[sp.SpanID]
		if sp.ParentID == "" {
			if root == nil {
				root = n
			}
			continue
		}
		if p, ok := nodes[sp.ParentID]; ok {
			p.Children = append(p.Children, n)
		} else {
			orphans = append(orphans, n)
		}
	}
	if root == nil {
		root = &model.Node{Span: model.Span{Name: "missing run span", Kind: model.KindRun, Status: model.StatusRunning}}
	}
	root.Children = append(root.Children, orphans...)
	return root
}

type GroupBy string

const (
	ByAgent  GroupBy = "agent"
	ByModel  GroupBy = "model"
	ByTenant GroupBy = "tenant"
)

type Rollup struct {
	Key          string `json:"key"`
	Calls        int    `json:"calls"`
	Errors       int    `json:"errors"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	CostMicros   int64  `json:"cost_micros"`
	P50Millis    int64  `json:"p50_ms"`
	P95Millis    int64  `json:"p95_ms"`
}

// Rollups aggregates llm spans in [since, until). Percentiles are computed
// here rather than in sql because sqlite has no percentile function and a
// day of spans fits comfortably in memory.
func (s *Store) Rollups(ctx context.Context, by GroupBy, since, until time.Time) ([]Rollup, error) {
	col := map[GroupBy]string{ByAgent: "agent", ByModel: "model", ByTenant: "tenant"}[by]
	if col == "" {
		return nil, fmt.Errorf("cannot group by %q", by)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+col+`, input_tokens, output_tokens, cost_micros,
		ended_at - started_at, status FROM spans
		WHERE kind = ? AND started_at >= ? AND started_at < ?`,
		model.KindLLM, since.UnixNano(), until.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	acc := map[string]*Rollup{}
	lat := map[string][]int64{}
	for rows.Next() {
		var key string
		var in, out, cost, dur int64
		var status model.Status
		if err := rows.Scan(&key, &in, &out, &cost, &dur, &status); err != nil {
			return nil, err
		}
		r, ok := acc[key]
		if !ok {
			r = &Rollup{Key: key}
			acc[key] = r
		}
		r.Calls++
		if status == model.StatusError {
			r.Errors++
		}
		r.InputTokens += in
		r.OutputTokens += out
		r.CostMicros += cost
		lat[key] = append(lat[key], dur/int64(time.Millisecond))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Rollup, 0, len(acc))
	for k, r := range acc {
		ls := lat[k]
		sort.Slice(ls, func(i, j int) bool { return ls[i] < ls[j] })
		r.P50Millis = percentile(ls, 50)
		r.P95Millis = percentile(ls, 95)
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CostMicros != out[j].CostMicros {
			return out[i].CostMicros > out[j].CostMicros
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// nearest rank on a sorted slice
func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}
