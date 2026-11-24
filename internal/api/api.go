// Package api is the http read side, plus a json ingest endpoint for
// scripts and notebooks that do not want to speak grpc.
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/erangamapa/runledger/internal/model"
	"github.com/erangamapa/runledger/internal/pricing"
	"github.com/erangamapa/runledger/internal/store"
)

const maxIngestBatch = 1000

type Store interface {
	Append(ctx context.Context, spans []model.Span) (store.AppendResult, error)
	Run(ctx context.Context, id string) (model.Run, error)
	Runs(ctx context.Context, f store.RunFilter) ([]model.Run, string, error)
	Spans(ctx context.Context, runID string) ([]model.Span, error)
	Rollups(ctx context.Context, by store.GroupBy, since, until time.Time) ([]store.Rollup, error)
	Ping(ctx context.Context) error
}

type Options struct {
	// Token guards everything under /v1. Empty disables auth, meant for local runs only.
	Token  string
	Prices *pricing.Table
	Log    *slog.Logger
	Now    func() time.Time
}

type handler struct {
	st     Store
	prices *pricing.Table
	now    func() time.Time
}

func New(st Store, opts Options) *gin.Engine {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Prices == nil {
		opts.Prices = pricing.Default()
	}
	h := &handler{st: st, prices: opts.Prices, now: opts.Now}

	r := gin.New()
	r.Use(gin.Recovery(), requestID(), accessLog(opts.Log))
	r.GET("/healthz", h.health)

	v1 := r.Group("/v1", bearer(opts.Token))
	v1.GET("/runs", h.listRuns)
	v1.GET("/runs/:id", h.getRun)
	v1.GET("/rollups", h.rollups)
	v1.POST("/spans", h.ingest)
	return r
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func fail(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"error": apiError{Code: code, Message: msg}})
}

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-Id")
		if id == "" || len(id) > 64 {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		c.Set("request_id", id)
		c.Header("X-Request-Id", id)
		c.Next()
	}
}

func accessLog(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http", "method", c.Request.Method, "route", c.FullPath(), "status", c.Writer.Status(),
			"ms", time.Since(start).Milliseconds(), "request_id", c.GetString("request_id"))
	}
}

func bearer(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		got := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			fail(c, http.StatusUnauthorized, "unauthorized", "missing or wrong bearer token")
			return
		}
		c.Next()
	}
}

func (h *handler) health(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	if err := h.st.Ping(ctx); err != nil {
		fail(c, http.StatusServiceUnavailable, "store_down", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func parseTime(c *gin.Context, key string, def time.Time) (time.Time, bool) {
	v := c.Query(key)
	if v == "" {
		return def, true
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		fail(c, http.StatusBadRequest, "bad_time", key+" must be RFC3339")
		return time.Time{}, false
	}
	return t, true
}

func (h *handler) listRuns(c *gin.Context) {
	since, ok := parseTime(c, "since", time.Time{})
	if !ok {
		return
	}
	limit := 50
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			fail(c, http.StatusBadRequest, "bad_limit", "limit must be between 1 and 200")
			return
		}
		limit = n
	}
	status := model.Status(c.Query("status"))
	switch status {
	case "", model.StatusRunning, model.StatusOK, model.StatusError, model.StatusInterrupted:
	default:
		fail(c, http.StatusBadRequest, "bad_status", "unknown status")
		return
	}
	runs, next, err := h.st.Runs(c.Request.Context(), store.RunFilter{
		Agent: c.Query("agent"), Tenant: c.Query("tenant"), Status: status,
		Since: since, Limit: limit, Cursor: c.Query("cursor"),
	})
	if errors.Is(err, store.ErrBadCursor) {
		fail(c, http.StatusBadRequest, "bad_cursor", "cursor is not valid")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if runs == nil {
		runs = []model.Run{}
	}
	c.JSON(http.StatusOK, gin.H{"runs": runs, "next_cursor": next})
}

func (h *handler) getRun(c *gin.Context) {
	ctx := c.Request.Context()
	run, err := h.st.Run(ctx, c.Param("id"))
	if errors.Is(err, store.ErrNotFound) {
		fail(c, http.StatusNotFound, "not_found", "no such run")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	spans, err := h.st.Spans(ctx, run.ID)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"run": run, "tree": store.Tree(spans)})
}

func (h *handler) rollups(c *gin.Context) {
	now := h.now()
	until, ok := parseTime(c, "until", now)
	if !ok {
		return
	}
	since, ok := parseTime(c, "since", until.Add(-24*time.Hour))
	if !ok {
		return
	}
	if !since.Before(until) {
		fail(c, http.StatusBadRequest, "bad_window", "since must be before until")
		return
	}
	if until.Sub(since) > 31*24*time.Hour {
		fail(c, http.StatusBadRequest, "bad_window", "window is capped at 31 days")
		return
	}
	group := store.GroupBy(c.DefaultQuery("group", string(store.ByAgent)))
	switch group {
	case store.ByAgent, store.ByModel, store.ByTenant:
	default:
		fail(c, http.StatusBadRequest, "bad_group", "group must be agent, model or tenant")
		return
	}
	rs, err := h.st.Rollups(c.Request.Context(), group, since, until)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	var total int64
	for _, r := range rs {
		total += r.CostMicros
	}
	c.JSON(http.StatusOK, gin.H{
		"group": group, "since": since.UTC(), "until": until.UTC(),
		"total_cost_micros": total, "rows": rs,
	})
}

type rejected struct {
	Index  int    `json:"index"`
	SpanID string `json:"span_id"`
	Reason string `json:"reason"`
}

func (h *handler) ingest(c *gin.Context) {
	var spans []model.Span
	if err := c.ShouldBindJSON(&spans); err != nil {
		fail(c, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if len(spans) == 0 || len(spans) > maxIngestBatch {
		fail(c, http.StatusBadRequest, "bad_batch", "send between 1 and 1000 spans")
		return
	}
	var good []model.Span
	var bad []rejected
	for i, sp := range spans {
		if err := sp.Validate(); err != nil {
			bad = append(bad, rejected{Index: i, SpanID: sp.SpanID, Reason: err.Error()})
			continue
		}
		if sp.Kind == model.KindLLM {
			sp.CostMicros = h.prices.CostMicros(sp.Model, sp.InputTokens, sp.OutputTokens)
		}
		good = append(good, sp)
	}
	res := store.AppendResult{}
	if len(good) > 0 {
		var err error
		res, err = h.st.Append(c.Request.Context(), good)
		if err != nil {
			fail(c, http.StatusServiceUnavailable, "store_down", err.Error())
			return
		}
	}
	status := http.StatusAccepted
	if len(good) == 0 {
		status = http.StatusUnprocessableEntity
	}
	if bad == nil {
		bad = []rejected{}
	}
	c.JSON(status, gin.H{"accepted": res.Accepted, "duplicates": res.Duplicates, "rejected": bad})
}
