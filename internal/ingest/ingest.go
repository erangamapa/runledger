// Package ingest is the grpc write path.
package ingest

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/erangamapa/runledger/gen/runledger/v1"
	"github.com/erangamapa/runledger/internal/model"
	"github.com/erangamapa/runledger/internal/pricing"
	"github.com/erangamapa/runledger/internal/store"
)

// Writer is the part of the store ingest needs.
type Writer interface {
	Append(ctx context.Context, spans []model.Span) (store.AppendResult, error)
}

const flushEvery = 256

type Server struct {
	pb.UnimplementedIngestServer
	w      Writer
	prices *pricing.Table
}

func New(w Writer, prices *pricing.Table) *Server {
	return &Server{w: w, prices: prices}
}

var kinds = map[pb.SpanKind]model.Kind{
	pb.SpanKind_SPAN_KIND_RUN:  model.KindRun,
	pb.SpanKind_SPAN_KIND_LLM:  model.KindLLM,
	pb.SpanKind_SPAN_KIND_TOOL: model.KindTool,
	pb.SpanKind_SPAN_KIND_GATE: model.KindGate,
}

var statuses = map[pb.SpanStatus]model.Status{
	pb.SpanStatus_SPAN_STATUS_OK:          model.StatusOK,
	pb.SpanStatus_SPAN_STATUS_ERROR:       model.StatusError,
	pb.SpanStatus_SPAN_STATUS_INTERRUPTED: model.StatusInterrupted,
}

// FromProto converts and prices a span. Cost is always computed here from
// the price table, callers do not get to tell the ledger what they spent.
func FromProto(in *pb.Span, prices *pricing.Table) model.Span {
	sp := model.Span{
		RunID: in.GetRunId(), SpanID: in.GetSpanId(), ParentID: in.GetParentId(),
		Kind: kinds[in.GetKind()], Name: in.GetName(), Agent: in.GetAgent(), Tenant: in.GetTenant(),
		Model: in.GetModel(), InputTokens: int(in.GetInputTokens()), OutputTokens: int(in.GetOutputTokens()),
		Status: statuses[in.GetStatus()], Error: in.GetError(), Attrs: in.GetAttrs(),
	}
	if in.GetStartedAt() != nil {
		sp.StartedAt = in.GetStartedAt().AsTime()
	}
	if in.GetEndedAt() != nil {
		sp.EndedAt = in.GetEndedAt().AsTime()
	}
	if sp.Kind == model.KindLLM {
		sp.CostMicros = prices.CostMicros(sp.Model, sp.InputTokens, sp.OutputTokens)
	}
	return sp
}

// PushSpans keeps reading until the client closes the stream. Bad spans are
// reported back individually instead of failing the whole stream, one broken
// span from a buggy tool should not drop a run's worth of good ones.
func (s *Server) PushSpans(stream pb.Ingest_PushSpansServer) error {
	ctx := stream.Context()
	summary := &pb.PushSummary{}
	batch := make([]model.Span, 0, flushEvery)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		res, err := s.w.Append(ctx, batch)
		if err != nil {
			return status.Errorf(codes.Unavailable, "store: %v", err)
		}
		summary.Accepted += int32(res.Accepted)
		summary.Duplicates += int32(res.Duplicates)
		batch = batch[:0]
		return nil
	}
	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if err := flush(); err != nil {
				return err
			}
			return stream.SendAndClose(summary)
		}
		if err != nil {
			return err
		}
		sp := FromProto(in, s.prices)
		if err := sp.Validate(); err != nil {
			summary.Rejected = append(summary.Rejected, &pb.Rejected{SpanId: in.GetSpanId(), Reason: err.Error()})
			continue
		}
		batch = append(batch, sp)
		if len(batch) >= flushEvery {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
