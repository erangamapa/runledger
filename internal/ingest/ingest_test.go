package ingest

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/erangamapa/runledger/gen/runledger/v1"
	"github.com/erangamapa/runledger/internal/pricing"
	"github.com/erangamapa/runledger/internal/store"
)

func client(t *testing.T) (pb.IngestClient, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterIngestServer(gs, New(st, pricing.Default()))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewIngestClient(conn), st
}

var base = time.Date(2026, 9, 20, 19, 0, 0, 0, time.UTC)

func pbSpan(id, parent string, kind pb.SpanKind) *pb.Span {
	return &pb.Span{
		RunId: "run-1", SpanId: id, ParentId: parent, Kind: kind, Name: id, Agent: "analyst", Tenant: "pulse-agents",
		StartedAt: timestamppb.New(base), EndedAt: timestamppb.New(base.Add(time.Second)), Status: pb.SpanStatus_SPAN_STATUS_OK,
	}
}

func TestPushPricesAndStores(t *testing.T) {
	c, st := client(t)
	stream, err := c.PushSpans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	call := pbSpan("llm-1", "root", pb.SpanKind_SPAN_KIND_LLM)
	call.Model, call.InputTokens, call.OutputTokens = "claude-sonnet-4-5", 2000, 400
	broken := pbSpan("bad", "root", pb.SpanKind_SPAN_KIND_TOOL)
	broken.EndedAt = timestamppb.New(base.Add(-time.Second))
	for _, sp := range []*pb.Span{call, call, broken, pbSpan("root", "", pb.SpanKind_SPAN_KIND_RUN)} {
		if err := stream.Send(sp); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if sum.Accepted != 2 || sum.Duplicates != 1 || len(sum.Rejected) != 1 || sum.Rejected[0].SpanId != "bad" {
		t.Fatalf("summary %+v", sum)
	}
	run, err := st.Run(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	// 2000 in at 3 dollars and 400 out at 15 dollars per million
	if run.CostMicros != 12_000 || run.Status != "ok" {
		t.Fatalf("run %+v", run)
	}
}

func TestClientCannotSetCost(t *testing.T) {
	sp := FromProto(&pb.Span{Kind: pb.SpanKind_SPAN_KIND_TOOL, Model: "claude-opus-4-1", InputTokens: 1000}, pricing.Default())
	if sp.CostMicros != 0 {
		t.Fatal("only llm spans carry cost")
	}
}

func TestEmptyStream(t *testing.T) {
	c, _ := client(t)
	stream, err := c.PushSpans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sum, err := stream.CloseAndRecv()
	if err != nil || sum.Accepted != 0 {
		t.Fatalf("empty stream %+v %v", sum, err)
	}
}
