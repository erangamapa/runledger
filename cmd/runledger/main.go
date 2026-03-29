package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/erangamapa/runledger/gen/runledger/v1"
	"github.com/erangamapa/runledger/internal/api"
	"github.com/erangamapa/runledger/internal/ingest"
	"github.com/erangamapa/runledger/internal/pricing"
	"github.com/erangamapa/runledger/internal/store"
)

func main() {
	httpAddr := flag.String("http", ":8080", "http listen address")
	grpcAddr := flag.String("grpc", ":9090", "grpc listen address")
	dbPath := flag.String("db", "runledger.db", "sqlite file")
	pricesPath := flag.String("prices", "", "optional price table yaml, defaults to the embedded one")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*httpAddr, *grpcAddr, *dbPath, *pricesPath, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(httpAddr, grpcAddr, dbPath, pricesPath string, log *slog.Logger) error {
	prices := pricing.Default()
	if pricesPath != "" {
		raw, err := os.ReadFile(pricesPath)
		if err != nil {
			return err
		}
		if prices, err = pricing.Parse(raw); err != nil {
			return err
		}
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	token := os.Getenv("RUNLEDGER_TOKEN")
	if token == "" {
		log.Warn("RUNLEDGER_TOKEN is empty, the http api is open")
	}
	gin.SetMode(gin.ReleaseMode)
	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           api.New(st, api.Options{Token: token, Prices: prices, Log: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	gs := grpc.NewServer(grpc.MaxRecvMsgSize(4 << 20))
	pb.RegisterIngestServer(gs, ingest.New(st, prices))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- gs.Serve(lis) }()
	go func() {
		if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	log.Info("listening", "http", httpAddr, "grpc", grpcAddr, "db", dbPath)

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	hs.Shutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// let open ingest streams finish so their last batch is flushed
	go func() { <-shutdownCtx.Done(); gs.Stop() }()
	gs.GracefulStop()
	return httpSrv.Shutdown(shutdownCtx)
}
