package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	collector "github.com/luanruisong/hyperliquid-collector"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	address, cfg, err := loadConfig()
	if err != nil {
		return err
	}
	source, err := collector.New(cfg)
	if err != nil {
		return fmt.Errorf("configure collector: %w", err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	hub := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.serveWebSocket)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	collectCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverResult := make(chan error, 1)
	collectorResult := make(chan error, 1)
	go func() {
		serverResult <- server.Serve(listener)
	}()
	go func() {
		collectorResult <- source.Run(collectCtx, hub.publish)
	}()
	log.Printf("WebSocket listening on %s/ws", listener.Addr())

	var serverErr, collectorErr error
	select {
	case <-ctx.Done():
	case serverErr = <-serverResult:
		serverResult = nil
	case collectorErr = <-collectorResult:
		collectorResult = nil
	}
	cancel()
	hub.close()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	if serverResult != nil {
		serverErr = <-serverResult
	}
	if collectorResult != nil {
		collectorErr = <-collectorResult
	}
	if serverErr == http.ErrServerClosed {
		serverErr = nil
	}
	if collectorErr == context.Canceled {
		collectorErr = nil
	}
	return errors.Join(serverErr, collectorErr, shutdownErr)
}
