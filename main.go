package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	pool := NewPool(cfg.Accounts)
	log.Printf("loaded %d accounts, listening on %s", len(cfg.Accounts), cfg.Listen)

	stop := make(chan struct{})
	StartProbeLoop(pool, cfg.ProbeInterval, stop)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	srv := &http.Server{Addr: cfg.Listen, Handler: NewProxyHandler(pool), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
	go func() {
		<-sigCh
		log.Printf("shutting down...")
		close(stop)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}
