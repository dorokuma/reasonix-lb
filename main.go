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

    srv := &http.Server{Addr: cfg.Listen, Handler: NewProxyHandler(pool)}
    go func() {
        <-sigCh
        log.Printf("shutting down...")
        close(stop)
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        srv.Shutdown(ctx)
    }()

    if err := srv.ListenAndServe(); err != nil {
        log.Fatalf("listen: %v", err)
    }
}
