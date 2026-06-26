package main

import (
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
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
    go func() {
        <-sigCh
        log.Printf("shutting down...")
        close(stop)
        os.Exit(0)
    }()

    if err := http.ListenAndServe(cfg.Listen, NewProxyHandler(pool)); err != nil {
        log.Fatalf("listen: %v", err)
    }
}
