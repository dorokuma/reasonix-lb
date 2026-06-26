package main

import (
    "io"
    "log"
    "net/http"
    "time"
)

// StartProbeLoop periodically probes exhausted accounts to check if their quota has refreshed.
// Sends GET {baseURL}/models; 200 → mark healthy, any other response → stays exhausted.
func StartProbeLoop(pool *Pool, interval time.Duration, stop <-chan struct{}) {
    log.Printf("probe loop started (interval=%v)", interval)
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-stop:
                log.Printf("probe loop stopped")
                return
            case <-ticker.C:
                probeExhausted(pool)
            }
        }
    }()
}

func probeExhausted(pool *Pool) {
    exhausted := pool.ExhaustedAccounts()
    if len(exhausted) == 0 {
        return
    }
    for _, acc := range exhausted {
        url := acc.BaseURL() + "/models"
        req, err := http.NewRequest("GET", url, nil)
        if err != nil {
            log.Printf("probe %s: bad url %s: %v", acc.Name(), url, err)
            continue
        }
        req.Header.Set("Authorization", "Bearer "+acc.Key())
        resp, err := acc.Client().Do(req)
        if err != nil {
            log.Printf("probe %s: request failed: %v", acc.Name(), err)
            continue
        }
        io.Copy(io.Discard, resp.Body)
        resp.Body.Close()
        if resp.StatusCode == 200 {
            acc.MarkHealthy()
            log.Printf("probe %s: recovered (200), returned to pool", acc.Name())
        } else {
            log.Printf("probe %s: still exhausted (status=%d)", acc.Name(), resp.StatusCode)
        }
    }
}
