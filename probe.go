package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// StartProbeLoop periodically probes exhausted accounts to check if their quota has refreshed.
// Sends GET {baseURL}/models; 200 → mark healthy, any other response → stays exhausted.
func StartProbeLoop(pool *Pool, interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	log.Printf("probe loop started (interval=%v)", interval)
	go func() {
		// Probe immediately, then periodically
		probeExhausted(pool)
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

	const maxAttempts = 3
	const retryDelay = 2 * time.Second

	var wg sync.WaitGroup
	for _, acc := range exhausted {
		wg.Add(1)
		go func(acc *Account) {
			defer wg.Done()

			for attempt := 1; attempt <= maxAttempts; attempt++ {
				url := acc.BaseURL() + "/models"
				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					log.Printf("probe %s: failed to create request: %v", acc.Name(), err)
					return
				}
				req.Header.Set("Authorization", "Bearer "+acc.Key())

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				req = req.WithContext(ctx)
				resp, err := acc.Client().Do(req)
				cancel()

				if err != nil {
					log.Printf("probe %s: request failed (attempt %d/%d): %v", acc.Name(), attempt, maxAttempts, err)
					if attempt < maxAttempts {
						time.Sleep(retryDelay)
						continue
					}
					return
				}

				if resp.StatusCode == 200 {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					pool.MarkHealthy(acc)
					log.Printf("probe %s: recovered (200), returned to pool", acc.Name())
					return
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				log.Printf("probe %s: still exhausted (status=%d, attempt %d/%d)", acc.Name(), resp.StatusCode, attempt, maxAttempts)

				if attempt < maxAttempts {
					time.Sleep(retryDelay)
				}
			}
		}(acc)
	}
	wg.Wait()
}
