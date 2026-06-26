package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"time"
)

var hopByHopHeaders = map[string]bool{
	"Connection":          true,
	"Transfer-Encoding":   true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Upgrade":             true,
}

func isHopByHop(key string) bool {
	return hopByHopHeaders[http.CanonicalHeaderKey(key)]
}

func isQuotaExhausted(status int) bool {
	return status == 402 || status == 429
}

func upstreamContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), upstreamTimeout)
}

func copyClientHeaders(dst http.Header, src http.Header) {
	for k, vs := range src {
		if isHopByHop(k) {
			continue
		}
		if http.CanonicalHeaderKey(k) == "Authorization" {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func copyUpstreamHeaders(dst http.ResponseWriter, src http.Header) {
	for k, vs := range src {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vs {
			dst.Header().Add(k, v)
		}
	}
}

func NewProxyHandler(pool *Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			w.Write([]byte("ok"))
			return
		}
		if r.URL.Path == "/v1/models" {
			proxyModels(pool, w, r)
			return
		}
		if r.URL.Path == "/v1/chat/completions" {
			proxyChat(pool, w, r)
			return
		}
		http.Error(w, "not found", 404)
	})
}

func proxyChat(pool *Pool, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("proxy: chat request from %s, body read error: %v", r.RemoteAddr, err)
		http.Error(w, "failed to read body", 500)
		return
	}
	log.Printf("proxy: chat request from %s, body=%d bytes", r.RemoteAddr, len(bodyBytes))

	maxAttempts := len(pool.accounts) * 2
	for attempts := 0; attempts < maxAttempts; attempts++ {
		acc := pool.Select()
		if acc == nil {
			http.Error(w, `{"error":{"message":"All accounts exhausted","code":"all_exhausted"}}`, 503)
			return
		}

		done, streamErr := func() (bool, error) {
			ctx, cancel := upstreamContext()
			defer cancel() // keep ctx alive until body is fully read — early cancel truncates SSE

			req, err := http.NewRequestWithContext(ctx, "POST", acc.BaseURL()+"/chat/completions", bytes.NewReader(bodyBytes))
			if err != nil {
				log.Printf("proxy: failed to create request for %s: %v", acc.Name(), err)
				return false, nil
			}
			copyClientHeaders(req.Header, r.Header)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+acc.Key())

			resp, err := acc.Client().Do(req)
			if err != nil {
				log.Printf("proxy: chat retry via %s (upstream error, not marking exhausted): %v", acc.Name(), err)
				return false, nil
			}
			defer resp.Body.Close()

			if isQuotaExhausted(resp.StatusCode) {
				acc.MarkExhausted()
				log.Printf("account %s: quota exhausted (%d), trying next", acc.Name(), resp.StatusCode)
				return false, nil
			}
			if resp.StatusCode >= 500 {
				log.Printf("proxy: chat retry via %s (upstream %d, not marking exhausted)", acc.Name(), resp.StatusCode)
				return false, nil
			}

			copyUpstreamHeaders(w, resp.Header)
			w.WriteHeader(resp.StatusCode)
			n, err := streamResponseBody(w, resp.Body, r, acc.Name())
			if err != nil {
				return true, err
			}
			if cl := resp.Header.Get("Content-Length"); cl != "" {
				log.Printf("proxy: chat done via %s, status=%d, written=%d, content-length=%s, elapsed=%v", acc.Name(), resp.StatusCode, n, cl, time.Since(start))
			} else {
				log.Printf("proxy: chat done via %s, status=%d, written=%d, elapsed=%v", acc.Name(), resp.StatusCode, n, time.Since(start))
			}
			return true, nil
		}()
		if done {
			if streamErr != nil {
				return
			}
			return
		}
	}
	log.Printf("proxy: chat failed, all exhausted")
	http.Error(w, `{"error":{"message":"All accounts exhausted after retries","code":"all_exhausted"}}`, 503)
}

func proxyModels(pool *Pool, w http.ResponseWriter, r *http.Request) {
	log.Printf("proxy: models request from %s", r.RemoteAddr)
	maxAttempts := len(pool.accounts) * 2
	for attempts := 0; attempts < maxAttempts; attempts++ {
		acc := pool.Select()
		if acc == nil {
			http.Error(w, `{"error":{"message":"No healthy accounts","code":"no_accounts"}}`, 503)
			return
		}

		done := func() bool {
			ctx, cancel := upstreamContext()
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", acc.BaseURL()+"/models", nil)
			if err != nil {
				log.Printf("proxy: failed to create models request for %s: %v", acc.Name(), err)
				return false
			}
			copyClientHeaders(req.Header, r.Header)
			req.Header.Set("Authorization", "Bearer "+acc.Key())

			resp, err := acc.Client().Do(req)
			if err != nil {
				log.Printf("proxy: models retry via %s (upstream error, not marking exhausted): %v", acc.Name(), err)
				return false
			}
			defer resp.Body.Close()

			if isQuotaExhausted(resp.StatusCode) {
				log.Printf("proxy: %s models quota exhausted (%d), marking exhausted", acc.Name(), resp.StatusCode)
				acc.MarkExhausted()
				return false
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("proxy: models retry via %s (status %d, not marking exhausted)", acc.Name(), resp.StatusCode)
				return false
			}

			copyUpstreamHeaders(w, resp.Header)
			w.WriteHeader(resp.StatusCode)
			n, err := io.Copy(w, resp.Body)
			if err != nil {
				log.Printf("proxy: failed to copy models response body for %s: %v", acc.Name(), err)
			} else {
				log.Printf("proxy: models done via %s, status=%d, written=%d", acc.Name(), resp.StatusCode, n)
			}
			return true
		}()
		if done {
			return
		}
	}
	log.Printf("proxy: models failed, all exhausted")
	http.Error(w, `{"error":{"message":"All accounts exhausted for /models","code":"all_exhausted"}}`, 503)
}