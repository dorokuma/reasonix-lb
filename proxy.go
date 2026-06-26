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

		ctx, cancel := upstreamContext()
		req, err := http.NewRequestWithContext(ctx, "POST", acc.BaseURL()+"/chat/completions", bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			log.Printf("proxy: failed to create request for %s: %v", acc.Name(), err)
			continue
		}
		for k, vs := range r.Header {
			if isHopByHop(k) {
				continue
			}
			if http.CanonicalHeaderKey(k) == "Authorization" {
				continue
			}
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+acc.Key())

		resp, err := acc.Client().Do(req)
		if err != nil {
			cancel()
			log.Printf("proxy: chat retry via %s (upstream error, not marking exhausted): %v", acc.Name(), err)
			continue
		}

		if isQuotaExhausted(resp.StatusCode) {
			acc.MarkExhausted()
			resp.Body.Close()
			cancel()
			log.Printf("account %s: quota exhausted (%d), trying next", acc.Name(), resp.StatusCode)
			continue
		}
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			cancel()
			log.Printf("proxy: chat retry via %s (upstream %d, not marking exhausted)", acc.Name(), resp.StatusCode)
			continue
		}

		for k, vs := range resp.Header {
			if isHopByHop(k) {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		n, err := streamResponseBody(w, resp.Body, r, acc.Name())
		resp.Body.Close()
		cancel() // keep request ctx alive until body is fully read — canceling after Do() truncates SSE
		if err != nil {
			return
		}
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			log.Printf("proxy: chat done via %s, status=%d, written=%d, content-length=%s, elapsed=%v", acc.Name(), resp.StatusCode, n, cl, time.Since(start))
		} else {
			log.Printf("proxy: chat done via %s, status=%d, written=%d, elapsed=%v", acc.Name(), resp.StatusCode, n, time.Since(start))
		}
		return
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
		ctx, cancel := upstreamContext()
		req, err := http.NewRequestWithContext(ctx, "GET", acc.BaseURL()+"/models", nil)
		if err != nil {
			cancel()
			log.Printf("proxy: failed to create models request for %s: %v", acc.Name(), err)
			continue
		}
		for k, vs := range r.Header {
			if isHopByHop(k) {
				continue
			}
			if http.CanonicalHeaderKey(k) == "Authorization" {
				continue
			}
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("Authorization", "Bearer "+acc.Key())
		resp, err := acc.Client().Do(req)
		if err != nil {
			cancel()
			log.Printf("proxy: models retry via %s (upstream error, not marking exhausted): %v", acc.Name(), err)
			continue
		}
		if isQuotaExhausted(resp.StatusCode) {
			log.Printf("proxy: %s models quota exhausted (%d), marking exhausted", acc.Name(), resp.StatusCode)
			acc.MarkExhausted()
			resp.Body.Close()
			cancel()
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			cancel()
			log.Printf("proxy: models retry via %s (status %d, not marking exhausted)", acc.Name(), resp.StatusCode)
			continue
		}
		for k, vs := range resp.Header {
			if isHopByHop(k) {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		n, err := io.Copy(w, resp.Body)
		resp.Body.Close()
		cancel()
		if err != nil {
			log.Printf("proxy: failed to copy models response body for %s: %v", acc.Name(), err)
		} else {
			log.Printf("proxy: models done via %s, status=%d, written=%d", acc.Name(), resp.StatusCode, n)
		}
		return
	}
	log.Printf("proxy: models failed, all exhausted")
	http.Error(w, `{"error":{"message":"All accounts exhausted for /models","code":"all_exhausted"}}`, 503)
}