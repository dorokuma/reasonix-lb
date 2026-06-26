package main

import (
	"bytes"
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

		req, err := http.NewRequestWithContext(r.Context(), "POST", acc.BaseURL()+"/chat/completions", bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("proxy: failed to create request for %s: %v", acc.Name(), err)
			continue
		}
		// Copy original request headers, skipping hop-by-hop and Authorization
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
			log.Printf("proxy: chat retry via %s: %v", acc.Name(), err)
			acc.MarkExhausted()
			continue
		}

		if resp.StatusCode == 402 || resp.StatusCode == 429 {
			acc.MarkExhausted()
			resp.Body.Close()
			log.Printf("account %s: exhausted (%d), trying next", acc.Name(), resp.StatusCode)
			continue
		}

		// Forward response headers to client (filter hop-by-hop)
		for k, vs := range resp.Header {
			if isHopByHop(k) {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("proxy: failed to copy response body for %s: %v", acc.Name(), err)
		}
		resp.Body.Close()
		log.Printf("proxy: chat done via %s, status=%d, elapsed=%v", acc.Name(), resp.StatusCode, time.Since(start))
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
		req, err := http.NewRequestWithContext(r.Context(), "GET", acc.BaseURL()+"/models", nil)
		if err != nil {
			log.Printf("proxy: failed to create models request for %s: %v", acc.Name(), err)
			continue
		}
		// Copy original request headers, skipping hop-by-hop and Authorization
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
			log.Printf("proxy: models retry via %s: %v", acc.Name(), err)
			acc.MarkExhausted()
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("proxy: %s models returned status %d, marking exhausted", acc.Name(), resp.StatusCode)
			acc.MarkExhausted()
			resp.Body.Close()
			continue
		}
		// Forward response headers to client (filter hop-by-hop)
		for k, vs := range resp.Header {
			if isHopByHop(k) {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("proxy: failed to copy models response body for %s: %v", acc.Name(), err)
		}
		resp.Body.Close()
		log.Printf("proxy: models done via %s, status=%d", acc.Name(), resp.StatusCode)
		return
	}
	log.Printf("proxy: models failed, all exhausted")
	http.Error(w, `{"error":{"message":"All accounts exhausted for /models","code":"all_exhausted"}}`, 503)
}
