package main

import (
    "bytes"
    "io"
    "log"
    "net/http"
)

var hopByHopHeaders = map[string]bool{
    "Connection":          true,
    "Transfer-Encoding":   true,
    "Proxy-Authenticate":  true,
    "Proxy-Authorization": true,
    "TE":                  true,
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
    defer r.Body.Close()
    bodyBytes, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "failed to read body", 500)
        return
    }

    maxAttempts := len(pool.accounts) * 2
    for attempts := 0; attempts < maxAttempts; attempts++ {
        acc := pool.Select()
        if acc == nil {
            http.Error(w, `{"error":{"message":"All accounts exhausted","code":"all_exhausted"}}`, 503)
            return
        }

        req, err := http.NewRequest("POST", acc.BaseURL()+"/chat/completions", bytes.NewReader(bodyBytes))
        if err != nil {
            log.Printf("proxy: failed to create request for %s: %v", acc.Name(), err)
            continue
        }
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Authorization", "Bearer "+acc.Key())

        resp, err := acc.Client().Do(req)
        if err != nil {
            log.Printf("proxy: %s request failed: %v, trying next", acc.Name(), err)
            acc.MarkExhausted()
            continue
        }

        if resp.StatusCode == 402 {
            acc.MarkExhausted()
            resp.Body.Close()
            log.Printf("account %s: exhausted (402), trying next", acc.Name())
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
        return
    }
    http.Error(w, `{"error":{"message":"All accounts exhausted after retries","code":"all_exhausted"}}`, 503)
}

func proxyModels(pool *Pool, w http.ResponseWriter, r *http.Request) {
    maxAttempts := len(pool.accounts) * 2
    for attempts := 0; attempts < maxAttempts; attempts++ {
        acc := pool.Select()
        if acc == nil {
            http.Error(w, `{"error":{"message":"No healthy accounts","code":"no_accounts"}}`, 503)
            return
        }
        req, err := http.NewRequest("GET", acc.BaseURL()+"/models", nil)
        if err != nil {
            log.Printf("proxy: failed to create models request for %s: %v", acc.Name(), err)
            continue
        }
        req.Header.Set("Authorization", "Bearer "+acc.Key())
        resp, err := acc.Client().Do(req)
        if err != nil {
            log.Printf("proxy: %s models request failed: %v, trying next", acc.Name(), err)
            acc.MarkExhausted()
            continue
        }
        defer resp.Body.Close()
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
        return
    }
    http.Error(w, `{"error":{"message":"All accounts exhausted for /models","code":"all_exhausted"}}`, 503)
}
