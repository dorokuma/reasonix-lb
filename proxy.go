package main

import (
    "bytes"
    "io"
    "log"
    "net/http"
)

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
    bodyBytes, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "failed to read body", 500)
        return
    }
    r.Body.Close()

    for {
        acc := pool.Select()
        if acc == nil {
            http.Error(w, `{"error":{"message":"All accounts exhausted","code":"all_exhausted"}}`, 503)
            return
        }

        req, _ := http.NewRequest("POST", acc.BaseURL()+"/chat/completions", bytes.NewReader(bodyBytes))
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Authorization", "Bearer "+acc.Key())

        resp, err := acc.Client().Do(req)
        if err != nil {
            log.Printf("proxy: %s request failed: %v, trying next", acc.Name(), err)
            continue
        }

        if resp.StatusCode == 402 {
            acc.MarkExhausted()
            resp.Body.Close()
            log.Printf("account %s: exhausted (402), trying next", acc.Name())
            continue
        }

        // Forward response to client
        for k, vs := range resp.Header {
            for _, v := range vs {
                w.Header().Add(k, v)
            }
        }
        w.WriteHeader(resp.StatusCode)
        io.Copy(w, resp.Body)
        resp.Body.Close()
        return
    }
}

func proxyModels(pool *Pool, w http.ResponseWriter, r *http.Request) {
    acc := pool.Select()
    if acc == nil {
        http.Error(w, `{"error":{"message":"No healthy accounts","code":"no_accounts"}}`, 503)
        return
    }
    req, _ := http.NewRequest("GET", acc.BaseURL()+"/models", nil)
    req.Header.Set("Authorization", "Bearer "+acc.Key())
    resp, err := acc.Client().Do(req)
    if err != nil {
        http.Error(w, `{"error":{"message":"Upstream unavailable"}}`, 502)
        return
    }
    defer resp.Body.Close()
    for k, vs := range resp.Header {
        for _, v := range vs {
            w.Header().Add(k, v)
        }
    }
    w.WriteHeader(resp.StatusCode)
    io.Copy(w, resp.Body)
}
