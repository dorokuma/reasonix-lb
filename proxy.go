package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
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

type OpenAIErrorResponse struct {
	Error struct {
		Message string      `json:"message"`
		Type    string      `json:"type"`
		Param   interface{} `json:"param"`
		Code    string      `json:"code"`
	} `json:"error"`
}

func isPermanentError(body []byte) bool {
	var errResp OpenAIErrorResponse
	var err error
	if len(body) > 0 {
		err = json.Unmarshal(body, &errResp)
	} else {
		return false
	}

	bodyStr := strings.ToLower(string(body))

	if err == nil && errResp.Error.Code != "" {
		code := strings.ToLower(errResp.Error.Code)
		if code == "insufficient_quota" || code == "invalid_api_key" || code == "revoked" || code == "account_deactivated" {
			return true
		}
	}

	if strings.Contains(bodyStr, "quota exceeded") || strings.Contains(bodyStr, "deactivated") {
		return true
	}

	return false
}

func handleUpstreamError(acc *Account, resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	limitReader := io.LimitReader(resp.Body, 4096)
	bodyBytes, _ := io.ReadAll(limitReader)

	if isPermanentError(bodyBytes) {
		acc.MarkExhausted()
		log.Printf("proxy: %s permanent error (status=%d), marking exhausted. body: %s", acc.Name(), resp.StatusCode, string(bodyBytes))
		return
	}

	failures := acc.IncrementFailures()
	if failures >= 3 {
		acc.MarkExhausted()
		log.Printf("proxy: %s consecutive failures >= 3 (status=%d), marking exhausted. body: %s", acc.Name(), resp.StatusCode, string(bodyBytes))
	} else {
		acc.SetCooldown(10 * time.Second)
		log.Printf("proxy: %s temporary error (status=%d), cooling down for 10s (failures=%d). body: %s", acc.Name(), resp.StatusCode, failures, string(bodyBytes))
	}
}

func isQuotaExhausted(status int) bool {
	return status == 402 || status == 429
}

func upstreamContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), upstreamTimeout)
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
	const maxBodySize = 10 << 20 // 10 MB
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("proxy: chat request from %s, body read error: %v", r.RemoteAddr, err)
		http.Error(w, "failed to read body", 500)
		return
	}
	log.Printf("proxy: chat request from %s, body=%d bytes", r.RemoteAddr, len(bodyBytes))

	maxAttempts := len(pool.accounts) * 2
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		acc, err := pool.Select(r.Context())
		if err != nil {
			log.Printf("proxy: select account failed: %v", err)
			http.Error(w, `{"error":{"message":"All accounts exhausted","code":"all_exhausted"}}`, 503)
			return
		}

		done, streamErr := func() (bool, error) {
			defer pool.Release(acc)
			ctx, cancel := upstreamContext(r)
			defer cancel() // keep ctx alive until body is fully read — early cancel truncates SSE

			targetURL := acc.BaseURL() + "/chat/completions"
			if r.URL.RawQuery != "" {
				targetURL += "?" + r.URL.RawQuery
			}
			req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewReader(bodyBytes))
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

			if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 402 {
				handleUpstreamError(acc, resp)
				return false, nil
			}
			if resp.StatusCode == 429 {
				resp.Body.Close()
				acc.SetCooldown(2 * time.Minute)
				log.Printf("proxy: %s rate-limited (429), cooling down for 2m", acc.Name())
				return false, nil
			}
			if resp.StatusCode >= 500 {
				log.Printf("proxy: chat retry via %s (upstream %d, not marking exhausted)", acc.Name(), resp.StatusCode)
				return false, nil
			}

			acc.ResetFailures()

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
		if attempts > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		acc, err := pool.Select(r.Context())
		if err != nil {
			log.Printf("proxy: select account for models failed: %v", err)
			http.Error(w, `{"error":{"message":"No healthy accounts","code":"no_accounts"}}`, 503)
			return
		}

		done := func() bool {
			defer pool.Release(acc)
			ctx, cancel := upstreamContext(r)
			defer cancel()

			targetURL := acc.BaseURL() + "/models"
			if r.URL.RawQuery != "" {
				targetURL += "?" + r.URL.RawQuery
			}
			req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
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

			if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 402 {
				handleUpstreamError(acc, resp)
				return false
			}
			if resp.StatusCode == 429 {
				resp.Body.Close()
				acc.SetCooldown(2 * time.Minute)
				log.Printf("proxy: %s models rate-limited (429), cooling down for 2m", acc.Name())
				return false
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("proxy: models retry via %s (status %d, not marking exhausted)", acc.Name(), resp.StatusCode)
				return false
			}

			acc.ResetFailures()

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