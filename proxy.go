package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sort"
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

func isPermanentCredentialError(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var errResp OpenAIErrorResponse
	err := json.Unmarshal(body, &errResp)

	if err == nil && errResp.Error.Code != "" {
		code := strings.ToLower(errResp.Error.Code)
		if code == "invalid_api_key" || code == "revoked" || code == "account_deactivated" {
			return true
		}
	}
	return false
}

func isQuotaError(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var errResp OpenAIErrorResponse
	_ = json.Unmarshal(body, &errResp)

	if errResp.Error.Code != "" {
		code := strings.ToLower(errResp.Error.Code)
		if code == "insufficient_quota" {
			return true
		}
	}
	bodyLower := strings.ToLower(string(body))
	if strings.Contains(bodyLower, "quota exceeded") ||
		strings.Contains(bodyLower, "usage limit") ||
		strings.Contains(bodyLower, "monthly usage limit") {
		return true
	}
	return false
}

func parseRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 0
	}
	if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func handleUpstreamError(acc *Account, resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	limitReader := io.LimitReader(resp.Body, 4096)
	bodyBytes, _ := io.ReadAll(limitReader)

	if isPermanentCredentialError(bodyBytes) {
		acc.MarkExhausted()
		log.Printf("proxy: %s permanent credential error (status=%d), marking exhausted. body: %s",
			acc.Name(), resp.StatusCode, string(bodyBytes))
		return
	}

	if resp.StatusCode == 429 {
		if isQuotaError(bodyBytes) {
			acc.SetCooldown(30 * time.Minute)
			acc.ResetFailures()
			log.Printf("proxy: %s 429+quota exhaustion, cooling down 30m. body: %s",
				acc.Name(), string(bodyBytes))
			return
		}
		cd := parseRetryAfter(resp)
		if cd <= 0 {
			cd = 30 * time.Second
		}
		if cd > 5*time.Minute {
			cd = 5 * time.Minute
		}
		acc.SetCooldown(cd)
		acc.ResetFailures()
		log.Printf("proxy: %s rate-limited 429, cooling down %v. body: %s",
			acc.Name(), cd, string(bodyBytes))
		return
	}

	if isQuotaError(bodyBytes) {
		acc.SetCooldown(30 * time.Minute)
		acc.ResetFailures()
		log.Printf("proxy: %s insufficient_quota (status=%d), cooling down 30m. body: %s",
			acc.Name(), resp.StatusCode, string(bodyBytes))
		return
	}

	failures := acc.IncrementFailures()
	if failures >= 5 {
		acc.MarkExhausted()
		log.Printf("proxy: %s consecutive failures >= 5 (status=%d), marking exhausted. body: %s",
			acc.Name(), resp.StatusCode, string(bodyBytes))
	} else {
		acc.SetCooldown(30 * time.Second)
		log.Printf("proxy: %s temporary error (status=%d), cooling down 30s (failures=%d). body: %s",
			acc.Name(), resp.StatusCode, failures, string(bodyBytes))
	}
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

func NewProxyHandler(pool *Pool, wire WireAPIMode, cfg *Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			w.Write([]byte("ok"))
			return
		}
		if r.URL.Path == "/v1/models" {
			proxyModels(pool, w, r, cfg)
			return
		}
		if r.URL.Path == "/v1/chat/completions" {
			if !wire.allowsLegacy() {
				http.Error(w, `{"error":{"message":"wire_api=responses: /v1/chat/completions disabled","code":"disabled"}}`, http.StatusNotFound)
				return
			}
			proxyChat(pool, w, r, cfg)
			return
		}
		if r.URL.Path == "/v1/responses" {
			if !wire.allowsResponses() {
				http.Error(w, `{"error":{"message":"wire_api=legacy: /v1/responses disabled","code":"disabled"}}`, http.StatusNotFound)
				return
			}
			proxyResponses(pool, w, r, cfg)
			return
		}
		http.Error(w, "not found", 404)
	})
}

func proxyResponses(pool *Pool, w http.ResponseWriter, r *http.Request, cfg *Config) {
	start := time.Now()
	defer r.Body.Close()
	const maxBodySize = 10 << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("proxy: responses body read error: %v", err)
		http.Error(w, "failed to read body", 500)
		return
	}
	chatBody, stream, err := responsesToChatCompletions(bodyBytes)
	if err != nil {
		log.Printf("proxy: responses convert error: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q,"code":"invalid_request"}}`, err.Error()), http.StatusBadRequest)
		return
	}
	var reqModel string
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(bodyBytes, &raw)
	reqModel, _ = rawStringField(raw, "model")
	reqModel = cfg.RemapModel(reqModel)

log.Printf("proxy: responses request from %s, stream=%v, chat_body=%d bytes", r.RemoteAddr, stream, len(chatBody))
	proxyChatWithBody(pool, w, r, chatBody, start, chatForwardOpts{
		responsesOut: true,
		stream:       stream,
		model:        reqModel,
	}, cfg)
}

type chatForwardOpts struct {
	responsesOut bool
	stream       bool
	model        string
}

func proxyChat(pool *Pool, w http.ResponseWriter, r *http.Request, cfg *Config) {
	start := time.Now()
	defer r.Body.Close()
	const maxBodySize = 10 << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("proxy: chat body read error: %v", err)
		http.Error(w, "failed to read body", 500)
		return
	}
	proxyChatWithBody(pool, w, r, bodyBytes, start, chatForwardOpts{}, cfg)
}

func proxyChatWithBody(pool *Pool, w http.ResponseWriter, r *http.Request, bodyBytes []byte, start time.Time, opts chatForwardOpts, cfg *Config) {
	// Remap model name if configured
	bodyBytes = remapModelInBody(bodyBytes, cfg)
	maxAttempts := len(pool.accounts) * 2

	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			time.Sleep(200 * time.Millisecond)
		}

		acc, err := pool.Select(r.Context())
		if err != nil {
			log.Printf("proxy: select account failed: %v", err)
			http.Error(w, `{"error":{"message":"No healthy accounts available","code":"no_accounts"}}`, 503)
			return
		}

		done, streamErr := func() (bool, error) {
			defer pool.Release(acc)

			ctx, cancel := upstreamContext(r)
			defer cancel()

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
			req.Header.Set("Authorization", "Bearer "+acc.Key())
			req.Header.Set("Content-Type", "application/json")

			resp, err := acc.Client().Do(req)
			if err != nil {
				log.Printf("proxy: chat retry via %s (upstream error): %v", acc.Name(), err)
				return false, nil
			}
			defer resp.Body.Close()

			if resp.StatusCode == 429 || resp.StatusCode == 402 || resp.StatusCode == 401 || resp.StatusCode == 403 {
				handleUpstreamError(acc, resp)
				return false, nil
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				acc.ResetFailures()
			} else {
				// Non-2xx not in special list (503, 500, 502, etc.)
				failures := acc.IncrementFailures()
				if failures >= 5 {
					acc.MarkExhausted()
					log.Printf("proxy: %s non-2xx failures >= 5 (status=%d), marking exhausted", acc.Name(), resp.StatusCode)
				} else {
					acc.SetCooldown(30 * time.Second)
					log.Printf("proxy: %s non-2xx error (status=%d), cooling down 30s (failures=%d)", acc.Name(), resp.StatusCode, failures)
				}
				// Still forward the error to client this time
				errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				if resp.StatusCode >= 400 {
	
				}
				log.Printf("proxy: chat upstream error via %s, status=%d, body=%s", acc.Name(), resp.StatusCode, string(errBody))
				copyUpstreamHeaders(w, resp.Header)
				w.WriteHeader(resp.StatusCode)
				n, _ := w.Write(errBody)
				log.Printf("proxy: chat upstream error via %s, written=%d", acc.Name(), n)
				return true, nil
			}

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				if resp.StatusCode >= 400 {
	
				}
				log.Printf("proxy: chat upstream error via %s, status=%d, body=%s", acc.Name(), resp.StatusCode, string(errBody))
				copyUpstreamHeaders(w, resp.Header)
				w.WriteHeader(resp.StatusCode)
				n, _ := w.Write(errBody)
				log.Printf("proxy: chat upstream error via %s, written=%d", acc.Name(), n)
				return true, nil
			}

			if opts.responsesOut && opts.stream {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("Connection", "keep-alive")
				w.WriteHeader(http.StatusOK)
				err = translateChatStreamToResponses(w, resp.Body, opts.model)
				if err != nil {
					log.Printf("proxy: responses stream translate via %s: %v", acc.Name(), err)
					return true, err
				}
				log.Printf("proxy: responses stream done via %s, elapsed=%v", acc.Name(), time.Since(start))
				return true, nil
			}

			if opts.responsesOut && !opts.stream {
				rawBody, err := io.ReadAll(resp.Body)
				if err != nil {
					return true, err
				}
				out, err := chatCompletionToResponse(rawBody, opts.model)
				if err != nil {
					log.Printf("proxy: responses json convert via %s: %v", acc.Name(), err)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(rawBody)
					return true, nil
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				n, _ := w.Write(out)
				log.Printf("proxy: responses json done via %s, written=%d, elapsed=%v", acc.Name(), n, time.Since(start))
				return true, nil
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

func proxyModels(pool *Pool, w http.ResponseWriter, r *http.Request, cfg *Config) {
	log.Printf("proxy: models request from %s", r.RemoteAddr)

	// Build model list from model_remap keys (the GPT model names users see).
	modelIDs := make([]string, 0, len(cfg.ModelRemap)+1)
	seen := make(map[string]bool)
	for k := range cfg.ModelRemap {
		if !seen[k] {
			modelIDs = append(modelIDs, k)
			seen[k] = true
		}
	}
	if cfg.DefaultModel != "" && !seen[cfg.DefaultModel] {
		modelIDs = append(modelIDs, cfg.DefaultModel)
	}
	if len(modelIDs) == 0 {
		http.Error(w, `{"error":{"message":"No models configured","code":"no_models"}}`, 503)
		return
	}
	sort.Strings(modelIDs)

	data := make([]map[string]any, len(modelIDs))
	for i, id := range modelIDs {
		data[i] = map[string]any{
			"id":       id,
			"object":   "model",
			"created":  1700000000,
			"owned_by": "reasonix-lb",
		}
	}
	resp := map[string]any{
		"object": "list",
		"data":   data,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	log.Printf("proxy: models returning %d models", len(modelIDs))
}

// readBodyPreview reads up to 4KB from resp.Body for inspection and closes it.

func readBodyPreview(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	preview, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	return preview
}

// remapModelInBody replaces the model field in a JSON chat completions body.
func remapModelInBody(body []byte, cfg *Config) []byte {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}
	model, ok := rawStringField(raw, "model")
	if !ok || model == "" {
		return body
	}
	remapped := cfg.RemapModel(model)
	if remapped == model {
		return body
	}
	rawBytes, _ := json.Marshal(remapped)
	raw["model"] = json.RawMessage(rawBytes)
	out, err := json.Marshal(raw)
	if err != nil {
		return body
	}
	log.Printf("proxy: model remap %s -> %s", model, remapped)
	return out
}
