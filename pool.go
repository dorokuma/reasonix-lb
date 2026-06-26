package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

type AccountStatus int

const (
	StatusHealthy AccountStatus = iota
	StatusExhausted
)

const upstreamTimeout = 10 * time.Minute

type Account struct {
	cfg         AccountConfig
	status      AccountStatus
	client      *http.Client
	mu          sync.Mutex
}

func (a *Account) Name() string         { return a.cfg.Name }
func (a *Account) Key() string          { return a.cfg.Key }
func (a *Account) BaseURL() string      { return a.cfg.BaseURL }
func (a *Account) Client() *http.Client { return a.client }

func (a *Account) IsHealthy() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status == StatusHealthy
}

func (a *Account) MarkExhausted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status == StatusHealthy {
		a.status = StatusExhausted
		log.Printf("account %s: marked exhausted (removed from pool)", a.Name())
	}
}

func newHTTPClient() *http.Client {
	return &http.Client{
		// Timeout must be 0: per-request context owns the full lifecycle
		// (headers + streaming body). Client.Timeout aborts body reads
		// independently and poisons shared keep-alive connections.
		Timeout: 0,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func (a *Account) MarkHealthy() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status == StatusExhausted {
		a.status = StatusHealthy
	}
}

func (a *Account) Status() AccountStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

type Pool struct {
	accounts []*Account
	nextIdx  uint64
	mu       sync.Mutex
}

func NewPool(cfgs []AccountConfig) *Pool {
	accs := make([]*Account, len(cfgs))
	for i, cfg := range cfgs {
		accs[i] = &Account{
			cfg:    cfg,
			status: StatusHealthy,
			client: newHTTPClient(),
		}
	}
	return &Pool{accounts: accs}
}

// Select returns a healthy account via round-robin. Returns nil if none healthy.
func (p *Pool) Select() *Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.accounts) == 0 {
		return nil
	}
	for range p.accounts {
		idx := int(p.nextIdx % uint64(len(p.accounts)))
		p.nextIdx++
		acc := p.accounts[idx]
		if acc.IsHealthy() {
			return acc
		}
	}
	return nil
}

// ExhaustedAccounts returns all accounts currently in exhausted state (for probing).
func (p *Pool) ExhaustedAccounts() []*Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*Account
	for _, a := range p.accounts {
		if a.Status() == StatusExhausted {
			out = append(out, a)
		}
	}
	return out
}
