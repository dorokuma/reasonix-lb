package main

import (
	"container/list"
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type AccountStatus int

const (
	StatusHealthy AccountStatus = iota
	StatusExhausted
)

const upstreamTimeout = 10 * time.Minute

type Account struct {
	cfg                 AccountConfig
	status              AccountStatus
	client              *http.Client
	mu                  sync.Mutex
	borrowed            atomic.Bool
	cooldownUntil       time.Time
	consecutiveFailures int
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
		a.cooldownUntil = time.Time{} // 清除冷却
		log.Printf("account %s: marked healthy (returned to pool)", a.Name())
	}
}

func (a *Account) Status() AccountStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

// TryBorrow 尝试借用该账号。如果已被借用返回 false。
func (a *Account) TryBorrow() bool {
	return a.borrowed.CompareAndSwap(false, true)
}

// Release 释放借用，请求完成后必须调用。
func (a *Account) Release() {
	a.borrowed.Store(false)
}

func (a *Account) ResetFailures() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecutiveFailures = 0
}

func (a *Account) IncrementFailures() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecutiveFailures++
	return a.consecutiveFailures
}

// SetCooldown 设置冷却时间（用于 429 临时限流场景）
func (a *Account) SetCooldown(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cooldownUntil = time.Now().Add(d)
}

// IsInCooldown 检查是否在冷却期内
func (a *Account) IsInCooldown() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Now().Before(a.cooldownUntil)
}

type waiter struct {
	ch     chan struct{}
	active bool
}

type Pool struct {
	accounts []*Account
	nextIdx  uint64
	mu       sync.Mutex
	waiters  *list.List
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
	return &Pool{
		accounts: accs,
		waiters:  list.New(),
	}
}

// Release 释放账号借用，并通知等待者
func (p *Pool) Release(a *Account) {
	a.Release()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waiters.Len() > 0 {
		elem := p.waiters.Front()
		p.waiters.Remove(elem)
		w := elem.Value.(*waiter)
		w.active = false
		close(w.ch)
	}
}

// MarkHealthy 标记账号健康并唤醒可能正在等待的协程
func (p *Pool) MarkHealthy(a *Account) {
	a.MarkHealthy()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waiters.Len() > 0 {
		elem := p.waiters.Front()
		p.waiters.Remove(elem)
		w := elem.Value.(*waiter)
		w.active = false
		close(w.ch)
	}
}



func (p *Pool) removeWaiterAndTransfer(elem *list.Element) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := elem.Value.(*waiter)
	if w.active {
		p.waiters.Remove(elem)
		w.active = false
	} else {
		// 说明该 waiter 已经被 Release 移出并 close(ch) 了。
		// 由于此 waiter 已超时/取消，必须将该释放信号转交给下一个 waiter。
		if p.waiters.Len() > 0 {
			nextElem := p.waiters.Front()
			p.waiters.Remove(nextElem)
			nextW := nextElem.Value.(*waiter)
			nextW.active = false
			close(nextW.ch)
		}
	}
}

func (p *Pool) trySelect() *Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.trySelectLocked()
}

func (p *Pool) trySelectLocked() *Account {
	if len(p.accounts) == 0 {
		return nil
	}
	startIdx := int(p.nextIdx % uint64(len(p.accounts)))
	for i := 0; i < len(p.accounts); i++ {
		idx := (startIdx + i) % len(p.accounts)
		p.nextIdx++
		acc := p.accounts[idx]
		if acc.IsInCooldown() {
			continue
		}
		if acc.IsHealthy() && acc.TryBorrow() {
			return acc
		}
	}
	return nil
}

var ErrNoHealthyAccounts = errors.New("no healthy accounts available")
var ErrSelectTimeout = errors.New("select account timeout")

// Select returns a healthy account via round-robin with blocking wait.
func (p *Pool) Select(ctx context.Context) (*Account, error) {
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()

	for {
		// 检查池中是否还有任何 status == StatusHealthy 的账号
		hasHealthy := false
		allHealthyInCooldown := true
		var minCooldown time.Duration
		now := time.Now()

		p.mu.Lock()
		for _, acc := range p.accounts {
			acc.mu.Lock()
			isHealthy := acc.status == StatusHealthy
			cooldownUntil := acc.cooldownUntil
			acc.mu.Unlock()

			if isHealthy {
				hasHealthy = true
				if cooldownUntil.After(now) {
					remaining := cooldownUntil.Sub(now)
					if minCooldown == 0 || remaining < minCooldown {
						minCooldown = remaining
					}
				} else {
					allHealthyInCooldown = false
				}
			}
		}

		if !hasHealthy {
			p.mu.Unlock()
			return nil, ErrNoHealthyAccounts
		}

		// 尝试获取一个可用账号
		if acc := p.trySelectLocked(); acc != nil {
			p.mu.Unlock()
			return acc, nil
		}

		// 无可用账号，将当前请求的等待通道加入队列
		w := &waiter{
			ch:     make(chan struct{}),
			active: true,
		}
		elem := p.waiters.PushBack(w)
		p.mu.Unlock()

		var cooldownChan <-chan time.Time
		var cooldownTimer *time.Timer
		if allHealthyInCooldown && minCooldown > 0 {
			cooldownTimer = time.NewTimer(minCooldown)
			cooldownChan = cooldownTimer.C
		}

		// 阻塞等待释放信号或超时
		var selectErr error
		var isClosed bool
		select {
		case <-ctx.Done():
			selectErr = ctx.Err()
		case <-timer.C:
			selectErr = ErrSelectTimeout
		case <-w.ch:
			isClosed = true
		case <-cooldownChan:
		}

		if selectErr != nil {
			p.removeWaiterAndTransfer(elem)
			if cooldownTimer != nil {
				cooldownTimer.Stop()
			}
			return nil, selectErr
		}

		if !isClosed {
			select {
			case <-w.ch:
				isClosed = true
			default:
			}
		}

		if cooldownTimer != nil {
			cooldownTimer.Stop()
		}

		if !isClosed {
			p.removeWaiterAndTransfer(elem)
		}
	}
}

// AllAccounts returns a copy of all accounts (healthy + exhausted).
func (p *Pool) AllAccounts() []*Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]*Account, len(p.accounts))
	copy(result, p.accounts)
	return result
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
