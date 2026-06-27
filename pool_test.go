package main

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"
)

func TestPoolFIFOAndRelease(t *testing.T) {
	log.Printf("TEST: TestPoolFIFOAndRelease started")
	cfgs := []AccountConfig{
		{Name: "acc-1", Key: "key-1", BaseURL: "http://localhost:8001"},
		{Name: "acc-2", Key: "key-2", BaseURL: "http://localhost:8002"},
	}
	p := NewPool(cfgs)

	ctx := context.Background()
	log.Printf("TEST: Selecting acc1")
	acc1, err := p.Select(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	log.Printf("TEST: Selecting acc2")
	acc2, err := p.Select(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ch1 := make(chan *Account, 1)
	ch2 := make(chan *Account, 1)

	go func() {
		log.Printf("TEST: Goroutine 1 calling Select")
		acc, err := p.Select(ctx)
		log.Printf("TEST: Goroutine 1 Select returned: acc=%v, err=%v", acc, err)
		ch1 <- acc
		log.Printf("TEST: Goroutine 1 sent to ch1")
	}()

	go func() {
		log.Printf("TEST: Goroutine 2 calling Select")
		acc, err := p.Select(ctx)
		log.Printf("TEST: Goroutine 2 Select returned: acc=%v, err=%v", acc, err)
		ch2 <- acc
		log.Printf("TEST: Goroutine 2 sent to ch2")
	}()

	time.Sleep(100 * time.Millisecond)
	log.Printf("TEST: Releasing acc1 and acc2")
	p.Release(acc1)
	p.Release(acc2)

	// 此时两个协程应该都被唤醒并返回
	var results []*Account
	for i := 0; i < 2; i++ {
		select {
		case acc := <-ch1:
			log.Printf("TEST: Main read from ch1: acc=%v", acc)
			results = append(results, acc)
		case acc := <-ch2:
			log.Printf("TEST: Main read from ch2: acc=%v", acc)
			results = append(results, acc)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for workers to be woken up")
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// 验证获取到的账号确实是 acc-1 和 acc-2 (顺序可能因调度而异)
	names := map[string]bool{}
	for _, acc := range results {
		names[acc.Name()] = true
	}
	if !names["acc-1"] || !names["acc-2"] {
		t.Errorf("expected to get acc-1 and acc-2, got: %v", results)
	}
}

func TestPoolCancelAndSignalTransfer(t *testing.T) {
	log.Printf("TEST: TestPoolCancelAndSignalTransfer started")
	cfgs := []AccountConfig{
		{Name: "acc-1", Key: "key-1", BaseURL: "http://localhost:8001"},
	}
	p := NewPool(cfgs)

	ctx := context.Background()
	acc1, _ := p.Select(ctx) // 占满唯一账号

	ctxCancel, cancel := context.WithCancel(ctx)
	errChA := make(chan error, 1)
	go func() {
		_, err := p.Select(ctxCancel)
		errChA <- err
	}()

	accChB := make(chan *Account, 1)
	go func() {
		acc, _ := p.Select(ctx)
		accChB <- acc
	}()

	time.Sleep(50 * time.Millisecond)

	cancel()
	p.Release(acc1)

	select {
	case err := <-errChA:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected A to fail with Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for A to exit")
	}

	select {
	case acc := <-accChB:
		if acc == nil || acc.Name() != "acc-1" {
			t.Errorf("expected B to be woken up and get acc-1, got %v", acc)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for B to get the transferred signal")
	}
}

func TestPoolMarkHealthyWakeup(t *testing.T) {
	log.Printf("TEST: TestPoolMarkHealthyWakeup started")
	cfgs := []AccountConfig{
		{Name: "acc-1", Key: "key-1", BaseURL: "http://localhost:8001"},
		{Name: "acc-2", Key: "key-2", BaseURL: "http://localhost:8002"},
	}
	p := NewPool(cfgs)

	ctx := context.Background()
	// 占满这两个账号，把 acc-1 变为 Exhausted，acc-2 在长冷却中
	acc1, _ := p.Select(ctx)
	acc2, _ := p.Select(ctx)

	acc1.MarkExhausted()
	p.Release(acc1) // 此时 acc1 在 Exhausted 状态，不能用于 Select

	acc2.SetCooldown(1 * time.Hour)
	p.Release(acc2) // 此时 acc2 在 Healthy 状态但在 cooldown，所以可以参与 Select 但需要等待

	// 此时启动 Select，因为 acc2 处于健康但 cooldown 中，会阻塞等待它的 cooldown 计时器。
	accCh := make(chan *Account, 1)
	go func() {
		acc, _ := p.Select(ctx)
		accCh <- acc
	}()

	time.Sleep(50 * time.Millisecond)

	// 现在调用 p.MarkHealthy(acc1) 模拟 probe 成功将 acc1 从 Exhausted 状态捞出来，
	// 它必须立马清除冷却并唤醒等待协程！
	p.MarkHealthy(acc1)

	select {
	case acc := <-accCh:
		if acc == nil || acc.Name() != "acc-1" {
			t.Errorf("expected to get acc-1 immediately, got %v", acc)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: waiting worker was not woken up by MarkHealthy")
	}
}
