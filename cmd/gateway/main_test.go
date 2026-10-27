package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadConfigRequiresDatabaseURLAndAppliesDefaults(t *testing.T) {
	t.Parallel()

	if _, err := loadConfig(func(string) string { return "" }); err == nil {
		t.Fatal("missing DATABASE_URL must fail")
	}

	values := map[string]string{"DATABASE_URL": "postgres://example"}
	cfg, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.httpAddr != ":8080" || cfg.workerInterval != 100*time.Millisecond {
		t.Fatalf("defaults = addr %q interval %s", cfg.httpAddr, cfg.workerInterval)
	}
}

func TestLoadConfigRejectsInvalidWorkerInterval(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"DATABASE_URL":    "postgres://example",
		"WORKER_INTERVAL": "immediately",
	}
	if _, err := loadConfig(func(key string) string { return values[key] }); err == nil {
		t.Fatal("invalid WORKER_INTERVAL must fail")
	}
}

func TestRunLoopStopsWhenContextIsCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	done := make(chan struct{})
	go func() {
		runLoop(ctx, time.Millisecond, func(context.Context) { calls.Add(1) })
		close(done)
	}()

	deadline := time.After(time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("loop never ran")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop after cancellation")
	}
}
