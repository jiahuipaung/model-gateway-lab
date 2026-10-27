package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/fakeprovider"
	"github.com/jiahuipaung/model-gateway-lab/internal/httpapi"
	gatewaypostgres "github.com/jiahuipaung/model-gateway-lab/internal/postgres"
	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

type config struct {
	databaseURL    string
	httpAddr       string
	workerInterval time.Duration
}

func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		databaseURL:    getenv("DATABASE_URL"),
		httpAddr:       getenv("HTTP_ADDR"),
		workerInterval: 100 * time.Millisecond,
	}
	if cfg.databaseURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.httpAddr == "" {
		cfg.httpAddr = ":8080"
	}
	if raw := getenv("WORKER_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return config{}, fmt.Errorf("WORKER_INTERVAL must be a positive duration")
		}
		cfg.workerInterval = interval
	}
	return cfg, nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := gatewaypostgres.Open(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	fake := fakeprovider.NewScenarioWithState(store)
	providers := map[string]provider.Provider{"fake-a": fake}
	service := controlplane.NewService(store, "fake-a")
	submitter := controlplane.NewSubmissionWorker(store, providers, "submit-local", 5*time.Second)
	reconciler := controlplane.NewReconciler(store, providers, "reconcile-local", 5*time.Second)
	poller := controlplane.NewPoller(store, providers, "poll-local", 5*time.Second)
	dispatcher := controlplane.NewDispatcher(store, logSink{}, "dispatch-local", 5*time.Second)

	go runLoop(ctx, cfg.workerInterval, func(loopCtx context.Context) {
		runWorker(loopCtx, "submit", submitter.RunOnce)
		runWorker(loopCtx, "reconcile", reconciler.RunOnce)
		runWorker(loopCtx, "poll", poller.RunOnce)
		runWorker(loopCtx, "dispatch", dispatcher.RunOnce)
	})

	mux := http.NewServeMux()
	mux.Handle("/v1/", httpapi.NewHandler(service))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\":\"ok\"}\n"))
	})
	server := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("model gateway lab listening", "addr", cfg.httpAddr)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func runLoop(ctx context.Context, interval time.Duration, work func(context.Context)) {
	work(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			work(ctx)
		}
	}
}

func runWorker(ctx context.Context, name string, runOnce func(context.Context) (bool, error)) {
	if _, err := runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("worker iteration failed", "worker", name, "error", err)
	}
}

type logSink struct{}

func (logSink) Publish(_ context.Context, event controlplane.Event) error {
	slog.Info("terminal event delivered", "event_id", event.ID, "type", event.Type, "task_id", event.TaskID)
	return nil
}
