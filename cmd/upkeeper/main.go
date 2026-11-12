// Command upkeeper works tremolo's investigations with a local model: it
// finds the cause, fixes what it safely can, and escalates the rest.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/tpural/upkeeper/internal/agent"
	"github.com/tpural/upkeeper/internal/cluster"
	"github.com/tpural/upkeeper/internal/config"
	"github.com/tpural/upkeeper/internal/llm"
	"github.com/tpural/upkeeper/internal/patrol"
	"github.com/tpural/upkeeper/internal/runner"
	"github.com/tpural/upkeeper/internal/tremolo"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "upkeeper:", err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config", "/etc/upkeeper/config.yaml", "configuration file")
	admin := flag.String("admin", ":9090", "address for /healthz, /status and /metrics")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig; in-cluster when empty")
	debug := flag.Bool("debug", false, "log debug messages")
	flag.Parse()
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if cfg.Tremolo.Token == "" {
		return errors.New("$TREMOLO_TOKEN is empty: set it to an assistant token from tremolo")
	}
	t, err := tremolo.New(cfg.Tremolo.URL, cfg.Tremolo.Token, cfg.Name)
	if err != nil {
		return err
	}
	rc, err := kubeConfig(*kubeconfig)
	if err != nil {
		return err
	}
	k, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	c := cluster.New(k)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	checks := &patrol.Checker{K: k, Coverage: t.Coverage, AsleepTaints: []string{"possum.dev/asleep"}}
	r := &runner.Runner{
		Config:  cfg,
		T:       t,
		Agent:   &agent.Agent{Model: llm.New(cfg.Model.URL, cfg.Model.Name), Log: log, Budget: 90_000},
		Toolbox: runner.NewToolbox(t, c, version),
		Checks:  checks.Check,
		Log:     log,
		Metrics: runner.NewMetrics(reg),
	}
	if cfg.GPU.Deployment != "" {
		r.Awake = func(ctx context.Context) (bool, error) { return c.Awake(ctx, cfg.GPU.Deployment) }
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Status())
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: *admin, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting", "version", version, "tremolo", cfg.Tremolo.URL, "model", cfg.Model.Name, "gpu", cfg.GPU.Deployment, "patrol", cfg.Patrol.At)
	go r.Run(ctx)
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	return err
}

func kubeConfig(path string) (*rest.Config, error) {
	if strings.TrimSpace(path) == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}
