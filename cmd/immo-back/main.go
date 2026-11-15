// Commande immo-back : API OpenHexa Immo, et job d'ingestion.
//
//	immo-back                         sert l'API (et ingère en tâche de fond si INGESTION_MODE=in_process)
//	immo-back ingest --source all     exécute l'ingestion une fois, puis s'arrête
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/openhexa-fr/core/es"

	coreconfig "github.com/openhexa-fr/core/config"

	"github.com/openhexa-fr/immo-back/internal/api"
	"github.com/openhexa-fr/immo-back/internal/config"
	"github.com/openhexa-fr/immo-back/internal/dvf"
	"github.com/openhexa-fr/immo-back/internal/httpcache"
	"github.com/openhexa-fr/immo-back/internal/indices"
	"github.com/openhexa-fr/immo-back/internal/jobs"
	"github.com/openhexa-fr/immo-back/internal/polling"
)

const shutdownTimeout = 15 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if len(os.Args) > 1 && os.Args[1] == "ingest" {
		os.Exit(ingest(os.Args[2:]))
	}
	if err := serve(); err != nil {
		slog.Error("immo_api_failed", "error", err.Error())
		os.Exit(1)
	}
}

func setup() (context.Context, context.CancelFunc, config.Settings, error) {
	if err := coreconfig.LoadDotEnv(".env"); err != nil {
		return nil, nil, config.Settings{}, err
	}
	settings, err := config.Load()
	if err != nil {
		return nil, nil, config.Settings{}, err
	}
	// SIGTERM : arrêt demandé par l'orchestrateur.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx, stop, settings, nil
}

func ingest(args []string) int {
	sources, err := jobs.ParseArgs(args, os.Stderr)
	if err != nil {
		return 2
	}
	ctx, stop, settings, err := setup()
	if err != nil {
		slog.Error("ingestion_job_failed", "error", err.Error())
		return 1
	}
	defer stop()
	client, err := es.Connect(ctx, settings.ES)
	if err != nil {
		slog.Error("ingestion_job_failed", "error", err.Error())
		return 1
	}
	return jobs.Run(ctx, sources, jobs.EnsureIndices(client, settings), jobs.NewRunner(client, settings))
}

func serve() error {
	ctx, stop, settings, err := setup()
	if err != nil {
		return err
	}
	defer stop()

	// Bloque jusqu'à ce que le cluster réponde (backoff exponentiel, ~100 s max).
	client, err := es.Connect(ctx, settings.ES)
	if err != nil {
		return err
	}
	if err := indices.Ensure(ctx, client, settings); err != nil {
		return err
	}

	background := &sync.WaitGroup{}
	if settings.IngestionMode == config.IngestionInProcess {
		background = polling.Start(ctx, client, settings)
	} else {
		slog.Info("ingestion_delegated_to_job", "mode", settings.IngestionMode)
	}

	zonesIndex := settings.Alias("dvf-zones")
	server := &http.Server{
		Addr: settings.Addr,
		Handler: (&api.Server{
			ES:     client,
			Prefix: settings.ES.IndexPrefix,
			Version: httpcache.NewDataVersion(httpcache.DefaultTTL, func(ctx context.Context) (*string, error) {
				return dvf.LatestCalculeLe(ctx, client, zonesIndex)
			}),
			CacheMaxAge:               settings.HTTPCacheMaxAge,
			CacheStaleWhileRevalidate: settings.HTTPCacheStaleWhileRevalidate,
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	slog.Info("immo_api_started", "addr", settings.Addr, "ingestion_mode", settings.IngestionMode)

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			stop()
			background.Wait()
			return err
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err = server.Shutdown(shutdownCtx)
	stop() // arrête les boucles d'ingestion en cours
	background.Wait()
	slog.Info("immo_api_stopped")
	return err
}
