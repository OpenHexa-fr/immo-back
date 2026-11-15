// Package polling fait tourner l'ingestion dans le serveur web
// (INGESTION_MODE=in_process) : une boucle par source, plus un rattrapage du
// calcul des zones au démarrage.
package polling

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"

	"github.com/openhexa-fr/immo-back/internal/config"
	"github.com/openhexa-fr/immo-back/internal/dpe"
	"github.com/openhexa-fr/immo-back/internal/dvf"
)

// IndexHasData indique si `indexAlias` contient déjà au moins un document.
// Sans ce test, un redémarrage relancerait l'ingestion initiale (fichiers
// nationaux volumineux) alors que les données sont déjà indexées. Si le
// comptage échoue, on retente l'ingestion par prudence.
func IndexHasData(ctx context.Context, client *elasticsearch.Client, indexAlias string) bool {
	n, err := es.Count(ctx, client, indexAlias)
	return err == nil && n > 0
}

// Loop interroge périodiquement une source et réindexe, sans jamais s'arrêter
// sur erreur. Si `skipInitial` retourne true (l'index a déjà des données), le
// premier passage immédiat est sauté au profit du prochain intervalle.
func Loop(ctx context.Context, name string, interval time.Duration, skipInitial func(context.Context) bool, ingest func(context.Context) error) {
	run := func() {
		if err := ingest(ctx); err != nil && ctx.Err() == nil {
			slog.Error(name+"_polling_failed", "error", err.Error())
		}
	}
	if skipInitial != nil && skipInitial(ctx) {
		slog.Info(name+"_polling_initial_run_skipped", "reason", "index_already_populated")
	} else {
		run()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

// Start lance les boucles d'ingestion en tâche de fond ; le WaitGroup
// retourné se libère quand `ctx` est annulé et que les boucles ont rendu la main.
//
// Le cycle DVF enchaîne ingestion des millésimes, rapprochement avec les DPE
// et recalcul des zones (qui en dérivent). Le cycle DPE est incrémental.
func Start(ctx context.Context, client *elasticsearch.Client, settings config.Settings) *sync.WaitGroup {
	dvfAlias, dpeAlias, zonesAlias := settings.Alias("dvf"), settings.Alias("dpe"), settings.Alias("dvf-zones")
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		// Rattrapage : sans lui, un index de zones vide (premier déploiement,
		// calcul interrompu) attendrait la prochaine ingestion DVF — jusqu'à une
		// semaine — puisque l'ingestion initiale est sautée quand DVF est peuplé.
		if !IndexHasData(ctx, client, dvfAlias) {
			return
		}
		if computed, err := dvf.ZonesAreComputed(ctx, client, zonesAlias); err == nil && computed {
			return
		}
		slog.Info("dvf_zones_backfill_started")
		if _, _, err := dvf.ComputeZones(ctx, client, dvfAlias, zonesAlias); err != nil && ctx.Err() == nil {
			slog.Error("dvf_zones_backfill_failed", "error", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		Loop(ctx, "dvf", settings.DVFPollingInterval,
			func(ctx context.Context) bool { return IndexHasData(ctx, client, dvfAlias) },
			func(ctx context.Context) error {
				dvf.IngestYears(ctx, client, dvfAlias, settings.DVFDataURLs())
				if _, _, err := dvf.JoindreDPE(ctx, client, dvfAlias, dpeAlias); err != nil {
					slog.Error("dvf_dpe_jointure_failed", "error", err.Error())
				}
				_, _, err := dvf.ComputeZones(ctx, client, dvfAlias, zonesAlias)
				return err
			})
	}()
	go func() {
		defer wg.Done()
		Loop(ctx, "dpe", settings.DPEPollingInterval,
			func(ctx context.Context) bool { return IndexHasData(ctx, client, dpeAlias) },
			func(ctx context.Context) error {
				_, _, err := dpe.Ingest(ctx, client, dpeAlias, settings.DPEDataURL, false)
				return err
			})
	}()
	return &wg
}
