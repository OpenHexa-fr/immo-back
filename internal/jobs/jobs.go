// Package jobs exécute l'ingestion hors du serveur web (`immo-back ingest`),
// pour une planification séparée (cron ou tout autre ordonnanceur) :
//
//	immo-back ingest --source all
//	immo-back ingest --source dvf --source zones
//
// Le code de retour vaut 0 si toutes les sources demandées ont abouti, 1
// sinon : c'est ce qui permet à l'ordonnanceur de signaler un cycle en échec.
package jobs

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/immo-back/internal/config"
	"github.com/openhexa-fr/immo-back/internal/dpe"
	"github.com/openhexa-fr/immo-back/internal/dvf"
	"github.com/openhexa-fr/immo-back/internal/indices"
)

// Sources est l'ordre retenu pour `--source all` : `jointure` a besoin de DVF
// et DPE fraîchement ingérés, `zones` dérive de DVF (et de la jointure pour la
// statistique DPE).
var Sources = []string{"dvf", "dpe", "jointure", "zones"}

// SourceDPEComplet force une moisson intégrale du dataset ADEME, là où `dpe`
// ne reprend que les diagnostics reçus depuis la dernière ingestion.
const SourceDPEComplet = "dpe-complet"

type sourceList []string

func (s *sourceList) String() string { return strings.Join(*s, ",") }

func (s *sourceList) Set(value string) error {
	if value != "all" && value != SourceDPEComplet && !slices.Contains(Sources, value) {
		return fmt.Errorf("source inconnue %q (attendu : %s, %s ou all)", value, strings.Join(Sources, ", "), SourceDPEComplet)
	}
	*s = append(*s, value)
	return nil
}

// ParseArgs lit les sources demandées (`--source`, répétable). `all`
// équivaut à toutes les sources, dans un ordre qui respecte leurs dépendances ;
// sinon l'ordre de la ligne de commande est respecté tel quel.
func ParseArgs(args []string, output io.Writer) ([]string, error) {
	flags := flag.NewFlagSet("ingest", flag.ContinueOnError)
	flags.SetOutput(output)
	var sources sourceList
	flags.Var(&sources, "source", "source à ingérer, répétable : "+strings.Join(Sources, ", ")+", "+SourceDPEComplet+" ou all")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, errors.New("au moins une --source est requise")
	}
	if slices.Contains(sources, "all") {
		return slices.Clone(Sources), nil
	}
	return sources, nil
}

// Runner exécute une source et retourne (succès, erreurs).
type Runner func(ctx context.Context, source string) (success, failed int, err error)

// NewRunner retourne l'exécuteur des sources réelles.
func NewRunner(client *elasticsearch.Client, settings config.Settings) Runner {
	return func(ctx context.Context, source string) (int, int, error) {
		switch source {
		case "dvf":
			s, f := dvf.IngestYears(ctx, client, settings.Alias("dvf"), settings.DVFDataURLs())
			return s, f, nil
		case "dpe":
			return dpe.Ingest(ctx, client, settings.Alias("dpe"), settings.DPEDataURL, false)
		case SourceDPEComplet:
			return dpe.Ingest(ctx, client, settings.Alias("dpe"), settings.DPEDataURL, true)
		case "jointure":
			return dvf.JoindreDPE(ctx, client, settings.Alias("dvf"), settings.Alias("dpe"))
		case "zones":
			return dvf.ComputeZones(ctx, client, settings.Alias("dvf"), settings.Alias("dvf-zones"))
		}
		return 0, 0, fmt.Errorf("source inconnue : %s", source)
	}
}

// Run exécute les sources dans l'ordre reçu et retourne le code de sortie.
// Une source en échec ne prive pas les suivantes de leur exécution.
func Run(ctx context.Context, sources []string, ensure func(context.Context) error, run Runner) int {
	if err := ensure(ctx); err != nil {
		slog.Error("ingestion_job_indices_failed", "error", err.Error())
		return 1
	}
	var failedSources []string
	for _, source := range sources {
		success, failed, err := run(ctx, source)
		if err != nil {
			slog.Error("ingestion_job_source_failed", "source", source, "error", err.Error())
			failedSources = append(failedSources, source)
			continue
		}
		slog.Info("ingestion_job_source_done", "source", source, "success", success, "errors", failed)
		if failed > 0 {
			failedSources = append(failedSources, source)
		}
	}
	if len(failedSources) > 0 {
		slog.Error("ingestion_job_failed", "sources", failedSources)
		return 1
	}
	slog.Info("ingestion_job_completed", "sources", sources)
	return 0
}

// EnsureIndices adapte indices.Ensure à la signature attendue par Run.
func EnsureIndices(client *elasticsearch.Client, settings config.Settings) func(context.Context) error {
	return func(ctx context.Context) error { return indices.Ensure(ctx, client, settings) }
}
