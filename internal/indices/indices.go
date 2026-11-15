// Package indices garantit la topologie des index du domaine immobilier
// (index + alias par domaine). Partagé par le serveur web et le job
// d'ingestion : l'un comme l'autre peut être le premier à démarrer sur un
// cluster vierge.
package indices

import (
	"context"
	"errors"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"

	"github.com/openhexa-fr/immo-back/internal/config"
	"github.com/openhexa-fr/immo-back/internal/dpe"
	"github.com/openhexa-fr/immo-back/internal/dvf"
)

// Domaines et leur mapping. `dvf-zones` n'est pas une source ingérée mais un
// index dérivé de `dvf`, alimenté par le calcul des zones : il suit néanmoins
// la même convention index + alias.
var domains = map[string]func() map[string]any{
	"dvf":       dvf.Mapping,
	"dpe":       dpe.Mapping,
	"dvf-zones": dvf.ZonesMapping,
}

// Ensure crée les index manquants et leurs alias, et met à jour les mappings
// existants. Les domaines sont indépendants : ils sont traités en parallèle.
func Ensure(ctx context.Context, client *elasticsearch.Client, settings config.Settings) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for domain, mapping := range domains {
		wg.Add(1)
		go func() {
			defer wg.Done()
			alias := settings.Alias(domain)
			index := alias + "-000001"
			_, err := es.CreateIndex(ctx, client, index, mapping(), nil)
			if err == nil {
				_, err = es.EnsureAlias(ctx, client, alias, index)
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
