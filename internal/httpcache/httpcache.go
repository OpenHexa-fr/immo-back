// Package httpcache gère la validation conditionnelle des réponses DVF (ETag).
//
// Le `Cache-Control` posé sur les réponses évite l'aller-retour tant qu'elles
// sont fraîches ; l'ETag traite ce qui se passe ensuite. Quand le client
// revalide, on peut répondre 304 **sans exécuter la requête Elasticsearch**,
// parce que l'empreinte ne dépend que de l'URL demandée et de la version des
// données.
//
// La version est la date du dernier calcul des zones, qui suit chaque
// ingestion DVF. Elle n'est donc valable que pour les routes DVF : une
// ingestion DPE ne la fait pas bouger.
package httpcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// DefaultTTL : une version périmée de quelques dizaines de secondes est sans
// conséquence, l'ingestion qui la fait changer durant des minutes.
const DefaultTTL = 60 * time.Second

// Fetcher lit la version courante des données (nil si inconnue).
type Fetcher func(ctx context.Context) (*string, error)

// DataVersion met en cache la version des données, pour ne pas interroger
// Elasticsearch à chaque requête.
type DataVersion struct {
	ttl   time.Duration
	fetch Fetcher
	now   func() time.Time

	mu        sync.Mutex
	value     *string
	fetchedAt time.Time
	fetched   bool
}

// NewDataVersion crée un cache de version.
func NewDataVersion(ttl time.Duration, fetch Fetcher) *DataVersion {
	return &DataVersion{ttl: ttl, fetch: fetch, now: time.Now}
}

// Get retourne la version des données, ou nil si elle est indisponible. Une
// lecture en échec n'est pas mise en cache : elle désactive l'ETag, sans plus.
func (d *DataVersion) Get(ctx context.Context) *string {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	if d.fetched && now.Sub(d.fetchedAt) < d.ttl {
		return d.value
	}
	value, err := d.fetch(ctx)
	if err != nil {
		return nil
	}
	d.value, d.fetchedAt, d.fetched = value, now, true
	return value
}

// BuildETag retourne l'empreinte faible d'une réponse : même version + même
// URL = même contenu. Faible (`W/`) parce qu'elle ne porte pas sur les octets
// rendus — la compression peut varier sans que la réponse change de sens.
func BuildETag(version, path, rawQuery string) string {
	digest := sha256.Sum256([]byte(version + "|" + path + "?" + rawQuery))
	return `W/"` + hex.EncodeToString(digest[:])[:32] + `"`
}
