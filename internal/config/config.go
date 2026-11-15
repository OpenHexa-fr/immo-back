// Package config lit la configuration de l'API Immo : paramètres
// Elasticsearch (via core) + sources de données.
//
// URLs validées face aux exports réels (voir la documentation des paquets dvf
// et dpe) :
//
//   - DVF : un fichier national annuel `full.csv.gz` par millésime. Le miroir
//     `geo-dvf/latest` ne conserve que les cinq derniers millésimes glissants
//     (à ce jour : 2021 à 2025, les années antérieures y renvoient 404) ;
//     l'année N n'est pas encore disponible en cours d'année N, d'où un défaut
//     sur le dernier millésime complet.
//   - DPE : l'identifiant de dataset ADEME data-fair n'est pas un slug stable
//     ("dpe-v2-logements-existants" renvoie 404) mais un identifiant opaque
//     propre au catalogue ; il doit être re-vérifié périodiquement via
//     `GET https://data.ademe.fr/data-fair/api/v1/datasets?q=DPE` si
//     l'ingestion se met à échouer.
package config

import (
	"fmt"
	"time"

	core "github.com/openhexa-fr/core/config"
)

// Modes d'ingestion.
const (
	// IngestionInProcess : les boucles de polling tournent dans le serveur web.
	IngestionInProcess = "in_process"
	// IngestionJob : le serveur web n'ingère rien, l'ingestion est assurée par
	// `immo-back ingest` lancé par un ordonnanceur séparé.
	IngestionJob = "job"
)

const defaultDPEDataURL = "https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe"

// Settings est la configuration complète de l'API.
type Settings struct {
	ES core.ESSettings

	// Addr est l'adresse d'écoute HTTP (variable PORT, 8000 par défaut).
	Addr string

	// IngestionMode vaut IngestionInProcess (défaut) ou IngestionJob. Le défaut
	// reste `in_process` : basculer sur `job` avant que l'ordonnanceur n'existe
	// laisserait les données se périmer en silence.
	IngestionMode string

	DVFYearStart int
	DVFYearEnd   int
	// DVFDataURL, si renseignée, remplace les URLs construites depuis les
	// millésimes (une seule URL).
	DVFDataURL string
	DPEDataURL string

	// Durées de cache HTTP des réponses de lecture. Les données ne bougent qu'au
	// rythme du polling (au mieux quotidien) : servir une réponse d'il y a une
	// heure est sans conséquence fonctionnelle.
	HTTPCacheMaxAge               time.Duration
	HTTPCacheStaleWhileRevalidate time.Duration

	// Sources volumineuses et peu volatiles (mises à jour mensuelles côté
	// producteurs) : polling nettement moins fréquent que les prix carburants.
	DVFPollingInterval time.Duration
	DPEPollingInterval time.Duration
}

// Load lit la configuration depuis l'environnement.
func Load() (Settings, error) {
	esSettings, err := core.LoadESSettings()
	if err != nil {
		return Settings{}, err
	}
	settings := Settings{
		ES:            esSettings,
		Addr:          ":" + core.String("PORT", "8000"),
		IngestionMode: core.String("INGESTION_MODE", IngestionInProcess),
		DVFDataURL:    core.String("DVF_DATA_URL", ""),
		DPEDataURL:    core.String("DPE_DATA_URL", defaultDPEDataURL),
	}
	if settings.IngestionMode != IngestionInProcess && settings.IngestionMode != IngestionJob {
		return Settings{}, fmt.Errorf("config: INGESTION_MODE doit valoir %q ou %q, reçu %q",
			IngestionInProcess, IngestionJob, settings.IngestionMode)
	}

	ints := []struct {
		key string
		def int
		set func(int)
	}{
		{"DVF_YEAR_START", 2021, func(v int) { settings.DVFYearStart = v }},
		{"DVF_YEAR_END", 2025, func(v int) { settings.DVFYearEnd = v }},
		{"HTTP_CACHE_MAX_AGE_SECONDS", 3600, func(v int) { settings.HTTPCacheMaxAge = seconds(v) }},
		{"HTTP_CACHE_STALE_WHILE_REVALIDATE_SECONDS", 86400,
			func(v int) { settings.HTTPCacheStaleWhileRevalidate = seconds(v) }},
		{"DVF_POLLING_INTERVAL_SECONDS", 7 * 24 * 3600, func(v int) { settings.DVFPollingInterval = seconds(v) }},
		{"DPE_POLLING_INTERVAL_SECONDS", 24 * 3600, func(v int) { settings.DPEPollingInterval = seconds(v) }},
	}
	for _, entry := range ints {
		value, err := core.Int(entry.key, entry.def)
		if err != nil {
			return Settings{}, err
		}
		entry.set(value)
	}
	return settings, nil
}

func seconds(n int) time.Duration { return time.Duration(n) * time.Second }

// DVFDataURLs retourne les URLs des fichiers DVF nationaux pour chaque
// millésime de DVFYearStart à DVFYearEnd inclus, sauf override explicite (une
// seule URL).
func (s Settings) DVFDataURLs() []string {
	if s.DVFDataURL != "" {
		return []string{s.DVFDataURL}
	}
	var urls []string
	for year := s.DVFYearStart; year <= s.DVFYearEnd; year++ {
		urls = append(urls, fmt.Sprintf("https://files.data.gouv.fr/geo-dvf/latest/csv/%d/full.csv.gz", year))
	}
	return urls
}

// Alias retourne l'alias Elasticsearch d'un domaine (ex. "dvf" → "openhexa-immo-dvf").
func (s Settings) Alias(domain string) string { return s.ES.IndexPrefix + "-" + domain }
