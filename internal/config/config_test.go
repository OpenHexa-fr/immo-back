package config

import (
	"reflect"
	"testing"
	"time"
)

func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv("ES_URL", "http://localhost:9200")
	for _, key := range []string{
		"PORT", "INGESTION_MODE", "DVF_YEAR_START", "DVF_YEAR_END", "DVF_DATA_URL", "DPE_DATA_URL",
		"HTTP_CACHE_MAX_AGE_SECONDS", "HTTP_CACHE_STALE_WHILE_REVALIDATE_SECONDS",
		"DVF_POLLING_INTERVAL_SECONDS", "DPE_POLLING_INTERVAL_SECONDS", "ES_INDEX_PREFIX",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)

	settings, err := Load()

	if err != nil {
		t.Fatal(err)
	}
	if settings.Addr != ":8000" || settings.IngestionMode != IngestionInProcess ||
		settings.DVFPollingInterval != 7*24*time.Hour || settings.DPEPollingInterval != 24*time.Hour ||
		settings.HTTPCacheMaxAge != time.Hour || settings.HTTPCacheStaleWhileRevalidate != 24*time.Hour {
		t.Errorf("settings = %+v", settings)
	}
	if settings.Alias("dvf-zones") != "openhexa-dvf-zones" {
		t.Errorf("alias = %s", settings.Alias("dvf-zones"))
	}
}

func TestDVFURLsCoverEveryYear(t *testing.T) {
	clearEnv(t)
	settings, _ := Load()

	want := []string{
		"https://files.data.gouv.fr/geo-dvf/latest/csv/2021/full.csv.gz",
		"https://files.data.gouv.fr/geo-dvf/latest/csv/2022/full.csv.gz",
		"https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz",
		"https://files.data.gouv.fr/geo-dvf/latest/csv/2024/full.csv.gz",
		"https://files.data.gouv.fr/geo-dvf/latest/csv/2025/full.csv.gz",
	}
	if got := settings.DVFDataURLs(); !reflect.DeepEqual(got, want) {
		t.Errorf("urls = %v", got)
	}
}

func TestDVFDataURLOverridesYears(t *testing.T) {
	clearEnv(t)
	t.Setenv("DVF_DATA_URL", "http://source/full.csv.gz")
	settings, _ := Load()

	if got := settings.DVFDataURLs(); !reflect.DeepEqual(got, []string{"http://source/full.csv.gz"}) {
		t.Errorf("urls = %v", got)
	}
}

func TestLoadReadsTheEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv("ES_INDEX_PREFIX", "openhexa-immo")
	t.Setenv("PORT", "9000")
	t.Setenv("INGESTION_MODE", "job")
	t.Setenv("DVF_YEAR_START", "2024")

	settings, err := Load()

	if err != nil {
		t.Fatal(err)
	}
	if settings.Addr != ":9000" || settings.IngestionMode != IngestionJob || len(settings.DVFDataURLs()) != 2 ||
		settings.Alias("dpe") != "openhexa-immo-dpe" {
		t.Errorf("settings = %+v", settings)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	clearEnv(t)
	t.Setenv("INGESTION_MODE", "cron")
	if _, err := Load(); err == nil {
		t.Error("mode inconnu : erreur attendue")
	}

	t.Setenv("INGESTION_MODE", "")
	t.Setenv("DVF_YEAR_START", "deux mille")
	if _, err := Load(); err == nil {
		t.Error("millésime non numérique : erreur attendue")
	}
}
