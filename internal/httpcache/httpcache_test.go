package httpcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBuildETag(t *testing.T) {
	base := BuildETag("v1", "/api/v1/dvf/search", "commune=Lille")
	if base != BuildETag("v1", "/api/v1/dvf/search", "commune=Lille") || len(base) != 36 {
		t.Errorf("ETag = %s", base)
	}
	for _, other := range []string{
		BuildETag("v2", "/api/v1/dvf/search", "commune=Lille"),
		BuildETag("v1", "/api/v1/dvf/search", "commune=Roubaix"),
		BuildETag("v1", "/api/v1/dvf/prix-carte", "commune=Lille"),
	} {
		if other == base {
			t.Error("version, requête et chemin doivent tous changer l'ETag")
		}
	}
}

func TestDataVersionIsFetchedOnceWithinItsTTL(t *testing.T) {
	calls := 0
	version := "v1"
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cache := NewDataVersion(time.Minute, func(context.Context) (*string, error) { calls++; return &version, nil })
	cache.now = func() time.Time { return now }

	cache.Get(t.Context())
	cache.Get(t.Context())
	if calls != 1 {
		t.Errorf("%d lectures dans le TTL", calls)
	}
	now = now.Add(2 * time.Minute)
	if *cache.Get(t.Context()) != "v1" || calls != 2 {
		t.Errorf("%d lectures après expiration", calls)
	}
}

func TestUnreachableClusterDisablesTheETagWithoutCaching(t *testing.T) {
	fail := true
	version := "v1"
	cache := NewDataVersion(time.Minute, func(context.Context) (*string, error) {
		if fail {
			return nil, errors.New("injoignable")
		}
		return &version, nil
	})

	if cache.Get(t.Context()) != nil {
		t.Error("version indisponible : nil attendu")
	}
	fail = false
	if got := cache.Get(t.Context()); got == nil || *got != "v1" {
		t.Error("un échec ne doit pas être mis en cache")
	}
}
