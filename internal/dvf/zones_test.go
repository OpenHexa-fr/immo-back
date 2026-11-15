package dvf

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openhexa-fr/core/estest"
)

func percentiles(p25, p50, p75 any, count int) map[string]any {
	return map[string]any{"doc_count": count, "prix": map[string]any{"values": map[string]any{"25.0": p25, "50.0": p50, "75.0": p75}}}
}

func zoneBucket(key map[string]any, bati, terrain map[string]any, dpe, passoire int) map[string]any {
	return map[string]any{
		"key": key, "doc_count": 10, "bati": bati, "terrain": terrain,
		"dpe":   map[string]any{"doc_count": dpe, "passoire": map[string]any{"doc_count": passoire}},
		"label": map[string]any{"hits": map[string]any{"hits": []any{map[string]any{"_source": map[string]any{"commune": "Lille"}}}}},
	}
}

// zonesServer simule Elasticsearch : `pages` sont les pages de buckets
// composite successives (une liste par niveau et par série), les bulks sont
// acceptés et enregistrés.
type zonesServer struct {
	mu        sync.Mutex
	pages     [][]any
	documents []map[string]any
}

func (z *zonesServer) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		z.mu.Lock()
		defer z.mu.Unlock()
		if r.URL.Path == "/_bulk" {
			body, _ := io.ReadAll(r.Body)
			scanner := bufio.NewScanner(bytes.NewReader(body))
			var items []any
			for scanner.Scan() {
				var line map[string]any
				_ = json.Unmarshal(scanner.Bytes(), &line)
				if action, ok := line["index"].(map[string]any); ok {
					items = append(items, map[string]any{"index": map[string]any{"_id": action["_id"], "status": 201}})
					continue
				}
				z.documents = append(z.documents, line)
			}
			estest.JSON(w, 200, map[string]any{"errors": false, "items": items})
			return
		}
		var buckets []any
		if len(z.pages) > 0 {
			buckets, z.pages = z.pages[0], z.pages[1:]
		}
		zones := map[string]any{"buckets": buckets}
		if len(buckets) > 0 {
			zones["after_key"] = buckets[len(buckets)-1].(map[string]any)["key"]
		}
		estest.JSON(w, 200, map[string]any{"aggregations": map[string]any{"zones": zones}})
	}
}

func TestComputeZonesBuildsDocumentsWithDeterministicIDs(t *testing.T) {
	server := &zonesServer{pages: [][]any{
		// départements : une page, puis page vide
		{zoneBucket(map[string]any{"code": "59"}, percentiles(2000.123, 2500.456, 3000.0, 8), percentiles(nil, nil, nil, 0), 4, 1)},
		{},
		// communes
		{zoneBucket(map[string]any{"parent": "59", "code": "59350"}, percentiles(nil, 3000.0, nil, 5), percentiles(nil, 150.0, nil, 2), 0, 0)},
		{},
		// sections : une zone sans médiane ni DPE, ignorée
		{zoneBucket(map[string]any{"parent": "59350", "code": "59350000AB"}, percentiles(nil, nil, nil, 0), percentiles(nil, nil, nil, 0), 0, 0)},
		{},
		// séries départements puis communes
		{zoneBucket(map[string]any{"code": "59", "annee": "2024"}, percentiles(nil, 2400.0, nil, 3), percentiles(nil, nil, nil, 0), 0, 0)},
		{},
		{},
	}}
	client, _ := estest.New(t, server.handler(t))

	success, failed, err := ComputeZones(t.Context(), client, "dvf", "dvf-zones")

	if err != nil || success != 3 || failed != 0 {
		t.Fatalf("success=%d failed=%d err=%v", success, failed, err)
	}
	byID := map[string]map[string]any{}
	for _, document := range server.documents {
		byID[document["niveau"].(string)+":"+document["code"].(string)+":"+strings.TrimSuffix(jsonString(document["annee"]), "<nil>")] = document
	}
	departement := byID["departement:59:"]
	if departement["prix_m2_median_bati"] != 2500.46 || departement["prix_m2_p25_bati"] != 2000.12 ||
		departement["prix_m2_median_terrain"] != nil || departement["dpe_part_passoire"] != 25.0 ||
		departement["code_parent"] != nil || departement["label"] != "59" {
		t.Errorf("département : %v", departement)
	}
	commune := byID["commune:59350:"]
	if commune["code_parent"] != "59" || commune["label"] != "Lille" || commune["prix_m2_median_terrain"] != 150.0 ||
		commune["dpe_part_passoire"] != nil || commune["dpe_nb_avec_etiquette"] != 0.0 {
		t.Errorf("commune : %v", commune)
	}
	if serie := byID["departement:59:2024"]; serie["annee"] != 2024.0 || serie["prix_m2_median_bati"] != 2400.0 {
		t.Errorf("série : %v (documents %v)", serie, server.documents)
	}
	if calcule, _ := departement["calcule_le"].(string); !strings.HasSuffix(calcule, "+00:00") {
		t.Errorf("calcule_le = %v", departement["calcule_le"])
	}
}

func jsonString(v any) string {
	if v == nil {
		return "<nil>"
	}
	payload, _ := json.Marshal(v)
	return string(payload)
}

func TestComputeZonesFollowsCompositePaginationAndPlausibleQuery(t *testing.T) {
	server := &zonesServer{}
	var bodies []string
	client, _ := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" {
			body, _ := io.ReadAll(r.Body)
			bodies = append(bodies, string(body))
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		server.handler(t)(w, r)
	})
	server.pages = [][]any{
		{zoneBucket(map[string]any{"code": "59"}, percentiles(nil, 2500.0, nil, 8), percentiles(nil, nil, nil, 0), 0, 0)},
		{zoneBucket(map[string]any{"code": "62"}, percentiles(nil, 2000.0, nil, 8), percentiles(nil, nil, nil, 0), 0, 0)},
	}

	if _, _, err := ComputeZones(t.Context(), client, "dvf", "dvf-zones"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bodies[1], `"after":{"code":"59"}`) {
		t.Errorf("la deuxième page doit reprendre après la première : %s", bodies[1])
	}
	if !strings.Contains(bodies[0], `{"range":{"prix_m2":{"gte":10,"lte":50000}}}`) {
		t.Errorf("bornes de plausibilité absentes : %s", bodies[0])
	}
	if len(server.documents) != 2 {
		t.Errorf("%d documents", len(server.documents))
	}
}

func TestZonesAreComputedChecksTheLastLevel(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"count": 3})
	})

	computed, err := ZonesAreComputed(t.Context(), client, "zones")

	if err != nil || !computed {
		t.Fatalf("computed=%v err=%v", computed, err)
	}
	if !strings.Contains(string(recorder.Requests()[0].Body), `{"term":{"niveau":"section"}}`) {
		t.Errorf("requête : %s", recorder.Requests()[0].Body)
	}
}

func TestFetchZonesFiltersOnLevelParentAndExcludesSeries(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_source": map[string]any{"niveau": "commune", "code": "59350", "prix_m2_median_bati": 3000}},
		}}})
	})

	zones, err := FetchZones(t.Context(), client, "zones", "commune", ptr("59"))

	if err != nil || len(zones) != 1 || *zones[0].PrixM2MedianBati != 3000 {
		t.Fatalf("zones=%+v err=%v", zones, err)
	}
	body := string(recorder.Requests()[0].Body)
	for _, want := range []string{`{"term":{"niveau":"commune"}}`, `{"term":{"code_parent":"59"}}`, `"must_not":[{"exists":{"field":"annee"}}]`} {
		if !strings.Contains(body, want) {
			t.Errorf("%s absent de %s", want, body)
		}
	}

	if _, err := FetchZones(t.Context(), client, "zones", "departement", nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(recorder.Requests()[1].Body), "code_parent") {
		t.Error("pas de filtre parent au niveau département")
	}
}

func TestFetchZoneReturnsNilWhenUnknown(t *testing.T) {
	client, _ := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{}}})
	})

	zone, err := FetchZone(t.Context(), client, "zones", "commune", "00000")

	if err != nil || zone != nil {
		t.Errorf("zone=%v err=%v", zone, err)
	}
}

func TestLatestCalculeLe(t *testing.T) {
	client, _ := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_source": map[string]any{"calcule_le": "2026-11-15T12:00:00.000000+00:00"}},
		}}})
	})

	version, err := LatestCalculeLe(t.Context(), client, "zones")

	if err != nil || version == nil || *version != "2026-11-15T12:00:00.000000+00:00" {
		t.Errorf("version=%v err=%v", version, err)
	}
}

var lille = Zone{
	Code: "59350", Label: ptr("Lille"),
	PrixM2MedianBati: ptr(3200.0), PrixM2P25Bati: ptr(2500.0), PrixM2P75Bati: ptr(4100.0), NbMutationsBati: ptr[int64](4200),
	PrixM2MedianTerrain: ptr(180.0), NbMutationsTerrain: ptr[int64](130),
	NbMutations: 4330, DPEPartPassoire: ptr(12.5), DPENbAvecEtiquette: ptr[int64](900),
}

func TestAplatirSelectsTheCategory(t *testing.T) {
	bati := Aplatir(lille, "bati")
	if bati.PrixM2Median != 3200 || *bati.PrixM2P25 != 2500 || bati.NbMutations != 4200 || bati.Label != "Lille" {
		t.Errorf("bâti : %+v", bati)
	}
	terrain := Aplatir(lille, "terrain")
	if terrain.PrixM2Median != 180 || terrain.PrixM2P25 != nil || terrain.NbMutations != 130 {
		t.Errorf("terrain : %+v", terrain)
	}
	// La statistique DPE est transmise quelle que soit la catégorie.
	if *terrain.DPEPartPassoire != 12.5 || *terrain.DPENbAvecEtiquette != 900 {
		t.Errorf("DPE : %+v", terrain)
	}
	sansTerrain := lille
	sansTerrain.PrixM2MedianTerrain = nil
	if Aplatir(sansTerrain, "terrain") != nil {
		t.Error("une catégorie sans médiane doit être ignorée")
	}
}

func TestVariationPct(t *testing.T) {
	tests := []struct {
		depuis, vers *float64
		want         *float64
	}{
		{ptr(2000.0), ptr(2400.0), ptr(20.0)},
		{ptr(2500.0), ptr(2000.0), ptr(-20.0)},
		{ptr(3000.0), ptr(3100.0), ptr(3.3)},
		{nil, ptr(2000.0), nil},
		{ptr(0.0), ptr(2000.0), nil},
	}
	for _, tc := range tests {
		got := VariationPct(tc.depuis, tc.vers)
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("VariationPct(%v, %v) = %v", tc.depuis, tc.vers, got)
		}
	}
}

func TestCalculeLeIsUTCWithOffset(t *testing.T) {
	paris := time.FixedZone("CET", 3600)
	if got := CalculeLe(time.Date(2026, 11, 15, 13, 0, 0, 0, paris)); got != "2026-11-15T12:00:00.000000+00:00" {
		t.Errorf("CalculeLe = %s", got)
	}
}
