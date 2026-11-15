package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openhexa-fr/core/estest"

	"github.com/openhexa-fr/immo-back/internal/httpcache"
)

const cacheControl = "public, max-age=3600, stale-while-revalidate=86400"

func newServer(t *testing.T, version *string, handler http.HandlerFunc) (*Server, *estest.Recorder) {
	t.Helper()
	if handler == nil {
		handler = func(w http.ResponseWriter, r *http.Request) {
			estest.JSON(w, 200, map[string]any{"hits": map[string]any{"total": map[string]any{"value": 0}, "hits": []any{}}})
		}
	}
	client, recorder := estest.New(t, handler)
	return &Server{
		ES: client, Prefix: "openhexa-immo",
		Version:     httpcache.NewDataVersion(time.Minute, func(context.Context) (*string, error) { return version, nil }),
		CacheMaxAge: time.Hour, CacheStaleWhileRevalidate: 24 * time.Hour,
	}, recorder
}

func get(handler http.Handler, target string, headers ...string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		request.Header.Set(headers[i], headers[i+1])
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func ptr[T any](v T) *T { return &v }

func TestSearchResponsesAreCachableAndCarryAnETag(t *testing.T) {
	server, _ := newServer(t, ptr("2026-08-17T18:00:00Z"), nil)

	response := get(server.Handler(), "/api/v1/dvf/search?commune=Lille")

	if response.Code != 200 || response.Header().Get("Cache-Control") != cacheControl ||
		!strings.HasPrefix(response.Header().Get("ETag"), `W/"`) {
		t.Errorf("statut=%d en-têtes=%v corps=%s", response.Code, response.Header(), response.Body)
	}
}

func TestMatchingETagReturns304WithoutQueryingElasticsearch(t *testing.T) {
	server, recorder := newServer(t, ptr("2026-08-17T18:00:00Z"), nil)
	handler := server.Handler()
	etag := get(handler, "/api/v1/dvf/search?commune=Lille").Header().Get("ETag")
	before := len(recorder.Requests())

	response := get(handler, "/api/v1/dvf/search?commune=Lille", "If-None-Match", etag)

	if response.Code != http.StatusNotModified || response.Header().Get("Cache-Control") != cacheControl {
		t.Errorf("statut=%d en-têtes=%v", response.Code, response.Header())
	}
	if len(recorder.Requests()) != before {
		t.Error("une revalidation ne doit exécuter aucune recherche")
	}
	if get(handler, "/api/v1/dvf/search?commune=Lille", "If-None-Match", `W/"perime"`).Code != 200 {
		t.Error("un ETag périmé doit être ignoré")
	}
}

func TestDPEResponsesCarryNoETag(t *testing.T) {
	server, _ := newServer(t, ptr("2026-08-17T18:00:00Z"), nil)

	response := get(server.Handler(), "/api/v1/dpe/search")

	if response.Header().Get("ETag") != "" || response.Header().Get("Cache-Control") != cacheControl {
		t.Errorf("en-têtes=%v corps=%s", response.Header(), response.Body)
	}
}

func TestMissingVersionDisablesTheETagButKeepsCaching(t *testing.T) {
	server, _ := newServer(t, nil, nil)

	response := get(server.Handler(), "/api/v1/dvf/search")

	if response.Header().Get("ETag") != "" || !strings.HasPrefix(response.Header().Get("Cache-Control"), "public") {
		t.Errorf("en-têtes=%v", response.Header())
	}
}

func TestStatusIsNeverCached(t *testing.T) {
	server, _ := newServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		count := 0
		if strings.Contains(r.URL.Path, "-dvf") {
			count = 5
		}
		estest.JSON(w, 200, map[string]any{"count": count})
	})

	response := get(server.Handler(), "/api/v1/status")

	if response.Header().Get("Cache-Control") != "no-store" || strings.TrimSpace(response.Body.String()) != `{"dvf":true,"dpe":false}` {
		t.Errorf("en-têtes=%v corps=%s", response.Header(), response.Body)
	}
}

func TestErrorResponsesAreNotCached(t *testing.T) {
	server, _ := newServer(t, ptr("v1"), func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 500, map[string]any{"error": "boom"})
	})

	for _, target := range []string{"/api/v1/dvf/search", "/api/v1/dvf/search?size=abc", "/api/v1/dvf/search?bbox=1,2"} {
		response := get(server.Handler(), target)
		if response.Code == 200 || response.Header().Get("Cache-Control") != "" || response.Header().Get("ETag") != "" {
			t.Errorf("%s : statut=%d en-têtes=%v", target, response.Code, response.Header())
		}
	}
}

func TestSearchValidatesParametersLikeFastAPI(t *testing.T) {
	server, _ := newServer(t, nil, nil)

	response := get(server.Handler(), "/api/v1/dvf/search?surface_min=80.5&size=5000&champs=tout")

	var body struct {
		Detail []validationError `json:"detail"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if response.Code != http.StatusUnprocessableEntity || len(body.Detail) != 3 {
		t.Fatalf("statut=%d corps=%s", response.Code, response.Body)
	}
	if body.Detail[0].Loc[1] != "surface_min" || body.Detail[0].Type != "int_parsing" {
		t.Errorf("détail : %+v", body.Detail)
	}
}

func TestSearchRejectsMalformedBBoxAndCursorWith400(t *testing.T) {
	server, _ := newServer(t, nil, nil)
	for _, target := range []string{"/api/v1/dvf/search?bbox=1,2,3", "/api/v1/dvf/search?cursor=%25%25"} {
		if response := get(server.Handler(), target); response.Code != http.StatusBadRequest {
			t.Errorf("%s : statut=%d", target, response.Code)
		}
	}
}

func TestSearchResponseShape(t *testing.T) {
	server, recorder := newServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{
			"total": map[string]any{"value": 10000, "relation": "gte"},
			"hits": []any{map[string]any{"_id": "1", "_source": map[string]any{
				"id_mutation": "m1", "date_mutation": "2024-01-15", "valeur_fonciere": 250000, "commune": "Lille",
				"location": map[string]any{"lat": 50.6, "lon": 3.05},
			}, "sort": []any{1705276800000, 42}}},
		}})
	})

	response := get(server.Handler(), "/api/v1/dvf/search?champs=carte&size=500&type_local=Maison&type_local=Appartement")

	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	item := body["items"].([]any)[0].(map[string]any)
	if body["total_relation"] != "gte" || body["next_cursor"] == nil || item["surface_reelle_bati"] != nil ||
		item["commune"] != "Lille" || item["location"].(map[string]any)["lat"] != 50.6 {
		t.Errorf("corps : %s", response.Body)
	}
	if _, present := item["etiquette_dpe_apres_vente"]; !present {
		t.Error("les champs absents doivent être rendus en null, pas omis")
	}
	request := recorder.Find("POST", "/openhexa-immo-dvf/_search")[0].JSONBody(t)
	if request["size"] != float64(500) || request["_source"] == nil {
		t.Errorf("requête : %v", request)
	}

	// Le curseur renvoyé permet de demander la page suivante.
	cursor := body["next_cursor"].(string)
	get(server.Handler(), "/api/v1/dvf/search?cursor="+cursor)
	next := recorder.Find("POST", "/openhexa-immo-dvf/_search")[1]
	if !strings.Contains(string(next.Body), `"search_after":[1705276800000,42]`) {
		t.Errorf("page suivante : %s", next.Body)
	}
}

func TestLargeResponsesAreGzipped(t *testing.T) {
	server, _ := newServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		hits := []any{}
		for range 20 {
			hits = append(hits, map[string]any{"_source": map[string]any{"id_mutation": "m", "commune": "Lille"}})
		}
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": hits}})
	})

	compressed := get(server.Handler(), "/api/v1/dvf/m", "Accept-Encoding", "gzip, deflate")
	plain := get(server.Handler(), "/api/v1/dvf/m")

	if compressed.Header().Get("Content-Encoding") != "gzip" || plain.Header().Get("Content-Encoding") != "" {
		t.Fatalf("encodages : %q / %q", compressed.Header().Get("Content-Encoding"), plain.Header().Get("Content-Encoding"))
	}
	reader, err := gzip.NewReader(compressed.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := io.ReadAll(reader)
	if string(decoded) != plain.Body.String() {
		t.Error("le contenu décompressé doit être identique")
	}
}

func TestPrixCarteRequiresTheParentZone(t *testing.T) {
	server, _ := newServer(t, nil, nil)
	tests := map[string]int{
		"/api/v1/dvf/prix-carte":                                                      422,
		"/api/v1/dvf/prix-carte?niveau=region":                                        422,
		"/api/v1/dvf/prix-carte?niveau=commune":                                       400,
		"/api/v1/dvf/prix-carte?niveau=section":                                       400,
		"/api/v1/dvf/prix-carte?niveau=departement":                                   200,
		"/api/v1/dvf/prix-carte?niveau=commune&categorie=terrain&code_departement=59": 200,
	}
	for target, want := range tests {
		if response := get(server.Handler(), target); response.Code != want {
			t.Errorf("%s : statut=%d, attendu %d (%s)", target, response.Code, want, response.Body)
		}
	}
}

func TestPrixCarteFallsBackToOnTheFlyAggregation(t *testing.T) {
	var aggregated atomic.Bool
	server, _ := newServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "dvf-zones") {
			estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{}}})
			return
		}
		aggregated.Store(true)
		estest.JSON(w, 200, map[string]any{"aggregations": map[string]any{"by_zone": map[string]any{"buckets": []any{
			map[string]any{"key": "59", "doc_count": 3, "prix_median": map[string]any{"values": map[string]any{"50.0": 2500.0}}},
		}}}})
	})

	response := get(server.Handler(), "/api/v1/dvf/prix-carte?niveau=departement")

	if !aggregated.Load() || !strings.Contains(response.Body.String(), `"calcule_le":null`) ||
		!strings.Contains(response.Body.String(), `"code":"59"`) {
		t.Errorf("corps : %s", response.Body)
	}
}

func zonesHandler(sources ...map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits := []any{}
		for _, source := range sources {
			hits = append(hits, map[string]any{"_source": source})
		}
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": hits}})
	}
}

func TestPrixSerieComputesVariations(t *testing.T) {
	server, _ := newServer(t, nil, zonesHandler(
		map[string]any{"code": "59350", "label": "Lille", "annee": 2021, "prix_m2_median_bati": 2000.0, "nb_mutations_bati": 10},
		map[string]any{"code": "59350", "label": "Lille", "annee": 2022, "prix_m2_median_terrain": 100.0},
		map[string]any{"code": "59350", "label": "Lille", "annee": 2023, "prix_m2_median_bati": 2400.0, "nb_mutations_bati": 12},
	))

	response := get(server.Handler(), "/api/v1/dvf/prix-serie?niveau=commune&code=59350")

	var body prixSerieResponse
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if len(body.Points) != 2 || body.Points[0].EvolutionPct != nil || *body.Points[1].EvolutionPct != 20 ||
		*body.EvolutionTotalePct != 20 || *body.Label != "Lille" {
		t.Errorf("corps : %s", response.Body)
	}
	if get(server.Handler(), "/api/v1/dvf/prix-serie?niveau=section&code=x").Code != 422 {
		t.Error("les sections n'ont pas de série")
	}
}

func TestZoneReturns404WithoutAggregate(t *testing.T) {
	server, _ := newServer(t, nil, zonesHandler(map[string]any{"code": "59350", "prix_m2_median_bati": 3000.0}))

	if response := get(server.Handler(), "/api/v1/dvf/zone?niveau=commune&code=59350&categorie=terrain"); response.Code != 404 ||
		!strings.Contains(response.Body.String(), "zone commune:59350 sans agrégat terrain") {
		t.Errorf("statut=%d corps=%s", response.Code, response.Body)
	}
	if response := get(server.Handler(), "/api/v1/dvf/zone?niveau=commune&code=59350"); response.Code != 200 ||
		!strings.Contains(response.Body.String(), `"prix_m2_median":3000`) {
		t.Errorf("statut=%d corps=%s", response.Code, response.Body)
	}
}

func TestParcelleRoutesAreNotCapturedByMutation(t *testing.T) {
	server, recorder := newServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{}}})
	})

	if response := get(server.Handler(), "/api/v1/dvf/parcelle/59350000AB0001"); !strings.Contains(response.Body.String(), `"mutations":[]`) {
		t.Errorf("parcelle : %s", response.Body)
	}
	if response := get(server.Handler(), "/api/v1/dvf/parcelle/59350000AB0001/dpe"); !strings.Contains(response.Body.String(), `"diagnostics":[]`) {
		t.Errorf("parcelle/dpe : %s", response.Body)
	}
	if !strings.Contains(string(recorder.Requests()[0].Body), "collapse") {
		t.Error("la route parcelle doit grouper par mutation")
	}
}

func TestCORSPreflight(t *testing.T) {
	server, _ := newServer(t, nil, nil)
	request := httptest.NewRequest(http.MethodOptions, "/api/v1/dvf/search", nil)
	request.Header.Set("Origin", "http://front")
	request.Header.Set("Access-Control-Request-Method", "GET")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != 200 || response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("statut=%d en-têtes=%v", response.Code, response.Header())
	}
}
