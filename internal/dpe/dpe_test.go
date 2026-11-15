package dpe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openhexa-fr/core/estest"
)

func row(t *testing.T, raw string) Row {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var r Row
	if err := decoder.Decode(&r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDocumentExtractsBANKeyAndLocation(t *testing.T) {
	document := Document(row(t, `{
		"numero_dpe": "2459E0000001A", "date_etablissement_dpe": "2024-05-02", "date_reception_dpe": "2024-05-03",
		"etiquette_dpe": "D", "etiquette_ges": "B", "nom_commune_ban": "Lille", "code_postal_ban": "59000",
		"surface_habitable_logement": 62.5, "identifiant_ban": "59350_0550_00028", "score_ban": "0.87",
		"type_batiment": "appartement", "_geopoint": "50.63,3.06"
	}`))

	if id, _ := document["_id"].(string); len(id) != 16 {
		t.Errorf("_id = %v", document["_id"])
	}
	if document["identifiant_ban"] != "59350_0550_00028" || document["surface_habitable"] != 62.5 ||
		document["score_ban"] != 0.87 || document["date_reception"] != "2024-05-03" {
		t.Errorf("document : %v", document)
	}
	if !reflect.DeepEqual(document["location"], map[string]any{"lat": 50.63, "lon": 3.06}) {
		t.Errorf("location = %v", document["location"])
	}
}

func TestDocumentSkipsRowsWithoutNumero(t *testing.T) {
	if Document(row(t, `{"etiquette_dpe": "D"}`)) != nil {
		t.Error("une ligne sans numero_dpe doit être ignorée")
	}
}

func TestParseGeopointToleratesMissingOrMalformedValues(t *testing.T) {
	for _, raw := range []any{nil, "", "50.63", "a,b", 12} {
		if ParseGeopoint(raw) != nil {
			t.Errorf("%v : nil attendu", raw)
		}
	}
}

func TestFiltreCombinesDateAndDepartement(t *testing.T) {
	depuis := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	if got := Filtre(&depuis, "2A"); got != "date_reception_dpe:[2024-05-01 TO *] AND code_departement_ban:2A" {
		t.Errorf("Filtre = %s", got)
	}
	if Filtre(nil, "") != "" {
		t.Error("aucun critère : filtre vide attendu")
	}
}

func TestDepartementsCoverMetropoleCorseAndDOM(t *testing.T) {
	if len(Departements) != 101 || Departements[0] != "01" || slices.Contains(Departements, "20") ||
		!slices.Contains(Departements, "2A") || !slices.Contains(Departements, "2B") ||
		!slices.Contains(Departements, "95") || Departements[len(Departements)-1] != "976" {
		t.Errorf("départements : %v", Departements)
	}
}

func TestChampsUtilesCoverTheDocument(t *testing.T) {
	sources := map[string]string{
		"date_etablissement_dpe": "", "date_reception_dpe": "", "nom_commune_ban": "", "code_postal_ban": "",
		"surface_habitable_logement": "", "_geopoint": "", "numero_dpe": "", "etiquette_dpe": "",
		"etiquette_ges": "", "identifiant_ban": "", "adresse_ban": "", "score_ban": "", "type_batiment": "",
	}
	for field := range sources {
		if !slices.Contains(ChampsUtiles, field) {
			t.Errorf("%s lu par Document mais non demandé à l'API", field)
		}
	}
}

// ademe simule l'API data-fair : deux pages chaînées par `next`, puis une page vide.
func ademe(t *testing.T, queries *[]string) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*queries = append(*queries, r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("after") {
		case "":
			fmt.Fprintf(w, `{"results":[{"numero_dpe":"A"},{"numero_dpe":"B"}],"next":"%s/lines?after=1&size=1000"}`, server.URL)
		case "1":
			fmt.Fprintf(w, `{"results":[{"numero_dpe":"C"}],"next":"%s/lines?after=2&size=1000"}`, server.URL)
		default:
			fmt.Fprint(w, `{"results":[]}`)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestFetchPagesFollowsTheCursorAndStopsOnEmptyPage(t *testing.T) {
	var queries []string
	source := ademe(t, &queries)

	var err error
	var pages [][]Row
	for page := range FetchPages(t.Context(), source.URL, nil, "", &err) {
		pages = append(pages, page)
	}

	if err != nil || len(pages) != 2 || len(pages[0]) != 2 || len(queries) != 3 {
		t.Fatalf("pages=%d requêtes=%v err=%v", len(pages), queries, err)
	}
	if !strings.Contains(queries[0], "select=numero_dpe") || strings.Contains(queries[1], "select") {
		t.Errorf("seule la première requête porte les paramètres, `next` les inclut déjà : %v", queries)
	}
}

func esForIngest(t *testing.T, derniere string) (*estest.Recorder, func(http.ResponseWriter, *http.Request)) {
	return nil, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_search") {
			hits := []any{}
			if derniere != "" {
				hits = append(hits, map[string]any{"_source": map[string]any{"date_reception": derniere}})
			}
			estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": hits}})
			return
		}
		estest.JSON(w, 200, map[string]any{"errors": false, "items": []any{}})
	}
}

func TestIngestResumesFromTheLastReception(t *testing.T) {
	var queries []string
	source := ademe(t, &queries)
	_, handler := esForIngest(t, "2024-05-10T00:00:00")
	client, _ := estest.New(t, handler)

	if _, _, err := Ingest(t.Context(), client, "dpe", source.URL, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(queries[0], "qs=date_reception_dpe%3A%5B2024-05-03+TO+%2A%5D") {
		t.Errorf("reprise attendue 7 jours avant la dernière réception : %s", queries[0])
	}
}

func TestIngestIsCompleteWhenTheIndexIsEmpty(t *testing.T) {
	var queries []string
	source := ademe(t, &queries)
	_, handler := esForIngest(t, "")
	client, _ := estest.New(t, handler)

	if _, _, err := Ingest(t.Context(), client, "dpe", source.URL, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(queries[0], "qs=") || len(queries) != 3 {
		t.Errorf("moisson complète sans découpage attendue : %v", queries)
	}
}

func TestIngestCompletSplitsByDepartement(t *testing.T) {
	var queries []string
	source := ademe(t, &queries)
	_, handler := esForIngest(t, "2024-05-10")
	client, _ := estest.New(t, handler)

	if _, _, err := Ingest(t.Context(), client, "dpe", source.URL, true); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 3*len(Departements) {
		t.Fatalf("%d requêtes", len(queries))
	}
	if !strings.Contains(queries[0], "qs=code_departement_ban%3A01") || strings.Contains(queries[0], "date_reception_dpe%3A") {
		t.Errorf("la moisson complète ignore le point de reprise : %s", queries[0])
	}
}

func TestBuildQuery(t *testing.T) {
	if !reflect.DeepEqual(BuildQuery(SearchParams{}), map[string]any{"match_all": map[string]any{}}) {
		t.Error("sans filtre : match_all attendu")
	}
	payload, _ := json.Marshal(BuildQuery(SearchParams{EtiquetteDPE: []string{"F", "G"}}))
	if string(payload) != `{"bool":{"filter":[{"terms":{"etiquette_dpe":["F","G"]}}]}}` {
		t.Errorf("requête : %s", payload)
	}
}
