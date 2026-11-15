package dvf

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/openhexa-fr/core/estest"
)

func diagnostic(date, etiquette string) Diagnostic {
	return Diagnostic{DateEtablissement: &date, EtiquetteDPE: &etiquette}
}

func TestMeilleurDPE(t *testing.T) {
	tests := []struct {
		name           string
		diagnostics    []Diagnostic
		wantEtiquette  string
		wantPosterieur bool
		wantOK         bool
	}{
		{"plus récent antérieur", []Diagnostic{diagnostic("2022-01-01", "E"), diagnostic("2023-05-01", "C"), diagnostic("2023-06-02", "A")}, "C", false, true},
		{"jour même accepté", []Diagnostic{diagnostic("2023-06-01T10:00:00", "D")}, "D", false, true},
		{"postérieur proche à défaut", []Diagnostic{diagnostic("2024-01-01", "B")}, "B", true, true},
		{"postérieur hors fenêtre", []Diagnostic{diagnostic("2025-06-01", "B")}, "", false, false},
		{"antérieur toujours préféré", []Diagnostic{diagnostic("2023-06-02", "A"), diagnostic("2020-01-01", "G")}, "G", false, true},
		{"postérieur le plus proche", []Diagnostic{diagnostic("2024-03-01", "C"), diagnostic("2023-09-01", "B")}, "B", true, true},
		{"sans date exploitable", []Diagnostic{{EtiquetteDPE: ptr("A")}}, "", false, false},
	}
	for _, tc := range tests {
		etiquette, posterieur, ok := MeilleurDPE(tc.diagnostics, "2023-06-01")
		if etiquette != tc.wantEtiquette || posterieur != tc.wantPosterieur || ok != tc.wantOK {
			t.Errorf("%s : (%q, %v, %v)", tc.name, etiquette, posterieur, ok)
		}
	}
	if _, _, ok := MeilleurDPE([]Diagnostic{diagnostic("2023-01-01", "A")}, "pas une date"); ok {
		t.Error("une date de vente illisible ne doit rien rapprocher")
	}
}

// jointureServer simule le parcours point-in-time : une page de mutations,
// puis une page vide ; les DPE répondent pour l'identifiant BAN "A".
type jointureServer struct {
	mu        sync.Mutex
	pages     int
	bulkBody  string
	dpeQuery  string
	closedPIT bool
	failBulk  bool
}

func (j *jointureServer) handle(w http.ResponseWriter, r *http.Request) {
	j.mu.Lock()
	defer j.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_pit"):
		estest.JSON(w, 200, map[string]any{"id": "pit-1"})
	case r.Method == http.MethodDelete && r.URL.Path == "/_pit":
		j.closedPIT = true
		estest.JSON(w, 200, map[string]any{"succeeded": true})
	case r.URL.Path == "/_search":
		j.pages++
		hits := []any{}
		if j.pages == 1 {
			hits = []any{
				map[string]any{"_id": "m1", "_source": map[string]any{"identifiant_ban": "A", "date_mutation": "2023-06-01"}, "sort": []any{1}},
				map[string]any{"_id": "m2", "_source": map[string]any{"identifiant_ban": "B", "date_mutation": "2023-06-01"}, "sort": []any{2}},
				map[string]any{"_id": "m3", "_source": map[string]any{"identifiant_ban": "A", "date_mutation": "2019-01-01"}, "sort": []any{3}},
			}
		}
		estest.JSON(w, 200, map[string]any{"pit_id": "pit-2", "hits": map[string]any{"hits": hits}})
	case strings.HasSuffix(r.URL.Path, "/dpe/_search"):
		j.dpeQuery = string(body)
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_source": map[string]any{"identifiant_ban": "A", "etiquette_dpe": "C", "date_etablissement": "2021-03-01"}},
		}}})
	case r.URL.Path == "/_bulk":
		j.bulkBody = string(body)
		status := 200
		if j.failBulk {
			status = 409
		}
		var items []any
		for _, id := range []string{"m1"} {
			items = append(items, map[string]any{"update": map[string]any{"_id": id, "status": status}})
		}
		estest.JSON(w, 200, map[string]any{"errors": j.failBulk, "items": items})
	default:
		estest.JSON(w, 404, map[string]any{"error": r.URL.Path})
	}
}

func TestJoindreDPEUpdatesMatchedMutations(t *testing.T) {
	server := &jointureServer{}
	client, recorder := estest.New(t, server.handle)

	rapproches, erreurs, err := JoindreDPE(t.Context(), client, "dvf", "dpe")

	if err != nil || rapproches != 1 || erreurs != 0 {
		t.Fatalf("rapproches=%d erreurs=%d err=%v", rapproches, erreurs, err)
	}
	// m1 : DPE antérieur → étiquette fiable ; m2 : pas de DPE ; m3 : DPE hors fenêtre.
	if !strings.Contains(server.bulkBody, `"_id":"m1"`) || !strings.Contains(server.bulkBody, `{"doc":{"etiquette_dpe":"C"}}`) ||
		strings.Contains(server.bulkBody, "m2") || strings.Contains(server.bulkBody, "m3") {
		t.Errorf("bulk : %s", server.bulkBody)
	}
	for _, want := range []string{`{"terms":{"identifiant_ban":["A","B"]}}`, `{"range":{"score_ban":{"gte":0.5}}}`} {
		if !strings.Contains(server.dpeQuery, want) {
			t.Errorf("%s absent de la requête DPE : %s", want, server.dpeQuery)
		}
	}
	// Le parcours se fait sur le point-in-time, renouvelé à chaque page, et le referme.
	searches := recorder.Find("POST", "/_search")
	if len(searches) != 2 || !strings.Contains(string(searches[1].Body), `"id":"pit-2"`) ||
		!strings.Contains(string(searches[1].Body), `"search_after":[3]`) || !server.closedPIT {
		t.Errorf("parcours PIT : %d recherches, fermé=%v", len(searches), server.closedPIT)
	}
}

func TestJoindreDPECountsRealBulkFailures(t *testing.T) {
	server := &jointureServer{failBulk: true}
	client, _ := estest.New(t, server.handle)

	rapproches, erreurs, err := JoindreDPE(t.Context(), client, "dvf", "dpe")

	if err != nil || rapproches != 0 || erreurs != 1 {
		t.Errorf("rapproches=%d erreurs=%d err=%v", rapproches, erreurs, err)
	}
}

func TestHistoriqueDPEParcelleGathersParcelAddresses(t *testing.T) {
	var dpeQuery string
	client, _ := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(r.URL.Path, "/dvf/") {
			estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
				map[string]any{"_source": map[string]any{"identifiant_ban": "B"}},
				map[string]any{"_source": map[string]any{"identifiant_ban": "A"}},
				map[string]any{"_source": map[string]any{"identifiant_ban": "A"}},
			}}})
			return
		}
		dpeQuery = string(body)
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_source": map[string]any{"numero_dpe": "D1", "etiquette_dpe": "C", "score_ban": 0.3}},
		}}})
	})

	diagnostics, err := HistoriqueDPEParcelle(t.Context(), client, "dvf", "dpe", "59350000AB0001")

	if err != nil || len(diagnostics) != 1 || diagnostics[0].NumeroDPE != "D1" || *diagnostics[0].ScoreBAN != 0.3 {
		t.Fatalf("diagnostics=%+v err=%v", diagnostics, err)
	}
	if !strings.Contains(dpeQuery, `{"terms":{"identifiant_ban":["A","B"]}}`) || !strings.Contains(dpeQuery, `{"date_etablissement":"desc"}`) {
		t.Errorf("requête DPE : %s", dpeQuery)
	}
}

func TestHistoriqueDPEParcelleEmptyWithoutAddress(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{}}})
	})

	diagnostics, err := HistoriqueDPEParcelle(t.Context(), client, "dvf", "dpe", "x")

	if err != nil || diagnostics == nil || len(diagnostics) != 0 || len(recorder.Requests()) != 1 {
		t.Errorf("diagnostics=%v err=%v requêtes=%d", diagnostics, err, len(recorder.Requests()))
	}
}
