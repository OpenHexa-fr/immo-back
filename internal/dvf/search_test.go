package dvf

import (
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/openhexa-fr/core/estest"
)

func filtersOf(t *testing.T, query map[string]any) []map[string]any {
	t.Helper()
	payload, _ := json.Marshal(query["bool"].(map[string]any)["filter"])
	var filters []map[string]any
	_ = json.Unmarshal(payload, &filters)
	return filters
}

func hasFilter(filters []map[string]any, want string) bool {
	for _, f := range filters {
		payload, _ := json.Marshal(f)
		if string(payload) == want {
			return true
		}
	}
	return false
}

func TestBuildQueryExcludesMutationsWithoutValeurFonciere(t *testing.T) {
	filters := filtersOf(t, BuildQuery(SearchParams{RadiusKm: 10}))

	if len(filters) != 1 || !hasFilter(filters, `{"exists":{"field":"valeur_fonciere"}}`) {
		t.Errorf("filters = %v", filters)
	}
}

func TestBuildQueryCombinesFiltersAndRanges(t *testing.T) {
	filters := filtersOf(t, BuildQuery(SearchParams{
		Commune: ptr("Lille"), TypeLocal: []string{"Maison", "Appartement"}, IDParcelle: ptr("59350000AB0001"),
		ValeurFonciereMin: ptr(100000.0), SurfaceMax: ptr[int64](90), PiecesMin: ptr[int64](3),
		PrixM2Max: ptr(4000.0), SurfaceTerrainMin: ptr[int64](200),
		DateMutationMin: ptr("2023-01-01"), DateMutationMax: ptr("2023-12-31"),
	}))

	for _, want := range []string{
		`{"term":{"commune":"Lille"}}`,
		`{"terms":{"type_local":["Maison","Appartement"]}}`,
		`{"term":{"id_parcelle":"59350000AB0001"}}`,
		`{"range":{"valeur_fonciere":{"gte":100000}}}`,
		`{"range":{"surface_reelle_bati":{"lte":90}}}`,
		`{"range":{"nombre_pieces_principales":{"gte":3}}}`,
		`{"range":{"prix_m2":{"lte":4000}}}`,
		`{"range":{"surface_terrain":{"gte":200}}}`,
		`{"range":{"date_mutation":{"gte":"2023-01-01","lte":"2023-12-31"}}}`,
	} {
		if !hasFilter(filters, want) {
			t.Errorf("filtre absent : %s\n%v", want, filters)
		}
	}
}

func TestBuildQueryPrefersBBoxOverRadius(t *testing.T) {
	box, err := ParseBBox("2.9,50.5,3.2,50.7")
	if err != nil {
		t.Fatal(err)
	}
	params := SearchParams{BBox: &box, Lat: ptr(50.6), Lon: ptr(3.05), RadiusKm: 10, Tri: SortDistance}

	filters := filtersOf(t, BuildQuery(params))

	if !hasFilter(filters, `{"geo_bounding_box":{"location":{"bottom_right":{"lat":50.5,"lon":3.2},"top_left":{"lat":50.7,"lon":2.9}}}}`) {
		t.Errorf("geo_bounding_box absent : %v", filters)
	}
	for _, f := range filters {
		if _, ok := f["geo_distance"]; ok {
			t.Error("la bbox doit primer sur le rayon")
		}
	}
	// Le centre reste utilisable pour trier par distance.
	if _, ok := BuildSort(params)[0]["_geo_distance"]; !ok {
		t.Errorf("tri : %v", BuildSort(params))
	}
}

func TestBuildQueryKeepsGeoDistanceWithoutBBox(t *testing.T) {
	filters := filtersOf(t, BuildQuery(SearchParams{Lat: ptr(50.6), Lon: ptr(3.05), RadiusKm: 2.5}))

	if !hasFilter(filters, `{"geo_distance":{"distance":"2.5km","location":{"lat":50.6,"lon":3.05}}}`) {
		t.Errorf("filters = %v", filters)
	}
}

func TestParseBBox(t *testing.T) {
	box, err := ParseBBox("2.9, 50.5, 3.2, 50.7")
	if err != nil || box != (BBox{MinLon: 2.9, MinLat: 50.5, MaxLon: 3.2, MaxLat: 50.7}) {
		t.Errorf("box=%+v err=%v", box, err)
	}
	// Antiméridien : min_lon > max_lon est légitime.
	if _, err := ParseBBox("170,-20,-170,-10"); err != nil {
		t.Errorf("antiméridien refusé : %v", err)
	}
	for _, raw := range []string{"1,2,3", "a,b,c,d", "200,0,10,10", "0,-95,10,10", "0,50,10,40"} {
		if _, err := ParseBBox(raw); err == nil {
			t.Errorf("%q aurait dû être refusée", raw)
		}
	}
}

func TestBuildSort(t *testing.T) {
	tests := map[Sort]string{
		SortPrix:       `[{"valeur_fonciere":"asc"},{"_seq_no":"asc"}]`,
		SortSurface:    `[{"surface_reelle_bati":"desc"},{"_seq_no":"asc"}]`,
		SortRecent:     `[{"date_mutation":"desc"},{"_seq_no":"asc"}]`,
		SortPertinence: `[{"date_mutation":"desc"},{"_seq_no":"asc"}]`,
		// Sans position, le tri par distance retombe sur le tri par défaut.
		SortDistance: `[{"date_mutation":"desc"},{"_seq_no":"asc"}]`,
	}
	for tri, want := range tests {
		payload, _ := json.Marshal(BuildSort(SearchParams{Tri: tri}))
		if string(payload) != want {
			t.Errorf("%s : %s", tri, payload)
		}
	}
}

func TestCarteSourceFieldsCoverMapNeeds(t *testing.T) {
	for _, field := range []string{"id_mutation", "id_parcelle", "location", "valeur_fonciere", "prix_m2"} {
		if !slices.Contains(CarteSourceFields, field) {
			t.Errorf("%s manquant", field)
		}
	}
}

func TestSearchForwardsSourceFiltering(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"total": map[string]any{"value": 0}, "hits": []any{}}})
	})

	if _, err := Search(t.Context(), client, "idx", SearchParams{}, nil, 500, CarteSourceFields); err != nil {
		t.Fatal(err)
	}
	body := recorder.Find("POST", "/idx/_search")[0].JSONBody(t)
	if len(body["_source"].([]any)) != len(CarteSourceFields) || body["size"] != float64(500) {
		t.Errorf("body = %v", body)
	}
}

func TestParcelleMutationsGroupsLotsByMutation(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{
				"_source": map[string]any{"id_mutation": "m1", "date_mutation": "2024-03-01"},
				"inner_hits": map[string]any{"lots": map[string]any{"hits": map[string]any{"hits": []any{
					map[string]any{"_source": map[string]any{"id_mutation": "m1", "valeur_fonciere": 100000, "commune": "Lille"}},
					map[string]any{"_source": map[string]any{"id_mutation": "m1", "valeur_fonciere": 100000, "commune": "Lille"}},
				}}}},
			},
		}}})
	})

	mutations, err := ParcelleMutations(t.Context(), client, "idx", "59350000AB0001", 50)

	if err != nil || len(mutations) != 1 || len(mutations[0].Lots) != 2 || mutations[0].DateMutation != "2024-03-01" {
		t.Fatalf("mutations=%+v err=%v", mutations, err)
	}
	body := recorder.Find("POST", "/idx/_search")[0].JSONBody(t)
	if body["collapse"].(map[string]any)["field"] != "id_mutation" {
		t.Errorf("collapse : %v", body["collapse"])
	}
	if !strings.Contains(string(recorder.Requests()[0].Body), `{"exists":{"field":"valeur_fonciere"}}`) {
		t.Error("les mutations sans valeur foncière doivent être exclues")
	}
}

func TestAggregatePrixCarteComputesMedianAndDropsEmptyBuckets(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"aggregations": map[string]any{"by_zone": map[string]any{"buckets": []any{
			map[string]any{"key": "59350", "doc_count": 12, "prix_median": map[string]any{"values": map[string]any{"50.0": 2512.3456}},
				"label": map[string]any{"hits": map[string]any{"hits": []any{map[string]any{"_source": map[string]any{"commune": "Lille"}}}}}},
			map[string]any{"key": "59000", "doc_count": 0, "prix_median": map[string]any{"values": map[string]any{"50.0": nil}}},
		}}}})
	})

	buckets, err := AggregatePrixCarte(t.Context(), client, "idx", "commune", ptr("59"), nil)

	want := []Bucket{{Code: "59350", Label: "Lille", PrixM2Median: 2512.35, NbMutations: 12}}
	if err != nil || !reflect.DeepEqual(buckets, want) {
		t.Fatalf("buckets=%+v err=%v", buckets, err)
	}
	body := string(recorder.Requests()[0].Body)
	if !strings.Contains(body, `{"term":{"code_departement":"59"}}`) || !strings.Contains(body, `{"exists":{"field":"prix_m2"}}`) {
		t.Errorf("requête : %s", body)
	}
}

func TestGetByMutationFiltersOnTerm(t *testing.T) {
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_source": map[string]any{"id_mutation": "m1", "commune": "Lille", "surface_reelle_bati": 80}},
		}}})
	})

	transactions, err := GetByMutation(t.Context(), client, "idx", "m1")

	if err != nil || len(transactions) != 1 || *transactions[0].SurfaceReelleBati != 80 {
		t.Fatalf("transactions=%+v err=%v", transactions, err)
	}
	if !strings.Contains(string(recorder.Requests()[0].Body), `{"term":{"id_mutation":"m1"}}`) {
		t.Errorf("requête : %s", recorder.Requests()[0].Body)
	}
}
