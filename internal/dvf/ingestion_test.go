package dvf

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/openhexa-fr/core/estest"
)

func ptr[T any](v T) *T { return &v }

func baseRow() Row {
	return Row{
		IDMutation: "2024-1", DateMutation: "2024-01-15",
		NumeroDisposition: ptr("000001"), IDParcelle: ptr("13001000AB0042"), Lot1Numero: ptr("001"),
		ValeurFonciere: 250000, SurfaceReelleBati: ptr[int64](80), TypeLocal: ptr("Appartement"),
		NomCommune: ptr("Marseille"), CodePostal: ptr("13001"), CodeDepartement: ptr("13"),
		CodeCommune: ptr("13001"), Latitude: ptr(43.29), Longitude: ptr(5.37),
	}
}

func TestDocumentBuildsDeterministicID(t *testing.T) {
	row := baseRow()

	document := Document(row)

	if id, _ := document["_id"].(string); len(id) != 16 || id != Document(baseRow())["_id"] {
		t.Errorf("_id = %v", document["_id"])
	}
	if !reflect.DeepEqual(document["location"], map[string]any{"lat": 43.29, "lon": 5.37}) {
		t.Errorf("location = %v", document["location"])
	}
	if *document["code_section"].(*string) != "13001000AB" || *document["prix_m2"].(*float64) != 3125 {
		t.Errorf("section=%v prix=%v", document["code_section"], document["prix_m2"])
	}
	row.Occurrence = 1
	if Document(row)["_id"] == document["_id"] {
		t.Error("l'occurrence doit distinguer deux lignes identiques")
	}
}

func TestPrixM2FallsBackToSurfaceTerrain(t *testing.T) {
	if got := PrixM2(100000, nil, ptr[int64](1000)); got == nil || *got != 100 {
		t.Errorf("terrain : %v", got)
	}
	if got := PrixM2(100000, ptr[int64](0), ptr[int64](1000)); got == nil || *got != 100 {
		t.Errorf("surface bâtie nulle : repli sur le terrain attendu, reçu %v", got)
	}
	if got := PrixM2(0, ptr[int64](80), nil); got != nil {
		t.Errorf("valeur nulle : %v", *got)
	}
	if got := PrixM2(100000, nil, nil); got != nil {
		t.Errorf("sans surface : %v", *got)
	}
	if got := PrixM2(100000, ptr[int64](3), nil); *got != 33333.33 {
		t.Errorf("arrondi : %v", *got)
	}
}

func TestDocumentWithoutParcelleOrLocation(t *testing.T) {
	row := baseRow()
	row.IDParcelle, row.Latitude = nil, nil

	document := Document(row)

	if document["location"] != nil || document["code_section"].(*string) != nil {
		t.Errorf("location=%v section=%v", document["location"], document["code_section"])
	}
}

func TestAdresse(t *testing.T) {
	tests := []struct {
		numero        *int64
		suffixe, voie *string
		want          *string
	}{
		{ptr[int64](28), nil, ptr("RUE DE LA POTERNE"), ptr("28 RUE DE LA POTERNE")},
		{ptr[int64](12), ptr("B"), ptr("RUE DE TUNIS"), ptr("12B RUE DE TUNIS")},
		{ptr[int64](12), nil, nil, nil},
		{nil, nil, ptr("LIEU DIT"), ptr("LIEU DIT")},
	}
	for _, tc := range tests {
		if got := Adresse(tc.numero, tc.suffixe, tc.voie); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Adresse = %v, attendu %v", deref(got), deref(tc.want))
		}
	}
}

func TestIdentifiantBAN(t *testing.T) {
	tests := []struct {
		name          string
		commune, voie *string
		numero        *int64
		suffixe       *string
		want          *string
	}{
		{"format ADEME", ptr("59350"), ptr("0550"), ptr[int64](28), nil, ptr("59350_0550_00028")},
		{"code voie alphanumérique", ptr("01158"), ptr("B078"), ptr[int64](4), nil, ptr("01158_B078_00004")},
		{"bis", ptr("59350"), ptr("0550"), ptr[int64](12), ptr("B"), ptr("59350_0550_00012_bis")},
		{"ter en minuscule", ptr("59350"), ptr("0550"), ptr[int64](12), ptr(" t "), ptr("59350_0550_00012_ter")},
		{"suffixe ambigu", ptr("59350"), ptr("0550"), ptr[int64](12), ptr("A"), nil},
		{"sans numéro", ptr("59350"), ptr("0550"), nil, nil, nil},
		{"sans voie", ptr("59350"), nil, ptr[int64](12), nil, nil},
	}
	for _, tc := range tests {
		if got := IdentifiantBAN(tc.commune, tc.voie, tc.numero, tc.suffixe); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s : %v, attendu %v", tc.name, deref(got), deref(tc.want))
		}
	}
}

// En-tête et lignes au format réel de geo-dvf (colonnes inutiles comprises).
const csvHeader = "id_mutation,date_mutation,numero_disposition,nature_mutation,valeur_fonciere,adresse_numero," +
	"adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,code_commune,nom_commune,code_departement," +
	"ancien_code_commune,ancien_nom_commune,id_parcelle,ancien_id_parcelle,numero_volume,lot1_numero," +
	"lot1_surface_carrez,lot2_numero,lot2_surface_carrez,lot3_numero,lot3_surface_carrez,lot4_numero," +
	"lot4_surface_carrez,lot5_numero,lot5_surface_carrez,nombre_lots,code_type_local,type_local," +
	"surface_reelle_bati,nombre_pieces_principales,code_nature_culture,nature_culture," +
	"code_nature_culture_speciale,nature_culture_speciale,surface_terrain,longitude,latitude\n"

const csvMaison = "2025-1,2025-01-07,000001,Vente,468000,454,,RUE DE LA REPUBLIQUE,0090,01550,01158,Farges,01,,," +
	"011580000B0910,,,,,,,,,,,,,0,1,Maison,111,5,S,sols,,,133,5.907186,46.170782\n"

func parse(t *testing.T, raw []byte) []Row {
	t.Helper()
	var err error
	var rows []Row
	for row := range ParseRows(bytes.NewReader(raw), &err) {
		rows = append(rows, row)
	}
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestParseRowsReadsTheRealFormat(t *testing.T) {
	rows := parse(t, []byte(csvHeader+csvMaison))

	if len(rows) != 1 {
		t.Fatalf("%d lignes", len(rows))
	}
	row := rows[0]
	// Les codes gardent leurs zéros initiaux (département de l'Ain).
	if *row.CodePostal != "01550" || *row.CodeDepartement != "01" || *row.CodeCommune != "01158" ||
		*row.AdresseCodeVoie != "0090" || *row.NumeroDisposition != "000001" {
		t.Errorf("codes : %+v", row)
	}
	if row.ValeurFonciere != 468000 || *row.SurfaceReelleBati != 111 || *row.SurfaceTerrain != 133 ||
		*row.NombrePiecesPrincipales != 5 || *row.AdresseNumero != 454 || *row.Latitude != 46.170782 {
		t.Errorf("valeurs : %+v", row)
	}
	if row.Lot1Numero != nil || row.AdresseSuffixe != nil {
		t.Errorf("les champs vides doivent être nil : %+v", row)
	}
	if got := *Document(row)["identifiant_ban"].(*string); got != "01158_0090_00454" {
		t.Errorf("identifiant_ban = %s", got)
	}
}

func TestParseRowsDecompressesGzip(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(csvHeader + csvMaison))
	_ = writer.Close()

	if rows := parse(t, compressed.Bytes()); len(rows) != 1 {
		t.Errorf("%d lignes", len(rows))
	}
}

func TestParseRowsDropsRowsWithoutValeurFonciere(t *testing.T) {
	sansValeur := strings.Replace(csvMaison, "Vente,468000,", "Vente,,", 1)
	sansValeur = strings.Replace(sansValeur, "2025-1,", "2025-0,", 1)

	rows := parse(t, []byte(csvHeader+sansValeur+csvMaison))

	if len(rows) != 1 || rows[0].IDMutation != "2025-1" {
		t.Errorf("rows = %+v", rows)
	}
}

func TestParseRowsDisambiguatesIdenticalRows(t *testing.T) {
	rows := parse(t, []byte(csvHeader+csvMaison+csvMaison+csvMaison))

	ids := map[any]bool{}
	for _, row := range rows {
		ids[Document(row)["_id"]] = true
	}
	if len(ids) != 3 {
		t.Errorf("%d _id distincts pour 3 lignes identiques", len(ids))
	}
}

func TestParseRowsRequiresKnownColumns(t *testing.T) {
	var err error
	for range ParseRows(strings.NewReader("id_mutation,valeur_fonciere\n1,2\n"), &err) {
		t.Error("aucune ligne attendue")
	}
	if err == nil || !strings.Contains(err.Error(), "absente") {
		t.Errorf("erreur attendue sur colonne manquante, reçu %v", err)
	}
}

func TestIngestStreamsTheFileIntoBulk(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, csvHeader+csvMaison)
	}))
	defer source.Close()
	client, recorder := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"errors": false, "items": []any{
			map[string]any{"index": map[string]any{"_id": "x", "status": 201}},
		}})
	})

	success, failed, err := Ingest(t.Context(), client, "openhexa-immo-dvf", source.URL)

	if err != nil || success != 1 || failed != 0 {
		t.Fatalf("success=%d failed=%d err=%v", success, failed, err)
	}
	bulk := recorder.Find("POST", "/_bulk")
	if len(bulk) != 1 || !strings.Contains(string(bulk[0].Body), `"identifiant_ban":"01158_0090_00454"`) {
		t.Errorf("bulk : %+v", bulk)
	}
}

func TestIngestYearsContinuesPastAFailingYear(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, csvHeader+csvMaison)
	}))
	defer good.Close()
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()
	client, _ := estest.New(t, func(w http.ResponseWriter, r *http.Request) {
		estest.JSON(w, 200, map[string]any{"errors": false, "items": []any{
			map[string]any{"index": map[string]any{"_id": "x", "status": 201}},
		}})
	})

	success, failed := IngestYears(t.Context(), client, "idx", []string{missing.URL, good.URL, good.URL})

	if success != 2 || failed != 0 {
		t.Errorf("success=%d failed=%d", success, failed)
	}
}
