// Package dpe contient le domaine DPE (Diagnostic de Performance
// Énergétique) : ingestion depuis l'API data-fair de l'ADEME et recherche.
//
// Contrairement à DVF (CSV téléchargé en une fois), la source DPE est une API
// REST paginée : l'ingestion consomme page par page jusqu'à obtenir une page
// vide. La pagination par numéro de page est plafonnée par data-fair à 10 000
// résultats — inutilisable pour ce dataset de plus de 15 millions de lignes.
// L'ingestion suit donc le lien `next` fourni par l'API, qui encode un curseur
// `after` stable au-delà de cette limite.
package dpe

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"

	"github.com/openhexa-fr/immo-back/internal/esx"
)

// Mapping retourne le mapping Elasticsearch explicite de l'index DPE.
func Mapping() map[string]any {
	return map[string]any{
		"properties": map[string]any{
			"numero_dpe":         es.Keyword(),
			"date_etablissement": es.Date(),
			// Date de réception par l'ADEME : c'est elle qui pilote l'apparition
			// d'une ligne dans le dataset, donc elle qui borne l'ingestion
			// incrémentale (et non la date d'établissement, qui peut être
			// antérieure de plusieurs semaines).
			"date_reception":    es.Date(),
			"etiquette_dpe":     es.Keyword(),
			"etiquette_ges":     es.Keyword(),
			"commune":           es.Keyword(),
			"code_postal":       es.Keyword(),
			"surface_habitable": es.Float(),
			// Clé de rapprochement avec DVF : identifiant d'adresse de la Base
			// Adresse Nationale, au format `{code_insee}_{code_voie}_{numero}`.
			"identifiant_ban": es.Keyword(),
			"adresse_ban":     es.Keyword(),
			// Qualité du géocodage BAN : sert à écarter les rapprochements douteux.
			"score_ban":     es.Float(),
			"type_batiment": es.Keyword(),
			"location":      es.GeoPoint(),
		},
	}
}

const pageSize = 1000

const requestTimeout = 60 * time.Second

// ChampsUtiles sont les colonnes demandées à l'API. Le dataset en expose 145 ;
// sans `select`, chaque page de 1 000 lignes pèse 6 Mo dont 94 % sont jetés
// (~90 Go pour une moisson complète, contre 6 Go en ne demandant que l'utile).
// La liste doit rester alignée sur Document.
var ChampsUtiles = []string{
	"numero_dpe", "date_etablissement_dpe", "date_reception_dpe", "etiquette_dpe",
	"etiquette_ges", "nom_commune_ban", "code_postal_ban", "surface_habitable_logement",
	"identifiant_ban", "adresse_ban", "score_ban", "type_batiment", "_geopoint",
}

// Filtre assemble le filtre `qs` de data-fair à partir des critères actifs
// (vide si aucun).
func Filtre(depuis *time.Time, departement string) string {
	var clauses []string
	if depuis != nil {
		clauses = append(clauses, fmt.Sprintf("date_reception_dpe:[%s TO *]", depuis.Format(time.DateOnly)))
	}
	if departement != "" {
		clauses = append(clauses, "code_departement_ban:"+departement)
	}
	return strings.Join(clauses, " AND ")
}

// Row est une ligne de l'API data-fair ; nombres en json.Number.
type Row = map[string]any

// FetchPages itère les pages de résultats de l'API via le curseur `next`.
//
// `depuis` restreint la moisson aux diagnostics reçus à partir de cette date :
// c'est ce qui rend une réingestion régulière tenable (une semaine apporte
// quelques milliers de lignes). `departement` restreint à un département : le
// débit d'une moisson intégrale s'effondre à mesure que le curseur s'enfonce
// dans le jeu de données, et découper fait repartir la pagination de zéro.
func FetchPages(ctx context.Context, sourceURL string, depuis *time.Time, departement string, errp *error) func(yield func([]Row) bool) {
	return func(yield func([]Row) bool) {
		params := url.Values{"size": {strconv.Itoa(pageSize)}, "select": {strings.Join(ChampsUtiles, ",")}}
		if filtre := Filtre(depuis, departement); filtre != "" {
			params.Set("qs", filtre)
		}
		next := sourceURL + "/lines?" + params.Encode()
		for next != "" {
			body, err := fetchPage(ctx, next)
			if err != nil {
				*errp = err
				return
			}
			if len(body.Results) == 0 || !yield(body.Results) {
				return
			}
			// Le lien `next` porte déjà le filtre et le curseur.
			next = body.Next
		}
	}
}

type page struct {
	Results []Row  `json:"results"`
	Next    string `json:"next"`
}

func fetchPage(ctx context.Context, pageURL string) (page, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return page{}, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return page{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return page{}, fmt.Errorf("dpe: %s a répondu %d", pageURL, response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	var body page
	if err := decoder.Decode(&body); err != nil {
		return page{}, fmt.Errorf("dpe: réponse illisible : %w", err)
	}
	return body, nil
}

// ParseGeopoint lit `_geopoint`, publié sous la forme "lat,lon" et non en objet.
func ParseGeopoint(raw any) map[string]any {
	text, ok := raw.(string)
	if !ok {
		return nil
	}
	lat, lon, found := strings.Cut(text, ",")
	if !found {
		return nil
	}
	latitude, err1 := strconv.ParseFloat(strings.TrimSpace(lat), 64)
	longitude, err2 := strconv.ParseFloat(strings.TrimSpace(lon), 64)
	if err1 != nil || err2 != nil {
		return nil
	}
	return map[string]any{"lat": latitude, "lon": longitude}
}

// textOrNil rend une valeur de l'API en chaîne (nil si absente ou vide).
func textOrNil(value any) any {
	if s := esx.String(value); s != "" {
		return s
	}
	return nil
}

// numberOrNil rend une valeur numérique de l'API en float64 (nil sinon), y
// compris quand l'API la publie sous forme de chaîne.
func numberOrNil(value any) any {
	if f := esx.Float(value); f != nil {
		return *f
	}
	if s, ok := value.(string); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
			return f
		}
	}
	return nil
}

// Document convertit une ligne en document indexable, ou nil si elle n'a pas
// de numéro de DPE.
func Document(row Row) es.Document {
	numero := esx.String(row["numero_dpe"])
	if numero == "" {
		return nil
	}
	return es.Document{
		"_id":                es.MakeDocumentID(numero),
		"numero_dpe":         numero,
		"date_etablissement": textOrNil(row["date_etablissement_dpe"]),
		"date_reception":     textOrNil(row["date_reception_dpe"]),
		"etiquette_dpe":      textOrNil(row["etiquette_dpe"]),
		"etiquette_ges":      textOrNil(row["etiquette_ges"]),
		"commune":            textOrNil(row["nom_commune_ban"]),
		"code_postal":        textOrNil(row["code_postal_ban"]),
		"surface_habitable":  numberOrNil(row["surface_habitable_logement"]),
		"identifiant_ban":    textOrNil(row["identifiant_ban"]),
		"adresse_ban":        textOrNil(row["adresse_ban"]),
		"score_ban":          numberOrNil(row["score_ban"]),
		"type_batiment":      textOrNil(row["type_batiment"]),
		"location":           ParseGeopoint(row["_geopoint"]),
	}
}

// Marge de recouvrement appliquée à la borne incrémentale : une ligne peut
// apparaître après coup avec une date de réception antérieure ; la reprendre
// est sans risque, les `_id` étant déterministes.
const recouvrementJours = 7

// Departements sont les codes départementaux tels que l'ADEME les renseigne
// dans `code_departement_ban` : métropole (Corse en 2A/2B) puis DOM.
var Departements = func() []string {
	var codes []string
	for n := 1; n <= 95; n++ {
		if n == 20 {
			codes = append(codes, "2A", "2B")
			continue
		}
		codes = append(codes, fmt.Sprintf("%02d", n))
	}
	return append(codes, "971", "972", "973", "974", "976")
}()

// DerniereReception retourne la date de réception la plus récente déjà
// indexée, ou nil si l'index est vide ou illisible : mieux vaut une moisson
// complète, coûteuse mais correcte, qu'un trou silencieux dans les données.
func DerniereReception(ctx context.Context, client *elasticsearch.Client, indexAlias string) *time.Time {
	hits, err := esx.SearchHits[map[string]any](ctx, client, indexAlias, map[string]any{
		"size":    1,
		"_source": []string{"date_reception"},
		"sort":    []any{map[string]any{"date_reception": "desc"}},
		"query":   map[string]any{"exists": map[string]any{"field": "date_reception"}},
	})
	if err != nil || len(hits) == 0 {
		return nil
	}
	raw := esx.String(hits[0].Source["date_reception"])
	if len(raw) < 10 {
		return nil
	}
	date, err := time.Parse(time.DateOnly, raw[:10])
	if err != nil {
		return nil
	}
	return &date
}

// Ingest indexe les diagnostics DPE depuis `sourceURL`, en incrémental par défaut.
//
// Le dataset dépasse 15 millions de lignes : le moissonner intégralement prend
// des heures. On ne reprend donc que ce qui a été reçu depuis la dernière
// ingestion, avec une marge de recouvrement. `complet` force la moisson
// totale, découpée par département — nécessaire après un élargissement des
// champs conservés, les documents déjà indexés n'étant jamais rétro-complétés.
func Ingest(ctx context.Context, client *elasticsearch.Client, indexAlias, sourceURL string, complet bool) (success, failed int, err error) {
	var depuis *time.Time
	if !complet {
		if derniere := DerniereReception(ctx, client, indexAlias); derniere != nil {
			borne := derniere.AddDate(0, 0, -recouvrementJours)
			depuis = &borne
		}
	}
	mode := "complet"
	if depuis != nil {
		mode = depuis.Format(time.DateOnly)
	}
	slog.Info("dpe_ingestion_started", "depuis", mode)

	// Une reprise incrémentale ne rapporte que quelques milliers de lignes et
	// n'a rien à gagner au découpage par département.
	tranches := []string{""}
	if complet {
		tranches = slices.Clone(Departements)
	}
	for _, departement := range tranches {
		var fetchErr error
		documents := func(yield func(es.Document) bool) {
			for rows := range FetchPages(ctx, sourceURL, depuis, departement, &fetchErr) {
				for _, row := range rows {
					if document := Document(row); document != nil && !yield(document) {
						return
					}
				}
			}
		}
		s, f, err := es.BulkIndex(ctx, client, indexAlias, documents)
		success += s
		failed += f
		if err == nil {
			err = fetchErr
		}
		if err != nil {
			return success, failed, err
		}
		if departement != "" {
			slog.Info("dpe_departement_ingere", "departement", departement, "success", s, "errors", f)
		}
	}
	slog.Info("dpe_ingestion_completed", "success", success, "errors", failed)
	return success, failed, nil
}

// Diagnostic est un DPE tel qu'exposé par l'API de recherche.
type Diagnostic struct {
	NumeroDPE         string   `json:"numero_dpe"`
	DateEtablissement *string  `json:"date_etablissement"`
	EtiquetteDPE      *string  `json:"etiquette_dpe"`
	EtiquetteGES      *string  `json:"etiquette_ges"`
	Commune           *string  `json:"commune"`
	CodePostal        *string  `json:"code_postal"`
	SurfaceHabitable  *float64 `json:"surface_habitable"`
}

// SearchParams sont les critères d'une recherche de diagnostics.
type SearchParams struct {
	Commune      *string
	CodePostal   *string
	EtiquetteDPE []string
}

// BuildQuery construit la requête d'une recherche de diagnostics.
func BuildQuery(params SearchParams) map[string]any {
	filters := es.BuildFilters(
		es.Filter{Field: "commune", Value: params.Commune},
		es.Filter{Field: "code_postal", Value: params.CodePostal},
		es.Filter{Field: "etiquette_dpe", Value: params.EtiquetteDPE},
	)
	if len(filters) == 0 {
		return map[string]any{"match_all": map[string]any{}}
	}
	return map[string]any{"bool": map[string]any{"filter": filters}}
}

// Search recherche des diagnostics selon `params`, paginés par `search_after`.
func Search(ctx context.Context, client *elasticsearch.Client, index string, params SearchParams, searchAfter []any, size int) (es.Page, error) {
	return es.Paginate(ctx, client, es.PaginateParams{
		Index: index, Query: BuildQuery(params),
		Sort:        []map[string]any{{"date_etablissement": "desc"}, {"_seq_no": "asc"}},
		SearchAfter: searchAfter, Size: size,
	})
}
