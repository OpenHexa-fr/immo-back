package dvf

import (
	"context"
	"strconv"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"

	"github.com/openhexa-fr/immo-back/internal/esx"
)

func rangeClause(field string, min, max any) map[string]any {
	clause := map[string]any{}
	if min != nil {
		clause["gte"] = min
	}
	if max != nil {
		clause["lte"] = max
	}
	if len(clause) == 0 {
		return nil
	}
	return map[string]any{"range": map[string]any{field: clause}}
}

// orNil évite qu'un pointeur nil typé ne passe pour une borne renseignée.
func orNil[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// BuildQuery construit la requête Elasticsearch d'une recherche de transactions.
func BuildQuery(params SearchParams) map[string]any {
	filters := es.BuildFilters(
		es.Filter{Field: "commune", Value: params.Commune},
		es.Filter{Field: "code_postal", Value: params.CodePostal},
		es.Filter{Field: "id_parcelle", Value: params.IDParcelle},
		es.Filter{Field: "type_local", Value: params.TypeLocal},
		es.Filter{Field: "etiquette_dpe", Value: params.EtiquetteDPE},
	)
	// Certaines mutations n'ont pas de valeur foncière renseignée (cessions sans
	// prix) : elles sont exclues plutôt que servies sans le champ principal.
	filters = append(filters, map[string]any{"exists": map[string]any{"field": "valeur_fonciere"}})

	ranges := []struct {
		field    string
		min, max any
	}{
		{"valeur_fonciere", orNil(params.ValeurFonciereMin), orNil(params.ValeurFonciereMax)},
		{"surface_reelle_bati", orNil(params.SurfaceMin), orNil(params.SurfaceMax)},
		{"surface_terrain", orNil(params.SurfaceTerrainMin), orNil(params.SurfaceTerrainMax)},
		{"nombre_pieces_principales", orNil(params.PiecesMin), orNil(params.PiecesMax)},
		{"prix_m2", orNil(params.PrixM2Min), orNil(params.PrixM2Max)},
		{"date_mutation", orNil(params.DateMutationMin), orNil(params.DateMutationMax)},
	}
	for _, r := range ranges {
		if clause := rangeClause(r.field, r.min, r.max); clause != nil {
			filters = append(filters, clause)
		}
	}

	// Une emprise rectangulaire décrit exactement ce qu'affiche une carte : la
	// requérir en `geo_distance` imposerait un cercle circonscrit au viewport,
	// soit ~57 % de surface rapatriée hors écran. `geo_distance` reste le bon
	// outil pour un rayon réellement circulaire ("autour de moi").
	switch {
	case params.BBox != nil:
		filters = append(filters, map[string]any{"geo_bounding_box": map[string]any{
			"location": map[string]any{
				"top_left":     map[string]any{"lat": params.BBox.MaxLat, "lon": params.BBox.MinLon},
				"bottom_right": map[string]any{"lat": params.BBox.MinLat, "lon": params.BBox.MaxLon},
			},
		}})
	case params.Lat != nil && params.Lon != nil:
		filters = append(filters, map[string]any{"geo_distance": map[string]any{
			"distance": strconv.FormatFloat(params.RadiusKm, 'f', -1, 64) + "km",
			"location": map[string]any{"lat": *params.Lat, "lon": *params.Lon},
		}})
	}
	return map[string]any{"bool": map[string]any{"filter": filters}}
}

// BuildSort construit le tri d'une recherche de transactions, avec `_seq_no`
// comme tie-breaker stable pour `search_after`.
func BuildSort(params SearchParams) []map[string]any {
	tieBreaker := map[string]any{"_seq_no": "asc"}
	switch {
	case params.Tri == SortPrix:
		return []map[string]any{{"valeur_fonciere": "asc"}, tieBreaker}
	case params.Tri == SortSurface:
		return []map[string]any{{"surface_reelle_bati": "desc"}, tieBreaker}
	case params.Tri == SortDistance && params.Lat != nil && params.Lon != nil:
		return []map[string]any{{"_geo_distance": map[string]any{
			"location": map[string]any{"lat": *params.Lat, "lon": *params.Lon},
			"order":    "asc",
			"unit":     "km",
		}}, tieBreaker}
	}
	return []map[string]any{{"date_mutation": "desc"}, tieBreaker}
}

// CarteSourceFields sont les champs strictement nécessaires pour placer une
// vente sur la carte (`champs=carte`). `date_mutation` et `commune` s'y
// ajoutent parce que l'API les expose toujours.
var CarteSourceFields = []string{
	"id_mutation", "id_parcelle", "location", "valeur_fonciere", "prix_m2", "date_mutation", "commune",
}

// Search recherche des transactions selon `params`, paginées par `search_after`.
func Search(
	ctx context.Context, client *elasticsearch.Client, index string, params SearchParams,
	searchAfter []any, size int, source []string,
) (es.Page, error) {
	return es.Paginate(ctx, client, es.PaginateParams{
		Index: index, Query: BuildQuery(params), Sort: BuildSort(params),
		SearchAfter: searchAfter, Size: size, Source: source,
	})
}

// GetByMutation retourne tous les lots associés à une mutation : une mutation
// peut porter sur plusieurs lots, donc plusieurs documents partagent le même
// `id_mutation` sans partager le même `_id`.
func GetByMutation(ctx context.Context, client *elasticsearch.Client, index, idMutation string) ([]Transaction, error) {
	hits, err := esx.SearchHits[Transaction](ctx, client, index, map[string]any{
		"query": map[string]any{"term": map[string]any{"id_mutation": idMutation}},
		"size":  50,
	})
	if err != nil {
		return nil, err
	}
	transactions := make([]Transaction, 0, len(hits))
	for _, hit := range hits {
		transactions = append(transactions, hit.Source)
	}
	return transactions, nil
}

// Une mutation porte rarement plus de quelques lots ; la borne évite qu'une
// mutation pathologique (lotissement entier) ne fasse exploser la réponse.
const maxLotsPerMutation = 50

// ParcelleMutations retourne les mutations d'une parcelle, chacune portant ses lots.
//
// Le regroupement par mutation est fait par Elasticsearch (`collapse` +
// `inner_hits`) : `size` borne ainsi un nombre de *mutations*, ce qu'affiche
// la fiche parcelle, et non un nombre de lots.
func ParcelleMutations(ctx context.Context, client *elasticsearch.Client, index, idParcelle string, size int) ([]ParcelleMutation, error) {
	var response struct {
		Hits struct {
			Hits []struct {
				Source    Transaction `json:"_source"`
				InnerHits struct {
					Lots esx.Hits[Transaction] `json:"lots"`
				} `json:"inner_hits"`
			} `json:"hits"`
		} `json:"hits"`
	}
	err := esx.Search(ctx, client, index, map[string]any{
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"id_parcelle": idParcelle}},
			map[string]any{"exists": map[string]any{"field": "valeur_fonciere"}},
		}}},
		"collapse": map[string]any{
			"field":      "id_mutation",
			"inner_hits": map[string]any{"name": "lots", "size": maxLotsPerMutation},
		},
		"sort": []any{map[string]any{"date_mutation": "desc"}},
		"size": size,
	}, &response)
	if err != nil {
		return nil, err
	}

	mutations := make([]ParcelleMutation, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		lots := make([]Transaction, 0, len(hit.InnerHits.Lots.Hits.Hits))
		for _, lot := range hit.InnerHits.Lots.Hits.Hits {
			lots = append(lots, lot.Source)
		}
		mutations = append(mutations, ParcelleMutation{
			IDMutation: hit.Source.IDMutation, DateMutation: hit.Source.DateMutation, Lots: lots,
		})
	}
	return mutations, nil
}

// Champ de regroupement par niveau de la carte des prix.
var niveauFields = map[string]string{
	"departement": "code_departement",
	"commune":     "code_commune",
	"section":     "code_section",
}

const prixCarteAggSize = 5000

// PrixCarteQuery restreint l'agrégation à la volée aux mutations dotées d'un
// prix au m², dans la zone parente demandée.
func PrixCarteQuery(codeDepartement, codeCommune *string) map[string]any {
	filters := es.BuildFilters(
		es.Filter{Field: "code_departement", Value: codeDepartement},
		es.Filter{Field: "code_commune", Value: codeCommune},
	)
	filters = append(filters, map[string]any{"exists": map[string]any{"field": "prix_m2"}})
	return map[string]any{"bool": map[string]any{"filter": filters}}
}

// AggregatePrixCarte agrège à la volée le prix médian au m² par zone. C'est le
// repli de la choroplèthe tant que les zones n'ont jamais été pré-agrégées :
// il ne distingue ni bâti/terrain ni quartiles.
func AggregatePrixCarte(
	ctx context.Context, client *elasticsearch.Client, index, niveau string, codeDepartement, codeCommune *string,
) ([]Bucket, error) {
	zoneAgg := map[string]any{
		"terms": map[string]any{"field": niveauFields[niveau], "size": prixCarteAggSize},
		"aggs": map[string]any{
			"prix_median": map[string]any{"percentiles": map[string]any{"field": "prix_m2", "percents": []int{50}}},
		},
	}
	if niveau == "commune" {
		// Nom de la commune pour l'affichage : la facette ne porte que sur son code INSEE.
		zoneAgg["aggs"].(map[string]any)["label"] = map[string]any{
			"top_hits": map[string]any{"size": 1, "_source": []string{"commune"}},
		}
	}

	var response struct {
		Aggregations struct {
			ByZone struct {
				Buckets []struct {
					Key        any   `json:"key"`
					DocCount   int64 `json:"doc_count"`
					PrixMedian struct {
						Values map[string]any `json:"values"`
					} `json:"prix_median"`
					Label esx.Hits[map[string]any] `json:"label"`
				} `json:"buckets"`
			} `json:"by_zone"`
		} `json:"aggregations"`
	}
	err := esx.Search(ctx, client, index, map[string]any{
		"query": PrixCarteQuery(codeDepartement, codeCommune),
		"aggs":  map[string]any{"by_zone": zoneAgg},
		"size":  0,
	}, &response)
	if err != nil {
		return nil, err
	}

	buckets := []Bucket{}
	for _, bucket := range response.Aggregations.ByZone.Buckets {
		median := esx.Float(bucket.PrixMedian.Values["50.0"])
		if median == nil {
			continue
		}
		code := esx.String(bucket.Key)
		label := code
		if niveau == "commune" && len(bucket.Label.Hits.Hits) > 0 {
			if commune := esx.String(bucket.Label.Hits.Hits[0].Source["commune"]); commune != "" {
				label = commune
			}
		}
		rounded := round(*median, 2)
		buckets = append(buckets, Bucket{
			Code: code, Label: label, PrixM2Median: rounded, NbMutations: bucket.DocCount,
		})
	}
	return buckets, nil
}
