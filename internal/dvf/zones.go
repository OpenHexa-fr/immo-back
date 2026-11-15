package dvf

// Pré-agrégation des prix DVF par zone (département, commune, section).
//
// La choroplèthe de la carte affiche un prix médian au m² par zone. Le calculer
// à la volée revenait à relancer, à chaque affichage, une agrégation sur
// l'index DVF entier — pour un résultat qui ne change qu'entre deux
// ingestions. Ce fichier le calcule une fois, à l'issue de l'ingestion, dans
// un index dédié que la carte se contente ensuite de lire.
//
// L'agrégation utilise un `composite` et non un `terms` plafonné : les ~35 000
// communes françaises dépassent largement tout plafond raisonnable, et
// `composite` pagine en restant exact. Les quartiles (p25/p75) sortent de la
// même agrégation `percentiles` que la médiane.

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"

	"github.com/openhexa-fr/immo-back/internal/esx"
)

const compositePageSize = 1000

// Nombre de zones renvoyées à la carte pour un parent donné. Majorant réel :
// le département comptant le plus de communes en compte moins de 900, une
// commune quelques centaines de sections, et la France 101 départements.
const maxZonesPerParent = 5000

type level struct {
	codeField, parentField, labelField string
}

// Niveaux de zones, du plus large au plus fin. `code_section` = les 10
// premiers caractères d'`id_parcelle` (voir ingestion.go).
var levels = map[string]level{
	"departement": {codeField: "code_departement"},
	"commune":     {codeField: "code_commune", parentField: "code_departement", labelField: "commune"},
	"section":     {codeField: "code_section", parentField: "code_commune"},
}

// Niveaux est l'ordre de calcul des zones ; le dernier, de loin le plus long,
// sert de témoin d'un calcul allé à son terme (voir ZonesAreComputed).
var Niveaux = []string{"departement", "commune", "section"}

// NiveauxSerie sont les niveaux dotés d'une série annuelle. Les sections en
// sont exclues à dessein : quelques ventes par section et par an ne donnent
// pas une médiane robuste, et le volume exploserait.
var NiveauxSerie = []string{"departement", "commune"}

// Bornes de plausibilité du prix au m². En deçà, il s'agit de cessions à
// valeur symbolique ou de très grandes parcelles agricoles ; au-delà, aucune
// transaction française n'est crédible (le haut du marché parisien plafonne
// autour de 25 000 €/m²). Sans ces bornes, une section entière peut afficher
// une médiane de 273 209 €/m² (constaté à Lille) ou de 0.
const (
	prixM2Min = 10
	prixM2Max = 50_000
)

// Categories sont les deux marchés agrégés séparément. Le critère reproduit
// exactement celui de PrixM2 : le prix au m² se base sur la surface bâtie dès
// qu'elle est non nulle, sinon sur celle du terrain. Toute évolution de cette
// règle doit être répercutée ici.
var Categories = []string{"bati", "terrain"}

var filtreBati = map[string]any{"range": map[string]any{"surface_reelle_bati": map[string]any{"gt": 0}}}

// Étiquettes considérées comme des passoires énergétiques (seuil
// réglementaire français).
var etiquettesPassoire = []string{"F", "G"}

func aggsPrixParCategorie() map[string]any {
	percentiles := map[string]any{"percentiles": map[string]any{"field": "prix_m2", "percents": []int{25, 50, 75}}}
	return map[string]any{
		"bati": map[string]any{"filter": filtreBati, "aggs": map[string]any{"prix": percentiles}},
		"terrain": map[string]any{
			"filter": map[string]any{"bool": map[string]any{"must_not": []any{filtreBati}}},
			"aggs":   map[string]any{"prix": percentiles},
		},
	}
}

// aggsDPE : part de passoires parmi les mutations dotées d'une étiquette. Ne
// concerne que le bâti, mais partage le parcours composite de l'agrégation de
// prix — aucun aller-retour supplémentaire.
func aggsDPE() map[string]any {
	return map[string]any{
		"filter": map[string]any{"exists": map[string]any{"field": "etiquette_dpe"}},
		"aggs": map[string]any{"passoire": map[string]any{
			"filter": map[string]any{"terms": map[string]any{"etiquette_dpe": etiquettesPassoire}},
		}},
	}
}

// zonesQuery retient les mutations dont le prix au m² est calculable *et* plausible.
func zonesQuery() map[string]any {
	return map[string]any{"bool": map[string]any{"filter": []any{
		map[string]any{"exists": map[string]any{"field": "prix_m2"}},
		map[string]any{"range": map[string]any{"prix_m2": map[string]any{"gte": prixM2Min, "lte": prixM2Max}}},
	}}}
}

type percentileBucket struct {
	DocCount int64 `json:"doc_count"`
	Prix     struct {
		Values map[string]any `json:"values"`
	} `json:"prix"`
}

type compositeBucket struct {
	Key      map[string]any   `json:"key"`
	DocCount int64            `json:"doc_count"`
	Bati     percentileBucket `json:"bati"`
	Terrain  percentileBucket `json:"terrain"`
	DPE      struct {
		DocCount int64 `json:"doc_count"`
		Passoire struct {
			DocCount int64 `json:"doc_count"`
		} `json:"passoire"`
	} `json:"dpe"`
	Label esx.Hits[map[string]any] `json:"label"`
}

// compositeBuckets itère tous les buckets d'une agrégation `composite`, page
// par page via `after_key`. Une erreur interrompt l'itération et est retournée
// par `errp`.
func compositeBuckets(
	ctx context.Context, client *elasticsearch.Client, index string,
	sources []any, aggs map[string]any, errp *error,
) func(yield func(compositeBucket) bool) {
	return func(yield func(compositeBucket) bool) {
		var after map[string]any
		for {
			composite := map[string]any{"sources": sources, "size": compositePageSize}
			if after != nil {
				composite["after"] = after
			}
			var response struct {
				Aggregations struct {
					Zones struct {
						AfterKey map[string]any    `json:"after_key"`
						Buckets  []compositeBucket `json:"buckets"`
					} `json:"zones"`
				} `json:"aggregations"`
			}
			err := esx.Search(ctx, client, index, map[string]any{
				"query": zonesQuery(),
				"size":  0,
				"aggs":  map[string]any{"zones": map[string]any{"composite": composite, "aggs": aggs}},
			}, &response)
			if err != nil {
				*errp = err
				return
			}
			zones := response.Aggregations.Zones
			if len(zones.Buckets) == 0 {
				return
			}
			for _, bucket := range zones.Buckets {
				if !yield(bucket) {
					return
				}
			}
			if zones.AfterKey == nil {
				return
			}
			after = zones.AfterKey
		}
	}
}

func roundPtr(value *float64, decimals int) *float64 {
	if value == nil {
		return nil
	}
	rounded := round(*value, decimals)
	return &rounded
}

// prixParCategorie aplatit les agrégats d'un bucket en champs suffixés par
// catégorie, et indique si au moins une médiane existe.
func prixParCategorie(bucket compositeBucket) (map[string]any, bool) {
	fields := map[string]any{}
	hasMedian := false
	for categorie, sub := range map[string]percentileBucket{"bati": bucket.Bati, "terrain": bucket.Terrain} {
		median := roundPtr(esx.Float(sub.Prix.Values["50.0"]), 2)
		hasMedian = hasMedian || median != nil
		fields["prix_m2_median_"+categorie] = median
		fields["prix_m2_p25_"+categorie] = roundPtr(esx.Float(sub.Prix.Values["25.0"]), 2)
		fields["prix_m2_p75_"+categorie] = roundPtr(esx.Float(sub.Prix.Values["75.0"]), 2)
		fields["nb_mutations_"+categorie] = sub.DocCount
	}
	return fields, hasMedian
}

func bucketLabel(bucket compositeBucket, lvl level, code string) string {
	if lvl.labelField != "" && len(bucket.Label.Hits.Hits) > 0 {
		if label := esx.String(bucket.Label.Hits.Hits[0].Source[lvl.labelField]); label != "" {
			return label
		}
	}
	return code
}

func levelSources(lvl level) []any {
	var sources []any
	if lvl.parentField != "" {
		sources = append(sources, map[string]any{"parent": map[string]any{"terms": map[string]any{"field": lvl.parentField}}})
	}
	return append(sources, map[string]any{"code": map[string]any{"terms": map[string]any{"field": lvl.codeField}}})
}

func parentOf(bucket compositeBucket) any {
	if parent, ok := bucket.Key["parent"]; ok {
		return esx.String(parent)
	}
	return nil
}

// zoneDocuments produit les documents d'agrégat « tous millésimes confondus » d'un niveau.
func zoneDocuments(
	ctx context.Context, client *elasticsearch.Client, dvfIndex, niveau, calculeLe string, errp *error,
) func(yield func(es.Document) bool) {
	return func(yield func(es.Document) bool) {
		lvl := levels[niveau]
		aggs := aggsPrixParCategorie()
		aggs["dpe"] = aggsDPE()
		if lvl.labelField != "" {
			// Le nom de la commune n'est pas une clé d'agrégation (seul son code
			// INSEE en est une) : on le lit sur un document du bucket.
			aggs["label"] = map[string]any{"top_hits": map[string]any{"size": 1, "_source": []string{lvl.labelField}}}
		}
		for bucket := range compositeBuckets(ctx, client, dvfIndex, levelSources(lvl), aggs, errp) {
			prix, hasMedian := prixParCategorie(bucket)
			var partPassoire *float64
			if bucket.DPE.DocCount > 0 {
				part := round(float64(bucket.DPE.Passoire.DocCount)/float64(bucket.DPE.DocCount)*100, 1)
				partPassoire = &part
			}
			// Une zone sans médiane ni statistique DPE n'a rien à colorer, quel
			// que soit le mode d'affichage choisi.
			if !hasMedian && bucket.DPE.DocCount == 0 {
				continue
			}
			code := esx.String(bucket.Key["code"])
			document := es.Document{
				// Déterministe : un recalcul écrase la zone au lieu de la dupliquer.
				"_id":                   niveau + ":" + code,
				"niveau":                niveau,
				"code":                  code,
				"code_parent":           parentOf(bucket),
				"label":                 bucketLabel(bucket, lvl, code),
				"nb_mutations":          bucket.DocCount,
				"calcule_le":            calculeLe,
				"dpe_part_passoire":     partPassoire,
				"dpe_nb_avec_etiquette": bucket.DPE.DocCount,
			}
			for k, v := range prix {
				document[k] = v
			}
			if !yield(document) {
				return
			}
		}
	}
}

// serieDocuments produit les documents d'agrégat par zone **et par
// millésime** : la carte affiche une médiane tous millésimes confondus, qui
// écrase le mouvement du marché ; la série le rend visible.
func serieDocuments(
	ctx context.Context, client *elasticsearch.Client, dvfIndex, niveau, calculeLe string, errp *error,
) func(yield func(es.Document) bool) {
	return func(yield func(es.Document) bool) {
		lvl := levels[niveau]
		sources := append(levelSources(lvl), map[string]any{"annee": map[string]any{"date_histogram": map[string]any{
			"field": "date_mutation", "calendar_interval": "year", "format": "yyyy",
		}}})
		aggs := aggsPrixParCategorie()
		if lvl.labelField != "" {
			aggs["label"] = map[string]any{"top_hits": map[string]any{"size": 1, "_source": []string{lvl.labelField}}}
		}
		for bucket := range compositeBuckets(ctx, client, dvfIndex, sources, aggs, errp) {
			prix, hasMedian := prixParCategorie(bucket)
			if !hasMedian {
				continue
			}
			code := esx.String(bucket.Key["code"])
			rawYear := esx.String(bucket.Key["annee"])
			if len(rawYear) > 4 {
				rawYear = rawYear[:4]
			}
			annee, err := strconv.Atoi(rawYear)
			if err != nil {
				*errp = fmt.Errorf("dvf: millésime illisible %q : %w", esx.String(bucket.Key["annee"]), err)
				return
			}
			document := es.Document{
				"_id":          fmt.Sprintf("%s:%s:%d", niveau, code, annee),
				"niveau":       niveau,
				"code":         code,
				"annee":        annee,
				"code_parent":  parentOf(bucket),
				"label":        bucketLabel(bucket, lvl, code),
				"nb_mutations": bucket.DocCount,
				"calcule_le":   calculeLe,
			}
			for k, v := range prix {
				document[k] = v
			}
			if !yield(document) {
				return
			}
		}
	}
}

// CalculeLe est l'horodatage d'un calcul de zones (UTC, ISO-8601).
func CalculeLe(now time.Time) string {
	return now.UTC().Format("2006-01-02T15:04:05.000000-07:00")
}

// ComputeZones recalcule les agrégats de prix par zone à partir de l'index DVF.
//
// Les documents portant un `_id` déterministe, un recalcul met à jour les
// zones existantes sans suppression préalable : la carte reste servie pendant
// tout le calcul. Le flux de documents est indexé au fil de l'eau : la mémoire
// ne dépend pas du nombre de zones (des centaines de milliers de sections).
func ComputeZones(ctx context.Context, client *elasticsearch.Client, dvfIndex, zonesAlias string) (success, failed int, err error) {
	calculeLe := CalculeLe(time.Now())
	run := func(kind, niveau string, documents func(errp *error) func(func(es.Document) bool)) error {
		var iterErr error
		s, f, err := es.BulkIndex(ctx, client, zonesAlias, documents(&iterErr))
		success += s
		failed += f
		if err == nil {
			err = iterErr
		}
		if err != nil {
			return fmt.Errorf("dvf: calcul des zones %s/%s : %w", kind, niveau, err)
		}
		slog.Info("dvf_"+kind+"_level_computed", "niveau", niveau, "success", s, "errors", f)
		return nil
	}

	for _, niveau := range Niveaux {
		if err := run("zones", niveau, func(errp *error) func(func(es.Document) bool) {
			return zoneDocuments(ctx, client, dvfIndex, niveau, calculeLe, errp)
		}); err != nil {
			return success, failed, err
		}
	}
	for _, niveau := range NiveauxSerie {
		if err := run("serie", niveau, func(errp *error) func(func(es.Document) bool) {
			return serieDocuments(ctx, client, dvfIndex, niveau, calculeLe, errp)
		}); err != nil {
			return success, failed, err
		}
	}
	slog.Info("dvf_zones_computed", "success", success, "errors", failed)
	return success, failed, nil
}

// ZonesAreComputed indique si le calcul des zones est allé à son terme : on
// teste la présence du dernier niveau calculé (les sections, de loin le plus
// long, donc celui qu'un redémarrage interrompt).
func ZonesAreComputed(ctx context.Context, client *elasticsearch.Client, zonesIndex string) (bool, error) {
	n, err := esx.CountQuery(ctx, client, zonesIndex,
		map[string]any{"term": map[string]any{"niveau": Niveaux[len(Niveaux)-1]}})
	return n > 0, err
}

// LatestCalculeLe retourne la date du dernier calcul des zones, ou nil si
// elles n'ont jamais été calculées.
//
// Sert de **version des données DVF** : toutes les zones d'un même calcul
// portent le même horodatage, et ce calcul suit chaque ingestion. Deux
// réponses produites sous la même version sont donc identiques — exactement
// ce qu'un ETag doit garantir.
func LatestCalculeLe(ctx context.Context, client *elasticsearch.Client, zonesIndex string) (*string, error) {
	hits, err := esx.SearchHits[map[string]any](ctx, client, zonesIndex, map[string]any{
		"size":    1,
		"_source": []string{"calcule_le"},
		"sort":    []any{map[string]any{"calcule_le": "desc"}},
	})
	if err != nil || len(hits) == 0 {
		return nil, err
	}
	if value := esx.String(hits[0].Source["calcule_le"]); value != "" {
		return &value, nil
	}
	return nil, nil
}

// Zone est un document d'agrégat tel que lu dans l'index des zones.
type Zone struct {
	Niveau     string  `json:"niveau"`
	Code       string  `json:"code"`
	Annee      *int    `json:"annee"`
	CodeParent *string `json:"code_parent"`
	Label      *string `json:"label"`

	PrixM2MedianBati    *float64 `json:"prix_m2_median_bati"`
	PrixM2P25Bati       *float64 `json:"prix_m2_p25_bati"`
	PrixM2P75Bati       *float64 `json:"prix_m2_p75_bati"`
	NbMutationsBati     *int64   `json:"nb_mutations_bati"`
	PrixM2MedianTerrain *float64 `json:"prix_m2_median_terrain"`
	PrixM2P25Terrain    *float64 `json:"prix_m2_p25_terrain"`
	PrixM2P75Terrain    *float64 `json:"prix_m2_p75_terrain"`
	NbMutationsTerrain  *int64   `json:"nb_mutations_terrain"`

	NbMutations        int64    `json:"nb_mutations"`
	DPEPartPassoire    *float64 `json:"dpe_part_passoire"`
	DPENbAvecEtiquette *int64   `json:"dpe_nb_avec_etiquette"`
	CalculeLe          *string  `json:"calcule_le"`
}

func searchZones(ctx context.Context, client *elasticsearch.Client, index string, request map[string]any) ([]Zone, error) {
	hits, err := esx.SearchHits[Zone](ctx, client, index, request)
	if err != nil {
		return nil, err
	}
	zones := make([]Zone, 0, len(hits))
	for _, hit := range hits {
		zones = append(zones, hit.Source)
	}
	return zones, nil
}

// FetchZones lit les zones pré-agrégées d'un niveau, restreintes à leur zone parente.
func FetchZones(ctx context.Context, client *elasticsearch.Client, zonesIndex, niveau string, codeParent *string) ([]Zone, error) {
	filters := []any{map[string]any{"term": map[string]any{"niveau": niveau}}}
	if codeParent != nil {
		filters = append(filters, map[string]any{"term": map[string]any{"code_parent": *codeParent}})
	}
	return searchZones(ctx, client, zonesIndex, map[string]any{
		// Les points de série temporelle partagent l'index : les exclure évite de
		// renvoyer chaque zone une fois par millésime à la choroplèthe.
		"query": map[string]any{"bool": map[string]any{
			"filter":   filters,
			"must_not": []any{map[string]any{"exists": map[string]any{"field": "annee"}}},
		}},
		"size": maxZonesPerParent,
		"sort": []any{map[string]any{"code": "asc"}},
	})
}

// FetchZone lit l'agrégat global (tous millésimes confondus) d'une zone, ou nil si elle est inconnue.
func FetchZone(ctx context.Context, client *elasticsearch.Client, zonesIndex, niveau, code string) (*Zone, error) {
	zones, err := searchZones(ctx, client, zonesIndex, map[string]any{
		"query": map[string]any{"bool": map[string]any{
			"filter": []any{
				map[string]any{"term": map[string]any{"niveau": niveau}},
				map[string]any{"term": map[string]any{"code": code}},
			},
			"must_not": []any{map[string]any{"exists": map[string]any{"field": "annee"}}},
		}},
		"size": 1,
	})
	if err != nil || len(zones) == 0 {
		return nil, err
	}
	return &zones[0], nil
}

// FetchSerie lit la série annuelle d'une zone, du plus ancien au plus récent millésime.
func FetchSerie(ctx context.Context, client *elasticsearch.Client, zonesIndex, niveau, code string) ([]Zone, error) {
	return searchZones(ctx, client, zonesIndex, map[string]any{
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"niveau": niveau}},
			map[string]any{"term": map[string]any{"code": code}},
			map[string]any{"exists": map[string]any{"field": "annee"}},
		}}},
		"size": 100,
		"sort": []any{map[string]any{"annee": "asc"}},
	})
}

// Bucket est l'agrégat d'une zone pour une catégorie, tel qu'exposé à la carte.
type Bucket struct {
	Code         string   `json:"code"`
	Label        string   `json:"label"`
	PrixM2Median float64  `json:"prix_m2_median"`
	PrixM2P25    *float64 `json:"prix_m2_p25"`
	PrixM2P75    *float64 `json:"prix_m2_p75"`
	NbMutations  int64    `json:"nb_mutations"`
	// Statistique DPE : indépendante de la catégorie (un DPE ne concerne que le
	// bâti) ; absente du repli à la volée.
	DPEPartPassoire    *float64 `json:"dpe_part_passoire"`
	DPENbAvecEtiquette *int64   `json:"dpe_nb_avec_etiquette"`

	CalculeLe *string `json:"-"`
	Annee     *int    `json:"-"`
}

// Aplatir ramène une zone à la forme exposée pour la catégorie demandée
// ("bati" ou "terrain"), ou nil si la catégorie n'a pas de médiane sur cette
// zone — une commune peut n'avoir connu que des ventes de terrain.
func Aplatir(zone Zone, categorie string) *Bucket {
	median, p25, p75, nb := zone.PrixM2MedianBati, zone.PrixM2P25Bati, zone.PrixM2P75Bati, zone.NbMutationsBati
	if categorie == "terrain" {
		median, p25, p75, nb = zone.PrixM2MedianTerrain, zone.PrixM2P25Terrain, zone.PrixM2P75Terrain, zone.NbMutationsTerrain
	}
	if median == nil {
		return nil
	}
	label := zone.Code
	if zone.Label != nil {
		label = *zone.Label
	}
	var count int64
	if nb != nil {
		count = *nb
	}
	return &Bucket{
		Code: zone.Code, Label: label, PrixM2Median: *median, PrixM2P25: p25, PrixM2P75: p75,
		NbMutations: count, DPEPartPassoire: zone.DPEPartPassoire, DPENbAvecEtiquette: zone.DPENbAvecEtiquette,
		CalculeLe: zone.CalculeLe, Annee: zone.Annee,
	}
}

// VariationPct retourne la variation relative en pourcentage, arrondie au
// dixième ; nil si l'une des bornes manque ou si la référence est nulle (une
// variation depuis zéro n'a pas de sens).
func VariationPct(depuis, vers *float64) *float64 {
	if depuis == nil || vers == nil || *depuis == 0 {
		return nil
	}
	variation := round((*vers-*depuis) / *depuis * 100, 1)
	return &variation
}
