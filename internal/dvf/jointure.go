package dvf

// Rapprochement des mutations DVF avec les diagnostics DPE.
//
// La jointure se fait sur l'**identifiant BAN**, que les deux sources
// permettent d'obtenir sans géocodage : l'ADEME le publie tel quel
// (`identifiant_ban`), et DVF fournit de quoi le reconstruire (voir
// IdentifiantBAN).
//
// Couverture attendue, mesurée sur des extraits réels : la clé est
// reconstructible pour ~97 % des mutations de logements, mais seulement ~65 %
// des DPE portent un `identifiant_ban` exploitable — l'ADEME ne géocode pas
// tout. Une mutation de logement sur trois restera donc sans étiquette, ce que
// l'interface doit présenter comme « non rapproché » et non comme « pas de DPE ».
//
// Une adresse porte souvent plusieurs diagnostics : on retient par priorité le
// plus récent antérieur à la mutation (fiable, décrit l'état au moment de la
// vente). À défaut — fréquent sur les millésimes 2021-2022, avant la
// généralisation du DPE — on accepte un DPE postérieur proche (fenêtre de 18
// mois), marqué explicitement (`etiquette_dpe_apres_vente`) pour que
// l'interface affiche la réserve.

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/immo-back/internal/esx"
)

const (
	lotMutations  = 1000
	pitKeepAlive  = "10m"
	maxDPEParLot  = 10_000
	scoreBANMin   = 0.5 // en deçà, le géocodage BAN de l'ADEME est trop incertain (score médian observé : 0,65)
	fenetreJours  = 548 // 18 mois : une rénovation majeure entre-temps reste l'exception
	maxAdresses   = 200
	maxHistorique = 200
)

// Diagnostic est un DPE candidat au rapprochement.
type Diagnostic struct {
	IdentifiantBAN    string  `json:"identifiant_ban"`
	EtiquetteDPE      *string `json:"etiquette_dpe"`
	DateEtablissement *string `json:"date_etablissement"`
}

func parseDate(raw *string) (time.Time, bool) {
	if raw == nil || len(*raw) < 10 {
		return time.Time{}, false
	}
	date, err := time.Parse(time.DateOnly, (*raw)[:10])
	return date, err == nil
}

// MeilleurDPE retourne l'étiquette retenue pour une mutation datée du
// `dateMutation`, et true si elle vient d'un DPE postérieur à la vente.
//
// Priorité au diagnostic antérieur (ou du jour même) le plus récent ; à
// défaut, le diagnostic postérieur le plus proche, dans la fenêtre de
// tolérance. `ok` vaut false si aucun diagnostic ne convient.
func MeilleurDPE(diagnostics []Diagnostic, dateMutation string) (etiquette string, posterieur, ok bool) {
	dateVente, valid := parseDate(&dateMutation)
	if !valid {
		return "", false, false
	}
	var anterieur, posterieurProche *Diagnostic
	var dateAnterieur, datePosterieur time.Time
	for i := range diagnostics {
		dateDPE, valid := parseDate(diagnostics[i].DateEtablissement)
		if !valid {
			continue
		}
		switch {
		case !dateDPE.After(dateVente):
			if anterieur == nil || dateDPE.After(dateAnterieur) {
				anterieur, dateAnterieur = &diagnostics[i], dateDPE
			}
		case dateDPE.Sub(dateVente) <= fenetreJours*24*time.Hour:
			if posterieurProche == nil || dateDPE.Before(datePosterieur) {
				posterieurProche, datePosterieur = &diagnostics[i], dateDPE
			}
		}
	}
	retenu, posterieur := anterieur, false
	if retenu == nil {
		retenu, posterieur = posterieurProche, true
	}
	if retenu == nil || retenu.EtiquetteDPE == nil || *retenu.EtiquetteDPE == "" {
		return "", false, false
	}
	return *retenu.EtiquetteDPE, posterieur, true
}

func dpeParIdentifiant(ctx context.Context, client *elasticsearch.Client, dpeIndex string, identifiants []string) (map[string][]Diagnostic, error) {
	hits, err := esx.SearchHits[Diagnostic](ctx, client, dpeIndex, map[string]any{
		"size":    maxDPEParLot,
		"_source": []string{"identifiant_ban", "etiquette_dpe", "date_etablissement"},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"terms": map[string]any{"identifiant_ban": identifiants}},
			map[string]any{"exists": map[string]any{"field": "etiquette_dpe"}},
			map[string]any{"range": map[string]any{"score_ban": map[string]any{"gte": scoreBANMin}}},
		}}},
	})
	if err != nil {
		return nil, err
	}
	byID := map[string][]Diagnostic{}
	for _, hit := range hits {
		byID[hit.Source.IdentifiantBAN] = append(byID[hit.Source.IdentifiantBAN], hit.Source)
	}
	return byID, nil
}

// JoindreDPE renseigne `etiquette_dpe` sur les mutations DVF rapprochables.
//
// Retourne (rapprochées, erreurs d'écriture). Les mutations examinées sans
// correspondance ne sont pas des erreurs : c'est le cas normal d'un bien sans
// diagnostic connu à son adresse.
//
// Le parcours s'appuie sur un **point-in-time** : sans lui, `search_after`
// dérive, car la jointure écrit dans l'index qu'elle parcourt (chaque mise à
// jour réinsère le document) et l'ordre se réorganise sous le curseur —
// constaté en production, le parcours s'arrêtait après 7,8 M de mutations sur
// 12,5 M éligibles.
func JoindreDPE(ctx context.Context, client *elasticsearch.Client, dvfIndex, dpeIndex string) (rapproches, erreurs int, err error) {
	pitID, err := esx.OpenPointInTime(ctx, client, dvfIndex, pitKeepAlive)
	if err != nil {
		return 0, 0, err
	}
	defer func() { esx.ClosePointInTime(context.WithoutCancel(ctx), client, pitID) }()

	type mutation struct {
		IdentifiantBAN string `json:"identifiant_ban"`
		DateMutation   string `json:"date_mutation"`
	}
	var examines int
	var searchAfter []any
	for {
		request := map[string]any{
			"size":    lotMutations,
			"_source": []string{"identifiant_ban", "date_mutation"},
			"query": map[string]any{"bool": map[string]any{
				"filter":   []any{map[string]any{"exists": map[string]any{"field": "identifiant_ban"}}},
				"must_not": []any{map[string]any{"exists": map[string]any{"field": "etiquette_dpe"}}},
			}},
			// `_shard_doc` n'existe qu'avec un point-in-time, et c'est le seul tri
			// qui garantisse un ordre total stable.
			"sort": []any{map[string]any{"_shard_doc": "asc"}},
			"pit":  map[string]any{"id": pitID, "keep_alive": pitKeepAlive},
		}
		if searchAfter != nil {
			request["search_after"] = searchAfter
		}
		var response struct {
			PitID string `json:"pit_id"`
			Hits  struct {
				Hits []esx.Hit[mutation] `json:"hits"`
			} `json:"hits"`
		}
		if err := esx.Search(ctx, client, "", request, &response); err != nil {
			return rapproches, erreurs, err
		}
		hits := response.Hits.Hits
		if len(hits) == 0 {
			break
		}
		examines += len(hits)
		searchAfter = hits[len(hits)-1].Sort
		if response.PitID != "" {
			pitID = response.PitID
		}

		identifiants := make([]string, 0, len(hits))
		for _, hit := range hits {
			identifiants = append(identifiants, hit.Source.IdentifiantBAN)
		}
		slices.Sort(identifiants)
		identifiants = slices.Compact(identifiants)
		diagnostics, err := dpeParIdentifiant(ctx, client, dpeIndex, identifiants)
		if err != nil {
			return rapproches, erreurs, err
		}

		var updates []esx.Update
		for _, hit := range hits {
			candidats := diagnostics[hit.Source.IdentifiantBAN]
			if len(candidats) == 0 {
				continue
			}
			etiquette, posterieur, ok := MeilleurDPE(candidats, hit.Source.DateMutation)
			if !ok {
				continue
			}
			doc := map[string]any{"etiquette_dpe": etiquette}
			// Uniquement écrit quand pertinent : son absence équivaut à false.
			if posterieur {
				doc["etiquette_dpe_apres_vente"] = true
			}
			updates = append(updates, esx.Update{ID: hit.ID, Doc: doc})
		}
		if len(updates) > 0 {
			success, failed, err := esx.BulkUpdate(ctx, client, dvfIndex, slices.Values(updates))
			rapproches += success
			erreurs += failed
			if err != nil {
				return rapproches, erreurs, err
			}
		}
	}

	slog.Info("dvf_dpe_jointure_terminee", "examines", examines, "rapproches", rapproches, "erreurs_bulk", erreurs)
	return rapproches, erreurs, nil
}

// HistoriqueDPEParcelle retourne tous les diagnostics connus aux adresses
// d'une parcelle, du plus récent au plus ancien, sans filtre de date ni de score.
//
// Une parcelle n'a pas d'identifiant BAN propre : seules les mutations qui la
// composent en portent un, potentiellement différent d'un lot à l'autre. On
// rassemble donc d'abord tous les identifiants vus sur cette parcelle.
func HistoriqueDPEParcelle(ctx context.Context, client *elasticsearch.Client, dvfIndex, dpeIndex, idParcelle string) ([]DPEHistoriqueEntry, error) {
	adresses, err := esx.SearchHits[struct {
		IdentifiantBAN string `json:"identifiant_ban"`
	}](ctx, client, dvfIndex, map[string]any{
		"size":    maxAdresses,
		"_source": []string{"identifiant_ban"},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"id_parcelle": idParcelle}},
			map[string]any{"exists": map[string]any{"field": "identifiant_ban"}},
		}}},
	})
	if err != nil {
		return nil, err
	}
	var identifiants []string
	for _, hit := range adresses {
		identifiants = append(identifiants, hit.Source.IdentifiantBAN)
	}
	slices.Sort(identifiants)
	identifiants = slices.Compact(identifiants)
	if len(identifiants) == 0 {
		return []DPEHistoriqueEntry{}, nil
	}

	hits, err := esx.SearchHits[DPEHistoriqueEntry](ctx, client, dpeIndex, map[string]any{
		"size": maxHistorique,
		"_source": []string{
			"numero_dpe", "date_etablissement", "etiquette_dpe", "etiquette_ges",
			"surface_habitable", "type_batiment", "score_ban",
		},
		"query": map[string]any{"terms": map[string]any{"identifiant_ban": identifiants}},
		"sort":  []any{map[string]any{"date_etablissement": "desc"}},
	})
	if err != nil {
		return nil, err
	}
	diagnostics := make([]DPEHistoriqueEntry, 0, len(hits))
	for _, hit := range hits {
		diagnostics = append(diagnostics, hit.Source)
	}
	return diagnostics, nil
}
