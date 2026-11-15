// Package dvf contient le domaine DVF (Données de Valeurs Foncières) :
// ingestion des fichiers nationaux, recherche, rapprochement avec les DPE et
// pré-agrégation des prix par zone.
package dvf

import "github.com/openhexa-fr/core/es"

// Mapping retourne le mapping Elasticsearch explicite de l'index DVF.
func Mapping() map[string]any {
	return map[string]any{
		"properties": map[string]any{
			"id_mutation":               es.Keyword(),
			"date_mutation":             es.Date(),
			"valeur_fonciere":           es.Float(),
			"surface_reelle_bati":       es.Integer(),
			"surface_terrain":           es.Integer(),
			"nombre_pieces_principales": es.Integer(),
			"type_local":                es.Keyword(),
			"commune":                   es.Keyword(),
			"code_postal":               es.Keyword(),
			"code_departement":          es.Keyword(),
			"code_commune":              es.Keyword(),
			"code_section":              es.Keyword(),
			"id_parcelle":               es.Keyword(),
			"adresse":                   es.Text(),
			// Clé de rapprochement avec les DPE (voir IdentifiantBAN).
			"identifiant_ban": es.Keyword(),
			"prix_m2":         es.Float(),
			"location":        es.GeoPoint(),
			"etiquette_dpe":   es.Keyword(),
			// Absent = DPE antérieur ou égal à la vente (cas fiable) ; true = DPE
			// postérieur accepté dans une fenêtre courte (voir jointure.go) —
			// l'interface doit alors signaler la réserve.
			"etiquette_dpe_apres_vente": es.Boolean(),
		},
	}
}

// ZonesMapping retourne le mapping de l'index des agrégats de prix par zone,
// recalculés à l'issue de chaque ingestion DVF. La choroplèthe de la carte n'a
// aucune raison de refaire à chaque affichage un calcul dont le résultat ne
// change qu'entre deux ingestions.
func ZonesMapping() map[string]any {
	return map[string]any{
		"properties": map[string]any{
			"niveau": es.Keyword(), // departement | commune | section
			"code":   es.Keyword(),
			// Millésime de la mutation. Absent sur les documents « tous
			// millésimes confondus » qui alimentent la choroplèthe : c'est ce qui
			// distingue la vue d'ensemble des points d'une série temporelle.
			"annee": es.Integer(),
			// Département d'une commune, commune d'une section : c'est ce champ
			// que filtre la carte pour ne charger que les zones du niveau parent.
			"code_parent": es.Keyword(),
			"label":       es.Keyword(),
			// Bâti et terrain nu sont deux marchés distincts, agrégés séparément :
			// au m², un appartement et un champ ne se comparent pas.
			"prix_m2_median_bati":    es.Float(),
			"prix_m2_p25_bati":       es.Float(),
			"prix_m2_p75_bati":       es.Float(),
			"nb_mutations_bati":      es.Integer(),
			"prix_m2_median_terrain": es.Float(),
			"prix_m2_p25_terrain":    es.Float(),
			"prix_m2_p75_terrain":    es.Float(),
			"nb_mutations_terrain":   es.Integer(),
			"nb_mutations":           es.Integer(),
			// Part de passoires énergétiques (étiquettes F/G) parmi les mutations
			// dotées d'une étiquette DPE, pour la vue "DPE" de la choroplèthe.
			"dpe_part_passoire":     es.Float(),
			"dpe_nb_avec_etiquette": es.Integer(),
			"calcule_le":            es.Date(),
		},
	}
}
