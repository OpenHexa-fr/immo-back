package dvf

import (
	"errors"
	"strconv"
	"strings"
)

// GeoPoint est une position en degrés décimaux.
type GeoPoint struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// BBox est une emprise rectangulaire, dans l'ordre GeoJSON
// (`min_lon, min_lat, max_lon, max_lat`) : c'est l'ordre rendu par
// `map.getBounds().toArray()` côté MapLibre, donc celui attendu du frontend
// sans réarrangement.
type BBox struct {
	MinLon, MinLat, MaxLon, MaxLat float64
}

// ParseBBox parse la forme `min_lon,min_lat,max_lon,max_lat` reçue en
// paramètre de requête, et valide les bornes.
//
// `min_lon > max_lon` est volontairement toléré : c'est le cas légitime d'une
// emprise qui franchit l'antiméridien, qu'Elasticsearch sait interpréter tel
// quel.
func ParseBBox(raw string) (BBox, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 4 {
		return BBox{}, errors.New("bbox attend 4 valeurs : min_lon,min_lat,max_lon,max_lat")
	}
	var values [4]float64
	for i, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return BBox{}, errors.New("bbox attend 4 nombres décimaux")
		}
		values[i] = value
	}
	box := BBox{MinLon: values[0], MinLat: values[1], MaxLon: values[2], MaxLat: values[3]}
	switch {
	case box.MinLon < -180 || box.MinLon > 180 || box.MaxLon < -180 || box.MaxLon > 180:
		return BBox{}, errors.New("longitude hors de [-180, 180]")
	case box.MinLat < -90 || box.MinLat > 90 || box.MaxLat < -90 || box.MaxLat > 90:
		return BBox{}, errors.New("latitude hors de [-90, 90]")
	case box.MinLat > box.MaxLat:
		return BBox{}, errors.New("min_lat doit être inférieure à max_lat")
	}
	return box, nil
}

// Transaction est un lot de mutation DVF tel qu'exposé par l'API. Les champs
// absents du document (ou non rapatriés, voir CarteSourceFields) sont rendus
// en `null`.
type Transaction struct {
	IDMutation              string  `json:"id_mutation"`
	DateMutation            string  `json:"date_mutation"`
	ValeurFonciere          float64 `json:"valeur_fonciere"`
	SurfaceReelleBati       *int64  `json:"surface_reelle_bati"`
	SurfaceTerrain          *int64  `json:"surface_terrain"`
	NombrePiecesPrincipales *int64  `json:"nombre_pieces_principales"`
	TypeLocal               *string `json:"type_local"`
	Commune                 string  `json:"commune"`
	CodePostal              *string `json:"code_postal"`
	CodeDepartement         *string `json:"code_departement"`
	CodeCommune             *string `json:"code_commune"`
	CodeSection             *string `json:"code_section"`
	IDParcelle              *string `json:"id_parcelle"`
	Adresse                 *string `json:"adresse"`
	// IdentifiantBAN est la clé du rapprochement avec les DPE. Exposé pour que
	// la couverture de la jointure soit vérifiable de l'extérieur. nil sur les
	// ventes sans numéro de voie — essentiellement des terrains nus.
	IdentifiantBAN *string   `json:"identifiant_ban"`
	PrixM2         *float64  `json:"prix_m2"`
	Location       *GeoPoint `json:"location"`
	EtiquetteDPE   *string   `json:"etiquette_dpe"`
	// EtiquetteDPEApresVente vaut true si le DPE retenu a été établi après la
	// vente (fenêtre de tolérance, voir jointure.go) : le bien a pu être rénové
	// entre-temps.
	EtiquetteDPEApresVente *bool `json:"etiquette_dpe_apres_vente"`
}

// Sort est un critère de tri des transactions.
type Sort string

// Critères de tri. "recent" et "pertinence" (défaut) trient par date de
// mutation décroissante.
const (
	SortPertinence Sort = "pertinence"
	SortPrix       Sort = "prix"
	SortSurface    Sort = "surface"
	SortDistance   Sort = "distance"
	SortRecent     Sort = "recent"
)

// SearchParams sont les critères d'une recherche de transactions.
type SearchParams struct {
	Commune           *string
	CodePostal        *string
	IDParcelle        *string
	TypeLocal         []string
	ValeurFonciereMin *float64
	ValeurFonciereMax *float64
	SurfaceMin        *int64
	SurfaceMax        *int64
	SurfaceTerrainMin *int64
	SurfaceTerrainMax *int64
	PiecesMin         *int64
	PiecesMax         *int64
	PrixM2Min         *float64
	PrixM2Max         *float64
	EtiquetteDPE      []string
	DateMutationMin   *string
	DateMutationMax   *string
	Lat, Lon          *float64
	// RadiusKm est le rayon autour de (Lat, Lon), 10 km par défaut côté API.
	RadiusKm float64
	// BBox prime sur Lat/Lon + RadiusKm pour restreindre les résultats ;
	// Lat/Lon restent alors utilisables pour trier par distance.
	BBox *BBox
	Tri  Sort
}

// ParcelleMutation est une mutation et l'ensemble des lots qu'elle porte sur une parcelle.
type ParcelleMutation struct {
	IDMutation   string        `json:"id_mutation"`
	DateMutation string        `json:"date_mutation"`
	Lots         []Transaction `json:"lots"`
}

// DPEHistoriqueEntry est un diagnostic tel qu'établi, sans filtre de qualité
// ni de date : contrairement à `etiquette_dpe` sur une transaction (le seul
// diagnostic retenu par la jointure), l'historique expose tous les diagnostics
// connus à une adresse, pour que l'utilisateur juge lui-même de leur
// pertinence (via la date et `score_ban`).
type DPEHistoriqueEntry struct {
	NumeroDPE         string   `json:"numero_dpe"`
	DateEtablissement *string  `json:"date_etablissement"`
	EtiquetteDPE      *string  `json:"etiquette_dpe"`
	EtiquetteGES      *string  `json:"etiquette_ges"`
	SurfaceHabitable  *float64 `json:"surface_habitable"`
	TypeBatiment      *string  `json:"type_batiment"`
	ScoreBAN          *float64 `json:"score_ban"`
}
