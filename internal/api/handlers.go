package api

import (
	"fmt"
	"net/http"

	"github.com/openhexa-fr/core/es"
	"github.com/openhexa-fr/core/models"
	"github.com/openhexa-fr/core/pagination"

	"github.com/openhexa-fr/immo-back/internal/dpe"
	"github.com/openhexa-fr/immo-back/internal/dvf"
)

// dvfSearch : recherche des transactions DVF.
func (s *Server) dvfSearch(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	params := dvf.SearchParams{
		Commune:           q.str("commune"),
		CodePostal:        q.str("code_postal"),
		IDParcelle:        q.str("id_parcelle"),
		TypeLocal:         q.list("type_local"),
		ValeurFonciereMin: q.float("valeur_fonciere_min"),
		ValeurFonciereMax: q.float("valeur_fonciere_max"),
		SurfaceMin:        q.integer("surface_min"),
		SurfaceMax:        q.integer("surface_max"),
		SurfaceTerrainMin: q.integer("surface_terrain_min"),
		SurfaceTerrainMax: q.integer("surface_terrain_max"),
		PiecesMin:         q.integer("pieces_min"),
		PiecesMax:         q.integer("pieces_max"),
		PrixM2Min:         q.float("prix_m2_min"),
		PrixM2Max:         q.float("prix_m2_max"),
		EtiquetteDPE:      q.list("etiquette_dpe"),
		DateMutationMin:   q.str("date_mutation_min"),
		DateMutationMax:   q.str("date_mutation_max"),
		Lat:               q.float("lat"),
		Lon:               q.float("lon"),
		RadiusKm:          10,
		Tri:               dvf.Sort(r.URL.Query().Get("tri")),
	}
	if radius := q.float("radius_km"); radius != nil {
		params.RadiusKm = *radius
	}
	champs := q.literal("champs", "complet", "complet", "carte")
	size := q.size("size", defaultSize)
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}

	if raw := r.URL.Query().Get("bbox"); raw != "" {
		box, err := dvf.ParseBBox(raw)
		if err != nil {
			writeDetail(w, r, http.StatusBadRequest, err.Error())
			return
		}
		params.BBox = &box
	}
	var searchAfter []any
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		values, err := pagination.DecodeCursor(cursor)
		if err != nil {
			writeDetail(w, r, http.StatusBadRequest, err.Error())
			return
		}
		searchAfter = values
	}
	var source []string
	if champs == "carte" {
		source = dvf.CarteSourceFields
	}

	c, cancel := ctx(r)
	defer cancel()
	page, err := dvf.Search(c, s.ES, s.alias("dvf"), params, searchAfter, size, source)
	if err != nil {
		internalError(w, r, err)
		return
	}
	response, err := models.FromPage[dvf.Transaction](page)
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, response)
}

type prixCarteResponse struct {
	Niveau    string       `json:"niveau"`
	Categorie *string      `json:"categorie"`
	Buckets   []dvf.Bucket `json:"buckets"`
	// Date du dernier calcul des zones — nil quand la réponse vient du repli à la volée.
	CalculeLe *string `json:"calcule_le"`
}

// prixCarte : prix médian au m² par zone, pour la choroplèthe de la carte.
//
// Servi depuis l'index de zones pré-agrégé. Tant que ce calcul n'a jamais
// tourné (index vide), on retombe sur l'agrégation à la volée : la carte reste
// fonctionnelle, simplement plus lente. Chaque niveau doit être filtré par le
// niveau parent pour ne pas agréger la France entière en une requête.
func (s *Server) prixCarte(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	niveau := q.literal("niveau", "", "departement", "commune", "section")
	categorie := q.literal("categorie", "bati", "bati", "terrain")
	codeDepartement, codeCommune := nonEmpty(q.str("code_departement")), nonEmpty(q.str("code_commune"))
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}
	if niveau == "commune" && codeDepartement == nil {
		writeDetail(w, r, http.StatusBadRequest, "code_departement est requis pour niveau=commune")
		return
	}
	if niveau == "section" && codeCommune == nil {
		writeDetail(w, r, http.StatusBadRequest, "code_commune est requis pour niveau=section")
		return
	}

	codeParent := codeCommune
	if niveau == "commune" {
		codeParent = codeDepartement
	}
	c, cancel := ctx(r)
	defer cancel()
	zones, err := dvf.FetchZones(c, s.ES, s.alias("dvf-zones"), niveau, codeParent)
	if err != nil {
		internalError(w, r, err)
		return
	}
	buckets := []dvf.Bucket{}
	for _, zone := range zones {
		if bucket := dvf.Aplatir(zone, categorie); bucket != nil {
			buckets = append(buckets, *bucket)
		}
	}
	if len(buckets) == 0 {
		// Repli tant que les zones n'ont jamais été calculées.
		buckets, err = dvf.AggregatePrixCarte(c, s.ES, s.alias("dvf"), niveau, codeDepartement, codeCommune)
		if err != nil {
			internalError(w, r, err)
			return
		}
	}
	var calculeLe *string
	if len(buckets) > 0 {
		calculeLe = buckets[0].CalculeLe
	}
	writeJSON(w, r, http.StatusOK, prixCarteResponse{
		Niveau: niveau, Categorie: &categorie, Buckets: buckets, CalculeLe: calculeLe,
	})
}

type prixSeriePoint struct {
	Annee        int      `json:"annee"`
	PrixM2Median float64  `json:"prix_m2_median"`
	PrixM2P25    *float64 `json:"prix_m2_p25"`
	PrixM2P75    *float64 `json:"prix_m2_p75"`
	NbMutations  int64    `json:"nb_mutations"`
	// Variation par rapport au millésime précédent, en pourcentage ; nil sur
	// le premier point, qui n'a rien à quoi se comparer.
	EvolutionPct *float64 `json:"evolution_pct"`
}

type prixSerieResponse struct {
	Niveau    string           `json:"niveau"`
	Categorie *string          `json:"categorie"`
	Code      string           `json:"code"`
	Label     *string          `json:"label"`
	Points    []prixSeriePoint `json:"points"`
	// Variation entre le premier et le dernier millésime de la série.
	EvolutionTotalePct *float64 `json:"evolution_totale_pct"`
}

// prixSerie : évolution annuelle du prix médian au m² d'une zone. Les
// sections cadastrales n'en ont pas : trop peu de ventes par section et par an.
func (s *Server) prixSerie(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	niveau := q.literal("niveau", "", "departement", "commune")
	code := q.required("code")
	categorie := q.literal("categorie", "bati", "bati", "terrain")
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}

	c, cancel := ctx(r)
	defer cancel()
	zones, err := dvf.FetchSerie(c, s.ES, s.alias("dvf-zones"), niveau, code)
	if err != nil {
		internalError(w, r, err)
		return
	}
	response := prixSerieResponse{Niveau: niveau, Categorie: &categorie, Code: code, Points: []prixSeriePoint{}}
	var precedent *float64
	for _, zone := range zones {
		bucket := dvf.Aplatir(zone, categorie)
		if bucket == nil || bucket.Annee == nil {
			continue
		}
		if response.Label == nil {
			label := bucket.Label
			response.Label = &label
		}
		median := bucket.PrixM2Median
		response.Points = append(response.Points, prixSeriePoint{
			Annee: *bucket.Annee, PrixM2Median: median, PrixM2P25: bucket.PrixM2P25, PrixM2P75: bucket.PrixM2P75,
			NbMutations: bucket.NbMutations, EvolutionPct: dvf.VariationPct(precedent, &median),
		})
		precedent = &median
	}
	if n := len(response.Points); n > 1 {
		response.EvolutionTotalePct = dvf.VariationPct(&response.Points[0].PrixM2Median, &response.Points[n-1].PrixM2Median)
	}
	writeJSON(w, r, http.StatusOK, response)
}

type zoneResponse struct {
	Niveau       string   `json:"niveau"`
	Categorie    *string  `json:"categorie"`
	Code         string   `json:"code"`
	Label        string   `json:"label"`
	PrixM2Median float64  `json:"prix_m2_median"`
	PrixM2P25    *float64 `json:"prix_m2_p25"`
	PrixM2P75    *float64 `json:"prix_m2_p75"`
	NbMutations  int64    `json:"nb_mutations"`
}

// zone : agrégat global d'une zone, pour situer une vente dans son marché local.
func (s *Server) zone(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	niveau := q.literal("niveau", "", "departement", "commune", "section")
	code := q.required("code")
	categorie := q.literal("categorie", "bati", "bati", "terrain")
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}

	c, cancel := ctx(r)
	defer cancel()
	found, err := dvf.FetchZone(c, s.ES, s.alias("dvf-zones"), niveau, code)
	if err != nil {
		internalError(w, r, err)
		return
	}
	var bucket *dvf.Bucket
	if found != nil {
		bucket = dvf.Aplatir(*found, categorie)
	}
	if bucket == nil {
		writeDetail(w, r, http.StatusNotFound, fmt.Sprintf("zone %s:%s sans agrégat %s", niveau, code, categorie))
		return
	}
	writeJSON(w, r, http.StatusOK, zoneResponse{
		Niveau: niveau, Categorie: &categorie, Code: bucket.Code, Label: bucket.Label,
		PrixM2Median: bucket.PrixM2Median, PrixM2P25: bucket.PrixM2P25, PrixM2P75: bucket.PrixM2P75,
		NbMutations: bucket.NbMutations,
	})
}

type parcelleResponse struct {
	IDParcelle string                 `json:"id_parcelle"`
	Mutations  []dvf.ParcelleMutation `json:"mutations"`
}

// parcelle : historique des ventes d'une parcelle, groupées par mutation.
func (s *Server) parcelle(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	size := q.size("size", 50)
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}
	idParcelle := r.PathValue("id_parcelle")
	c, cancel := ctx(r)
	defer cancel()
	mutations, err := dvf.ParcelleMutations(c, s.ES, s.alias("dvf"), idParcelle, size)
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, parcelleResponse{IDParcelle: idParcelle, Mutations: mutations})
}

type parcelleDPEResponse struct {
	IDParcelle  string                   `json:"id_parcelle"`
	Diagnostics []dvf.DPEHistoriqueEntry `json:"diagnostics"`
}

// parcelleDPE : tous les diagnostics connus aux adresses d'une parcelle, sans
// filtre, pour la vue « historique » de la carte.
func (s *Server) parcelleDPE(w http.ResponseWriter, r *http.Request) {
	idParcelle := r.PathValue("id_parcelle")
	c, cancel := ctx(r)
	defer cancel()
	diagnostics, err := dvf.HistoriqueDPEParcelle(c, s.ES, s.alias("dvf"), s.alias("dpe"), idParcelle)
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, parcelleDPEResponse{IDParcelle: idParcelle, Diagnostics: diagnostics})
}

// byMutation : lots d'une mutation DVF donnée (fiche bien).
func (s *Server) byMutation(w http.ResponseWriter, r *http.Request) {
	c, cancel := ctx(r)
	defer cancel()
	transactions, err := dvf.GetByMutation(c, s.ES, s.alias("dvf"), r.PathValue("id_mutation"))
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, transactions)
}

// dpeSearch : recherche des diagnostics DPE.
func (s *Server) dpeSearch(w http.ResponseWriter, r *http.Request) {
	q := newQuery(r.URL.Query())
	params := dpe.SearchParams{
		Commune:      q.str("commune"),
		CodePostal:   q.str("code_postal"),
		EtiquetteDPE: q.list("etiquette_dpe"),
	}
	size := q.size("size", defaultSize)
	if len(q.problems) > 0 {
		writeDetail(w, r, http.StatusUnprocessableEntity, q.problems)
		return
	}
	// `search_after` se répète : les valeurs restent des chaînes, qu'Elasticsearch
	// coerce vers le type du champ de tri.
	var searchAfter []any
	for _, value := range r.URL.Query()["search_after"] {
		searchAfter = append(searchAfter, value)
	}

	c, cancel := ctx(r)
	defer cancel()
	page, err := dpe.Search(c, s.ES, s.alias("dpe"), params, searchAfter, size)
	if err != nil {
		internalError(w, r, err)
		return
	}
	response, err := models.FromPage[dpe.Diagnostic](page)
	if err != nil {
		internalError(w, r, err)
		return
	}
	// Cette route exposait les valeurs de tri brutes : conservées pour les
	// clients existants, en plus du curseur opaque.
	response.NextSearchAfter = page.NextSearchAfter
	writeJSON(w, r, http.StatusOK, response)
}

type domainStatus struct {
	DVF bool `json:"dvf"`
	DPE bool `json:"dpe"`
}

// status : indique, pour chaque domaine, si au moins un document a été ingéré.
// Utilisé par le frontend pour masquer un bandeau "synchronisation en cours".
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	c, cancel := ctx(r)
	defer cancel()
	dvfCount, err := es.Count(c, s.ES, s.alias("dvf"))
	if err != nil {
		internalError(w, r, err)
		return
	}
	dpeCount, err := es.Count(c, s.ES, s.alias("dpe"))
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, domainStatus{DVF: dvfCount > 0, DPE: dpeCount > 0})
}

// nonEmpty traite un paramètre vide comme absent (`?code_commune=`).
func nonEmpty(value *string) *string {
	if value == nil || *value == "" {
		return nil
	}
	return value
}
