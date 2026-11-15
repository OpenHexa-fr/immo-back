package dvf

// Ingestion des fichiers DVF nationaux dans Elasticsearch.
//
// Source réelle : `https://files.data.gouv.fr/geo-dvf/latest/csv/<annee>/full.csv.gz`
// (fichier national unique, compressé gzip, ~100 Mo par millésime). Le fichier
// est lu en flux : téléchargement, décompression, parsing CSV et indexation
// avancent ensemble, sans jamais charger le fichier entier en mémoire.
//
// Il n'existe pas de colonne `numero_lot` unique : le CSV distingue les lots
// via `numero_disposition` et jusqu'à 5 colonnes `lotN_numero`. Une mutation
// (`id_mutation`) peut en outre porter sur plusieurs parcelles cadastrales et
// plusieurs natures de local sans que `lot1_numero` ne varie pour autant :
// l'identifiant de document intègre donc aussi `id_parcelle` et
// `code_type_local`.
//
// Même avec ces cinq colonnes, certains groupes de lignes restent strictement
// identiques dans le CSV source (ex : plusieurs caves vendues dans la même
// mutation, sans lot ni surface renseignés). Un index d'occurrence, calculé par
// position dans le fichier au sein de chaque groupe de clé identique, sert de
// dernier recours pour garantir des `_id` uniques. Il rend l'ingestion
// idempotente d'un run à l'autre tant que l'ordre des lignes du fichier source
// ne change pas (stable en pratique pour un millésime déjà publié).

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/core/es"
)

// Colonnes lues dans le CSV source ; les autres sont ignorées.
var dvfColumns = []string{
	"id_mutation", "numero_disposition", "id_parcelle", "code_type_local",
	"date_mutation", "valeur_fonciere", "surface_reelle_bati", "surface_terrain",
	"nombre_pieces_principales", "type_local", "nom_commune", "code_postal",
	"code_departement", "code_commune", "adresse_numero", "adresse_suffixe",
	"adresse_nom_voie",
	// Code FANTOIR de la voie : deuxième segment de l'identifiant BAN, donc
	// indispensable au rapprochement avec les DPE (voir IdentifiantBAN).
	"adresse_code_voie",
	"lot1_numero", "longitude", "latitude",
}

// sectionPrefixLength est la longueur du préfixe d'`id_parcelle` identifiant
// la section cadastrale (commune INSEE sur 5 caractères + préfixe de commune
// associée sur 3 + lettres de section sur 2), le numéro de parcelle (4 derniers
// caractères) en étant exclu.
const sectionPrefixLength = 10

// Le téléchargement et le parsing d'un millésime prennent quelques minutes ;
// au-delà, la source est considérée comme bloquée.
const downloadTimeout = 30 * time.Minute

// Row est une ligne du CSV DVF, valeurs vides rendues en nil.
type Row struct {
	IDMutation              string
	NumeroDisposition       *string
	IDParcelle              *string
	CodeTypeLocal           *string
	DateMutation            string
	ValeurFonciere          float64
	SurfaceReelleBati       *int64
	SurfaceTerrain          *int64
	NombrePiecesPrincipales *int64
	TypeLocal               *string
	NomCommune              *string
	CodePostal              *string
	CodeDepartement         *string
	CodeCommune             *string
	AdresseNumero           *int64
	AdresseSuffixe          *string
	AdresseNomVoie          *string
	AdresseCodeVoie         *string
	Lot1Numero              *string
	Longitude               *float64
	Latitude                *float64
	// Occurrence est le rang de la ligne parmi celles de même clé de
	// dédoublonnage, dans l'ordre du fichier (0 pour la première).
	Occurrence int
}

func text(raw string) *string {
	if raw == "" {
		return nil
	}
	return &raw
}

// integer lit un entier ; une valeur entière écrite en décimal ("80.0") est
// acceptée, toute autre valeur illisible devient nil.
func integer(raw string) *int64 {
	if raw == "" {
		return nil
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return &v
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil && f == math.Trunc(f) && !math.IsInf(f, 0) {
		v := int64(f)
		return &v
	}
	return nil
}

func decimal(raw string) *float64 {
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ParseRows lit un CSV DVF (brut ou compressé gzip, détecté par sa signature)
// et itère ses lignes exploitables.
//
// Les lignes sans `valeur_fonciere` lisible sont écartées : nominalement
// obligatoire, elle manque pourtant sur certaines lignes réelles. Une erreur de
// lecture interrompt l'itération et est retournée par `errp`.
func ParseRows(source io.Reader, errp *error) func(yield func(Row) bool) {
	return func(yield func(Row) bool) {
		reader, err := decompress(source)
		if err != nil {
			*errp = err
			return
		}
		records := csv.NewReader(reader)
		records.ReuseRecord = true
		records.FieldsPerRecord = -1
		records.LazyQuotes = true

		header, err := records.Read()
		if err != nil {
			*errp = fmt.Errorf("dvf: en-tête CSV illisible : %w", err)
			return
		}
		columns := map[string]int{}
		for i, name := range header {
			columns[strings.TrimPrefix(strings.TrimSpace(name), "\xef\xbb\xbf")] = i
		}
		for _, name := range dvfColumns {
			if _, ok := columns[name]; !ok {
				*errp = fmt.Errorf("dvf: colonne %q absente du CSV", name)
				return
			}
		}

		// Clé de dédoublonnage hachée : un fichier national compte plusieurs
		// millions de lignes, garder les clés en clair coûterait des centaines
		// de Mo. Une collision ne ferait que décaler une numérotation
		// d'occurrence, sans créer de doublon : la clé entre aussi dans l'_id.
		occurrences := map[uint64]int{}
		for {
			record, err := records.Read()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				// Une ligne mal formée est ignorée ; une erreur de lecture
				// (réseau, gzip tronqué) interrompt l'ingestion du millésime.
				var parseErr *csv.ParseError
				if errors.As(err, &parseErr) {
					slog.Warn("dvf_csv_line_skipped", "line", parseErr.Line, "error", parseErr.Err.Error())
					continue
				}
				*errp = fmt.Errorf("dvf: lecture du CSV interrompue : %w", err)
				return
			}
			field := func(name string) string {
				if i := columns[name]; i < len(record) {
					return strings.TrimSpace(record[i])
				}
				return ""
			}

			valeur := decimal(field("valeur_fonciere"))
			if valeur == nil || field("id_mutation") == "" {
				continue
			}
			row := Row{
				IDMutation:              field("id_mutation"),
				NumeroDisposition:       text(field("numero_disposition")),
				IDParcelle:              text(field("id_parcelle")),
				CodeTypeLocal:           text(field("code_type_local")),
				DateMutation:            field("date_mutation"),
				ValeurFonciere:          *valeur,
				SurfaceReelleBati:       integer(field("surface_reelle_bati")),
				SurfaceTerrain:          integer(field("surface_terrain")),
				NombrePiecesPrincipales: integer(field("nombre_pieces_principales")),
				TypeLocal:               text(field("type_local")),
				NomCommune:              text(field("nom_commune")),
				CodePostal:              text(field("code_postal")),
				CodeDepartement:         text(field("code_departement")),
				CodeCommune:             text(field("code_commune")),
				AdresseNumero:           integer(field("adresse_numero")),
				AdresseSuffixe:          text(field("adresse_suffixe")),
				AdresseNomVoie:          text(field("adresse_nom_voie")),
				AdresseCodeVoie:         text(field("adresse_code_voie")),
				Lot1Numero:              text(field("lot1_numero")),
				Longitude:               decimal(field("longitude")),
				Latitude:                decimal(field("latitude")),
			}
			key := fnv.New64a()
			for _, part := range []string{
				row.IDMutation, deref(row.NumeroDisposition), deref(row.IDParcelle),
				deref(row.CodeTypeLocal), deref(row.Lot1Numero),
			} {
				_, _ = key.Write([]byte(part))
				_, _ = key.Write([]byte{0})
			}
			hash := key.Sum64()
			row.Occurrence = occurrences[hash]
			occurrences[hash]++

			if !yield(row) {
				return
			}
		}
	}
}

func decompress(source io.Reader) (io.Reader, error) {
	buffered := bufio.NewReaderSize(source, 1<<20)
	magic, err := buffered.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		return gzip.NewReader(buffered)
	}
	return buffered, nil
}

// PrixM2 retourne le prix au m² : surface bâtie si disponible, sinon surface
// du terrain (ventes de terrains nus). nil sans valeur ni surface exploitable.
func PrixM2(valeur float64, surfaceBati, surfaceTerrain *int64) *float64 {
	if valeur == 0 {
		return nil
	}
	var surface int64
	switch {
	case surfaceBati != nil && *surfaceBati != 0:
		surface = *surfaceBati
	case surfaceTerrain != nil && *surfaceTerrain != 0:
		surface = *surfaceTerrain
	default:
		return nil
	}
	prix := round(valeur/float64(surface), 2)
	return &prix
}

func round(value float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Round(value*factor) / factor
}

// Adresse compose l'adresse à partir du numéro, du suffixe et de la voie.
func Adresse(numero *int64, suffixe, nomVoie *string) *string {
	if nomVoie == nil {
		return nil
	}
	prefix := ""
	if numero != nil && *numero != 0 {
		prefix = strconv.FormatInt(*numero, 10) + deref(suffixe) + " "
	}
	adresse := prefix + *nomVoie
	return &adresse
}

// Correspondance des suffixes d'adresse DVF vers leur forme dans l'identifiant
// BAN. DVF code le suffixe sur une lettre, la BAN l'écrit en toutes lettres
// pour les rangs ("bis", "ter") mais garde la lettre pour les indices de
// bâtiment. La lettre seule étant ambiguë, on ne traduit que les rangs usuels
// et on renonce au rapprochement pour le reste — un faux positif serait pire
// qu'une absence.
var suffixesBAN = map[string]string{"B": "bis", "T": "ter", "Q": "quater"}

// IdentifiantBAN reconstruit l'identifiant BAN `{code_insee}_{code_voie}_{numero}`.
//
// C'est la clé de jointure avec les diagnostics DPE, que l'ADEME publie déjà
// sous cette forme. La reconstruire ici évite d'appeler le service de
// géocodage de la BAN sur des millions de mutations.
//
// Retourne nil dès qu'un segment manque — typiquement les ventes de terrains
// nus, dépourvues de numéro de voie, et qui n'ont de toute façon aucun DPE.
func IdentifiantBAN(codeCommune, codeVoie *string, numero *int64, suffixe *string) *string {
	if codeCommune == nil || codeVoie == nil || numero == nil {
		return nil
	}
	identifiant := fmt.Sprintf("%s_%s_%05d", *codeCommune, *codeVoie, *numero)
	if s := strings.ToUpper(strings.TrimSpace(deref(suffixe))); s != "" {
		rang, ok := suffixesBAN[s]
		if !ok {
			return nil
		}
		identifiant += "_" + rang
	}
	return &identifiant
}

// Document convertit une ligne en document indexable.
func Document(row Row) es.Document {
	var location any
	if row.Latitude != nil && row.Longitude != nil {
		location = map[string]any{"lat": *row.Latitude, "lon": *row.Longitude}
	}
	var codeSection *string
	if row.IDParcelle != nil {
		section := *row.IDParcelle
		if len(section) > sectionPrefixLength {
			section = section[:sectionPrefixLength]
		}
		codeSection = &section
	}
	return es.Document{
		"_id": es.MakeDocumentID(
			row.IDMutation, deref(row.NumeroDisposition), deref(row.IDParcelle),
			deref(row.CodeTypeLocal), deref(row.Lot1Numero), row.Occurrence,
		),
		"id_mutation":               row.IDMutation,
		"date_mutation":             row.DateMutation,
		"valeur_fonciere":           row.ValeurFonciere,
		"surface_reelle_bati":       row.SurfaceReelleBati,
		"surface_terrain":           row.SurfaceTerrain,
		"nombre_pieces_principales": row.NombrePiecesPrincipales,
		"type_local":                row.TypeLocal,
		"commune":                   row.NomCommune,
		"code_postal":               row.CodePostal,
		"code_departement":          row.CodeDepartement,
		"code_commune":              row.CodeCommune,
		"code_section":              codeSection,
		"id_parcelle":               row.IDParcelle,
		"adresse":                   Adresse(row.AdresseNumero, row.AdresseSuffixe, row.AdresseNomVoie),
		"identifiant_ban":           IdentifiantBAN(row.CodeCommune, row.AdresseCodeVoie, row.AdresseNumero, row.AdresseSuffixe),
		"prix_m2":                   PrixM2(row.ValeurFonciere, row.SurfaceReelleBati, row.SurfaceTerrain),
		"location":                  location,
	}
}

// Ingest télécharge, parse et indexe les transactions d'un millésime DVF.
func Ingest(ctx context.Context, client *elasticsearch.Client, indexAlias, sourceURL string) (success, failed int, err error) {
	ctx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return 0, 0, err
	}
	// files.data.gouv.fr redirige (302) vers un bucket S3 : le client HTTP de
	// Go suit les redirections par défaut.
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return 0, 0, fmt.Errorf("dvf: %s a répondu %d", sourceURL, response.StatusCode)
	}

	var parseErr error
	documents := func(yield func(es.Document) bool) {
		for row := range ParseRows(response.Body, &parseErr) {
			if !yield(Document(row)) {
				return
			}
		}
	}
	success, failed, err = es.BulkIndex(ctx, client, indexAlias, documents)
	if err == nil {
		err = parseErr
	}
	if err != nil {
		return success, failed, err
	}
	slog.Info("dvf_ingestion_completed", "source_url", sourceURL, "success", success, "errors", failed)
	return success, failed, nil
}

// IngestYears ingère séquentiellement plusieurs millésimes DVF (un par URL).
//
// L'échec d'un millésime (fichier pas encore publié, incident réseau
// ponctuel) n'interrompt pas les autres : chaque téléchargement pouvant
// dépasser une centaine de Mo, une erreur transitoire sur l'un d'eux ne doit
// pas priver les autres millésimes, déjà disponibles, de mise à jour.
func IngestYears(ctx context.Context, client *elasticsearch.Client, indexAlias string, sourceURLs []string) (success, failed int) {
	for _, sourceURL := range sourceURLs {
		s, f, err := Ingest(ctx, client, indexAlias, sourceURL)
		if err != nil {
			slog.Error("dvf_year_ingestion_failed", "source_url", sourceURL, "error", err.Error())
			if ctx.Err() != nil {
				break
			}
			continue
		}
		success += s
		failed += f
	}
	return success, failed
}
