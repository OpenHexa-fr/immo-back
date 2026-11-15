// Package api expose l'API HTTP de l'application Immo : routes DVF, DPE et
// état des données, avec cache HTTP, ETag, compression, CORS et logs d'accès.
package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/openhexa-fr/immo-back/internal/httpcache"
)

// Server regroupe les dépendances des handlers.
type Server struct {
	ES *elasticsearch.Client
	// Prefix est le préfixe des index (ex. "openhexa-immo").
	Prefix string
	// Version fournit la version des données DVF pour les ETag ; nil les désactive.
	Version *httpcache.DataVersion

	CacheMaxAge               time.Duration
	CacheStaleWhileRevalidate time.Duration
}

func (s *Server) alias(domain string) string { return s.Prefix + "-" + domain }

// Handler retourne le routeur complet, middlewares inclus.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/dvf/search", s.dvfSearch)
	mux.HandleFunc("GET /api/v1/dvf/prix-carte", s.prixCarte)
	mux.HandleFunc("GET /api/v1/dvf/prix-serie", s.prixSerie)
	mux.HandleFunc("GET /api/v1/dvf/zone", s.zone)
	mux.HandleFunc("GET /api/v1/dvf/parcelle/{id_parcelle}", s.parcelle)
	mux.HandleFunc("GET /api/v1/dvf/parcelle/{id_parcelle}/dpe", s.parcelleDPE)
	mux.HandleFunc("GET /api/v1/dvf/{id_mutation}", s.byMutation)
	mux.HandleFunc("GET /api/v1/dpe/search", s.dpeSearch)
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeDetail(w, r, http.StatusNotFound, "Not Found")
	})

	return accessLog(cors(s.cacheHeaders(mux)))
}

// Seuil de compression : les réponses de recherche sont du JSON très
// répétitif, que gzip divise par un ordre de grandeur ; en dessous de ce
// seuil, la compression coûterait plus qu'elle ne rapporte.
const gzipMinSize = 1000

// writeJSON écrit une réponse JSON, compressée si le client l'accepte et
// qu'elle dépasse gzipMinSize.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		slog.Error("response_encoding_failed", "error", err.Error())
		status, payload = http.StatusInternalServerError, []byte(`{"detail":"Internal Server Error"}`)
	}
	payload = append(payload, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Vary", "Accept-Encoding")
	if len(payload) >= gzipMinSize && acceptsGzip(r) {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write(payload)
		_ = writer.Close()
		payload = compressed.Bytes()
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		encoding, _, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(encoding, "gzip") {
			return true
		}
	}
	return false
}

// writeDetail écrit une erreur au format `{"detail": ...}` de FastAPI, que les
// clients de l'API Python savent déjà lire.
func writeDetail(w http.ResponseWriter, r *http.Request, status int, detail any) {
	writeJSON(w, r, status, map[string]any{"detail": detail})
}

func internalError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return // client parti : rien à lui répondre
	}
	slog.Error("request_failed", "method", r.Method, "path", r.URL.Path, "error", err.Error())
	writeDetail(w, r, http.StatusInternalServerError, "Internal Server Error")
}

// ctx borne les requêtes Elasticsearch d'un handler.
func ctx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 30*time.Second)
}
