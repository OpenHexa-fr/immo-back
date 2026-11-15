package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/openhexa-fr/immo-back/internal/httpcache"
)

// `/status` pilote le bandeau "synchronisation en cours" du frontend : il doit
// refléter l'état réel de l'ingestion, jamais une réponse mise en cache.
const statusPath = "/api/v1/status"

// L'ETag dérive de la date du dernier calcul des zones, qui ne suit que les
// ingestions DVF : l'appliquer aux routes DPE servirait des 304 sur des
// données qui, elles, auraient changé.
const etagPathPrefix = "/api/v1/dvf/"

func (s *Server) cacheControl() string {
	return fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d",
		int(s.CacheMaxAge.Seconds()), int(s.CacheStaleWhileRevalidate.Seconds()))
}

// headerWriter pose des en-têtes au moment où le statut est connu : ils ne
// s'appliquent qu'aux réponses 200.
type headerWriter struct {
	http.ResponseWriter
	onOK        map[string]string
	wroteHeader bool
}

func (h *headerWriter) WriteHeader(code int) {
	if !h.wroteHeader {
		h.wroteHeader = true
		if code == http.StatusOK {
			for key, value := range h.onOK {
				h.Header().Set(key, value)
			}
		}
	}
	h.ResponseWriter.WriteHeader(code)
}

func (h *headerWriter) Write(b []byte) (int, error) {
	if !h.wroteHeader {
		h.WriteHeader(http.StatusOK)
	}
	return h.ResponseWriter.Write(b)
}

// cacheHeaders pose les en-têtes de cache et répond 304 aux revalidations DVF
// inchangées, sans exécuter la requête Elasticsearch.
//
// Sans en-tête explicite, ni le navigateur ni un éventuel CDN ne peuvent
// réutiliser une réponse — alors que les données ne changent qu'au rythme de
// l'ingestion. Les en-têtes ne s'appliquent qu'aux GET aboutis : une erreur ne
// doit pas être mémorisée.
func (s *Server) cacheHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == statusPath {
			next.ServeHTTP(&headerWriter{ResponseWriter: w, onOK: map[string]string{"Cache-Control": "no-store"}}, r)
			return
		}

		headers := map[string]string{"Cache-Control": s.cacheControl()}
		if strings.HasPrefix(r.URL.Path, etagPathPrefix) && s.Version != nil {
			if version := s.Version.Get(r.Context()); version != nil {
				etag := httpcache.BuildETag(*version, r.URL.Path, r.URL.RawQuery)
				headers["ETag"] = etag
				if r.Header.Get("If-None-Match") == etag {
					w.Header().Set("ETag", etag)
					w.Header().Set("Cache-Control", s.cacheControl())
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
		next.ServeHTTP(&headerWriter{ResponseWriter: w, onOK: headers}, r)
	})
}

// cors : API publique en lecture seule (données ouvertes, pas de cookies ni de
// session) : CORS permissif, nécessaire puisque le frontend est servi sur une
// origine distincte (origines `*`, méthode GET, en-têtes quelconques).
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == "" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")

		requestedMethod := r.Header.Get("Access-Control-Request-Method")
		if r.Method == http.MethodOptions && requestedMethod != "" {
			if requestedMethod != http.MethodGet {
				http.Error(w, "Disallowed CORS method", http.StatusBadRequest)
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET")
			w.Header().Set("Access-Control-Max-Age", "600")
			if headers := r.Header.Get("Access-Control-Request-Headers"); headers != "" {
				w.Header().Set("Access-Control-Allow-Headers", headers)
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// accessLog journalise chaque requête (méthode, chemin, statut, durée).
func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		slog.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"query", r.URL.RawQuery,
		)
	})
}
