// Package esx complète core/es pour les besoins propres au domaine immobilier :
// recherches arbitraires (agrégations, `collapse`, point-in-time), mises à jour
// partielles en masse et comptages filtrés.
package esx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"strings"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"

	"github.com/openhexa-fr/core/es"
)

func body(v any) (io.Reader, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(payload), nil
}

func check(res *esapi.Response) error {
	if !res.IsError() {
		return nil
	}
	payload, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	return &es.APIError{Status: res.StatusCode, Body: strings.TrimSpace(string(payload))}
}

// Search exécute `request` sur `index` (vide : requête point-in-time, sans
// index) et décode la réponse dans `out`. Les nombres sont décodés en
// json.Number quand `out` contient des `any`.
func Search(ctx context.Context, client *elasticsearch.Client, index string, request map[string]any, out any) error {
	reader, err := body(request)
	if err != nil {
		return err
	}
	options := []func(*esapi.SearchRequest){client.Search.WithContext(ctx), client.Search.WithBody(reader)}
	if index != "" {
		options = append(options, client.Search.WithIndex(index))
	}
	res, err := client.Search(options...)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if err := check(res); err != nil {
		return err
	}
	decoder := json.NewDecoder(res.Body)
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("esx: réponse de recherche illisible : %w", err)
	}
	return nil
}

// Hit est un résultat de recherche dont le `_source` est décodé en T.
type Hit[T any] struct {
	ID     string `json:"_id"`
	Source T      `json:"_source"`
	Sort   []any  `json:"sort"`
}

// Hits est l'enveloppe `hits` d'une réponse de recherche.
type Hits[T any] struct {
	Hits struct {
		Hits []Hit[T] `json:"hits"`
	} `json:"hits"`
}

// SearchHits exécute `request` et retourne les hits, `_source` décodé en T.
func SearchHits[T any](ctx context.Context, client *elasticsearch.Client, index string, request map[string]any) ([]Hit[T], error) {
	var response Hits[T]
	if err := Search(ctx, client, index, request, &response); err != nil {
		return nil, err
	}
	return response.Hits.Hits, nil
}

// CountQuery retourne le nombre de documents de `index` répondant à `query`.
func CountQuery(ctx context.Context, client *elasticsearch.Client, index string, query map[string]any) (int64, error) {
	reader, err := body(map[string]any{"query": query})
	if err != nil {
		return 0, err
	}
	res, err := client.Count(client.Count.WithContext(ctx), client.Count.WithIndex(index), client.Count.WithBody(reader))
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if err := check(res); err != nil {
		return 0, err
	}
	var response struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0, err
	}
	return response.Count, nil
}

// OpenPointInTime ouvre un point-in-time sur `index`.
func OpenPointInTime(ctx context.Context, client *elasticsearch.Client, index, keepAlive string) (string, error) {
	res, err := client.OpenPointInTime([]string{index}, keepAlive, client.OpenPointInTime.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if err := check(res); err != nil {
		return "", err
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return "", err
	}
	return response.ID, nil
}

// ClosePointInTime ferme un point-in-time. Les erreurs sont ignorées : un PIT
// non fermé expire de lui-même à l'issue de son keep-alive.
func ClosePointInTime(ctx context.Context, client *elasticsearch.Client, id string) {
	reader, err := body(map[string]any{"id": id})
	if err != nil {
		return
	}
	res, err := client.ClosePointInTime(client.ClosePointInTime.WithContext(ctx), client.ClosePointInTime.WithBody(reader))
	if err == nil {
		res.Body.Close()
	}
}

// Update est une mise à jour partielle d'un document existant.
type Update struct {
	ID  string
	Doc map[string]any
}

// BulkUpdate applique en masse des mises à jour partielles (`update` + `doc`)
// sur `index`. Retourne le nombre de succès et d'échecs unitaires ; une erreur
// n'est retournée que pour un problème qui empêche l'écriture elle-même.
func BulkUpdate(ctx context.Context, client *elasticsearch.Client, index string, updates iter.Seq[Update]) (success, failed int, err error) {
	var (
		mu       sync.Mutex
		firstErr error
		example  string
	)
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: client,
		OnError: func(_ context.Context, e error) {
			mu.Lock()
			defer mu.Unlock()
			if firstErr == nil {
				firstErr = e
			}
		},
	})
	if err != nil {
		return 0, 0, err
	}
	onFailure := func(_ context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, _ error) {
		mu.Lock()
		defer mu.Unlock()
		if example == "" {
			example = fmt.Sprintf("%s : %d %s %s", item.DocumentID, res.Status, res.Error.Type, res.Error.Reason)
		}
	}

	var addErr error
	for update := range updates {
		payload, e := json.Marshal(map[string]any{"doc": update.Doc})
		if e == nil {
			e = indexer.Add(ctx, esutil.BulkIndexerItem{
				Action: "update", Index: index, DocumentID: update.ID,
				Body: bytes.NewReader(payload), OnFailure: onFailure,
			})
		}
		if e != nil {
			addErr = e
			break
		}
	}
	closeErr := indexer.Close(ctx)
	stats := indexer.Stats()
	success, failed = int(stats.NumUpdated), int(stats.NumFailed)
	if failed > 0 {
		slog.Warn("elasticsearch_bulk_update_errors", "index", index, "errors", failed, "example", example)
	}
	for _, e := range []error{addErr, closeErr, firstErr} {
		if e != nil {
			return success, failed, e
		}
	}
	return success, failed, nil
}

// String rend une valeur JSON décodée (chaîne, json.Number) en chaîne ; ""
// pour nil.
func String(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	}
	return fmt.Sprint(value)
}

// Float rend une valeur JSON décodée en *float64 ; nil si absente ou non numérique.
func Float(value any) *float64 {
	switch v := value.(type) {
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return &f
		}
	case float64:
		return &v
	}
	return nil
}
