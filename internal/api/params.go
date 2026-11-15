package api

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const (
	defaultSize = 20
	maxSize     = 1000
)

// validationError décrit un paramètre invalide, dans la forme d'une entrée de
// `detail` d'un 422 FastAPI.
type validationError struct {
	Loc  []string `json:"loc"`
	Msg  string   `json:"msg"`
	Type string   `json:"type"`
}

// query lit et valide les paramètres d'une requête, en accumulant les erreurs.
type query struct {
	values   url.Values
	problems []validationError
}

func newQuery(values url.Values) *query { return &query{values: values} }

func (q *query) fail(name, msg, kind string) {
	q.problems = append(q.problems, validationError{Loc: []string{"query", name}, Msg: msg, Type: kind})
}

// str retourne la valeur du paramètre, ou nil s'il est absent.
func (q *query) str(name string) *string {
	if !q.values.Has(name) {
		return nil
	}
	value := q.values.Get(name)
	return &value
}

func (q *query) list(name string) []string {
	return q.values[name]
}

func (q *query) float(name string) *float64 {
	raw := q.values.Get(name)
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		q.fail(name, "Input should be a valid number, unable to parse string as a number", "float_parsing")
		return nil
	}
	return &value
}

func (q *query) integer(name string) *int64 {
	raw := q.values.Get(name)
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		q.fail(name, "Input should be a valid integer, unable to parse string as an integer", "int_parsing")
		return nil
	}
	return &value
}

// size lit une taille de page, bornée à [1, maxSize].
func (q *query) size(name string, def int) int {
	value := q.integer(name)
	switch {
	case value == nil:
		return def
	case *value < 1 || *value > maxSize:
		q.fail(name, fmt.Sprintf("Input should be between 1 and %d", maxSize), "value_error")
		return def
	}
	return int(*value)
}

// literal lit un paramètre à valeurs énumérées ; `def` vide le rend obligatoire.
func (q *query) literal(name, def string, allowed ...string) string {
	raw := q.values.Get(name)
	if raw == "" {
		if def == "" {
			q.fail(name, "Field required", "missing")
		}
		return def
	}
	if !slices.Contains(allowed, raw) {
		quoted := make([]string, len(allowed))
		for i, value := range allowed {
			quoted[i] = "'" + value + "'"
		}
		q.fail(name, "Input should be "+strings.Join(quoted[:len(quoted)-1], ", ")+" or "+quoted[len(quoted)-1], "literal_error")
		return def
	}
	return raw
}

// required lit un paramètre texte obligatoire.
func (q *query) required(name string) string {
	if !q.values.Has(name) {
		q.fail(name, "Field required", "missing")
		return ""
	}
	return q.values.Get(name)
}
