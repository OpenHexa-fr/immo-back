package jobs

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestAllExpandsToEverySourceInDependencyOrder(t *testing.T) {
	sources, err := ParseArgs([]string{"--source", "all"}, io.Discard)

	if err != nil || !reflect.DeepEqual(sources, []string{"dvf", "dpe", "jointure", "zones"}) {
		t.Errorf("sources=%v err=%v", sources, err)
	}
}

func TestSourcesAreRepeatableAndOrderedAsGiven(t *testing.T) {
	sources, err := ParseArgs([]string{"--source", "zones", "--source", "dpe-complet"}, io.Discard)

	if err != nil || !reflect.DeepEqual(sources, []string{"zones", "dpe-complet"}) {
		t.Errorf("sources=%v err=%v", sources, err)
	}
}

func TestUnknownOrMissingSourceIsRejected(t *testing.T) {
	if _, err := ParseArgs([]string{"--source", "sitadel"}, io.Discard); err == nil {
		t.Error("source inconnue : erreur attendue")
	}
	if _, err := ParseArgs(nil, io.Discard); err == nil {
		t.Error("aucune source : erreur attendue")
	}
}

func ok(context.Context) error { return nil }

func TestRunSucceedsWhenEverySourceSucceeds(t *testing.T) {
	var ran []string
	code := Run(t.Context(), []string{"dvf", "zones"}, ok, func(_ context.Context, source string) (int, int, error) {
		ran = append(ran, source)
		return 10, 0, nil
	})

	if code != 0 || !reflect.DeepEqual(ran, []string{"dvf", "zones"}) {
		t.Errorf("code=%d ran=%v", code, ran)
	}
}

func TestRunReportsFailureButRunsTheOtherSources(t *testing.T) {
	var ran []string
	code := Run(t.Context(), []string{"dvf", "dpe", "zones"}, ok, func(_ context.Context, source string) (int, int, error) {
		ran = append(ran, source)
		switch source {
		case "dvf":
			return 0, 0, errors.New("source indisponible")
		case "dpe":
			return 5, 2, nil // erreurs d'écriture
		}
		return 1, 0, nil
	})

	if code != 1 || len(ran) != 3 {
		t.Errorf("code=%d ran=%v", code, ran)
	}
}

func TestRunFailsWhenIndicesCannotBeEnsured(t *testing.T) {
	called := false
	code := Run(t.Context(), []string{"dvf"}, func(context.Context) error { return errors.New("cluster absent") },
		func(context.Context, string) (int, int, error) { called = true; return 0, 0, nil })

	if code != 1 || called {
		t.Errorf("code=%d called=%v", code, called)
	}
}
