"""Tests du calcul de variation en pourcentage."""

from __future__ import annotations

from app.api.v1.dvf import _variation_pct


def test_hausse() -> None:
    assert _variation_pct(2000.0, 2400.0) == 20.0


def test_baisse() -> None:
    assert _variation_pct(2500.0, 2000.0) == -20.0


def test_arrondi_au_dixieme() -> None:
    assert _variation_pct(3000.0, 3100.0) == 3.3


def test_sans_reference_pas_de_variation() -> None:
    """Le premier point d'une série n'a rien à quoi se comparer."""
    assert _variation_pct(None, 2000.0) is None


def test_reference_nulle_refusee() -> None:
    """Une variation depuis zéro n'a pas de sens : mieux vaut ne rien afficher."""
    assert _variation_pct(0.0, 2000.0) is None
