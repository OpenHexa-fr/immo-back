"""Tests du calcul de variation en pourcentage."""

from __future__ import annotations

from app.api.v1.dvf import _aplatir, _variation_pct


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


_ZONE = {
    "code": "59350",
    "label": "Lille",
    "prix_m2_median_bati": 3200.0,
    "prix_m2_p25_bati": 2500.0,
    "prix_m2_p75_bati": 4100.0,
    "nb_mutations_bati": 4200,
    "prix_m2_median_terrain": 180.0,
    "prix_m2_p25_terrain": 90.0,
    "prix_m2_p75_terrain": 300.0,
    "nb_mutations_terrain": 130,
    "nb_mutations": 4330,
}


def test_aplatir_selectionne_le_bati() -> None:
    aplatie = _aplatir(_ZONE, "bati")

    assert aplatie is not None
    assert aplatie["prix_m2_median"] == 3200.0
    assert aplatie["nb_mutations"] == 4200


def test_aplatir_selectionne_le_terrain() -> None:
    """Deux marchés sans rapport : 180 €/m² de terrain contre 3 200 € de bâti."""
    aplatie = _aplatir(_ZONE, "terrain")

    assert aplatie is not None
    assert aplatie["prix_m2_median"] == 180.0
    assert aplatie["nb_mutations"] == 130


def test_aplatir_ignore_une_categorie_absente() -> None:
    """Une commune peut n'avoir connu que des ventes de terrain, ou l'inverse."""
    sans_terrain = {**_ZONE, "prix_m2_median_terrain": None}

    assert _aplatir(sans_terrain, "terrain") is None
