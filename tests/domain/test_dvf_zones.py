"""Tests de la pré-agrégation des prix DVF par zone."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

from app.domain.dvf.zones import (
    NIVEAUX_SERIE,
    compute_zones,
    fetch_serie,
    fetch_zone,
    fetch_zones,
    zones_are_computed,
)


def _composite_page(
    buckets: list[dict[str, Any]], after_key: dict[str, Any] | None = None
) -> dict[str, Any]:
    zones: dict[str, Any] = {"buckets": buckets}
    if after_key is not None:
        zones["after_key"] = after_key
    return {"aggregations": {"zones": zones}}


def _bucket(
    code: str,
    doc_count: int = 10,
    median: float | None = 2000.0,
    parent: str | None = None,
    label_commune: str | None = None,
) -> dict[str, Any]:
    bucket: dict[str, Any] = {
        "key": {"code": code},
        "doc_count": doc_count,
        "prix": {"values": {"25.0": 1500.0, "50.0": median, "75.0": 2500.0}},
    }
    if parent is not None:
        bucket["key"]["parent"] = parent
    if label_commune is not None:
        bucket["label"] = {"hits": {"hits": [{"_source": {"commune": label_commune}}]}}
    return bucket


def _sequence_recherches(*pages: dict[str, Any]) -> Any:
    """`side_effect` rendant les pages fournies, puis des pages vides à volonté.

    `compute_zones` enchaîne trois niveaux de zones puis deux niveaux de série :
    une liste figée obligerait chaque test à compter les appels qui ne
    l'intéressent pas.
    """
    restantes = list(pages)

    def _suivante(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return restantes.pop(0) if restantes else _composite_page([])

    return _suivante


async def test_compute_zones_builds_documents_with_deterministic_ids() -> None:
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(_composite_page([_bucket("59", doc_count=42)]))

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(1, 0))) as bulk:
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    documents = bulk.call_args_list[0].args[2]
    assert documents == [
        {
            "_id": "departement:59",
            "niveau": "departement",
            "code": "59",
            "code_parent": None,
            "label": "59",
            "prix_m2_median": 2000.0,
            "prix_m2_p25": 1500.0,
            "prix_m2_p75": 2500.0,
            "nb_mutations": 42,
            "calcule_le": documents[0]["calcule_le"],
        }
    ]


async def test_compute_zones_reads_commune_label_and_parent() -> None:
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(
        _composite_page([]),  # départements
        _composite_page([_bucket("59043", parent="59", label_commune="Bailleul")]),
    )

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(1, 0))) as bulk:
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    document = bulk.call_args_list[0].args[2][0]
    assert document["niveau"] == "commune"
    assert document["code_parent"] == "59"
    assert document["label"] == "Bailleul"


async def test_compute_zones_skips_zones_without_median() -> None:
    """Une zone dont aucune mutation n'a de prix au m² n'a rien à colorer."""
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(
        _composite_page([_bucket("59", median=None), _bucket("62")])
    )

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(1, 0))) as bulk:
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    codes = [document["code"] for document in bulk.call_args_list[0].args[2]]
    assert codes == ["62"]


async def test_compute_zones_follows_composite_pagination() -> None:
    """`composite` n'a pas de plafond de buckets : il se parcourt via `after_key`."""
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(
        _composite_page([_bucket("59")], after_key={"code": "59"}),
        _composite_page([_bucket("62")]),
    )

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(1, 0))) as bulk:
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    codes = [document["code"] for document in bulk.call_args_list[0].args[2]]
    assert codes == ["59", "62"]
    assert client.search.call_args_list[1].kwargs["aggs"]["zones"]["composite"]["after"] == {
        "code": "59"
    }


async def test_compute_zones_aggregates_only_mutations_with_prix_m2() -> None:
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches()

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(0, 0))):
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    query = client.search.call_args_list[0].kwargs["query"]
    assert query == {"bool": {"filter": [{"exists": {"field": "prix_m2"}}]}}


async def test_compute_zones_returns_totals_across_levels() -> None:
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(
        _composite_page([_bucket("59")]),
        _composite_page([_bucket("59043", parent="59", label_commune="Bailleul")]),
    )

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(1, 0))):
        success, errors = await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    assert (success, errors) == (2, 0)


async def test_zones_are_computed_checks_the_last_level() -> None:
    """Les sections sont le niveau le plus long : leur présence atteste d'un calcul complet."""
    client = AsyncMock()
    client.count.return_value = {"count": 0}

    assert await zones_are_computed(client, "openhexa-dvf-zones") is False
    assert client.count.call_args.kwargs["query"] == {"term": {"niveau": "section"}}

    client.count.return_value = {"count": 1}
    assert await zones_are_computed(client, "openhexa-dvf-zones") is True


async def test_fetch_zones_filters_on_level_and_parent() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [{"_source": {"code": "59043"}}]}}

    zones = await fetch_zones(client, "openhexa-dvf-zones", "commune", code_parent="59")

    assert zones == [{"code": "59043"}]
    assert client.search.call_args.kwargs["query"]["bool"]["filter"] == [
        {"term": {"niveau": "commune"}},
        {"term": {"code_parent": "59"}},
    ]


async def test_fetch_zones_omits_parent_filter_at_top_level() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}

    await fetch_zones(client, "openhexa-dvf-zones", "departement")

    assert client.search.call_args.kwargs["query"]["bool"]["filter"] == [
        {"term": {"niveau": "departement"}}
    ]


def _serie_page(buckets: list[dict[str, Any]]) -> dict[str, Any]:
    return {"aggregations": {"zones": {"buckets": buckets}}}


def _serie_bucket(
    code: str, annee: str, median: float, parent: str | None = None
) -> dict[str, Any]:
    bucket: dict[str, Any] = {
        "key": {"code": code, "annee": annee},
        "doc_count": 120,
        "prix": {"values": {"25.0": 1000.0, "50.0": median, "75.0": 3000.0}},
    }
    if parent is not None:
        bucket["key"]["parent"] = parent
    return bucket


async def test_compute_zones_produit_une_serie_annuelle() -> None:
    client = AsyncMock()
    client.search.side_effect = _sequence_recherches(
        _composite_page([]),  # zones : département, commune, section
        _composite_page([]),
        _composite_page([]),
        _serie_page(
            [_serie_bucket("59", "2021", 1800.0), _serie_bucket("59", "2022", 1950.0)]
        ),
    )

    with patch("app.domain.dvf.zones.bulk_index", new=AsyncMock(return_value=(2, 0))) as bulk:
        await compute_zones(client, "openhexa-dvf", "openhexa-dvf-zones")

    documents = bulk.call_args_list[-1].args[2]
    assert [(d["annee"], d["prix_m2_median"]) for d in documents] == [
        (2021, 1800.0),
        (2022, 1950.0),
    ]
    # `_id` distinct de celui de la zone tous millésimes confondus.
    assert documents[0]["_id"] == "departement:59:2021"


async def test_la_serie_ignore_les_sections_cadastrales() -> None:
    """Trop peu de ventes par section et par an pour une médiane robuste."""
    assert "section" not in NIVEAUX_SERIE


async def test_fetch_zones_exclut_les_points_de_serie() -> None:
    """Sans cette exclusion, la choroplèthe recevrait chaque zone une fois par millésime."""
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}

    await fetch_zones(client, "openhexa-dvf-zones", "departement")

    assert client.search.call_args.kwargs["query"]["bool"]["must_not"] == [
        {"exists": {"field": "annee"}}
    ]


async def test_fetch_serie_ne_retourne_que_les_points_annuels_tries() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [{"_source": {"annee": 2021}}]}}

    points = await fetch_serie(client, "openhexa-dvf-zones", "commune", "59043")

    assert points == [{"annee": 2021}]
    kwargs = client.search.call_args.kwargs
    assert {"exists": {"field": "annee"}} in kwargs["query"]["bool"]["filter"]
    assert kwargs["sort"] == [{"annee": "asc"}]


async def test_fetch_zone_retourne_une_seule_zone_globale() -> None:
    client = AsyncMock()
    client.search.return_value = {
        "hits": {"hits": [{"_source": {"code": "59043", "prix_m2_median": 1800.0}}]}
    }

    zone = await fetch_zone(client, "openhexa-dvf-zones", "commune", "59043")

    assert zone == {"code": "59043", "prix_m2_median": 1800.0}
    # Sans cette exclusion, on pourrait tomber sur un point de série annuelle.
    assert client.search.call_args.kwargs["query"]["bool"]["must_not"] == [
        {"exists": {"field": "annee"}}
    ]


async def test_fetch_zone_retourne_none_si_inconnue() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}

    assert await fetch_zone(client, "openhexa-dvf-zones", "commune", "00000") is None
