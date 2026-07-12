"""Tests du domaine DVF."""

from __future__ import annotations

from unittest.mock import AsyncMock

from app.domain.dvf.ingestion import _row_to_document, parse_dvf_csv
from app.domain.dvf.schemas import DVFSearchParams
from app.domain.dvf.search import _build_dvf_query, get_dvf_by_mutation, search_dvf


def test_row_to_document_builds_deterministic_id() -> None:
    row = {
        "id_mutation": "2024-1",
        "date_mutation": "2024-01-15",
        "numero_lot": "001",
        "valeur_fonciere": 250000.0,
        "surface_reelle_bati": 80,
        "type_local": "Appartement",
        "nom_commune": "Marseille",
        "code_postal": "13001",
        "latitude": 43.29,
        "longitude": 5.37,
    }

    document = _row_to_document(row)

    assert document["_id"] == _row_to_document(row)["_id"]
    assert len(document["_id"]) == 16
    assert document["commune"] == "Marseille"
    assert document["location"] == {"lat": 43.29, "lon": 5.37}


def test_row_to_document_handles_missing_location() -> None:
    row = {
        "id_mutation": "2024-2",
        "date_mutation": "2024-01-15",
        "numero_lot": None,
        "valeur_fonciere": 100000.0,
        "surface_reelle_bati": None,
        "type_local": None,
        "nom_commune": "Lyon",
        "code_postal": "69001",
        "latitude": None,
        "longitude": None,
    }

    document = _row_to_document(row)

    assert document["location"] is None


def test_parse_dvf_csv_keeps_only_known_columns() -> None:
    raw_csv = (
        b"id_mutation,date_mutation,valeur_fonciere,surface_reelle_bati,type_local,"
        b"nom_commune,code_postal,numero_lot,longitude,latitude,extra_column\n"
        b"2024-1,2024-01-15,250000.0,80,Appartement,Marseille,13001,001,5.37,43.29,ignored\n"
    )

    dataframe = parse_dvf_csv(raw_csv)

    assert "extra_column" not in dataframe.columns
    assert dataframe.shape[0] == 1


def test_build_dvf_query_returns_match_all_without_filters() -> None:
    assert _build_dvf_query(DVFSearchParams()) == {"match_all": {}}


def test_build_dvf_query_combines_filters_and_range() -> None:
    params = DVFSearchParams(
        commune="Marseille", valeur_fonciere_min=100000, valeur_fonciere_max=300000
    )

    query = _build_dvf_query(params)

    assert {"term": {"commune": "Marseille"}} in query["bool"]["filter"]
    assert {"range": {"valeur_fonciere": {"gte": 100000, "lte": 300000}}} in query["bool"][
        "filter"
    ]


async def test_search_dvf_calls_paginate_with_built_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    result = await search_dvf(client, "openhexa-dvf", DVFSearchParams(commune="Marseille"))

    assert result["total"] == 0
    client.search.assert_called_once()


async def test_get_dvf_by_mutation_filters_on_term_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [{"_source": {"id_mutation": "2024-1"}}]}}

    hits = await get_dvf_by_mutation(client, "openhexa-dvf", "2024-1")

    assert len(hits) == 1
    client.search.assert_called_once_with(
        index="openhexa-dvf", query={"term": {"id_mutation": "2024-1"}}, size=50
    )
