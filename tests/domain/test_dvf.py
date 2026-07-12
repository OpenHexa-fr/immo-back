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
        "numero_disposition": "000001",
        "lot1_numero": "001",
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
        "numero_disposition": "000001",
        "lot1_numero": None,
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


_RAW_CSV_HEADER = (
    b"id_mutation,numero_disposition,id_parcelle,code_type_local,date_mutation,"
    b"valeur_fonciere,surface_reelle_bati,type_local,nom_commune,code_postal,"
    b"lot1_numero,longitude,latitude"
)


def test_parse_dvf_csv_keeps_only_known_columns() -> None:
    raw_csv = (
        _RAW_CSV_HEADER
        + b",extra_column\n"
        + b"2024-1,000001,010760000B0514,1,2024-01-15,250000.0,80,Appartement,Marseille,"
        b"13001,001,5.37,43.29,ignored\n"
    )

    dataframe = parse_dvf_csv(raw_csv)

    assert "extra_column" not in dataframe.columns
    assert dataframe.shape[0] == 1


def test_parse_dvf_csv_decompresses_gzip_input() -> None:
    import gzip

    raw_csv = (
        _RAW_CSV_HEADER + b"\n"
        b"2024-1,000001,010760000B0514,1,2024-01-15,250000.0,80,Appartement,Marseille,"
        b"13001,001,5.37,43.29\n"
    )

    dataframe = parse_dvf_csv(gzip.compress(raw_csv))

    assert dataframe.shape[0] == 1


def test_parse_dvf_csv_disambiguates_identical_rows_via_occurrence_index() -> None:
    row = (
        b"2025-1,1,33193000AD0001,1,2025-01-07,243596.0,,,Bordeaux,33000,,-0.567,44.84\n"
    )
    raw_csv = _RAW_CSV_HEADER + b"\n" + row * 3

    dataframe = parse_dvf_csv(raw_csv)
    documents = [_row_to_document(r) for r in dataframe.iter_rows(named=True)]
    ids = [d["_id"] for d in documents]

    assert len(set(ids)) == 3


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
