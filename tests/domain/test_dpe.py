"""Tests du domaine DPE."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from app.domain.dpe.ingestion import _row_to_document, fetch_dpe_pages
from app.domain.dpe.schemas import DPESearchParams
from app.domain.dpe.search import _build_dpe_query, search_dpe


def test_row_to_document_builds_deterministic_id() -> None:
    row = {
        "numero_dpe": "2400E0000001A",
        "date_etablissement_dpe": "2024-01-15",
        "etiquette_dpe": "C",
        "etiquette_ges": "B",
        "nom_commune_ban": "Lyon",
        "code_postal_ban": "69001",
        "surface_habitable_logement": 65.0,
    }

    document = _row_to_document(row)

    assert len(document["_id"]) == 16
    assert document["etiquette_dpe"] == "C"


def test_build_dpe_query_returns_match_all_without_filters() -> None:
    assert _build_dpe_query(DPESearchParams()) == {"match_all": {}}


def test_build_dpe_query_filters_on_etiquette() -> None:
    query = _build_dpe_query(DPESearchParams(etiquette_dpe=["A", "B"]))

    assert {"terms": {"etiquette_dpe": ["A", "B"]}} in query["bool"]["filter"]


async def test_search_dpe_calls_paginate_with_built_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    result = await search_dpe(client, "openhexa-dpe", DPESearchParams(commune="Lyon"))

    assert result["total"] == 0
    client.search.assert_called_once()


async def test_fetch_dpe_pages_stops_on_empty_page(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeResponse:
        def __init__(self, results: list[dict[str, str]]) -> None:
            self._results = results

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, list[dict[str, str]]]:
            return {"results": self._results}

    call_count = {"n": 0}

    async def fake_get(
        self: httpx.AsyncClient, url: str, params: dict[str, int]
    ) -> _FakeResponse:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _FakeResponse([{"numero_dpe": "1"}])
        return _FakeResponse([])

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    pages = [page async for page in fetch_dpe_pages("https://example.test/dataset")]

    assert len(pages) == 1
    assert pages[0][0]["numero_dpe"] == "1"
