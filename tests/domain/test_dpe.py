"""Tests du domaine DPE."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.domain.dpe.ingestion import _row_to_document, fetch_dpe_pages, ingest_dpe
from app.domain.dpe.schemas import DPESearchParams
from app.domain.dpe.search import _build_dpe_query, search_dpe


async def _pages(pages: list[list[dict[str, Any]]]) -> AsyncIterator[list[dict[str, Any]]]:
    for page in pages:
        yield page


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
        def __init__(self, results: list[dict[str, str]], next_url: str | None) -> None:
            self._results = results
            self._next_url = next_url

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"results": self._results, "next": self._next_url}

    call_count = {"n": 0}

    async def fake_get(
        self: httpx.AsyncClient, url: str, params: dict[str, int] | None = None
    ) -> _FakeResponse:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _FakeResponse([{"numero_dpe": "1"}], "https://example.test/dataset/lines?after=1")
        return _FakeResponse([], None)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    pages = [page async for page in fetch_dpe_pages("https://example.test/dataset")]

    assert len(pages) == 1
    assert pages[0][0]["numero_dpe"] == "1"


async def test_fetch_dpe_pages_follows_cursor_beyond_first_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeResponse:
        def __init__(self, results: list[dict[str, str]], next_url: str | None) -> None:
            self._results = results
            self._next_url = next_url

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"results": self._results, "next": self._next_url}

    requested_urls: list[str] = []

    async def fake_get(
        self: httpx.AsyncClient, url: str, params: dict[str, int] | None = None
    ) -> _FakeResponse:
        requested_urls.append(url)
        if len(requested_urls) == 1:
            return _FakeResponse(
                [{"numero_dpe": "1"}], "https://example.test/dataset/lines?after=cursor1"
            )
        if len(requested_urls) == 2:
            return _FakeResponse([{"numero_dpe": "2"}], None)
        return _FakeResponse([], None)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    pages = [page async for page in fetch_dpe_pages("https://example.test/dataset")]

    assert len(pages) == 2
    assert requested_urls[1] == "https://example.test/dataset/lines?after=cursor1"


async def test_ingestion_incrementale_repart_de_la_derniere_reception() -> None:
    """Le dataset dépasse 15 M de lignes : le remoissonner en entier chaque semaine
    dépassait le budget du job planifié."""
    client = AsyncMock()
    client.search.return_value = {
        "hits": {"hits": [{"_source": {"date_reception": "2026-08-10"}}]}
    }

    with patch("app.domain.dpe.ingestion.fetch_dpe_pages") as fetch:
        fetch.return_value = _pages([])
        await ingest_dpe(client, "openhexa-dpe", "https://exemple.test/dataset")

    # Marge de recouvrement de 7 jours : une ligne peut apparaître après coup
    # avec une date de réception antérieure.
    assert fetch.call_args.kwargs["depuis"] == date(2026, 8, 3)


async def test_ingestion_complete_quand_l_index_est_vide() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}

    with patch("app.domain.dpe.ingestion.fetch_dpe_pages") as fetch:
        fetch.return_value = _pages([])
        await ingest_dpe(client, "openhexa-dpe", "https://exemple.test/dataset")

    assert fetch.call_args.kwargs["depuis"] is None


async def test_option_complet_ignore_le_point_de_reprise() -> None:
    """Nécessaire après un élargissement des champs : les documents déjà
    indexés ne sont jamais rétro-complétés."""
    client = AsyncMock()
    client.search.return_value = {
        "hits": {"hits": [{"_source": {"date_reception": "2026-08-10"}}]}
    }

    with patch("app.domain.dpe.ingestion.fetch_dpe_pages") as fetch:
        fetch.return_value = _pages([])
        await ingest_dpe(client, "openhexa-dpe", "https://exemple.test/dataset", complet=True)

    assert fetch.call_args.kwargs["depuis"] is None


def test_row_to_document_extrait_la_cle_ban_et_la_position() -> None:
    document = _row_to_document(
        {
            "numero_dpe": "2226E0123456X",
            "identifiant_ban": "11069_0550_00025",
            "adresse_ban": "25 Rue de Belfort 11000 Carcassonne",
            "score_ban": 0.66,
            "type_batiment": "immeuble",
            "date_reception_dpe": "2026-01-07",
            "_geopoint": "43.21658904532532,2.359097060835481",
        }
    )

    assert document["identifiant_ban"] == "11069_0550_00025"
    assert document["date_reception"] == "2026-01-07"
    assert document["location"] == {"lat": 43.21658904532532, "lon": 2.359097060835481}


def test_row_to_document_tolere_un_geopoint_absent_ou_malforme() -> None:
    """35 % des lignes ne sont pas géocodées par l'ADEME."""
    assert _row_to_document({"numero_dpe": "X"})["location"] is None
    assert _row_to_document({"numero_dpe": "X", "_geopoint": "n/a"})["location"] is None


async def test_la_moisson_ne_demande_que_les_champs_conserves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """145 colonnes exposées, 13 gardées : sans `select`, 94 % du transfert est jeté."""
    vus: dict[str, Any] = {}

    class _Reponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"results": [], "next": None}

    async def faux_get(
        self: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None
    ) -> _Reponse:
        vus.update(params or {})
        return _Reponse()

    monkeypatch.setattr(httpx.AsyncClient, "get", faux_get)
    [page async for page in fetch_dpe_pages("https://exemple.test/dataset")]

    assert "select" in vus
    assert "numero_dpe" in vus["select"]
    # Un champ non conservé n'a rien à faire dans la requête.
    assert "conso_chauffage_generateur_n1_installation_n1" not in vus["select"]


def test_les_champs_selectionnes_couvrent_le_document_produit() -> None:
    """Un champ retiré de la sélection sortirait silencieusement du document."""
    from app.domain.dpe.ingestion import _CHAMPS_UTILES

    document = _row_to_document({champ: "x" for champ in _CHAMPS_UTILES})

    renseignes = {cle for cle, valeur in document.items() if valeur is not None}
    assert renseignes >= {"numero_dpe", "identifiant_ban", "date_reception", "etiquette_dpe"}


def test_le_filtre_combine_date_et_departement() -> None:
    from app.domain.dpe.ingestion import _filtre

    assert _filtre(date(2026, 8, 3), "59") == (
        "date_reception_dpe:[2026-08-03 TO *] AND code_departement_ban:59"
    )
    assert _filtre(None, "2A") == "code_departement_ban:2A"
    assert _filtre(None, None) is None


def test_la_liste_des_departements_couvre_metropole_corse_et_dom() -> None:
    from app.domain.dpe.ingestion import DEPARTEMENTS

    assert "01" in DEPARTEMENTS and "95" in DEPARTEMENTS
    assert "2A" in DEPARTEMENTS and "2B" in DEPARTEMENTS
    assert "20" not in DEPARTEMENTS  # la Corse n'a pas de code 20
    assert "971" in DEPARTEMENTS and "976" in DEPARTEMENTS
    assert len(DEPARTEMENTS) == 101


async def test_la_moisson_complete_se_decoupe_par_departement() -> None:
    """Le débit s'effondre à mesure que le curseur s'enfonce : chaque tranche le remet à zéro."""
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}
    appels: list[str | None] = []

    def _pages(*_args: Any, **kwargs: Any) -> AsyncIterator[list[dict[str, Any]]]:
        appels.append(kwargs.get("departement"))

        async def vide() -> AsyncIterator[list[dict[str, Any]]]:
            return
            yield []

        return vide()

    with patch("app.domain.dpe.ingestion.fetch_dpe_pages", new=_pages):
        await ingest_dpe(client, "openhexa-dpe", "https://exemple.test", complet=True)

    assert len(appels) == 101
    assert appels[0] == "01"


async def test_la_reprise_incrementale_ne_se_decoupe_pas() -> None:
    """Quelques milliers de lignes n'ont rien à gagner à 101 requêtes séparées."""
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": []}}
    appels: list[str | None] = []

    def _pages(*_args: Any, **kwargs: Any) -> AsyncIterator[list[dict[str, Any]]]:
        appels.append(kwargs.get("departement"))

        async def vide() -> AsyncIterator[list[dict[str, Any]]]:
            return
            yield []

        return vide()

    with patch("app.domain.dpe.ingestion.fetch_dpe_pages", new=_pages):
        await ingest_dpe(client, "openhexa-dpe", "https://exemple.test")

    assert appels == [None]
