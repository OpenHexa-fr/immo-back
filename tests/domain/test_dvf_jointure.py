"""Tests du rapprochement DVF ↔ DPE."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

from app.domain.dvf.ingestion import _compute_identifiant_ban
from app.domain.dvf.jointure import _meilleur_dpe, joindre_dpe


def test_identifiant_ban_suit_le_format_de_l_ademe() -> None:
    """Format observé côté ADEME : `11069_0550_00025`."""
    row = {"code_commune": "11069", "adresse_code_voie": "0550", "adresse_numero": 25}

    assert _compute_identifiant_ban(row) == "11069_0550_00025"


def test_identifiant_ban_accepte_un_code_voie_alphanumerique() -> None:
    """Les codes FANTOIR ne sont pas tous numériques (ex. « B078 »)."""
    row = {"code_commune": "01158", "adresse_code_voie": "B078", "adresse_numero": 454}

    assert _compute_identifiant_ban(row) == "01158_B078_00454"


def test_identifiant_ban_traduit_les_suffixes_de_rang() -> None:
    row = {
        "code_commune": "56083",
        "adresse_code_voie": "1650",
        "adresse_numero": 37,
        "adresse_suffixe": "B",
    }

    assert _compute_identifiant_ban(row) == "56083_1650_00037_bis"


def test_identifiant_ban_renonce_sur_un_suffixe_ambigu() -> None:
    """« A » peut désigner un bâtiment comme un rang : mieux vaut ne pas rapprocher."""
    row = {
        "code_commune": "56083",
        "adresse_code_voie": "1650",
        "adresse_numero": 37,
        "adresse_suffixe": "A",
    }

    assert _compute_identifiant_ban(row) is None


def test_identifiant_ban_absent_sans_numero_de_voie() -> None:
    """Cas des terrains nus, qui n'ont de toute façon aucun DPE."""
    row = {"code_commune": "01158", "adresse_code_voie": "B078", "adresse_numero": None}

    assert _compute_identifiant_ban(row) is None


def test_meilleur_dpe_retient_le_plus_recent_anterieur() -> None:
    diagnostics = [
        {"etiquette_dpe": "F", "date_etablissement": "2021-05-01"},
        {"etiquette_dpe": "D", "date_etablissement": "2023-02-10"},
    ]

    assert _meilleur_dpe(diagnostics, "2024-01-01") == "D"


def test_meilleur_dpe_ignore_un_diagnostic_posterieur_a_la_vente() -> None:
    """Un DPE établi après la vente peut décrire un logement rénové depuis."""
    diagnostics = [{"etiquette_dpe": "A", "date_etablissement": "2025-06-01"}]

    assert _meilleur_dpe(diagnostics, "2024-01-01") is None


def _page(hits: list[dict[str, Any]]) -> dict[str, Any]:
    return {"hits": {"hits": hits}}


async def test_joindre_dpe_met_a_jour_les_mutations_rapprochees() -> None:
    client = AsyncMock()
    client.search.side_effect = [
        _page(
            [
                {
                    "_id": "m1",
                    "sort": [1],
                    "_source": {
                        "identifiant_ban": "11069_0550_00025",
                        "date_mutation": "2024-03-01",
                    },
                }
            ]
        ),
        _page(
            [
                {
                    "_source": {
                        "identifiant_ban": "11069_0550_00025",
                        "etiquette_dpe": "C",
                        "date_etablissement": "2023-01-01",
                    }
                }
            ]
        ),
        _page([]),
    ]

    with patch(
        "app.domain.dvf.jointure.async_bulk", new=AsyncMock(return_value=(1, []))
    ) as bulk:
        rapproches, _ = await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    assert rapproches == 1
    action = bulk.call_args.args[1][0]
    assert action["_op_type"] == "update"
    assert action["doc"] == {"etiquette_dpe": "C"}


async def test_joindre_dpe_ne_traite_que_les_mutations_sans_etiquette() -> None:
    """Relancer la jointure ne doit pas refaire le travail déjà accompli."""
    client = AsyncMock()
    client.search.side_effect = [_page([])]

    await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    requete = client.search.call_args.kwargs["query"]
    assert {"exists": {"field": "identifiant_ban"}} in requete["bool"]["filter"]
    assert {"exists": {"field": "etiquette_dpe"}} in requete["bool"]["must_not"]


async def test_joindre_dpe_ecarte_les_diagnostics_mal_geocodes() -> None:
    client = AsyncMock()
    client.search.side_effect = [
        _page(
            [
                {
                    "_id": "m1",
                    "sort": [1],
                    "_source": {"identifiant_ban": "x", "date_mutation": "2024-01-01"},
                }
            ]
        ),
        _page([]),
        _page([]),
    ]

    await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    filtres = client.search.call_args_list[1].kwargs["query"]["bool"]["filter"]
    assert {"range": {"score_ban": {"gte": 0.5}}} in filtres
