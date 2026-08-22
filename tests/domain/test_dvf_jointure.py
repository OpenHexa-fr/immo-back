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

    assert _meilleur_dpe(diagnostics, "2024-01-01") == ("D", False)


def test_meilleur_dpe_accepte_un_posterieur_proche_a_defaut_d_anterieur() -> None:
    """Fréquent sur 2021-2022, avant la généralisation du DPE : aucun diagnostic
    antérieur n'existe. Le repli sur un DPE postérieur proche est marqué
    explicitement, pour que l'interface affiche la réserve."""
    diagnostics = [{"etiquette_dpe": "A", "date_etablissement": "2024-06-01"}]  # +152 jours

    assert _meilleur_dpe(diagnostics, "2024-01-01") == ("A", True)


def test_meilleur_dpe_rejette_un_posterieur_hors_fenetre() -> None:
    """Au-delà de 18 mois, le risque de rénovation entre-temps n'est plus négligeable."""
    diagnostics = [{"etiquette_dpe": "A", "date_etablissement": "2025-08-01"}]  # +578 jours

    assert _meilleur_dpe(diagnostics, "2024-01-01") is None


def test_meilleur_dpe_prefere_toujours_l_anterieur_au_posterieur() -> None:
    diagnostics = [
        {"etiquette_dpe": "A", "date_etablissement": "2024-03-01"},  # postérieur proche
        {"etiquette_dpe": "D", "date_etablissement": "2023-02-10"},  # antérieur
    ]

    assert _meilleur_dpe(diagnostics, "2024-01-01") == ("D", False)


def test_meilleur_dpe_retient_le_posterieur_le_plus_proche() -> None:
    diagnostics = [
        {"etiquette_dpe": "A", "date_etablissement": "2024-02-01"},
        {"etiquette_dpe": "B", "date_etablissement": "2024-08-01"},
    ]

    assert _meilleur_dpe(diagnostics, "2024-01-01") == ("A", True)


def _page(hits: list[dict[str, Any]]) -> dict[str, Any]:
    return {"hits": {"hits": hits}}


def _client_avec_pit() -> AsyncMock:
    client = AsyncMock()
    client.open_point_in_time.return_value = {"id": "pit-1"}
    return client


async def test_joindre_dpe_met_a_jour_les_mutations_rapprochees() -> None:
    client = _client_avec_pit()
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


async def test_joindre_dpe_marque_le_repli_posterieur() -> None:
    """Le doc écrit doit porter la réserve quand aucun DPE antérieur n'existe."""
    client = _client_avec_pit()
    client.search.side_effect = [
        _page(
            [
                {
                    "_id": "m1",
                    "sort": [1],
                    "_source": {
                        "identifiant_ban": "11069_0550_00025",
                        "date_mutation": "2021-11-18",
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
                        "date_etablissement": "2022-03-01",  # postérieur, dans la fenêtre
                    }
                }
            ]
        ),
        _page([]),
    ]

    with patch("app.domain.dvf.jointure.async_bulk", new=AsyncMock(return_value=(1, []))) as bulk:
        await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    action = bulk.call_args.args[1][0]
    assert action["doc"] == {"etiquette_dpe": "C", "etiquette_dpe_apres_vente": True}


async def test_joindre_dpe_ne_traite_que_les_mutations_sans_etiquette() -> None:
    """Relancer la jointure ne doit pas refaire le travail déjà accompli."""
    client = _client_avec_pit()
    client.search.side_effect = [_page([])]

    await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    requete = client.search.call_args.kwargs["query"]
    assert {"exists": {"field": "identifiant_ban"}} in requete["bool"]["filter"]
    assert {"exists": {"field": "etiquette_dpe"}} in requete["bool"]["must_not"]


async def test_joindre_dpe_ecarte_les_diagnostics_mal_geocodes() -> None:
    client = _client_avec_pit()
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


async def test_le_parcours_utilise_un_point_in_time() -> None:
    """Sans PIT, `search_after` dérive : la jointure écrit dans l'index qu'elle lit.

    Constaté en production : le parcours s'est arrêté à 7,8 M de mutations sur
    12,5 M éligibles.
    """
    client = _client_avec_pit()
    client.search.side_effect = [_page([])]

    await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    client.open_point_in_time.assert_awaited_once()
    params = client.search.call_args.kwargs
    assert params["pit"]["id"] == "pit-1"
    assert params["sort"] == [{"_shard_doc": "asc"}]
    # Un PIT non refermé retient des segments : le nettoyage doit être garanti.
    client.close_point_in_time.assert_awaited_once()


async def test_une_mutation_sans_dpe_n_est_pas_une_erreur() -> None:
    """Le second membre du retour compte les erreurs bulk réelles, pas les
    mutations examinées.

    Deux contrats erronés ont précédé celui-ci : `examinées - rapprochées` puis
    `examinées` telles quelles, tous deux lus comme un décompte d'erreurs par
    l'ordonnanceur — qui déclarait le job en échec à chaque passage alors
    qu'aucune écriture n'échouait réellement.
    """
    client = _client_avec_pit()
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
        _page([]),  # aucun DPE a cette adresse
        _page([]),
    ]

    rapproches, erreurs = await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    # Aucune écriture n'a été tentée (aucun DPE trouvé) : zéro erreur, pas 1.
    assert (rapproches, erreurs) == (0, 0)


async def test_le_decompte_d_erreurs_ne_gonfle_pas_avec_le_volume_examine() -> None:
    """Verrou direct contre le bug constaté en production : un grand nombre de
    mutations examinées sans correspondance ne doit jamais se traduire par un
    grand nombre d'« erreurs » signalées à l'ordonnanceur."""
    client = _client_avec_pit()
    # Beaucoup de mutations, aucune ne trouve de DPE correspondant.
    gros_lot = [
        {
            "_id": f"m{i}",
            "sort": [i],
            "_source": {"identifiant_ban": f"id{i}", "date_mutation": "2024-01-01"},
        }
        for i in range(500)
    ]
    client.search.side_effect = [_page(gros_lot), _page([]), _page([])]

    _, erreurs = await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    assert erreurs == 0


async def test_les_echecs_bulk_reels_sont_comptes() -> None:
    """Une vraie erreur d'écriture Elasticsearch doit, elle, remonter."""
    client = _client_avec_pit()
    client.search.side_effect = [
        _page(
            [
                {
                    "_id": "m1",
                    "sort": [1],
                    "_source": {"identifiant_ban": "x", "date_mutation": "2024-03-01"},
                }
            ]
        ),
        _page(
            [
                {
                    "_source": {
                        "identifiant_ban": "x",
                        "etiquette_dpe": "C",
                        "date_etablissement": "2023-01-01",
                    }
                }
            ]
        ),
        _page([]),
    ]

    with patch(
        "app.domain.dvf.jointure.async_bulk",
        new=AsyncMock(return_value=(0, [{"update": {"error": "version_conflict"}}])),
    ):
        rapproches, erreurs = await joindre_dpe(client, "openhexa-dvf", "openhexa-dpe")

    assert (rapproches, erreurs) == (0, 1)
