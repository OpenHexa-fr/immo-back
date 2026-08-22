"""Rapprochement des mutations DVF avec les diagnostics DPE.

`etiquette_dpe` figurait dans le mapping DVF, était filtrable par l'API et
affichée par le frontend — mais n'était jamais peuplée : le filtre ne renvoyait
donc jamais rien et le badge ne s'affichait jamais. Ce module comble ce trou.

La jointure se fait sur l'**identifiant BAN**, que les deux sources permettent
d'obtenir sans géocodage : l'ADEME le publie tel quel (`identifiant_ban`), et
DVF fournit de quoi le reconstruire (`code_commune` + `adresse_code_voie` +
`adresse_numero`, cf. `dvf/ingestion.py`).

Couverture attendue, mesurée sur des extraits réels : la clé est reconstructible
pour ~97 % des mutations de logements (les 3 % restants étant surtout des
suffixes d'adresse ambigus), mais seulement ~65 % des DPE portent un
`identifiant_ban` exploitable — l'ADEME ne géocode pas tout. Une mutation de
logement sur trois restera donc sans étiquette, ce que l'interface doit
présenter comme « non rapproché » et non comme « pas de DPE ».

Une adresse porte souvent plusieurs diagnostics (immeuble entier, diagnostics
successifs d'un même logement) : on retient le plus récent antérieur à la
mutation, faute de pouvoir distinguer les logements entre eux.
"""

from __future__ import annotations

import contextlib
from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

logger = structlog.get_logger(__name__)

_LOT_MUTATIONS = 1000

# Durée de vie du point-in-time, renouvelée à chaque page.
_PIT_DUREE = "10m"
_MAX_DPE_PAR_LOT = 10_000

# En deçà, le géocodage BAN de l'ADEME est trop incertain pour fonder un
# rapprochement (score médian observé : 0,65).
_SCORE_BAN_MINIMUM = 0.5


async def _dpe_par_identifiant(
    client: AsyncElasticsearch, dpe_index: str, identifiants: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Charge les diagnostics des adresses demandées, groupés par identifiant BAN."""
    response = await client.search(
        index=dpe_index,
        size=_MAX_DPE_PAR_LOT,
        source_includes=["identifiant_ban", "etiquette_dpe", "date_etablissement"],
        query={
            "bool": {
                "filter": [
                    {"terms": {"identifiant_ban": identifiants}},
                    {"exists": {"field": "etiquette_dpe"}},
                    {"range": {"score_ban": {"gte": _SCORE_BAN_MINIMUM}}},
                ]
            }
        },
    )

    par_identifiant: dict[str, list[dict[str, Any]]] = {}
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        par_identifiant.setdefault(source["identifiant_ban"], []).append(source)
    return par_identifiant


def _meilleur_dpe(diagnostics: list[dict[str, Any]], date_mutation: str) -> str | None:
    """Étiquette du diagnostic le plus récent antérieur à la mutation.

    Un DPE établi après la vente décrit un logement éventuellement rénové depuis :
    le rattacher à la mutation donnerait une information fausse. À défaut de
    diagnostic antérieur, on ne rapproche pas.
    """
    anterieurs = [
        d
        for d in diagnostics
        if d.get("date_etablissement") and d["date_etablissement"] <= date_mutation
    ]
    if not anterieurs:
        return None
    plus_recent = max(anterieurs, key=lambda d: str(d["date_etablissement"]))
    etiquette = plus_recent.get("etiquette_dpe")
    return str(etiquette) if etiquette else None


async def joindre_dpe(
    client: AsyncElasticsearch, dvf_index: str, dpe_index: str
) -> tuple[int, int]:
    """Renseigne `etiquette_dpe` sur les mutations DVF rapprochables.

    Retourne `(rapprochées, examinées)`. Attention : une mutation examinée sans
    correspondance n'est **pas** une erreur — c'est le cas normal d'un bien sans
    diagnostic connu à son adresse. Une première version retournait
    `(rapprochées, examinées - rapprochées)`, que l'ordonnanceur interprétait
    comme un décompte d'erreurs et qui faisait échouer le job à chaque passage.

    Le parcours s'appuie sur un **point-in-time**. Sans lui, `search_after` sur
    `_doc` dérive : la jointure écrit dans l'index qu'elle parcourt, chaque mise
    à jour étant une suppression suivie d'une réinsertion, et l'ordre des
    documents se réorganise sous le curseur. Constaté en production : le
    parcours s'est interrompu après 7,8 M de mutations sur 12,5 M éligibles.
    """
    rapproches = 0
    examines = 0
    search_after: list[Any] | None = None

    pit = await client.open_point_in_time(index=dvf_index, keep_alive=_PIT_DUREE)
    pit_id = pit["id"]

    try:
        while True:
            requete: dict[str, Any] = {
                "bool": {
                    "filter": [{"exists": {"field": "identifiant_ban"}}],
                    "must_not": [{"exists": {"field": "etiquette_dpe"}}],
                }
            }
            params: dict[str, Any] = {
                "size": _LOT_MUTATIONS,
                "source_includes": ["identifiant_ban", "date_mutation"],
                "query": requete,
                # `_shard_doc` n'est disponible qu'avec un point-in-time, et
                # c'est le seul tri qui garantisse un ordre total stable.
                "sort": [{"_shard_doc": "asc"}],
                "pit": {"id": pit_id, "keep_alive": _PIT_DUREE},
            }
            if search_after is not None:
                params["search_after"] = search_after

            response = await client.search(**params)
            hits = response["hits"]["hits"]
            if not hits:
                break
            examines += len(hits)
            search_after = hits[-1]["sort"]
            pit_id = response.get("pit_id", pit_id)

            identifiants = sorted({h["_source"]["identifiant_ban"] for h in hits})
            diagnostics = await _dpe_par_identifiant(client, dpe_index, identifiants)

            actions = []
            for hit in hits:
                source = hit["_source"]
                candidats = diagnostics.get(source["identifiant_ban"])
                if not candidats:
                    continue
                etiquette = _meilleur_dpe(candidats, str(source.get("date_mutation") or ""))
                if etiquette is None:
                    continue
                actions.append(
                    {
                        "_op_type": "update",
                        "_index": dvf_index,
                        "_id": hit["_id"],
                        "doc": {"etiquette_dpe": etiquette},
                    }
                )

            if actions:
                succes, erreurs = await async_bulk(client, actions, raise_on_error=False)
                rapproches += succes
                # `async_bulk` renvoie soit la liste des erreurs, soit leur
                # nombre, selon `stats_only` — même normalisation que
                # `core.bulk_index`.
                nb_erreurs = len(erreurs) if isinstance(erreurs, list) else int(erreurs)
                if nb_erreurs:
                    logger.warning("dvf_dpe_jointure_erreurs", erreurs=nb_erreurs)
    finally:
        with contextlib.suppress(Exception):
            await client.close_point_in_time(id=pit_id)

    logger.info("dvf_dpe_jointure_terminee", examines=examines, rapproches=rapproches)
    return rapproches, examines
