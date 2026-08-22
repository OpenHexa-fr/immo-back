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
successifs d'un même logement) : on retient par priorité le plus récent
antérieur à la mutation (fiable, décrit l'état au moment de la vente).

Si aucun DPE antérieur n'existe — fréquent sur les millésimes 2021-2022, avant
la généralisation du DPE — on accepte, à titre de repli, un DPE postérieur
proche (fenêtre de `_FENETRE_POST_VENTE_JOURS`, 18 mois : le risque de
rénovation entre-temps y reste limité). Ce repli est marqué explicitement
(`etiquette_dpe_apres_vente`) pour que l'interface affiche la réserve plutôt
que de présenter l'étiquette comme une certitude.
"""

from __future__ import annotations

import contextlib
from datetime import date
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

# Fenêtre de tolérance pour un DPE postérieur à la vente, en dernier recours.
# 18 mois : assez court pour qu'une rénovation majeure entre-temps reste
# l'exception plutôt que la norme.
_FENETRE_POST_VENTE_JOURS = 548


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


def _parse_date(brut: object) -> date | None:
    if not brut:
        return None
    try:
        return date.fromisoformat(str(brut)[:10])
    except ValueError:
        return None


def _meilleur_dpe(
    diagnostics: list[dict[str, Any]], date_mutation: str
) -> tuple[str, bool] | None:
    """Étiquette retenue pour la mutation, et `True` si elle vient d'un DPE postérieur.

    Priorité au diagnostic antérieur (ou le jour même) le plus récent — c'est
    celui qui décrit fidèlement l'état du bien au moment de la vente. À défaut,
    on se rabat sur le diagnostic postérieur le plus proche, dans la fenêtre de
    tolérance : mieux vaut une étiquette marquée incertaine que pas d'étiquette
    du tout, tant que le doute reste visible pour l'utilisateur.
    """
    date_vente = _parse_date(date_mutation)
    if date_vente is None:
        return None

    anterieurs: list[tuple[date, dict[str, Any]]] = []
    posterieurs_proches: list[tuple[date, dict[str, Any]]] = []
    for diagnostic in diagnostics:
        date_dpe = _parse_date(diagnostic.get("date_etablissement"))
        if date_dpe is None:
            continue
        if date_dpe <= date_vente:
            anterieurs.append((date_dpe, diagnostic))
        elif (date_dpe - date_vente).days <= _FENETRE_POST_VENTE_JOURS:
            posterieurs_proches.append((date_dpe, diagnostic))

    if anterieurs:
        _, retenu = max(anterieurs, key=lambda paire: paire[0])
        etiquette = retenu.get("etiquette_dpe")
        return (str(etiquette), False) if etiquette else None

    if posterieurs_proches:
        # Le plus proche de la vente, donc le moins susceptible d'être
        # précédé d'une rénovation.
        _, retenu = min(posterieurs_proches, key=lambda paire: paire[0])
        etiquette = retenu.get("etiquette_dpe")
        return (str(etiquette), True) if etiquette else None

    return None


async def joindre_dpe(
    client: AsyncElasticsearch, dvf_index: str, dpe_index: str
) -> tuple[int, int]:
    """Renseigne `etiquette_dpe` sur les mutations DVF rapprochables.

    Retourne `(rapprochées, erreurs_bulk)`. `erreurs_bulk` compte les échecs
    réels d'écriture Elasticsearch — pas les mutations examinées sans
    correspondance, qui sont le cas normal d'un bien sans diagnostic connu à son
    adresse. Deux contrats erronés ont précédé celui-ci :
    - `(rapprochées, examinées - rapprochées)`, où l'ordonnanceur lisait le
      second membre comme un décompte d'erreurs et faisait échouer le job à
      chaque passage, alors qu'aucune écriture n'échouait réellement ;
    - `(rapprochées, examinées)`, qui reproduisait exactement le même défaut
      puisque `examinées` est presque toujours non nul.

    Le parcours s'appuie sur un **point-in-time**. Sans lui, `search_after` sur
    `_doc` dérive : la jointure écrit dans l'index qu'elle parcourt, chaque mise
    à jour étant une suppression suivie d'une réinsertion, et l'ordre des
    documents se réorganise sous le curseur. Constaté en production : le
    parcours s'est interrompu après 7,8 M de mutations sur 12,5 M éligibles.
    """
    rapproches = 0
    examines = 0
    erreurs_bulk = 0
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
                resultat = _meilleur_dpe(candidats, str(source.get("date_mutation") or ""))
                if resultat is None:
                    continue
                etiquette, posterieur = resultat
                doc: dict[str, Any] = {"etiquette_dpe": etiquette}
                # Uniquement écrit quand pertinent : les documents déjà
                # rapprochés via un DPE antérieur (avant ce lot) n'ont pas ce
                # champ, ce qui équivaut à `False` côté lecture.
                if posterieur:
                    doc["etiquette_dpe_apres_vente"] = True
                actions.append(
                    {
                        "_op_type": "update",
                        "_index": dvf_index,
                        "_id": hit["_id"],
                        "doc": doc,
                    }
                )

            if actions:
                succes, erreurs = await async_bulk(client, actions, raise_on_error=False)
                rapproches += succes
                # `async_bulk` renvoie soit la liste des erreurs, soit leur
                # nombre, selon `stats_only` — même normalisation que
                # `core.bulk_index`.
                nb_erreurs = len(erreurs) if isinstance(erreurs, list) else int(erreurs)
                erreurs_bulk += nb_erreurs
                if nb_erreurs:
                    logger.warning(
                        "dvf_dpe_jointure_erreurs",
                        erreurs=nb_erreurs,
                        # Journalisé pour diagnostiquer un écart déjà observé
                        # entre ce compteur et l'état réel de l'index (mesuré
                        # via requête directe) : `succes` a valu 0 sur une
                        # exécution entière malgré une progression réelle
                        # constatée côté Elasticsearch.
                        exemple=erreurs[0] if isinstance(erreurs, list) and erreurs else None,
                    )
    finally:
        with contextlib.suppress(Exception):
            await client.close_point_in_time(id=pit_id)

    logger.info(
        "dvf_dpe_jointure_terminee",
        examines=examines,
        rapproches=rapproches,
        erreurs_bulk=erreurs_bulk,
    )
    return rapproches, erreurs_bulk
