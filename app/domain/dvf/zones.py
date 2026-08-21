"""Pré-agrégation des prix DVF par zone (département, commune, section).

La choroplèthe de la carte affiche un prix médian au m² par zone. Le calculer à
la volée revenait à relancer, à chaque affichage, une agrégation sur l'index DVF
entier — pour un résultat qui ne change qu'entre deux ingestions. Ce module le
calcule une fois, à l'issue de l'ingestion, dans un index dédié que la carte se
contente ensuite de lire.

Deux différences avec l'agrégation à la volée qu'il remplace :

- l'agrégation utilise un `composite` et non un `terms` plafonné à 5 000
  buckets. Les ~35 000 communes françaises dépassaient largement ce plafond, et
  un `terms` en environnement distribué ne garantit de toute façon pas
  l'exactitude des `doc_count` ; le `composite` pagine et reste exact ;
- les quartiles (p25/p75) sont calculés en plus de la médiane, gratuitement,
  puisqu'ils sortent de la même agrégation `percentiles`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.ingestion import bulk_index

logger = structlog.get_logger(__name__)

_COMPOSITE_PAGE_SIZE = 1000
_BULK_CHUNK_SIZE = 5000

# Nombre de zones renvoyées à la carte pour un parent donné. Majorant réel : le
# département comptant le plus de communes en compte moins de 900, une commune
# quelques centaines de sections cadastrales, et la France 101 départements.
_MAX_ZONES_PER_PARENT = 5000

_LEVELS: dict[str, dict[str, str | None]] = {
    "departement": {"code_field": "code_departement", "parent_field": None, "label_field": None},
    "commune": {
        "code_field": "code_commune",
        "parent_field": "code_departement",
        "label_field": "commune",
    },
    # `code_section` = les 10 premiers caractères d'`id_parcelle` (cf. ingestion).
    "section": {"code_field": "code_section", "parent_field": "code_commune", "label_field": None},
}

NIVEAUX = tuple(_LEVELS)

# Niveaux pour lesquels une série annuelle est calculée. Les sections
# cadastrales en sont exclues à dessein : quelques ventes par section et par an
# ne donnent pas une médiane robuste, et le volume exploserait (des centaines de
# milliers de sections multipliées par le nombre de millésimes).
NIVEAUX_SERIE = ("departement", "commune")


def _zones_query() -> dict[str, Any]:
    """Seules les mutations dont le prix au m² est calculable alimentent une médiane."""
    return {"bool": {"filter": [{"exists": {"field": "prix_m2"}}]}}


async def _iter_composite_buckets(
    client: AsyncElasticsearch,
    index: str,
    sources: list[dict[str, Any]],
    aggs: dict[str, Any],
) -> AsyncIterator[dict[str, Any]]:
    """Itère tous les buckets d'une agrégation `composite`, page par page.

    Contrairement à `terms`, `composite` n'a pas de plafond de buckets : il se
    parcourt via `after_key` jusqu'à épuisement.
    """
    after: dict[str, Any] | None = None
    while True:
        composite: dict[str, Any] = {"sources": sources, "size": _COMPOSITE_PAGE_SIZE}
        if after is not None:
            composite["after"] = after

        response = await client.search(
            index=index,
            query=_zones_query(),
            size=0,
            aggs={"zones": {"composite": composite, "aggs": aggs}},
        )
        aggregation = response["aggregations"]["zones"]
        buckets = aggregation["buckets"]
        if not buckets:
            return
        for bucket in buckets:
            yield bucket

        after = aggregation.get("after_key")
        if after is None:
            return


async def _iter_zone_documents(
    client: AsyncElasticsearch, dvf_index: str, niveau: str, calcule_le: str
) -> AsyncIterator[dict[str, Any]]:
    level = _LEVELS[niveau]
    code_field, parent_field, label_field = (
        level["code_field"],
        level["parent_field"],
        level["label_field"],
    )

    sources: list[dict[str, Any]] = []
    if parent_field is not None:
        sources.append({"parent": {"terms": {"field": parent_field}}})
    sources.append({"code": {"terms": {"field": code_field}}})

    aggs: dict[str, Any] = {
        "prix": {"percentiles": {"field": "prix_m2", "percents": [25, 50, 75]}}
    }
    if label_field is not None:
        # Le nom de la commune n'existe pas comme clé d'agrégation (seul son code
        # INSEE en est une) : on le lit sur un document du bucket.
        aggs["label"] = {"top_hits": {"size": 1, "_source": [label_field]}}

    async for bucket in _iter_composite_buckets(client, dvf_index, sources, aggs):
        values = bucket["prix"]["values"]
        median = values["50.0"]
        if median is None:
            continue

        code = bucket["key"]["code"]
        label = code
        if label_field is not None:
            hits = bucket["label"]["hits"]["hits"]
            if hits:
                label = hits[0]["_source"].get(label_field, code)

        yield {
            # Déterministe : un recalcul écrase la zone au lieu de la dupliquer.
            "_id": f"{niveau}:{code}",
            "niveau": niveau,
            "code": code,
            "code_parent": bucket["key"].get("parent"),
            "label": label,
            "prix_m2_median": round(median, 2),
            "prix_m2_p25": round(values["25.0"], 2) if values["25.0"] is not None else None,
            "prix_m2_p75": round(values["75.0"], 2) if values["75.0"] is not None else None,
            "nb_mutations": bucket["doc_count"],
            "calcule_le": calcule_le,
        }


async def _iter_serie_documents(
    client: AsyncElasticsearch, dvf_index: str, niveau: str, calcule_le: str
) -> AsyncIterator[dict[str, Any]]:
    """Documents d'agrégat par zone **et par millésime**, pour la série temporelle.

    La carte affiche une médiane tous millésimes confondus, ce qui écrase
    justement l'information la plus parlante : entre 2021 et 2025, le marché
    s'est retourné. Une dimension annuelle suffit à la faire apparaître.
    """
    level = _LEVELS[niveau]
    code_field, parent_field, label_field = (
        level["code_field"],
        level["parent_field"],
        level["label_field"],
    )

    sources: list[dict[str, Any]] = []
    if parent_field is not None:
        sources.append({"parent": {"terms": {"field": parent_field}}})
    sources.append({"code": {"terms": {"field": code_field}}})
    sources.append(
        {
            "annee": {
                "date_histogram": {
                    "field": "date_mutation",
                    "calendar_interval": "year",
                    "format": "yyyy",
                }
            }
        }
    )

    aggs: dict[str, Any] = {
        "prix": {"percentiles": {"field": "prix_m2", "percents": [25, 50, 75]}}
    }
    if label_field is not None:
        aggs["label"] = {"top_hits": {"size": 1, "_source": [label_field]}}

    async for bucket in _iter_composite_buckets(client, dvf_index, sources, aggs):
        values = bucket["prix"]["values"]
        median = values["50.0"]
        if median is None:
            continue

        code = bucket["key"]["code"]
        annee = int(str(bucket["key"]["annee"])[:4])
        label = code
        if label_field is not None:
            hits = bucket["label"]["hits"]["hits"]
            if hits:
                label = hits[0]["_source"].get(label_field, code)

        yield {
            "_id": f"{niveau}:{code}:{annee}",
            "niveau": niveau,
            "code": code,
            "annee": annee,
            "code_parent": bucket["key"].get("parent"),
            "label": label,
            "prix_m2_median": round(median, 2),
            "prix_m2_p25": round(values["25.0"], 2) if values["25.0"] is not None else None,
            "prix_m2_p75": round(values["75.0"], 2) if values["75.0"] is not None else None,
            "nb_mutations": bucket["doc_count"],
            "calcule_le": calcule_le,
        }


async def _indexer_par_lots(
    client: AsyncElasticsearch,
    zones_alias: str,
    documents: AsyncIterator[dict[str, Any]],
) -> tuple[int, int]:
    """Indexe un flux de documents par lots bornés.

    Les sections cadastrales se comptent en centaines de milliers : la mémoire
    du process ne doit pas dépendre du nombre de zones. `bulk_index` n'accepte
    qu'un itérable synchrone, d'où l'accumulation explicite.
    """
    success = 0
    errors = 0
    chunk: list[dict[str, Any]] = []
    async for document in documents:
        chunk.append(document)
        if len(chunk) >= _BULK_CHUNK_SIZE:
            chunk_success, chunk_errors = await bulk_index(client, zones_alias, chunk)
            success += chunk_success
            errors += chunk_errors
            chunk = []
    if chunk:
        chunk_success, chunk_errors = await bulk_index(client, zones_alias, chunk)
        success += chunk_success
        errors += chunk_errors
    return success, errors


async def compute_zones(
    client: AsyncElasticsearch, dvf_index: str, zones_alias: str
) -> tuple[int, int]:
    """Recalcule les agrégats de prix par zone à partir de l'index DVF.

    Les documents portant un `_id` déterministe, un recalcul met à jour les zones
    existantes sans passer par une suppression préalable : la carte reste servie
    pendant toute la durée du calcul. Une zone qui disparaîtrait des données
    sources resterait en revanche présente jusqu'au prochain cycle — cas
    théorique, le découpage administratif ne bougeant pas d'une ingestion à
    l'autre.
    """
    calcule_le = datetime.now(UTC).isoformat()
    total_success = 0
    total_errors = 0

    for niveau in NIVEAUX:
        success, errors = await _indexer_par_lots(
            client, zones_alias, _iter_zone_documents(client, dvf_index, niveau, calcule_le)
        )
        total_success += success
        total_errors += errors
        logger.info("dvf_zones_level_computed", niveau=niveau, success=success, errors=errors)

    for niveau in NIVEAUX_SERIE:
        success, errors = await _indexer_par_lots(
            client, zones_alias, _iter_serie_documents(client, dvf_index, niveau, calcule_le)
        )
        total_success += success
        total_errors += errors
        logger.info("dvf_serie_level_computed", niveau=niveau, success=success, errors=errors)

    logger.info("dvf_zones_computed", success=total_success, errors=total_errors)
    return total_success, total_errors


async def zones_are_computed(client: AsyncElasticsearch, zones_index: str) -> bool:
    """True si le calcul des zones est allé à son terme.

    On teste la présence du dernier niveau calculé plutôt que celle d'un
    document quelconque : les sections cadastrales sont de loin le niveau le
    plus long, donc celui qu'un redémarrage de conteneur interrompt. Un niveau
    « sections » lui-même incomplet passerait cette vérification — la prochaine
    ingestion DVF recalculant tout, l'écart se résorbe de lui-même.
    """
    response = await client.count(index=zones_index, query={"term": {"niveau": NIVEAUX[-1]}})
    return int(response["count"]) > 0


async def latest_calcule_le(client: AsyncElasticsearch, zones_index: str) -> str | None:
    """Date du dernier calcul des zones, ou `None` si elles n'ont jamais été calculées.

    Sert de **version des données DVF** : toutes les zones d'un même calcul
    portent le même horodatage, et ce calcul suit chaque ingestion. Deux
    réponses produites sous la même version sont donc identiques, ce qui est
    exactement ce qu'un ETag doit garantir.
    """
    response = await client.search(
        index=zones_index,
        size=1,
        source_includes=["calcule_le"],
        sort=[{"calcule_le": "desc"}],
    )
    hits = response["hits"]["hits"]
    if not hits:
        return None
    calcule_le = hits[0]["_source"].get("calcule_le")
    return str(calcule_le) if calcule_le else None


async def fetch_zones(
    client: AsyncElasticsearch, zones_index: str, niveau: str, code_parent: str | None = None
) -> list[dict[str, Any]]:
    """Lit les zones pré-agrégées d'un niveau, restreintes à leur zone parente."""
    filters: list[dict[str, Any]] = [{"term": {"niveau": niveau}}]
    if code_parent is not None:
        filters.append({"term": {"code_parent": code_parent}})

    response = await client.search(
        index=zones_index,
        # Les points de série temporelle partagent l'index : les exclure évite
        # de renvoyer chaque zone une fois par millésime à la choroplèthe.
        query={"bool": {"filter": filters, "must_not": [{"exists": {"field": "annee"}}]}},
        size=_MAX_ZONES_PER_PARENT,
        sort=[{"code": "asc"}],
    )
    return [hit["_source"] for hit in response["hits"]["hits"]]


async def fetch_zone(
    client: AsyncElasticsearch, zones_index: str, niveau: str, code: str
) -> dict[str, Any] | None:
    """Agrégat global d'une zone unique, tous millésimes confondus.

    Sert à situer une vente par rapport à son marché local sans rapatrier
    l'ensemble des zones voisines, comme le fait la choroplèthe.
    """
    response = await client.search(
        index=zones_index,
        query={
            "bool": {
                "filter": [{"term": {"niveau": niveau}}, {"term": {"code": code}}],
                "must_not": [{"exists": {"field": "annee"}}],
            }
        },
        size=1,
    )
    hits = response["hits"]["hits"]
    return dict(hits[0]["_source"]) if hits else None


async def fetch_serie(
    client: AsyncElasticsearch, zones_index: str, niveau: str, code: str
) -> list[dict[str, Any]]:
    """Série annuelle du prix médian au m² d'une zone, du plus ancien au plus récent."""
    response = await client.search(
        index=zones_index,
        query={
            "bool": {
                "filter": [
                    {"term": {"niveau": niveau}},
                    {"term": {"code": code}},
                    {"exists": {"field": "annee"}},
                ]
            }
        },
        size=100,
        sort=[{"annee": "asc"}],
    )
    return [hit["_source"] for hit in response["hits"]["hits"]]
