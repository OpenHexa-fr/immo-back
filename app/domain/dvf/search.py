"""Requêtes de recherche Elasticsearch pour le domaine DVF."""

from __future__ import annotations

from typing import Any

from elasticsearch import AsyncElasticsearch
from openhexa_core.elasticsearch.search import build_filters, paginate

from app.domain.dvf.schemas import DVFSearchParams


def _build_dvf_query(params: DVFSearchParams) -> dict[str, Any]:
    filters = build_filters(
        commune=params.commune,
        code_postal=params.code_postal,
        id_parcelle=params.id_parcelle,
        type_local=params.type_local,
        etiquette_dpe=params.etiquette_dpe,
    )

    price_range: dict[str, Any] = {}
    if params.valeur_fonciere_min is not None:
        price_range["gte"] = params.valeur_fonciere_min
    if params.valeur_fonciere_max is not None:
        price_range["lte"] = params.valeur_fonciere_max
    if price_range:
        filters.append({"range": {"valeur_fonciere": price_range}})

    surface_range: dict[str, Any] = {}
    if params.surface_min is not None:
        surface_range["gte"] = params.surface_min
    if params.surface_max is not None:
        surface_range["lte"] = params.surface_max
    if surface_range:
        filters.append({"range": {"surface_reelle_bati": surface_range}})

    date_range: dict[str, Any] = {}
    if params.date_mutation_min is not None:
        date_range["gte"] = params.date_mutation_min
    if params.date_mutation_max is not None:
        date_range["lte"] = params.date_mutation_max
    if date_range:
        filters.append({"range": {"date_mutation": date_range}})

    if params.lat is not None and params.lon is not None:
        filters.append(
            {
                "geo_distance": {
                    "distance": f"{params.radius_km}km",
                    "location": {"lat": params.lat, "lon": params.lon},
                }
            }
        )

    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


def _build_dvf_sort(params: DVFSearchParams) -> list[dict[str, Any]]:
    if params.tri == "prix":
        return [{"valeur_fonciere": "asc"}, {"_seq_no": "asc"}]
    if params.tri == "surface":
        return [{"surface_reelle_bati": "desc"}, {"_seq_no": "asc"}]
    if params.tri == "distance" and params.lat is not None and params.lon is not None:
        return [
            {
                "_geo_distance": {
                    "location": {"lat": params.lat, "lon": params.lon},
                    "order": "asc",
                    "unit": "km",
                }
            },
            {"_seq_no": "asc"},
        ]
    # "recent" et "pertinence" (par défaut) : les mutations les plus récentes d'abord.
    return [{"date_mutation": "desc"}, {"_seq_no": "asc"}]


async def search_dvf(
    client: AsyncElasticsearch,
    index: str,
    params: DVFSearchParams,
    search_after: list[Any] | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Recherche des transactions DVF selon `params`, paginée par `search_after`."""
    query = _build_dvf_query(params)
    sort = _build_dvf_sort(params)
    return await paginate(
        client, index=index, query=query, sort=sort, search_after=search_after, size=size
    )


async def get_dvf_by_mutation(
    client: AsyncElasticsearch, index: str, id_mutation: str
) -> list[dict[str, Any]]:
    """Retourne tous les lots (documents) associés à une mutation DVF donnée.

    Une mutation peut porter sur plusieurs lots, donc plusieurs documents
    partagent le même `id_mutation` sans partager le même `_id`.
    """
    query = {"term": {"id_mutation": id_mutation}}
    response = await client.search(index=index, query=query, size=50)
    return list(response["hits"]["hits"])


# Champ de regroupement par niveau de zoom de la carte des prix : la France
# entière au niveau département, puis les communes d'un département donné,
# puis les sections cadastrales d'une commune donnée (`code_section` = les 10
# premiers caractères d'`id_parcelle`, cf. ingestion.py).
_NIVEAU_FIELDS = {
    "departement": "code_departement",
    "commune": "code_commune",
    "section": "code_section",
}

_PRIX_CARTE_AGG_SIZE = 5000


def _build_prix_carte_query(
    code_departement: str | None, code_commune: str | None
) -> dict[str, Any]:
    filters = build_filters(code_departement=code_departement, code_commune=code_commune)
    filters.append({"exists": {"field": "prix_m2"}})
    return {"bool": {"filter": filters}}


async def aggregate_prix_carte(
    client: AsyncElasticsearch,
    index: str,
    niveau: str,
    code_departement: str | None = None,
    code_commune: str | None = None,
) -> list[dict[str, Any]]:
    """Agrège le prix médian au m² par zone (département, commune ou section cadastrale).

    `niveau="commune"` doit être filtré par `code_departement` et
    `niveau="section"` par `code_commune`, sous peine d'agréger la France
    entière en une seule requête.
    """
    field = _NIVEAU_FIELDS[niveau]
    query = _build_prix_carte_query(code_departement, code_commune)

    zone_agg: dict[str, Any] = {
        "terms": {"field": field, "size": _PRIX_CARTE_AGG_SIZE},
        "aggs": {"prix_median": {"percentiles": {"field": "prix_m2", "percents": [50]}}},
    }
    if niveau == "commune":
        # Nom de la commune pour l'affichage (ex. "Bailleul : 2022 €"), la
        # facette d'agrégation ne portant que sur son code INSEE.
        zone_agg["aggs"]["label"] = {"top_hits": {"size": 1, "_source": ["commune"]}}

    response = await client.search(index=index, query=query, aggs={"by_zone": zone_agg}, size=0)

    results: list[dict[str, Any]] = []
    for bucket in response["aggregations"]["by_zone"]["buckets"]:
        median = bucket["prix_median"]["values"]["50.0"]
        if median is None:
            continue
        label = bucket["key"]
        if niveau == "commune":
            hits = bucket["label"]["hits"]["hits"]
            if hits:
                label = hits[0]["_source"].get("commune", label)
        results.append(
            {
                "code": bucket["key"],
                "label": label,
                "prix_m2_median": round(median, 2),
                "nb_mutations": bucket["doc_count"],
            }
        )
    return results
