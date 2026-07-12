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
