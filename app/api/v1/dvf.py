"""Routes DVF (Données de Valeurs Foncières)."""

from __future__ import annotations

from typing import Any, Literal

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from openhexa_core.elasticsearch.client import get_client
from openhexa_core.pagination import decode_cursor

from app.config import Settings, get_settings
from app.domain.dvf.schemas import (
    BBox,
    DVFSearchParams,
    DVFSearchResponse,
    DVFTransaction,
    ParcelleMutation,
    ParcelleResponse,
    PrixCarteBucket,
    PrixCarteResponse,
    PrixSeriePoint,
    PrixSerieResponse,
    ZoneResponse,
)
from app.domain.dvf.search import (
    CARTE_SOURCE_FIELDS,
    aggregate_prix_carte,
    get_dvf_by_mutation,
    get_parcelle_mutations,
    search_dvf,
)
from app.domain.dvf.zones import fetch_serie, fetch_zone, fetch_zones

router = APIRouter(prefix="/dvf", tags=["dvf"])

Categorie = Literal["bati", "terrain"]

_DESCRIPTION_CATEGORIE = (
    "`bati` (défaut) agrège les ventes de biens construits, `terrain` celles de "
    "terrains nus. Les deux marchés ne se comparent pas au m²."
)


def _aplatir(zone: dict[str, Any], categorie: Categorie) -> dict[str, Any] | None:
    """Ramène un document de zone à la forme exposée, pour la catégorie demandée.

    `None` quand la catégorie n'a pas de médiane sur cette zone — une commune
    peut n'avoir connu que des ventes de terrain, ou l'inverse.
    """
    median = zone.get(f"prix_m2_median_{categorie}")
    if median is None:
        return None
    return {
        "code": zone["code"],
        "label": zone.get("label", zone["code"]),
        "prix_m2_median": median,
        "prix_m2_p25": zone.get(f"prix_m2_p25_{categorie}"),
        "prix_m2_p75": zone.get(f"prix_m2_p75_{categorie}"),
        "nb_mutations": zone.get(f"nb_mutations_{categorie}", 0),
        "calcule_le": zone.get("calcule_le"),
        "annee": zone.get("annee"),
    }


def _variation_pct(depuis: float | None, vers: float | None) -> float | None:
    """Variation relative en pourcentage, arrondie au dixième.

    `None` si l'une des bornes manque ou si la référence est nulle — une
    variation depuis zéro n'a pas de sens.
    """
    if depuis is None or vers is None or depuis == 0:
        return None
    return round((vers - depuis) / depuis * 100, 1)


async def _es_client() -> AsyncElasticsearch:
    return await get_client()


@router.get("/search", response_model=DVFSearchResponse)
async def search(
    commune: str | None = None,
    code_postal: str | None = None,
    id_parcelle: str | None = None,
    type_local: list[str] | None = Query(None),
    valeur_fonciere_min: float | None = None,
    valeur_fonciere_max: float | None = None,
    surface_min: int | None = None,
    surface_max: int | None = None,
    etiquette_dpe: list[str] | None = Query(None),
    date_mutation_min: str | None = None,
    date_mutation_max: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_km: float = 10.0,
    bbox: str | None = Query(
        None,
        description="Emprise `min_lon,min_lat,max_lon,max_lat` ; prime sur lat/lon+radius_km.",
    ),
    tri: str | None = None,
    champs: Literal["complet", "carte"] = Query(
        "complet",
        description="`carte` ne rapatrie que les champs nécessaires à l'affichage cartographique.",
    ),
    cursor: str | None = Query(
        None, description="Curseur opaque de page suivante, renvoyé tel quel."
    ),
    size: int = 20,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> DVFSearchResponse:
    """Recherche des transactions DVF."""
    try:
        parsed_bbox = BBox.parse(bbox) if bbox else None
        search_after = decode_cursor(cursor) if cursor else None
    except ValueError as error:
        raise HTTPException(400, str(error)) from error

    params = DVFSearchParams(
        commune=commune,
        code_postal=code_postal,
        id_parcelle=id_parcelle,
        type_local=type_local,
        valeur_fonciere_min=valeur_fonciere_min,
        valeur_fonciere_max=valeur_fonciere_max,
        surface_min=surface_min,
        surface_max=surface_max,
        etiquette_dpe=etiquette_dpe,
        date_mutation_min=date_mutation_min,
        date_mutation_max=date_mutation_max,
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        bbox=parsed_bbox,
        tri=tri,
    )
    index = f"{settings.es_index_prefix}-dvf"
    page = await search_dvf(
        client,
        index,
        params,
        search_after=search_after,
        size=size,
        source=CARTE_SOURCE_FIELDS if champs == "carte" else None,
    )

    items = [DVFTransaction.model_validate(hit["_source"]) for hit in page["hits"]]
    return DVFSearchResponse(
        items=items,
        total=page["total"],
        total_relation=page["total_relation"],
        next_cursor=page["next_cursor"],
    )


@router.get("/prix-carte", response_model=PrixCarteResponse)
async def prix_carte(
    niveau: Literal["departement", "commune", "section"],
    code_departement: str | None = None,
    code_commune: str | None = None,
    categorie: Categorie = Query("bati", description=_DESCRIPTION_CATEGORIE),
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> PrixCarteResponse:
    """Prix médian au m² par zone, pour la choroplèthe de la carte.

    Servi depuis l'index de zones pré-agrégé, alimenté à l'issue de chaque
    ingestion DVF. Tant que ce calcul n'a jamais tourné (premier déploiement,
    index vide), on retombe sur l'agrégation à la volée : la carte reste
    fonctionnelle, simplement plus lente.

    Chaque niveau de zoom doit être filtré par le niveau parent pour éviter
    d'agréger la France entière en une seule requête.
    """
    if niveau == "commune" and not code_departement:
        raise HTTPException(400, "code_departement est requis pour niveau=commune")
    if niveau == "section" and not code_commune:
        raise HTTPException(400, "code_commune est requis pour niveau=section")

    code_parent = code_departement if niveau == "commune" else code_commune
    zones = await fetch_zones(
        client, f"{settings.es_index_prefix}-dvf-zones", niveau, code_parent=code_parent
    )
    aplaties = [z for z in (_aplatir(zone, categorie) for zone in zones) if z is not None]

    if not aplaties:
        # Repli tant que les zones n'ont jamais été calculées. Il ne distingue
        # pas les catégories : la carte reste fonctionnelle, simplement moins
        # précise, le temps qu'une ingestion passe.
        aplaties = await aggregate_prix_carte(
            client,
            f"{settings.es_index_prefix}-dvf",
            niveau,
            code_departement=code_departement,
            code_commune=code_commune,
        )

    return PrixCarteResponse(
        niveau=niveau,
        categorie=categorie,
        buckets=[PrixCarteBucket.model_validate(bucket) for bucket in aplaties],
        calcule_le=next((bucket.get("calcule_le") for bucket in aplaties), None),
    )


@router.get("/prix-serie", response_model=PrixSerieResponse)
async def prix_serie(
    niveau: Literal["departement", "commune"],
    code: str,
    categorie: Categorie = Query("bati", description=_DESCRIPTION_CATEGORIE),
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> PrixSerieResponse:
    """Évolution annuelle du prix médian au m² d'une zone.

    La choroplèthe affiche une médiane tous millésimes confondus, qui écrase le
    mouvement du marché. Cette série le rend visible.

    Les sections cadastrales n'en ont pas : trop peu de ventes par section et
    par an pour qu'une médiane annuelle veuille dire quelque chose.
    """
    zones = await fetch_serie(client, f"{settings.es_index_prefix}-dvf-zones", niveau, code)
    bruts = [z for z in (_aplatir(zone, categorie) for zone in zones) if z is not None]

    points: list[PrixSeriePoint] = []
    precedent: float | None = None
    for brut in bruts:
        point = PrixSeriePoint.model_validate(brut)
        # Les points sont triés par millésime croissant : chacun se compare au
        # précédent, le premier n'ayant rien à quoi se comparer.
        point.evolution_pct = _variation_pct(precedent, point.prix_m2_median)
        precedent = point.prix_m2_median
        points.append(point)

    return PrixSerieResponse(
        niveau=niveau,
        categorie=categorie,
        code=code,
        label=next((brut.get("label") for brut in bruts), None),
        points=points,
        evolution_totale_pct=(
            _variation_pct(points[0].prix_m2_median, points[-1].prix_m2_median)
            if len(points) > 1
            else None
        ),
    )


@router.get("/zone", response_model=ZoneResponse)
async def zone(
    niveau: Literal["departement", "commune", "section"],
    code: str,
    categorie: Categorie = Query("bati", description=_DESCRIPTION_CATEGORIE),
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> ZoneResponse:
    """Agrégat global d'une zone, pour situer une vente dans son marché local."""
    trouvee = await fetch_zone(client, f"{settings.es_index_prefix}-dvf-zones", niveau, code)
    aplatie = _aplatir(trouvee, categorie) if trouvee else None
    if aplatie is None:
        raise HTTPException(404, f"zone {niveau}:{code} sans agrégat {categorie}")
    return ZoneResponse(niveau=niveau, categorie=categorie, **{
        k: v for k, v in aplatie.items() if k in ZONE_FIELDS
    })


ZONE_FIELDS = {"code", "label", "prix_m2_median", "prix_m2_p25", "prix_m2_p75", "nb_mutations"}


# Déclarée avant `/{id_mutation}` : FastAPI résout les routes dans l'ordre de
# déclaration, et le paramètre attrape-tout capterait sinon `/parcelle/...`.
@router.get("/parcelle/{id_parcelle}", response_model=ParcelleResponse)
async def parcelle(
    id_parcelle: str,
    size: int = 50,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> ParcelleResponse:
    """Historique des ventes d'une parcelle, groupées par mutation."""
    index = f"{settings.es_index_prefix}-dvf"
    mutations = await get_parcelle_mutations(client, index, id_parcelle, size=size)
    return ParcelleResponse(
        id_parcelle=id_parcelle,
        mutations=[ParcelleMutation.model_validate(mutation) for mutation in mutations],
    )


@router.get("/{id_mutation}", response_model=list[DVFTransaction])
async def get_by_mutation(
    id_mutation: str,
    client: AsyncElasticsearch = Depends(_es_client),
    settings: Settings = Depends(get_settings),
) -> list[DVFTransaction]:
    """Retourne les lots d'une mutation DVF donnée (fiche bien)."""
    index = f"{settings.es_index_prefix}-dvf"
    hits = await get_dvf_by_mutation(client, index, id_mutation)
    return [DVFTransaction.model_validate(hit["_source"]) for hit in hits]
