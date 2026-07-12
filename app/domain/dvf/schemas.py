"""Modèles Pydantic du domaine DVF (Données de Valeurs Foncières)."""

from __future__ import annotations

from openhexa_core.models import BaseDocument, BasePaginatedResponse
from pydantic import BaseModel


class GeoPoint(BaseModel):
    lat: float
    lon: float


class DVFTransaction(BaseDocument):
    id_mutation: str
    date_mutation: str
    valeur_fonciere: float
    surface_reelle_bati: int | None = None
    type_local: str | None = None
    commune: str
    code_postal: str
    location: GeoPoint | None = None
    etiquette_dpe: str | None = None


class DVFSearchParams(BaseModel):
    commune: str | None = None
    code_postal: str | None = None
    type_local: list[str] | None = None
    valeur_fonciere_min: float | None = None
    valeur_fonciere_max: float | None = None
    surface_min: int | None = None
    surface_max: int | None = None
    etiquette_dpe: list[str] | None = None
    lat: float | None = None
    lon: float | None = None
    radius_km: float = 10.0
    tri: str | None = None


class DVFSearchResponse(BasePaginatedResponse[DVFTransaction]):
    pass
