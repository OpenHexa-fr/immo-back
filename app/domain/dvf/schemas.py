"""Modèles Pydantic du domaine DVF (Données de Valeurs Foncières)."""

from __future__ import annotations

from openhexa_core.models import BaseDocument, BasePaginatedResponse
from pydantic import BaseModel, model_validator


class GeoPoint(BaseModel):
    lat: float
    lon: float


class BBox(BaseModel):
    """Emprise rectangulaire, dans l'ordre GeoJSON (`min_lon, min_lat, max_lon, max_lat`).

    C'est l'ordre rendu par `map.getBounds().toArray()` côté MapLibre, donc
    celui attendu du frontend sans réarrangement.
    """

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @model_validator(mode="after")
    def _check_bounds(self) -> BBox:
        if not (-180 <= self.min_lon <= 180 and -180 <= self.max_lon <= 180):
            raise ValueError("longitude hors de [-180, 180]")
        if not (-90 <= self.min_lat <= 90 and -90 <= self.max_lat <= 90):
            raise ValueError("latitude hors de [-90, 90]")
        if self.min_lat > self.max_lat:
            raise ValueError("min_lat doit être inférieure à max_lat")
        # `min_lon > max_lon` est volontairement toléré : c'est le cas légitime
        # d'une emprise qui franchit l'antiméridien, qu'Elasticsearch sait
        # interpréter tel quel.
        return self

    @classmethod
    def parse(cls, raw: str) -> BBox:
        """Parse la forme `min_lon,min_lat,max_lon,max_lat` reçue en query param."""
        parts = raw.split(",")
        if len(parts) != 4:
            raise ValueError("bbox attend 4 valeurs : min_lon,min_lat,max_lon,max_lat")
        try:
            min_lon, min_lat, max_lon, max_lat = (float(part) for part in parts)
        except ValueError as error:
            raise ValueError("bbox attend 4 nombres décimaux") from error
        return cls(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


class DVFTransaction(BaseDocument):
    id_mutation: str
    date_mutation: str
    valeur_fonciere: float
    surface_reelle_bati: int | None = None
    surface_terrain: int | None = None
    nombre_pieces_principales: int | None = None
    type_local: str | None = None
    commune: str
    code_postal: str | None = None
    code_departement: str | None = None
    code_commune: str | None = None
    code_section: str | None = None
    id_parcelle: str | None = None
    adresse: str | None = None
    prix_m2: float | None = None
    location: GeoPoint | None = None
    etiquette_dpe: str | None = None


class DVFSearchParams(BaseModel):
    commune: str | None = None
    code_postal: str | None = None
    id_parcelle: str | None = None
    type_local: list[str] | None = None
    valeur_fonciere_min: float | None = None
    valeur_fonciere_max: float | None = None
    surface_min: int | None = None
    surface_max: int | None = None
    etiquette_dpe: list[str] | None = None
    date_mutation_min: str | None = None
    date_mutation_max: str | None = None
    lat: float | None = None
    lon: float | None = None
    radius_km: float = 10.0
    # Une emprise rectangulaire prime sur le couple `lat`/`lon` + `radius_km`
    # pour restreindre les résultats : `lat`/`lon` restent alors utilisables
    # pour trier par distance. Voir `_build_dvf_query`.
    bbox: BBox | None = None
    tri: str | None = None


class DVFSearchResponse(BasePaginatedResponse[DVFTransaction]):
    pass


class PrixCarteBucket(BaseModel):
    """Agrégat de prix médian au m² pour une zone (département, commune ou section)."""

    code: str
    label: str
    prix_m2_median: float
    nb_mutations: int


class PrixCarteResponse(BaseModel):
    niveau: str
    buckets: list[PrixCarteBucket]
