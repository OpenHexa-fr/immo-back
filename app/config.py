"""Configuration de l'API Immo : paramètres Elasticsearch (via core) + sources de données."""

from __future__ import annotations

from functools import lru_cache

from openhexa_core.config import ESSettings
from pydantic_settings import SettingsConfigDict


class Settings(ESSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    dvf_data_url: str = "https://files.data.gouv.fr/geo-dvf/latest/csv/"
    dpe_data_url: str = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/"
    sitadel_data_url: str = "https://files.data.gouv.fr/sitadel/latest/csv/"


@lru_cache
def get_settings() -> Settings:
    return Settings()
