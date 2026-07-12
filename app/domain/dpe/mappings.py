"""Mapping Elasticsearch explicite du domaine DPE."""

from typing import Any

from openhexa_core.elasticsearch.mappings import DATE, FLOAT, KEYWORD

DPE_MAPPING: dict[str, Any] = {
    "properties": {
        "numero_dpe": KEYWORD,
        "date_etablissement": DATE,
        "etiquette_dpe": KEYWORD,
        "etiquette_ges": KEYWORD,
        "commune": KEYWORD,
        "code_postal": KEYWORD,
        "surface_habitable": FLOAT,
    }
}
