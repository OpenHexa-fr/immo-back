"""Mapping Elasticsearch explicite du domaine DVF."""

from typing import Any

from openhexa_core.elasticsearch.mappings import DATE, FLOAT, GEO_POINT, INTEGER, KEYWORD

DVF_MAPPING: dict[str, Any] = {
    "properties": {
        "id_mutation": KEYWORD,
        "date_mutation": DATE,
        "valeur_fonciere": FLOAT,
        "surface_reelle_bati": INTEGER,
        "type_local": KEYWORD,
        "commune": KEYWORD,
        "code_postal": KEYWORD,
        "location": GEO_POINT,
        "etiquette_dpe": KEYWORD,
    }
}
