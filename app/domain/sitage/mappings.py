"""Mapping Elasticsearch explicite du domaine Sit@del2."""

from typing import Any

from openhexa_core.elasticsearch.mappings import DATE, FLOAT, GEO_POINT, INTEGER, KEYWORD

SITADEL_MAPPING: dict[str, Any] = {
    "properties": {
        "numero_permis": KEYWORD,
        "date_autorisation": DATE,
        "type_permis": KEYWORD,
        "commune": KEYWORD,
        "code_postal": KEYWORD,
        "nombre_logements": INTEGER,
        "surface_plancher": FLOAT,
        "location": GEO_POINT,
    }
}
