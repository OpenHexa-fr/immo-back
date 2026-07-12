"""Tests du domaine Sitadel (permis de construire)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from app.domain.sitage.ingestion import _row_to_document, parse_sitadel_csv
from app.domain.sitage.schemas import SitadelSearchParams
from app.domain.sitage.search import _build_sitadel_query, search_sitadel


def test_row_to_document_builds_deterministic_id() -> None:
    row = {
        "NUM_DAU": "00100113V0003",
        "DATE_REELLE_AUTORISATION": "2013-09-20",
        "TYPE_DAU": "PC",
        "COMM": "01001",
        "ADR_LOCALITE_TER": "L ABERGEMENT-CLEMENCIAT",
        "ADR_CODPOST_TER": "01400",
        "NB_LGT_TOT_CREES": 1,
        "SURF_HAB_CREEE": 90.0,
    }

    document = _row_to_document(row)

    assert document["_id"] == _row_to_document(row)["_id"]
    assert len(document["_id"]) == 16
    assert document["numero_permis"] == "00100113V0003"
    assert document["commune"] == "L ABERGEMENT-CLEMENCIAT"
    assert document["location"] is None


def test_row_to_document_falls_back_to_commune_code_when_locality_missing() -> None:
    row = {
        "NUM_DAU": "00100113V0004",
        "DATE_REELLE_AUTORISATION": "2013-09-21",
        "TYPE_DAU": "DP",
        "COMM": "01001",
        "ADR_LOCALITE_TER": None,
        "ADR_CODPOST_TER": None,
        "NB_LGT_TOT_CREES": None,
        "SURF_HAB_CREEE": None,
    }

    document = _row_to_document(row)

    assert document["commune"] == "01001"
    assert document["nombre_logements"] is None


def test_row_to_document_disambiguates_dp_and_pc_sharing_num_dau() -> None:
    dp_row = {
        "NUM_DAU": "01309825000010",
        "DATE_REELLE_AUTORISATION": "2025-03-17",
        "TYPE_DAU": "DP",
        "COMM": "13098",
        "ADR_LOCALITE_TER": "SAINT-MITRE LES REMPARTS",
        "ADR_CODPOST_TER": "13920",
        "NB_LGT_TOT_CREES": 1,
        "SURF_HAB_CREEE": 0.0,
    }
    pc_row = {**dp_row, "TYPE_DAU": "PC", "DATE_REELLE_AUTORISATION": "2025-08-05"}

    assert _row_to_document(dp_row)["_id"] != _row_to_document(pc_row)["_id"]


def test_parse_sitadel_csv_reads_semicolon_delimited_columns() -> None:
    raw_csv = (
        b'"NUM_DAU";"DATE_REELLE_AUTORISATION";"TYPE_DAU";"COMM";"ADR_LOCALITE_TER";'
        b'"ADR_CODPOST_TER";"NB_LGT_TOT_CREES";"SURF_HAB_CREEE";"EXTRA_COLUMN"\n'
        b'"00100113V0003";"2013-09-20";"PC";"01001";"L ABERGEMENT-CLEMENCIAT";"01400";1;90.0;'
        b'"ignored"\n'
    )

    dataframe = parse_sitadel_csv(raw_csv)

    assert "EXTRA_COLUMN" not in dataframe.columns
    assert dataframe.shape[0] == 1


def test_parse_sitadel_csv_keeps_comm_and_codpost_as_string() -> None:
    # COMM (code INSEE) et ADR_CODPOST_TER sont numériques dans le CSV réel :
    # sans schema_overrides explicite, polars les infère en entier, tronquant
    # le zéro initial (même bug que sur DVF, voir test_dvf.py).
    raw_csv = (
        b'"NUM_DAU";"DATE_REELLE_AUTORISATION";"TYPE_DAU";"COMM";"ADR_LOCALITE_TER";'
        b'"ADR_CODPOST_TER";"NB_LGT_TOT_CREES";"SURF_HAB_CREEE"\n'
        b'"00100113V0003";"2013-09-20";"PC";01001;"L ABERGEMENT-CLEMENCIAT";01400;1;90.0\n'
    )

    dataframe = parse_sitadel_csv(raw_csv)
    row = next(iter(dataframe.iter_rows(named=True)))

    assert row["COMM"] == "01001"
    assert row["ADR_CODPOST_TER"] == "01400"


def test_build_sitadel_query_returns_match_all_without_filters() -> None:
    assert _build_sitadel_query(SitadelSearchParams()) == {"match_all": {}}


def test_build_sitadel_query_filters_on_type_permis() -> None:
    query = _build_sitadel_query(SitadelSearchParams(type_permis=["PC", "DP"]))

    assert {"terms": {"type_permis": ["PC", "DP"]}} in query["bool"]["filter"]


async def test_search_sitadel_calls_paginate_with_built_query() -> None:
    client = AsyncMock()
    client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    result = await search_sitadel(
        client, "openhexa-sitage", SitadelSearchParams(commune="Lyon")
    )

    assert result["total"] == 0
    client.search.assert_called_once()
