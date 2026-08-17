"""Tests du job d'ingestion hors process web."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.jobs.ingest import SOURCES, _parse_args, run


def test_all_expands_to_every_source_with_zones_after_dvf() -> None:
    """`zones` dérive de `dvf` : le recalcul doit suivre l'ingestion, pas la précéder."""
    sources = _parse_args(["--source", "all"])

    assert sources == list(SOURCES)
    assert sources.index("dvf") < sources.index("zones")


def test_sources_are_repeatable_and_ordered_as_given() -> None:
    assert _parse_args(["--source", "dvf", "--source", "zones"]) == ["dvf", "zones"]


def test_unknown_source_is_rejected() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--source", "carburants"])


async def test_run_succeeds_and_closes_the_client() -> None:
    with (
        patch("app.jobs.ingest.get_client", new=AsyncMock()),
        patch("app.jobs.ingest.close_client", new=AsyncMock()) as close,
        patch("app.jobs.ingest.ensure_indices", new=AsyncMock()),
        patch("app.jobs.ingest.compute_zones", new=AsyncMock(return_value=(10, 0))),
    ):
        exit_code = await run(["zones"])

    assert exit_code == 0
    close.assert_awaited_once()


async def test_run_reports_failure_when_a_source_raises() -> None:
    with (
        patch("app.jobs.ingest.get_client", new=AsyncMock()),
        patch("app.jobs.ingest.close_client", new=AsyncMock()),
        patch("app.jobs.ingest.ensure_indices", new=AsyncMock()),
        patch("app.jobs.ingest.ingest_dvf_years", new=AsyncMock(side_effect=RuntimeError)),
        patch("app.jobs.ingest.compute_zones", new=AsyncMock(return_value=(10, 0))) as zones,
    ):
        exit_code = await run(["dvf", "zones"])

    # Une source en échec ne prive pas les suivantes, mais le cycle est signalé
    # comme raté à l'ordonnanceur.
    assert exit_code == 1
    zones.assert_awaited_once()


async def test_run_reports_failure_on_bulk_errors() -> None:
    """Des documents rejetés à l'indexation sont un échec, même sans exception."""
    with (
        patch("app.jobs.ingest.get_client", new=AsyncMock()),
        patch("app.jobs.ingest.close_client", new=AsyncMock()),
        patch("app.jobs.ingest.ensure_indices", new=AsyncMock()),
        patch("app.jobs.ingest.compute_zones", new=AsyncMock(return_value=(10, 3))),
    ):
        assert await run(["zones"]) == 1


async def test_run_closes_the_client_even_when_indices_fail() -> None:
    with (
        patch("app.jobs.ingest.get_client", new=AsyncMock()),
        patch("app.jobs.ingest.close_client", new=AsyncMock()) as close,
        patch("app.jobs.ingest.ensure_indices", new=AsyncMock(side_effect=RuntimeError)),
        pytest.raises(RuntimeError),
    ):
        await run(["zones"])

    close.assert_awaited_once()
