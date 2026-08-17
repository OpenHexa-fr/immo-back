"""Tests de la validation conditionnelle (ETag)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from app.http_cache import DataVersion, build_etag


def test_same_version_and_url_give_the_same_etag() -> None:
    a = build_etag("2026-08-17T18:00:00Z", "/api/v1/dvf/search", "commune=Lille")
    b = build_etag("2026-08-17T18:00:00Z", "/api/v1/dvf/search", "commune=Lille")

    assert a == b
    assert a.startswith('W/"')


def test_etag_changes_with_the_data_version() -> None:
    """Une nouvelle ingestion doit invalider les réponses mises en cache."""
    before = build_etag("2026-08-17T18:00:00Z", "/api/v1/dvf/search", "commune=Lille")
    after = build_etag("2026-08-24T18:00:00Z", "/api/v1/dvf/search", "commune=Lille")

    assert before != after


def test_etag_changes_with_the_query() -> None:
    assert build_etag("v1", "/api/v1/dvf/search", "commune=Lille") != build_etag(
        "v1", "/api/v1/dvf/search", "commune=Lyon"
    )


def test_etag_changes_with_the_path() -> None:
    assert build_etag("v1", "/api/v1/dvf/search", "") != build_etag(
        "v1", "/api/v1/dvf/prix-carte", ""
    )


async def test_data_version_is_fetched_once_within_its_ttl() -> None:
    version = DataVersion(ttl_seconds=60)
    with patch(
        "app.http_cache.latest_calcule_le", new=AsyncMock(return_value="2026-08-17")
    ) as fetch:
        assert await version.get(AsyncMock(), "openhexa-dvf-zones") == "2026-08-17"
        assert await version.get(AsyncMock(), "openhexa-dvf-zones") == "2026-08-17"

    fetch.assert_awaited_once()


async def test_data_version_refetches_once_expired() -> None:
    version = DataVersion(ttl_seconds=0)
    with patch(
        "app.http_cache.latest_calcule_le", new=AsyncMock(return_value="2026-08-17")
    ) as fetch:
        await version.get(AsyncMock(), "openhexa-dvf-zones")
        await version.get(AsyncMock(), "openhexa-dvf-zones")

    assert fetch.await_count == 2


async def test_unreachable_cluster_disables_the_etag_rather_than_failing() -> None:
    """Une version indisponible doit dégrader le cache, pas casser la requête."""
    version = DataVersion()
    with patch("app.http_cache.latest_calcule_le", new=AsyncMock(side_effect=RuntimeError)):
        assert await version.get(AsyncMock(), "openhexa-dvf-zones") is None


async def test_zones_never_computed_yield_no_version() -> None:
    version = DataVersion()
    with patch("app.http_cache.latest_calcule_le", new=AsyncMock(return_value=None)):
        assert await version.get(AsyncMock(), "openhexa-dvf-zones") is None
