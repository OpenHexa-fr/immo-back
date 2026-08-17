"""Tests des middlewares de l'application."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import Request, Response

from app.main import add_cache_headers


def _request(
    path: str, method: str = "GET", headers: list[tuple[bytes, bytes]] | None = None
) -> Request:
    return Request(
        scope={
            "type": "http",
            "method": method,
            "path": path,
            "query_string": b"",
            "headers": headers or [],
            "scheme": "http",
            "server": ("testserver", 80),
        }
    )


def _call_next(status_code: int = 200) -> Callable[[Request], Awaitable[Response]]:
    async def call_next(_: Request) -> Response:
        return Response(status_code=status_code)

    return call_next


async def test_search_responses_are_cachable() -> None:
    response = await add_cache_headers(_request("/api/v1/dvf/search"), _call_next())

    assert response.headers["Cache-Control"] == (
        "public, max-age=3600, stale-while-revalidate=86400"
    )


def _with_version(version: str | None) -> Any:
    return patch.multiple(
        "app.main",
        es_client=AsyncMock(),
        data_version=SimpleNamespace(get=AsyncMock(return_value=version)),
    )


async def test_dvf_responses_carry_an_etag() -> None:
    with _with_version("2026-08-17T18:00:00Z"):
        response = await add_cache_headers(_request("/api/v1/dvf/search"), _call_next())

    assert response.headers["ETag"].startswith('W/"')


async def test_matching_etag_returns_304_without_querying_elasticsearch() -> None:
    """Tout l'intérêt de l'ETag ici : la revalidation n'exécute aucune recherche."""
    with _with_version("2026-08-17T18:00:00Z"):
        first = await add_cache_headers(_request("/api/v1/dvf/search"), _call_next())
        etag = first.headers["ETag"]

        called = False

        async def call_next(_: Request) -> Response:
            nonlocal called
            called = True
            return Response(status_code=200)

        second = await add_cache_headers(
            _request("/api/v1/dvf/search", headers=[(b"if-none-match", etag.encode())]),
            call_next,
        )

    assert second.status_code == 304
    assert called is False
    assert second.headers["Cache-Control"] == "public, max-age=3600, stale-while-revalidate=86400"


async def test_stale_etag_is_ignored() -> None:
    with _with_version("2026-08-17T18:00:00Z"):
        response = await add_cache_headers(
            _request("/api/v1/dvf/search", headers=[(b"if-none-match", b'W/"perime"')]),
            _call_next(),
        )

    assert response.status_code == 200


async def test_dpe_responses_carry_no_etag() -> None:
    """La version des données ne suit que les ingestions DVF."""
    with _with_version("2026-08-17T18:00:00Z"):
        response = await add_cache_headers(_request("/api/v1/dpe/search"), _call_next())

    assert "ETag" not in response.headers
    assert response.headers["Cache-Control"] == "public, max-age=3600, stale-while-revalidate=86400"


async def test_missing_version_disables_the_etag_but_keeps_caching() -> None:
    with _with_version(None):
        response = await add_cache_headers(_request("/api/v1/dvf/search"), _call_next())

    assert "ETag" not in response.headers
    assert response.headers["Cache-Control"].startswith("public")


async def test_status_is_never_cached() -> None:
    """Le bandeau de synchronisation du frontend interroge `/status` en boucle."""
    response = await add_cache_headers(_request("/api/v1/status"), _call_next())

    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("status_code", [400, 404, 500])
async def test_error_responses_are_not_cached(status_code: int) -> None:
    response = await add_cache_headers(
        _request("/api/v1/dvf/search"), _call_next(status_code)
    )

    assert "Cache-Control" not in response.headers
