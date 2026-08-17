"""Tests des middlewares de l'application."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest
from fastapi import Request, Response

from app.main import add_cache_headers


def _request(path: str, method: str = "GET") -> Request:
    return Request(
        scope={
            "type": "http",
            "method": method,
            "path": path,
            "query_string": b"",
            "headers": [],
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
