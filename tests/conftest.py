"""Fixtures partagées pour les tests de immo-back."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _es_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ES_URL", "http://localhost:9200")
    monkeypatch.setenv("ES_USER", "elastic")
    monkeypatch.setenv("ES_PASSWORD", "changeme")
