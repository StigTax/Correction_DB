from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from corrector import SchemaCorrector


@pytest.fixture()
def sqlite_urls(tmp_path: Path) -> tuple[str, str]:
    """Возвращает DSN для двух SQLite баз во временной директории."""
    src = tmp_path / 'src_unit.db'
    tgt = tmp_path / 'tgt_unit.db'
    return f'sqlite:///{src}', f'sqlite:///{tgt}'


@pytest.fixture()
def make_corrector(sqlite_urls) -> Callable[..., SchemaCorrector]:
    """Фабрика SchemaCorrector для unit-тестов."""
    src_url, tgt_url = sqlite_urls

    def _make(**kwargs) -> SchemaCorrector:
        return SchemaCorrector(
            source_url=kwargs.pop('source_url', src_url),
            target_url=kwargs.pop('target_url', tgt_url),
            schema=kwargs.pop('schema', None),
            lock_timeout_seconds=kwargs.pop('lock_timeout_seconds', 0),
            statement_timeout_seconds=kwargs.pop(
                'statement_timeout_seconds',
                0
            ),
            allow_destructive=kwargs.pop('allow_destructive', False),
            logger=kwargs.pop('logger', None),
        )

    return _make


@pytest.fixture()
def corrector(make_corrector) -> SchemaCorrector:
    """Обычный corrector для unit-тестов."""
    return make_corrector()


@pytest.fixture()
def non_sqlite_corrector(make_corrector, monkeypatch) -> SchemaCorrector:
    """Corrector, который ведёт себя как не-SQLite (для веток кода)."""
    c = make_corrector()
    monkeypatch.setattr(c, '_is_sqlite', lambda: False)
    return c


@pytest.fixture()
def postgres_dialect_corrector(make_corrector, monkeypatch) -> SchemaCorrector:
    """Corrector с postgres dialect.name, без реального PostgreSQL."""
    c = make_corrector(lock_timeout_seconds=2, statement_timeout_seconds=3)
    monkeypatch.setattr(
        c.target_engine.dialect,
        'name',
        'postgresql',
        raising=False
    )
    return c
