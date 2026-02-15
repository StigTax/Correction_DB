from __future__ import annotations

from unittest.mock import Mock

import pytest

from corrector import Operation, SchemaCorrector


pytestmark = [pytest.mark.unit]


def test_operation_is_immutable():
    """Проверяет, что Operation является неизменяемым dataclass."""
    op = Operation(kind='report', sql='-- no-op', comment='x')

    with pytest.raises(Exception):
        # frozen=True -> попытка изменить поле должна упасть
        op.kind = 'create_table'  # type: ignore[misc]


def test_apply_timeouts_noop_for_non_postgres(tmp_path):
    """Проверяет, что таймауты не выставляются для не-PostgreSQL диалекта."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'

    corrector = SchemaCorrector(
        source_url=src,
        target_url=tgt,
        lock_timeout_seconds=10,
        statement_timeout_seconds=10,
    )

    conn = Mock()
    corrector._apply_timeouts(conn)

    conn.execute.assert_not_called()


def test_q_quotes_identifier_for_sqlite(tmp_path):
    """Проверяет, что _q() использует quoting диалекта target."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'

    corrector = SchemaCorrector(
        source_url=src,
        target_url=tgt,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    quoted = corrector._q('users')
    assert quoted in {'"users"', 'users'}


def test_qt_includes_schema_when_provided(tmp_path):
    """Проверяет, что _qt() добавляет схему, если она задана."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'

    corrector = SchemaCorrector(
        source_url=src,
        target_url=tgt,
        schema='public',
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    table = corrector._qt('users')

    assert 'public' in table
    assert 'users' in table
    assert '.' in table
