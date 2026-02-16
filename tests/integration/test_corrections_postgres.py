from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine, inspect, text

from corrector import Operation, SchemaCorrector


pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def _qualified(schema: str, table_name: str) -> str:
    """Возвращает schema-qualified имя таблицы для SQL."""
    return f'"{schema}"."{table_name}"'


def _reflect_tables(engine, schema: str) -> set[str]:
    """Возвращает множество таблиц в указанной схеме."""
    insp = inspect(engine)
    return set(insp.get_table_names(schema=schema))


def _reflect_columns(engine, schema: str, table_name: str) -> set[str]:
    """Возвращает множество колонок таблицы в указанной схеме."""
    insp = inspect(engine)
    return {c['name'] for c in insp.get_columns(table_name, schema=schema)}


def _is_nullable(
    engine,
    schema: str,
    table_name: str,
    column_name: str
) -> bool:
    """Проверяет nullable колонки (с учётом схемы)."""
    insp = inspect(engine)
    cols = insp.get_columns(table_name, schema=schema)
    col = next(c for c in cols if c['name'] == column_name)
    return bool(col.get('nullable', True))


def _count_rows(engine, schema: str, table_name: str) -> int:
    """Считает строки в schema-qualified таблице."""
    with engine.connect() as conn:
        return int(conn.execute(text(
            f'SELECT COUNT(*) FROM {_qualified(schema, table_name)}'
        )).scalar())


def _get_user_legacy(engine, schema: str, user_id: int) -> str | None:
    """Достаёт legacy пользователя из schema-qualified users."""
    with engine.connect() as conn:
        return conn.execute(
            text(
                f'SELECT legacy FROM {_qualified(schema, "users")} '
                'WHERE id = :id'
            ),
            {'id': user_id},
        ).scalar()


def test_diff_plans_expected_ops_and_reports(prepared_postgres_dbs, caplog):
    """Проверяет diff() на PostgreSQL: план + отчёты + warning по nullable."""
    src_url, tgt_url, schema = prepared_postgres_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        schema=schema,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    caplog.set_level(logging.WARNING)
    ops = corrector.diff()

    kinds = {op.kind for op in ops}
    assert 'create_table' in kinds
    assert 'add_column' in kinds
    assert 'add_foreign_key' in kinds
    assert 'report' in kinds

    assert any(
        op.kind == 'add_column' and op.comment == 'Add column users.age'
        for op in ops
    )
    assert any(
        op.kind == 'create_table' and op.comment == 'Create table orders'
        for op in ops
    )
    assert any(
        op.kind == 'report'
        and 'EXTRA: table exists only in target: notes' in op.comment
        for op in ops
    )
    assert any(
        op.kind == 'report'
        and 'EXTRA: column exists only in target: users.legacy' in op.comment
        for op in ops
    )

    assert any(
        'nullable mismatch users.email' in rec.message
        for rec in caplog.records
    )


@pytest.mark.dry_run
def test_apply_dry_run_does_not_modify_target(prepared_postgres_dbs, capsys):
    """Проверяет, что dry_run не меняет PostgreSQL target."""
    src_url, tgt_url, schema = prepared_postgres_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        schema=schema,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    ops = corrector.diff()
    corrector.apply(ops, dry_run=True)

    out = capsys.readouterr().out
    assert 'ALTER TABLE' in out or 'CREATE TABLE' in out

    tgt_engine = create_engine(tgt_url)
    assert 'orders' not in _reflect_tables(tgt_engine, schema)
    assert 'age' not in _reflect_columns(tgt_engine, schema, 'users')

    assert _count_rows(tgt_engine, schema, 'notes') == 1
    assert _get_user_legacy(tgt_engine, schema, 1) == 'keep-me'


def test_apply_executes_safe_ops_and_preserves_data(prepared_postgres_dbs):
    """Проверяет применение safe-операций и сохранность данных в Postgres."""
    src_url, tgt_url, schema = prepared_postgres_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        schema=schema,
        lock_timeout_seconds=1,
        statement_timeout_seconds=1,
    )

    ops = corrector.diff()
    safe_ops = [op for op in ops if op.kind != 'report']

    corrector.apply(safe_ops, dry_run=False)

    tgt_engine = create_engine(tgt_url)

    assert 'orders' in _reflect_tables(tgt_engine, schema)
    assert 'age' in _reflect_columns(tgt_engine, schema, 'users')

    order_fks = inspect(tgt_engine).get_foreign_keys('orders', schema=schema)

    assert _is_nullable(tgt_engine, schema, 'users', 'email') is True

    assert any(
        fk.get('referred_table') == 'users'
        and fk.get('constrained_columns') == ['user_id']
        and fk.get('referred_columns') == ['id']
        for fk in order_fks
    )

    user_indexes = inspect(tgt_engine).get_indexes('users', schema=schema)
    order_indexes = inspect(tgt_engine).get_indexes('orders', schema=schema)

    assert any(i.get('name') == 'ix_users_email' for i in user_indexes)
    assert any(i.get('name') == 'ix_orders_user_id' for i in order_indexes)

    assert 'notes' in _reflect_tables(tgt_engine, schema)
    assert 'legacy' in _reflect_columns(tgt_engine, schema, 'users')

    assert _count_rows(tgt_engine, schema, 'notes') == 1
    assert _get_user_legacy(tgt_engine, schema, 1) == 'keep-me'


@pytest.mark.logging
def test_apply_logs_error_and_critical_on_failure(
    prepared_postgres_dbs,
    caplog
):
    """Проверяет, что при ошибке apply() пишет error/critical на Postgres."""
    _, tgt_url, schema = prepared_postgres_dbs

    corrector = SchemaCorrector(
        source_url=tgt_url,
        target_url=tgt_url,
        schema=schema,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    caplog.set_level(logging.ERROR)

    bad_ops = [
        Operation(kind='add_column', sql='THIS IS BAD SQL;', comment='boom')
    ]

    with pytest.raises(Exception):
        corrector.apply(bad_ops, dry_run=False)

    assert any('Apply failed' in rec.message for rec in caplog.records)
    assert any(
        'Schema correction aborted' in rec.message for rec in caplog.records
    )
