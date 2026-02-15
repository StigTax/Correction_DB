from __future__ import annotations

import logging

import pytest
from sqlalchemy import (
    create_engine,
    inspect,
    text,
)

from corrector import Operation, SchemaCorrector

pytestmark = [pytest.mark.integration, pytest.mark.sqlite]


def _reflect_tables(engine):
    """Возвращает набор имён таблиц в базе данных.

    Args:
        engine: SQLAlchemy Engine, подключённый к БД.

    Returns:
        set[str]: Множество имён таблиц.
    """
    insp = inspect(engine)
    return set(insp.get_table_names())


def _reflect_columns(engine, table_name: str):
    """Возвращает набор имён колонок для указанной таблицы.

    Args:
        engine: SQLAlchemy Engine, подключённый к БД.
        table_name: Имя таблицы.

    Returns:
        set[str]: Множество имён колонок.
    """
    insp = inspect(engine)
    return {c['name'] for c in insp.get_columns(table_name)}


def _is_nullable(
    engine,
    table_name: str,
    column_name: str
) -> bool:
    """Проверяет, допускает ли колонка NULL.

    Args:
        engine: SQLAlchemy Engine, подключённый к БД.
        table_name: Имя таблицы.
        column_name: Имя колонки.

    Returns:
        bool: True, если колонка nullable, иначе False.
    """
    insp = inspect(engine)
    cols = insp.get_columns(table_name)
    col = next(c for c in cols if c['name'] == column_name)
    return bool(col.get('nullable', True))


def _count_rows(engine, table_name: str) -> int:
    """Считает количество строк в таблице.

    Args:
        engine: SQLAlchemy Engine, подключённый к БД.
        table_name: Имя таблицы.

    Returns:
        int: Количество строк.
    """
    with engine.connect() as conn:
        return int(
            conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
        )


def _get_user_legacy(engine, user_id: int) -> str | None:
    """Возвращает значение поля legacy для пользователя.

    Нужен для проверки, что данные в target не повреждаются после применения
    операций коррекции схемы.

    Args:
        engine: SQLAlchemy Engine, подключённый к БД.
        user_id: Идентификатор пользователя.

    Returns:
        str | None: Значение legacy или None, если записи нет.
    """
    with engine.connect() as conn:
        return conn.execute(
            text('SELECT legacy FROM "users" WHERE id = :id'),
            {'id': user_id},
        ).scalar()


def test_diff_plans_create_missing_table_and_column_and_reports_risks(
    prepared_dbs,
    caplog
):
    """Проверяет, что diff() строит корректный план и пишет отчёты.

    Проверяем, что:
    - план содержит создание отсутствующей таблицы;
    - план содержит добавление отсутствующей колонки;
    - есть report-операции по “лишним” сущностям в target;
    - рискованные расхождения (nullable mismatch) логируются как warning.

    Args:
        prepared_dbs: Фикстура с DSN source/target БД.
        caplog: Pytest-фикстура для перехвата логов.
    """
    src_url, tgt_url = prepared_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    caplog.set_level(logging.WARNING)
    ops = corrector.diff()

    kinds = [op.kind for op in ops]

    assert 'create_table' in kinds
    assert 'add_column' in kinds
    assert 'report' in kinds

    assert any(
        op.kind == 'add_column' and op.comment == 'Add column users.age'
        for op in ops
    )

    assert any(
        op.kind == 'create_table'
        and op.comment == 'Create table orders'
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
def test_apply_dry_run_does_not_modify_target(prepared_dbs, capsys):
    """Проверяет, что dry_run не изменяет целевую БД.

    Метод apply(..., dry_run=True) должен:
    - выводить SQL в stdout;
    - не создавать новые таблицы/колонки;
    - не повреждать данные в target.

    Args:
        prepared_dbs: Фикстура с DSN source/target БД.
        capsys: Pytest-фикстура для перехвата stdout/stderr.
    """
    src_url, tgt_url = prepared_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    ops = corrector.diff()
    corrector.apply(ops, dry_run=True)

    out = capsys.readouterr().out
    assert 'ALTER TABLE' in out or 'CREATE TABLE' in out

    tgt_engine = create_engine(tgt_url)

    assert 'orders' not in _reflect_tables(tgt_engine)
    assert 'age' not in _reflect_columns(tgt_engine, 'users')

    assert _count_rows(tgt_engine, 'notes') == 1
    assert _get_user_legacy(tgt_engine, 1) == 'keep-me'


def test_apply_executes_safe_ops_and_preserves_data_constraints(prepared_dbs):
    """Проверяет применение безопасных операций и сохранность данных.

    Применяем только операции, которые не являются report. После apply():
    - появляется таблица orders;
    - добавляется колонка users.age;
    - nullable-ограничения не ужесточаются автоматически;
    - создаются индексы из source (включая индекс на новой таблице);
    - лишние сущности target остаются на месте;
    - данные в target сохраняются.

    Args:
        prepared_dbs: Фикстура с DSN source/target БД.
    """
    src_url, tgt_url = prepared_dbs

    corrector = SchemaCorrector(
        source_url=src_url,
        target_url=tgt_url,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    ops = corrector.diff()
    safe_ops = [op for op in ops if op.kind != 'report']

    corrector.apply(safe_ops, dry_run=False)

    tgt_engine = create_engine(tgt_url)

    indexes = inspect(tgt_engine).get_indexes('users')
    order_indexes = inspect(tgt_engine).get_indexes('orders')

    assert 'orders' in _reflect_tables(tgt_engine)

    assert 'age' in _reflect_columns(tgt_engine, 'users')

    assert _is_nullable(tgt_engine, 'users', 'email') is True

    assert any(i.get('name') == 'ix_users_email' for i in indexes)
    assert any(i.get('name') == 'ix_orders_user_id' for i in order_indexes)

    # Лишняя таблица и колонка никуда не делись
    assert 'notes' in _reflect_tables(tgt_engine)
    assert 'legacy' in _reflect_columns(tgt_engine, 'users')

    # Данные не повреждены
    assert _count_rows(tgt_engine, 'notes') == 1
    assert _get_user_legacy(tgt_engine, 1) == 'keep-me'


@pytest.mark.logging
def test_apply_logs_error_and_critical_on_failure(prepared_dbs, caplog):
    """Проверяет, что при ошибке apply() пишет логи error и critical.

    Передаём заведомо невалидный SQL и убеждаемся, что:
    - выбрасывается исключение;
    - в логах есть сообщение об ошибке и сообщение о прерывании процесса.

    Args:
        prepared_dbs: Фикстура с DSN source/target БД.
        caplog: Pytest-фикстура для перехвата логов.
    """
    _, tgt_url = prepared_dbs

    corrector = SchemaCorrector(
        source_url=tgt_url,  # неважно
        target_url=tgt_url,
        lock_timeout_seconds=0,
        statement_timeout_seconds=0,
    )

    caplog.set_level(logging.ERROR)

    bad_ops = [
        Operation(
            kind='add_column',
            sql='THIS IS BAD SQL;',
            comment='boom'
        )
    ]

    with pytest.raises(Exception):
        corrector.apply(bad_ops, dry_run=False)

    assert any('Apply failed' in rec.message for rec in caplog.records)
    assert any(
        'Schema correction aborted' in rec.message for rec in caplog.records
    )
