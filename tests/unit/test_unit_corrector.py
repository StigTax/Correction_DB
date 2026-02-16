from __future__ import annotations

from unittest.mock import Mock

import pytest
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    create_engine,
    inspect,
    String
)

import corrector as corrector_mod
from corrector import Operation, SchemaCorrector


pytestmark = [pytest.mark.unit]


def test_sort_missing_tables_by_fk_detects_cycle_and_falls_back(
    caplog,
    tmp_path
):
    """Проверяет fallback-поведение при цикле FK-зависимостей."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'

    c = SchemaCorrector(source_url=src, target_url=tgt, schema=None)

    class FakeInspector:
        def get_foreign_keys(self, table_name: str, schema=None):
            # a -> b, b -> a (цикл)
            if table_name == 'a':
                return [{'referred_table': 'b'}]
            if table_name == 'b':
                return [{'referred_table': 'a'}]
            return []

    caplog.set_level('WARNING')
    missing = ['a', 'b']

    out = c._sort_missing_tables_by_fk(FakeInspector(), missing)

    assert out == missing
    assert any('cycle detected' in rec.message for rec in caplog.records)


def test_apply_timeouts_sets_lock_and_statement_for_postgres(tmp_path):
    """Проверяет, что _apply_timeouts выставляет SET для PostgreSQL."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'

    c = SchemaCorrector(
        source_url=src,
        target_url=tgt,
        lock_timeout_seconds=2,
        statement_timeout_seconds=3,
    )

    c.target_engine.dialect.name = 'postgresql'

    conn = Mock()
    c._apply_timeouts(conn)

    assert conn.execute.call_count == 2
    lock_call = conn.execute.call_args_list[0].args[0]
    stmt_call = conn.execute.call_args_list[1].args[0]

    assert lock_call.text == "SET lock_timeout = '2s'"
    assert stmt_call.text == "SET statement_timeout = '3s'"


def test_build_fk_operation_none_when_insufficient_data(tmp_path):
    """
    Проверяет, что _build_fk_operation возвращает None при нехватке данных.
    """
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt, schema='corr_manual')

    assert c._build_fk_operation('orders', {}) is None
    assert c._build_fk_operation('orders', {'referred_table': 'users'}) is None
    assert c._build_fk_operation(
        'orders',
        {'referred_table': 'users', 'constrained_columns': ['user_id']},
    ) is None


def test_build_fk_operation_includes_options_and_not_valid(tmp_path):
    """Проверяет генерацию SQL для FK: ON DELETE/UPDATE и NOT VALID для PG."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt, schema='corr_manual')

    c.target_engine.dialect.name = 'postgresql'

    fk = {
        'name': None,
        'referred_table': 'users',
        'referred_schema': 'corr_manual',
        'constrained_columns': ['user_id'],
        'referred_columns': ['id'],
        'options': {'ondelete': 'CASCADE', 'onupdate': 'RESTRICT'},
    }

    op = c._build_fk_operation('orders', fk)
    assert op is not None
    assert op.kind == 'add_foreign_key'
    assert 'ON DELETE CASCADE' in op.sql
    assert 'ON UPDATE RESTRICT' in op.sql
    assert 'NOT VALID' in op.sql
    assert 'REFERENCES' in op.sql


def test_make_fk_name_truncates_to_60(tmp_path):
    """Проверяет, что _make_fk_name ограничивает длину имени FK до 60."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)

    fk = {
        'constrained_columns': ['col_' + 'x' * 50],
        'referred_table': 'ref_' + 'y' * 50,
    }

    name = c._make_fk_name('t' * 50, fk)
    assert len(name) <= 60


def test_plan_create_table_can_exclude_foreign_keys(tmp_path):
    """Проверяет include_foreign_keys в _plan_create_table (SQLite-путь)."""
    src_url = f'sqlite:///{tmp_path / "s.db"}'
    tgt_url = f'sqlite:///{tmp_path / "t.db"}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    md = MetaData()
    users = Table(
        'users',
        md,
        Column('id', Integer, primary_key=True),
    )
    Table(
        'orders',
        md,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    )
    md.create_all(src_engine)

    c = SchemaCorrector(source_url=src_url, target_url=tgt_url, schema=None)

    sql_no_fk = c._plan_create_table(
        'orders',
        include_foreign_keys=False
    )[0].sql
    sql_with_fk = c._plan_create_table(
        'orders',
        include_foreign_keys=True
    )[0].sql

    assert 'REFERENCES' not in sql_no_fk and 'FOREIGN KEY' not in sql_no_fk
    assert ('REFERENCES' in sql_with_fk) or ('FOREIGN KEY' in sql_with_fk)

    src_engine.dispose()
    tgt_engine.dispose()


def test_sqlite_reports_missing_fk_for_existing_table(tmp_path):
    """
    Проверяет sqlite-ветку: нельзя добавить FK через ALTER TABLE -> report.
    """
    src_url = f'sqlite:///{tmp_path / "s.db"}'
    tgt_url = f'sqlite:///{tmp_path / "t.db"}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    # source: orders с FK
    md1 = MetaData()
    users1 = Table('users', md1, Column('id', Integer, primary_key=True))
    Table(
        'orders',
        md1,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    )
    md1.create_all(src_engine)

    md2 = MetaData()
    Table('users', md2, Column('id', Integer, primary_key=True))
    Table(
        'orders',
        md2,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, nullable=False),
    )
    md2.create_all(tgt_engine)

    c = SchemaCorrector(source_url=src_url, target_url=tgt_url, schema=None)
    ops = c.diff()

    assert any(
        op.kind == 'report'
        and 'SQLite cannot add FK via ALTER TABLE' in op.comment
        for op in ops
    )

    src_engine.dispose()
    tgt_engine.dispose()


def test_report_risky_type_mismatch(tmp_path):
    """Проверяет report по type mismatch (source vs target)."""
    src_url = f'sqlite:///{tmp_path / "s.db"}'
    tgt_url = f'sqlite:///{tmp_path / "t.db"}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    md1 = MetaData()
    Table(
        'users',
        md1,
        Column('id', Integer, primary_key=True),
        Column('email', String(255), nullable=False),
    )
    md1.create_all(src_engine)

    md2 = MetaData()
    Table(
        'users',
        md2,
        Column('id', Integer, primary_key=True),
        Column('email', Integer, nullable=False),
    )
    md2.create_all(tgt_engine)

    c = SchemaCorrector(source_url=src_url, target_url=tgt_url, schema=None)
    ops = c.diff()

    assert any(
        op.kind == 'report' and 'type mismatch users.email' in op.comment
        for op in ops
    )

    src_engine.dispose()
    tgt_engine.dispose()


def test_plan_add_missing_indexes_falls_back_to_inspector(
    monkeypatch,
    tmp_path
):
    """
    Проверяет fallback по индексам: reflection пустой -> берём из inspector.
    """
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt, schema=None)

    class FakeTargetInspector:
        def get_indexes(self, table_name: str, schema=None):
            raise RuntimeError('boom')

    class FakeSourceInspector:
        def get_indexes(self, table_name: str, schema=None):
            return [{
                'name': 'ix_users_email_unique',
                'column_names': ['email'],
                'unique': True,
            }]

    def fake_inspect(engine):
        if engine is c.target_engine:
            return FakeTargetInspector()
        if engine is c.source_engine:
            return FakeSourceInspector()
        raise AssertionError('unexpected engine')

    class FakeTable:
        indexes = set()

    def fake_table(*args, **kwargs):
        return FakeTable()

    monkeypatch.setattr(corrector_mod, 'inspect', fake_inspect)
    monkeypatch.setattr(corrector_mod, 'Table', fake_table)

    ops = c._plan_add_missing_indexes('users')

    assert len(ops) == 1
    assert ops[0].kind == 'create_index'
    assert 'UNIQUE' in ops[0].sql
    assert 'ix_users_email_unique' in ops[0].sql


def test_sort_missing_tables_by_fk_orders_after_users(caplog, tmp_path):
    """Проверяет сортировку недостающих таблиц по FK (без циклов)."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)

    class FakeInspector:
        def get_foreign_keys(self, table_name: str, schema=None):
            if table_name == 'orders':
                return [{'referred_table': 'users'}]
            return []

    out = c._sort_missing_tables_by_fk(FakeInspector(), ['users', 'orders'])
    assert out == ['users', 'orders']


def test_apply_skips_report_ops_and_executes_sql(caplog, tmp_path):
    """Проверяет, что apply() пропускает report-операции (ветка 287-288)."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)

    ops = [
        Operation(kind='report', sql='-- no-op', comment='just a report'),
        Operation(
            kind='create_table',
            sql='CREATE TABLE demo_table (id INTEGER PRIMARY KEY);',
            comment='Create demo_table',
        ),
    ]

    caplog.set_level('INFO')
    c.apply(ops, dry_run=False)

    eng = create_engine(tgt)
    tables = set(inspect(eng).get_table_names())
    assert 'demo_table' in tables
    assert any('Skipping report op' in r.message for r in caplog.records)


def test_diff_plans_fks_for_new_tables_when_not_sqlite(tmp_path):
    """Проверяет ветку diff(): FK для новых таблиц + планирование FK."""
    src_url = f'sqlite:///{tmp_path / "src.db"}'
    tgt_url = f'sqlite:///{tmp_path / "tgt.db"}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    md_src = MetaData()
    Table('users', md_src, Column('id', Integer, primary_key=True))
    Table(
        'orders',
        md_src,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    )
    md_src.create_all(src_engine)

    md_tgt = MetaData()
    Table('users', md_tgt, Column('id', Integer, primary_key=True))
    md_tgt.create_all(tgt_engine)

    c = SchemaCorrector(source_url=src_url, target_url=tgt_url)
    c._is_sqlite = lambda: False

    ops = c.diff()

    assert any(op.kind == 'add_foreign_key' for op in ops)

    src_engine.dispose()
    tgt_engine.dispose()


def test_plan_add_foreign_keys_for_new_table_sqlite_returns_empty(tmp_path):
    """Проверяет sqlite-ветку _plan_add_foreign_keys_for_new_table."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)

    fake_src_insp = Mock()
    fake_src_insp.get_foreign_keys.return_value = []

    ops = c._plan_add_foreign_keys_for_new_table(fake_src_insp, 'orders')
    assert ops == []


def test_plan_add_foreign_keys_for_new_table_non_sqlite_builds_ops(tmp_path):
    """
    Проверяет не-sqlite ветку _plan_add_foreign_keys_for_new_table.
    """
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)

    c._is_sqlite = lambda: False

    fake_src_insp = Mock()
    fake_src_insp.get_foreign_keys.return_value = [{
        'referred_table': 'users',
        'constrained_columns': ['user_id'],
        'referred_columns': ['id'],
        'options': {},
    }]

    ops = c._plan_add_foreign_keys_for_new_table(fake_src_insp, 'orders')
    assert len(ops) == 1
    assert ops[0].kind == 'add_foreign_key'


def test_plan_add_missing_foreign_keys_non_sqlite_success_path(tmp_path):
    """
    Проверяет _plan_add_missing_foreign_keys не-sqlite ветку + _fk_signature.
    """
    src_url = f'sqlite:///{tmp_path / "src_fk.db"}'
    tgt_url = f'sqlite:///{tmp_path / "tgt_fk.db"}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    md_src = MetaData()
    Table('parent', md_src, Column('id', Integer, primary_key=True))
    Table(
        'child',
        md_src,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, ForeignKey('parent.id'), nullable=False),
    )
    md_src.create_all(src_engine)

    md_tgt = MetaData()
    Table('parent', md_tgt, Column('id', Integer, primary_key=True))
    Table(
        'child',
        md_tgt,
        Column('id', Integer, primary_key=True),
        Column('parent_id', Integer, nullable=False),
    )
    md_tgt.create_all(tgt_engine)

    c = SchemaCorrector(source_url=src_url, target_url=tgt_url)
    c._is_sqlite = lambda: False

    ops = c._plan_add_missing_foreign_keys('child')

    assert len(ops) == 1
    assert ops[0].kind == 'add_foreign_key'

    src_engine.dispose()
    tgt_engine.dispose()


def test_plan_add_missing_fks_non_sqlite_exception_on_target_inspector(
    monkeypatch,
    tmp_path
):
    """
    Проверяет except-блок при падении target inspector.get_foreign_keys().
    """
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt)
    c._is_sqlite = lambda: False

    class FakeSourceInspector:
        def get_foreign_keys(self, table_name: str, schema=None):
            return [{
                'referred_table': 'users',
                'constrained_columns': ['user_id'],
                'referred_columns': ['id'],
                'options': {},
            }]

    class FakeTargetInspector:
        def get_foreign_keys(self, table_name: str, schema=None):
            raise RuntimeError('boom')

    def fake_inspect(engine):
        if engine is c.source_engine:
            return FakeSourceInspector()
        if engine is c.target_engine:
            return FakeTargetInspector()
        raise AssertionError('unexpected engine')

    monkeypatch.setattr(corrector_mod, 'inspect', fake_inspect)

    ops = c._plan_add_missing_foreign_keys('orders')
    assert len(ops) == 1
    assert ops[0].kind == 'add_foreign_key'


def test_build_fk_operation_without_schema_uses_unqualified_reference(
    tmp_path
):
    """Проверяет ветку ref_schema=False (646): REFERENCES без schema."""
    src = f'sqlite:///{tmp_path / "s.db"}'
    tgt = f'sqlite:///{tmp_path / "t.db"}'
    c = SchemaCorrector(source_url=src, target_url=tgt, schema=None)

    fk = {
        'referred_table': 'users',
        'constrained_columns': ['user_id'],
        'referred_columns': ['id'],
    }

    op = c._build_fk_operation('orders', fk)
    assert op is not None
    assert 'REFERENCES "users" ("id")' in op.sql
    assert '."users"' not in op.sql
