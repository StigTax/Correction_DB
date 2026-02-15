from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)


def _tbl(schema: str | None, table_name: str) -> str:
    """Формирует schema-qualified имя таблицы для SQL.

    Args:
        schema: Имя схемы или None.
        table_name: Имя таблицы.

    Returns:
        str: '"schema"."table"' или '"table"'.
    """
    if schema:
        return f'"{schema}"."{table_name}"'
    return f'"{table_name}"'


def _create_source_schema(engine, *, schema: str | None = None) -> None:
    """Создаёт эталонную схему (source).

    Args:
        engine: SQLAlchemy Engine для source базы.
        schema: Имя схемы (для PostgreSQL) или None (для SQLite).

    Returns:
        None.
    """
    md = MetaData(schema=schema)

    users = Table(
        'users',
        md,
        Column('id', Integer, primary_key=True),
        Column('email', String(255), nullable=False),
        Column('age', Integer, nullable=True),
    )
    Index('ix_users_email', users.c.email)

    orders = Table(
        'orders',
        md,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, nullable=False),
        Column('total', Integer, nullable=False),
    )
    Index('ix_orders_user_id', orders.c.user_id)

    md.create_all(engine)


def _create_target_schema(engine, *, schema: str | None = None) -> None:
    """Создаёт текущую схему (target) и наполняет данными.

    Args:
        engine: SQLAlchemy Engine для target базы.
        schema: Имя схемы (для PostgreSQL) или None (для SQLite).

    Returns:
        None.
    """
    md = MetaData(schema=schema)

    Table(
        'users',
        md,
        Column('id', Integer, primary_key=True),
        Column('email', String(255), nullable=True),
        Column('legacy', String(50), nullable=True),
    )

    Table(
        'notes',
        md,
        Column('id', Integer, primary_key=True),
        Column('text', String(255), nullable=True),
    )

    md.create_all(engine)

    users_t = _tbl(schema, 'users')
    notes_t = _tbl(schema, 'notes')

    with engine.begin() as conn:
        conn.execute(
            text(
                f'INSERT INTO {users_t} (id, email, legacy) '
                'VALUES (1, :email, :legacy)'
            ),
            {'email': 'user@example.com', 'legacy': 'keep-me'},
        )
        conn.execute(
            text(
                f'INSERT INTO {notes_t} (id, text) '
                'VALUES (1, :text)'
            ),
            {'text': 'hello'},
        )


@pytest.fixture()
def db_urls(tmp_path: Path):
    """
    Возвращает DSN для source и target SQLite баз во временной директории.
    """
    src_path = tmp_path / 'source.db'
    tgt_path = tmp_path / 'target.db'
    return f'sqlite:///{src_path}', f'sqlite:///{tgt_path}'


@pytest.fixture()
def prepared_dbs(db_urls):
    """Создаёт source/target SQLite базы и возвращает их DSN."""
    src_url, tgt_url = db_urls
    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    _create_source_schema(src_engine)
    _create_target_schema(tgt_engine)

    src_engine.dispose()
    tgt_engine.dispose()

    return src_url, tgt_url


@pytest.fixture()
def postgres_urls():
    """Берёт DSN source/target Postgres из переменных окружения."""
    src_url = os.getenv('POSTGRES_SOURCE_URL')
    tgt_url = os.getenv('POSTGRES_TARGET_URL')

    if not src_url or not tgt_url:
        pytest.skip('POSTGRES_SOURCE_URL/POSTGRES_TARGET_URL are not set')

    return src_url, tgt_url


@pytest.fixture()
def prepared_postgres_dbs(postgres_urls):
    """Создаёт временную схему в двух Postgres БД и наполняет её.

    Yields:
        tuple[str, str, str]: (source_url, target_url, schema_name)
    """
    src_url, tgt_url = postgres_urls
    schema = f'corr_test_{uuid.uuid4().hex[:10]}'

    src_engine = create_engine(src_url)
    tgt_engine = create_engine(tgt_url)

    try:
        with src_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        with tgt_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except Exception as exc:
        src_engine.dispose()
        tgt_engine.dispose()
        pytest.skip(f'Cannot create schema for tests: {exc}')

    try:
        _create_source_schema(src_engine, schema=schema)
        _create_target_schema(tgt_engine, schema=schema)
        yield src_url, tgt_url, schema
    finally:
        with src_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        with tgt_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        src_engine.dispose()
        tgt_engine.dispose()
