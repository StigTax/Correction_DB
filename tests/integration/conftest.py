from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import (
    create_engine,
    text,
)

from tests._helpers.schema_builders import (
    _create_source_schema,
    _create_target_schema,
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
