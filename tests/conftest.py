from __future__ import annotations

from pathlib import Path
import os
import uuid

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text
)


def _make_sqlite_engine(db_path: Path):
    """Создаёт SQLAlchemy Engine для файловой SQLite базы.

    Args:
        db_path: Путь к файлу SQLite базы данных.

    Returns:
        sqlalchemy.engine.Engine: Engine для подключения к БД.
    """
    return create_engine(f'sqlite:///{db_path}')


def _create_source_schema(engine) -> None:
    """Создаёт эталонную схему (source).

    В эталонной схеме присутствуют:
    - таблица users с NOT NULL полем email и дополнительной колонкой age;
    - индекс ix_users_email;
    - таблица orders;
    - индекс ix_orders_user_id на orders.user_id.

    Args:
        engine: SQLAlchemy Engine для source базы.

    Returns:
        None.
    """
    md = MetaData()

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


def _create_target_schema(engine) -> None:
    """Создаёт текущую схему (target) и наполняет данными.

    Target специально отличается от source:
    - в users.email разрешён NULL (чтобы получить risky report);
    - отсутствует колонка users.age (должна добавиться);
    - есть лишняя колонка users.legacy (должна попасть в report);
    - есть лишняя таблица notes (должна попасть в report);
    - добавляются тестовые данные, чтобы проверить, что apply() их не ломает.

    Args:
        engine: SQLAlchemy Engine для target базы.

    Returns:
        None.
    """
    md = MetaData()

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

    with engine.begin() as conn:
        conn.execute(
            text(
                'INSERT INTO "users" (id, email, legacy) '
                'VALUES (1, :email, :legacy)'
            ),
            {'email': 'user@example.com', 'legacy': 'keep-me'},
        )
        conn.execute(
            text('INSERT INTO "notes" (id, text) VALUES (1, :text)'),
            {'text': 'hello'},
        )


@pytest.fixture()
def db_urls(tmp_path: Path):
    """Возвращает DSN для source и target SQLite баз в temp-директории.

    Args:
        tmp_path: Встроенная pytest-фикстура временной директории.

    Returns:
        tuple[str, str]: (source_url, target_url)
    """
    src_path = tmp_path / 'source.db'
    tgt_path = tmp_path / 'target.db'
    return f'sqlite:///{src_path}', f'sqlite:///{tgt_path}'


@pytest.fixture()
def prepared_dbs(db_urls):
    """Создаёт source и target базы данных и возвращает их DSN.

    Фикстура:
    - создаёт движки для source/target;
    - накатывает схемы через helper-функции;
    - освобождает ресурсы (dispose);
    - возвращает DSN для использования в тестах.

    Args:
        db_urls: Фикстура с DSN (source_url, target_url).

    Returns:
        tuple[str, str]: (source_url, target_url)
    """
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
    """Возвращает DSN для Postgres source/target из переменных окружения.

    Требуются:
        POSTGRES_SOURCE_URL
        POSTGRES_TARGET_URL
    """
    src_url = os.getenv('POSTGRES_SOURCE_URL')
    tgt_url = os.getenv('POSTGRES_TARGET_URL')

    if not src_url or not tgt_url:
        pytest.skip('POSTGRES_SOURCE_URL/POSTGRES_TARGET_URL are not set')

    return src_url, tgt_url


@pytest.fixture()
def prepared_postgres_dbs(postgres_urls):
    """Создаёт изолированную схему в двух Postgres БД и наполняет её.

    Возвращает (source_url, target_url, schema_name).
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
