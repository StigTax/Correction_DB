from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
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
        Column(
            'user_id',
            Integer,
            ForeignKey('users.id'),
            nullable=False,
        ),
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
