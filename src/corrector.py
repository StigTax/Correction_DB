from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.inspection import inspect
from sqlalchemy.schema import CreateIndex, CreateTable


@dataclass(frozen=True)
class Operation:
    """Описывает одну операцию синхронизации схемы.

    Экземпляры этого класса используются как элементы плана миграции, который
    возвращает `SchemaCorrector.diff()` и затем может быть применён методом
    `SchemaCorrector.apply()`.

    Attributes:
        kind: Тип операции. Возможные значения:
            - 'create_table' — создание отсутствующей таблицы;
            - 'add_column' — добавление отсутствующей колонки;
            - 'create_index' — создание отсутствующего индекса;
            - 'report' — отчёт о различиях,
                которые не применяются автоматически.
        sql: SQL-код операции. Для kind='report' обычно содержит '-- no-op'.
        comment: Человеко-читаемое описание операции.
    """
    kind: str
    sql: str
    comment: str = ''


class SchemaCorrector:
    """Сравнивает две базы и подтягивает схему целевой базы к эталонной.

    Класс решает задачу безопасной коррекции схемы: целевая БД уже содержит
    данные, поэтому автоматически выполняются только аддитивные операции
    (создание таблиц/колонок/индексов). Потенциально опасные расхождения
    (например, изменение типов или ужесточение nullable) не применяются, а
    попадают в отчёт (Operation(kind='report')) и логируются как warning.

    Args:
        source_url: DSN эталонной БД (БД №1).
        target_url: DSN корректируемой БД (БД №2).
        schema: Имя схемы (например, 'public'). Если None — используется
            схема по умолчанию для выбранной СУБД.
        lock_timeout_seconds: Таймаут ожидания блокировок (секунды).
            Применяется только для PostgreSQL.
        statement_timeout_seconds: Таймаут выполнения запросов (секунды).
            0 означает отсутствие таймаута. Применяется только для PostgreSQL.
        allow_destructive: Флаг для потенциально деструктивных операций.
            В текущей реализации используется как настройка, но сами
            деструктивные операции не выполняются автоматически.
        logger: Логгер. Если не передан — используется logger по имени класса.
    """

    def __init__(
        self,
        source_url: str,
        target_url: str,
        *,
        schema: Optional[str] = None,
        lock_timeout_seconds: int = 10,
        statement_timeout_seconds: int = 0,
        allow_destructive: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Инициализирует корректор схемы и создаёт подключения к БД.

        Args:
            source_url: DSN эталонной БД (БД №1).
            target_url: DSN целевой БД (БД №2).
            schema: Имя схемы (например, 'public').
            lock_timeout_seconds: Таймаут ожидания блокировок (секунды).
            statement_timeout_seconds: Таймаут выполнения запросов (секунды).
            allow_destructive: Флаг разрешения деструктивных операций.
            logger: Логгер для записи сообщений.

        Returns:
            None.
        """
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.schema = schema
        self.lock_timeout_seconds = lock_timeout_seconds
        self.statement_timeout_seconds = statement_timeout_seconds
        self.allow_destructive = allow_destructive
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self.logger.info(
            'SchemaCorrector initialized '
            '(schema=%s, lock_timeout=%ss, '
            'statement_timeout=%ss, allow_destructive=%s)',
            self.schema,
            self.lock_timeout_seconds,
            self.statement_timeout_seconds,
            self.allow_destructive,
        )

    def diff(self) -> list[Operation]:
        """Строит план синхронизации схемы целевой БД по эталону.

        Метод сравнивает схемы source и target и возвращает список операций:
        - создание отсутствующих таблиц;
        - добавление отсутствующих колонок;
        - создание отсутствующих индексов;
        - отчёты о лишних сущностях в target и рискованных различиях.

        Важно: рискованные различия (тип/nullable)
            не применяются автоматически.

        Returns:
            list[Operation]: Список операций для синхронизации (план).
        """
        self.logger.info('Starting schema diff...')
        src_inspector = inspect(self.source_engine)
        tgt_inspector = inspect(self.target_engine)

        src_tables = set(src_inspector.get_table_names(
            schema=self.schema
        ))
        tgt_tables = set(tgt_inspector.get_table_names(
            schema=self.schema
        ))

        self.logger.info(
            'Introspected tables: source=%d, target=%d',
            len(src_tables),
            len(tgt_tables),
        )

        ops: list[Operation] = []

        extra_tables = sorted(tgt_tables - src_tables)
        for table_name in extra_tables:
            msg = f'EXTRA: table exists only in target: {table_name}'
            self.logger.warning(msg)
            ops.append(
                Operation(
                    kind='report',
                    sql='-- no-op',
                    comment=msg
                )
            )

        missing_tables = sorted(src_tables - tgt_tables)
        if missing_tables:
            self.logger.info(
                'Missing tables in target: %d',
                len(missing_tables)
            )
        for table_name in missing_tables:
            self.logger.info('Planning create table: %s', table_name)
            ops.extend(self._plan_create_table(table_name))
            idx_ops = self._plan_add_missing_indexes(table_name)
            if idx_ops:
                self.logger.info(
                    'Planning add indexes for new table: table=%s, count=%d',
                    table_name,
                    len(idx_ops),
                )
            ops.extend(idx_ops)

        common_tables = sorted(src_tables & tgt_tables)
        self.logger.info('Common tables: %d', len(common_tables))

        for table_name in common_tables:
            extra_col_reports = self._report_extra_columns(table_name)
            for r in extra_col_reports:
                self.logger.warning(r.comment)
            ops.extend(extra_col_reports)

            add_col_ops = self._plan_add_missing_columns(table_name)
            if add_col_ops:
                self.logger.info(
                    'Planning add columns: table=%s, count=%d',
                    table_name,
                    len(add_col_ops),
                )
            ops.extend(add_col_ops)

        for table_name in common_tables:
            idx_ops = self._plan_add_missing_indexes(table_name)
            if idx_ops:
                self.logger.info(
                    'Planning add indexes: table=%s, count=%d',
                    table_name,
                    len(idx_ops),
                )
            ops.extend(idx_ops)

        risky_count = 0
        for table_name in common_tables:
            reports = self._report_risky_differences(table_name)
            for r in reports:
                self.logger.warning(r.comment)
            risky_count += len(reports)
            ops.extend(reports)

        self.logger.info(
            'Diff done. Planned ops=%d (risky reports=%d)',
            len(ops),
            risky_count,
        )
        return ops

    def apply(
        self,
        ops: Iterable[Operation],
        *,
        dry_run: bool = True
    ) -> None:
        """Применяет план операций к целевой базе данных.

        По умолчанию работает в режиме dry-run: не выполняет SQL, а печатает
        операции. В режиме исполнения выполняет SQL внутри транзакции.

        Операции kind='report' пропускаются.

        Args:
            ops: Последовательность операций для применения.
            dry_run: Если True — только выводит SQL и не применяет изменения.

        Raises:
            Exception: Любая ошибка выполнения SQL пробрасывается наружу
                после логирования на уровнях error/critical.

        Returns:
            None.
        """
        ops_list = list(ops)
        self.logger.info(
            'Apply called. dry_run=%s, ops=%d',
            dry_run,
            len(ops_list)
        )

        if dry_run:
            for op in ops_list:
                print(f'-- {op.kind}: {op.comment}\n{op.sql}\n')
            self.logger.info('Dry-run finished. No changes applied.')
            return

        try:
            with self.target_engine.begin() as conn:
                self._apply_timeouts(conn)
                for i, op in enumerate(ops_list, start=1):
                    self.logger.info(
                        'Executing op %d/%d: %s (%s)',
                        i,
                        len(ops_list),
                        op.kind,
                        op.comment
                    )
                    if op.kind == 'report':
                        self.logger.info('Skipping report op: %s', op.comment)
                        continue
                    conn.execute(text(op.sql))
            self.logger.info('Apply finished successfully.')
        except Exception as exc:
            self.logger.error('Apply failed: %s', exc, exc_info=True)
            self.logger.critical('Schema correction aborted due to error.')
            raise

    def _plan_create_table(self, table_name: str) -> list[Operation]:
        """Формирует операцию создания отсутствующей таблицы.

        Таблица отражается (autoload) из source и затем генерируется DDL
        под диалект target.

        Args:
            table_name: Имя таблицы, которую нужно создать в target.

        Returns:
            list[Operation]: Список операций
            (обычно одна операция create_table).
        """
        md = MetaData(schema=self.schema)
        table = Table(table_name, md, autoload_with=self.source_engine)

        ddl = str(
            CreateTable(table).compile(self.target_engine)
        ).rstrip() + ';'
        return [
            Operation(
                kind='create_table',
                sql=ddl,
                comment=f'Create table {table_name}'
            )
        ]

    def _plan_add_missing_columns(self, table_name: str) -> list[Operation]:
        """Планирует добавление колонок, отсутствующих в target.

        Метод сравнивает набор колонок source и target в рамках одной таблицы и
        создаёт операции `ALTER TABLE ... ADD COLUMN ...` для тех колонок,
        которых нет в target.

        Важно: добавление выполняется “безопасно” — без автоматического
        ужесточения nullable и без миграции типов.

        Args:
            table_name: Имя таблицы для сравнения.

        Returns:
            list[Operation]: Операции add_column для таблицы.
        """
        src_cols = self._get_columns(self.source_engine, table_name)
        tgt_cols = self._get_columns(self.target_engine, table_name)

        ops: list[Operation] = []

        for col_name, src_col in src_cols.items():
            if col_name in tgt_cols:
                continue

            col_type_sql = src_col['type_sql']

            sql = (
                f'ALTER TABLE {self._qt(table_name)} '
                f'ADD COLUMN {self._q(col_name)} {col_type_sql};'
            )
            ops.append(
                Operation(
                    kind='add_column',
                    sql=sql,
                    comment=f'Add column {table_name}.{col_name}'
                )
            )

        return ops

    def _plan_add_missing_indexes(self, table_name: str) -> list[Operation]:
        """Планирует создание индексов, отсутствующих в target.

        Алгоритм:
        1) Получает список индексов target через Inspector.get_indexes().
        2) Пытается получить индексы source через
            Table(...).indexes (reflection).
        3) Если reflection не дал полной картины, добирает индексы из source
           через Inspector.get_indexes().

        Args:
            table_name: Имя таблицы, для которой нужно синхронизировать
            индексы.

        Returns:
            list[Operation]: Операции create_index для отсутствующих индексов.
        """
        tgt_inspector = inspect(self.target_engine)
        try:
            tgt_indexes = tgt_inspector.get_indexes(
                table_name,
                schema=self.schema
            )
        except Exception:
            tgt_indexes = []
        tgt_index_names = {i.get('name') for i in tgt_indexes if i.get('name')}

        ops: list[Operation] = []

        md = MetaData(schema=self.schema)
        src_table = Table(table_name, md, autoload_with=self.source_engine)

        for idx in src_table.indexes:
            if idx.name and idx.name not in tgt_index_names:
                ddl = str(
                    CreateIndex(idx).compile(self.target_engine)
                ).rstrip() + ';'
                ops.append(
                    Operation(
                        kind='create_index',
                        sql=ddl,
                        comment=f'Create index {idx.name}'
                    )
                )

        prefix = 'Create index '

        planned_names = {
            op.comment.removeprefix(prefix)
            for op in ops
            if op.kind == 'create_index' and op.comment.startswith(prefix)
        }

        src_inspector = inspect(self.source_engine)
        for info in src_inspector.get_indexes(
            table_name,
            schema=self.schema
        ):
            name = info.get('name')
            cols = info.get('column_names') or []
            unique = bool(info.get('unique'))
            if (
                not name
                or not cols
                or name in tgt_index_names
                or name in planned_names
            ):
                continue

            unique_sql = 'UNIQUE ' if unique else ''
            cols_sql = ', '.join(self._q(c) for c in cols)
            sql = (
                f'CREATE {unique_sql}INDEX {self._q(name)} '
                f'ON {self._qt(table_name)} ({cols_sql});'
            )
            ops.append(
                Operation(
                    kind='create_index',
                    sql=sql,
                    comment=f'Create index {name}'
                )
            )
        return ops

    def _report_extra_columns(self, table_name: str) -> list[Operation]:
        """Формирует отчёт по “лишним” колонкам, которые есть только в target.

        Лишние колонки не удаляются автоматически, так как это может повредить
        данные. Вместо этого возвращаются операции kind='report'.

        Args:
            table_name: Имя таблицы для анализа.

        Returns:
            list[Operation]: Операции отчёта (report) по лишним колонкам.
        """
        src_cols = self._get_columns(self.source_engine, table_name)
        tgt_cols = self._get_columns(self.target_engine, table_name)

        extra_cols = sorted(set(tgt_cols) - set(src_cols))
        ops: list[Operation] = []
        for col_name in extra_cols:
            msg = (
                f'EXTRA: column exists only in target: {table_name}.{col_name}'
            )
            ops.append(Operation(kind='report', sql='-- no-op', comment=msg))
        return ops

    def _report_risky_differences(self, table_name: str) -> list[Operation]:
        """Формирует отчёт по рискованным различиям между source и target.

        К рискованным различиям относятся изменения, которые потенциально могут
        повредить данные или привести к ошибкам при выполнении:
        - различие типов колонок;
        - попытка ужесточить nullable (source NOT NULL, target NULL).

        Args:
            table_name: Имя таблицы для анализа.

        Returns:
            list[Operation]: Операции отчёта (report) по рискованным различиям.
        """
        src_cols = self._get_columns(self.source_engine, table_name)
        tgt_cols = self._get_columns(self.target_engine, table_name)

        ops: list[Operation] = []

        for col_name, src in src_cols.items():
            tgt = tgt_cols.get(col_name)
            if not tgt:
                continue

            if src['type_sql'] != tgt['type_sql']:
                ops.append(Operation(
                    kind='report',
                    sql='-- no-op',
                    comment=(
                        f'RISKY: type mismatch {table_name}.{col_name}: '
                        f'source={src["type_sql"]} target={tgt["type_sql"]}'
                    ),
                ))

            if src['nullable'] is False and tgt['nullable'] is True:
                ops.append(Operation(
                    kind='report',
                    sql='-- no-op',
                    comment=(
                        f'RISKY: nullable mismatch {table_name}.{col_name}: '
                        'source NOT NULL, target NULL '
                        '(need staged backfill + ALTER)'
                    ),
                ))

        return ops

    def _get_columns(self, engine: Engine, table_name: str) -> dict:
        """Возвращает метаданные колонок таблицы.

        Использует SQLAlchemy Inspector для получения списка колонок и приводит
        типы к строковому SQL-представлению для сравнения.

        Args:
            engine: SQLAlchemy Engine, из которого нужно прочитать схему.
            table_name: Имя таблицы.

        Returns:
            dict: Словарь вида {column_name: meta}, где meta содержит:
                - type_sql: SQL-представление типа колонки;
                - nullable: True/False;
                - default: значение default (если доступно инспектору).
        """
        insp = inspect(engine)
        cols = insp.get_columns(table_name, schema=self.schema)

        out = {}
        for c in cols:
            type_sql = c['type'].compile(dialect=engine.dialect)
            out[c['name']] = {
                'type_sql': str(type_sql),
                'nullable': bool(c.get('nullable', True)),
                'default': c.get('default'),
            }
        return out

    def _apply_timeouts(self, conn) -> None:
        """Устанавливает таймауты для транзакции (только PostgreSQL).

        В текущей реализации поддерживается только PostgreSQL, т.к. параметры
        `lock_timeout` и `statement_timeout` являются PostgreSQL-специфичными.

        Args:
            conn: SQLAlchemy Connection/Session connection из begin().

        Returns:
            None.
        """
        if self.target_engine.dialect.name != 'postgresql':
            return

        if self.lock_timeout_seconds > 0:
            lock_sql = f"SET lock_timeout = '{self.lock_timeout_seconds}s'"
            conn.execute(text(lock_sql))

        if self.statement_timeout_seconds > 0:
            stmt_sql = (
                f"SET statement_timeout = '{self.statement_timeout_seconds}s'"
            )
            conn.execute(text(stmt_sql))

    def _q(self, name: str) -> str:
        """Квотит идентификатор с учётом диалекта целевой СУБД.

        Используется для безопасного quoting имён таблиц/колонок/индексов
        под конкретный диалект SQLAlchemy.

        Args:
            name: Имя идентификатора (таблица/колонка/индекс).

        Returns:
            str: Квотированное имя идентификатора.
        """
        return self.target_engine.dialect.identifier_preparer.quote_identifier(
            name
        )

    def _qt(self, table_name: str) -> str:
        """Возвращает квотированное имя таблицы с учётом схемы.

        Если schema задана, результат будет вида: "<schema>"."<table>".
        Если schema не задана — только квотированное имя таблицы.

        Args:
            table_name: Имя таблицы.

        Returns:
            str: Квотированное имя таблицы (возможно schema-qualified).
        """
        if self.schema:
            return f'{self._q(self.schema)}.{self._q(table_name)}'
        return self._q(table_name)
