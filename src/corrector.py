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

    Экземпляры Operation формируют план изменений, который возвращает
    `SchemaCorrector.diff()` и который может быть применён методом
    `SchemaCorrector.apply()`.

    Attributes:
        kind: Тип операции. Поддерживаемые значения:
            - create_table: создание отсутствующей таблицы;
            - add_column: добавление отсутствующей колонки;
            - create_index: создание отсутствующего индекса;
            - add_foreign_key: добавление отсутствующего внешнего ключа;
            - report: отчёт о различиях, которые не применяются автоматически.
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
        schema: Имя схемы (например, 'public'). Если None — используется схема
            по умолчанию для выбранной СУБД.
        lock_timeout_seconds: Таймаут ожидания блокировок (секунды).
            Применяется только для PostgreSQL.
        statement_timeout_seconds: Таймаут выполнения запросов (секунды).
            Значение 0 отключает таймаут. Применяется только для PostgreSQL.
        allow_destructive: Флаг потенциально деструктивных операций. В текущей
            реализации используется как настройка, но деструктивные операции
            автоматически не выполняются.
        logger: Логгер. Если не передан — используется логгер по имени класса.
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

        - создание отсутствующих таблиц;
        - добавление отсутствующих колонок;
        - создание отсутствующих индексов;
        - добавление отсутствующих внешних ключей (FK);
        - отчёты о лишних сущностях в target и рискованных расхождениях.

        Важно: рискованные различия (тип/nullable)
            не применяются автоматически и возвращаются как
            Operation(kind='report').

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
        missing_tables = self._sort_missing_tables_by_fk(
            src_inspector,
            missing_tables
        )
        if missing_tables:
            self.logger.info(
                'Missing tables in target: %d',
                len(missing_tables)
            )
        for table_name in missing_tables:
            self.logger.info('Planning create table: %s', table_name)
            include_fk = self._is_sqlite()

            ops.extend(
                self._plan_create_table(
                    table_name,
                    include_foreign_keys=include_fk,
                )
            )

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

        fk_ops: list[Operation] = []
        if not self._is_sqlite():
            for table_name in missing_tables:
                fk_ops.extend(self._plan_add_foreign_keys_for_new_table(
                    src_inspector,
                    table_name,
                ))

        for table_name in common_tables:
            fk_ops.extend(self._plan_add_missing_foreign_keys(table_name))

        if fk_ops:
            self.logger.info('Planned foreign keys: %d', len(fk_ops))
        ops.extend(fk_ops)

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

    def _sort_missing_tables_by_fk(
        self,
        src_inspector,
        missing: list[str]
    ) -> list[str]:
        """Сортирует список недостающих таблиц с учётом зависимостей FK.

        Метод пытается упорядочить создание таблиц так,
        чтобы таблицы-«родители» создавались раньше таблиц-«потомков»,
        которые ссылаются на них через FK. Если обнаружен цикл зависимостей,
        возвращает исходный порядок и логирует
        предупреждение.

        Args:
            src_inspector: SQLAlchemy Inspector для source БД.
            missing: Список таблиц, отсутствующих в target.

        Returns:
            list[str]: Таблицы в порядке, безопасном для создания
            (насколько возможно).
        """
        missing_set = set(missing)
        deps: dict[str, set[str]] = {t: set() for t in missing}

        for t in missing:
            for fk in src_inspector.get_foreign_keys(
                t,
                schema=self.schema
            ) or []:
                rt = fk.get('referred_table')
                if rt and rt in missing_set:
                    deps[t].add(rt)

        ready = [t for t in missing if not deps[t]]
        out: list[str] = []

        while ready:
            n = ready.pop()
            out.append(n)
            for t in missing:
                if n in deps[t]:
                    deps[t].remove(n)
                    if not deps[t] and t not in out and t not in ready:
                        ready.append(t)

        if len(out) != len(missing):
            self.logger.warning(
                'RISKY: cycle detected in FK dependencies, '
                'using fallback order'
            )
            return missing
        return out

    def _plan_create_table(
        self,
        table_name: str,
        *,
        include_foreign_keys: bool,
    ) -> list[Operation]:
        """Формирует операцию создания отсутствующей таблицы.

        Таблица отражается (autoload) из source и затем генерируется DDL
        под диалект target.

        Args:
            table_name: Имя таблицы, которую нужно создать в target.
            include_foreign_keys: Если True — включает FK в CREATE TABLE.
                Если False — исключает FK из CREATE TABLE
                (для последующего добавления через ALTER).

        Returns:
            list[Operation]: Список операций
            (обычно одна операция create_table).
        """
        md = MetaData(schema=self.schema)
        table = Table(table_name, md, autoload_with=self.source_engine)

        fk_constraints = None if include_foreign_keys else frozenset()
        ddl = str(
            CreateTable(
                table,
                include_foreign_key_constraints=fk_constraints,
            ).compile(self.target_engine)
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

        tgt_index_names = {
            i.get('name') for i in tgt_indexes if i.get('name')
        }

        ops: list[Operation] = []

        md = MetaData(schema=self.schema)
        src_table = Table(
            table_name,
            md,
            autoload_with=self.source_engine
        )

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

    def _plan_add_foreign_keys_for_new_table(
        self,
        src_inspector,
        table_name: str,
    ) -> list[Operation]:
        """Планирует добавление FK для новой таблицы.

        Используется для диалектов, где возможно добавлять FK через ALTER TABLE
        (например, PostgreSQL). Для SQLite возвращает пустой список.

        Args:
            src_inspector: SQLAlchemy Inspector для source БД.
            table_name: Имя таблицы, для которой нужно добавить FK.

        Returns:
            list[Operation]: Операции add_foreign_key.
        """
        if self._is_sqlite():
            return []

        src_fks = src_inspector.get_foreign_keys(
            table_name,
            schema=self.schema
        ) or []
        return self._plan_foreign_keys(
            table_name=table_name,
            src_fks=src_fks,
            tgt_fks=None,
        )

    def _plan_add_missing_foreign_keys(
        self,
        table_name: str
    ) -> list[Operation]:
        """Планирует добавление отсутствующих FK для существующей таблицы.

        Сравнивает список FK в source и target и формирует операции добавления
        отсутствующих FK.

        Для SQLite добавление FK через ALTER TABLE не поддерживается, поэтому
        возвращается report-операция.

        Args:
            table_name: Имя таблицы для сравнения.

        Returns:
            list[Operation]: Операции add_foreign_key или report.
        """
        src_inspector = inspect(self.source_engine)
        tgt_inspector = inspect(self.target_engine)

        src_fks = src_inspector.get_foreign_keys(
            table_name,
            schema=self.schema
        ) or []

        try:
            tgt_fks = tgt_inspector.get_foreign_keys(
                table_name,
                schema=self.schema
            ) or []
        except Exception:
            tgt_fks = []

        if self._is_sqlite():
            src_sigs = {self._fk_signature(fk) for fk in src_fks}
            tgt_sigs = {self._fk_signature(fk) for fk in tgt_fks}
            missing = src_sigs - tgt_sigs
            if not missing:
                return []
            return [Operation(
                kind='report',
                sql='-- no-op',
                comment=(
                    'RISKY: SQLite cannot add FK via ALTER TABLE: '
                    f'table={table_name}, missing={len(missing)}'
                ),
            )]

        return self._plan_foreign_keys(
            table_name=table_name,
            src_fks=src_fks,
            tgt_fks=tgt_fks,
        )

    def _plan_foreign_keys(
        self,
        *,
        table_name: str,
        src_fks: list[dict],
        tgt_fks: list[dict] | None = None,
    ) -> list[Operation]:
        """Планирует операции добавления внешних ключей.

        - Если tgt_fks=None, планирует добавление всех FK из source
          (новая таблица).
        - Если tgt_fks передан, планирует добавление только отсутствующих FK.

        Args:
            table_name: Имя таблицы.
            src_fks: FK из source (Inspector.get_foreign_keys()).
            tgt_fks: FK из target или None.

        Returns:
            list[Operation]: Операции add_foreign_key.
        """
        tgt_sigs = {self._fk_signature(fk) for fk in (tgt_fks or [])}
        ops: list[Operation] = []

        for fk in src_fks:
            sig = self._fk_signature(fk)
            if tgt_fks is not None and sig in tgt_sigs:
                continue
            op = self._build_fk_operation(table_name, fk)
            if op is not None:
                ops.append(op)

        return ops

    def _build_fk_operation(
        self,
        table_name: str,
        fk: dict
    ) -> Operation | None:
        """Строит операцию добавления внешнего ключа по данным инспектора.

        Args:
            table_name: Имя таблицы, в которую добавляется FK.
            fk: Словарь с описанием FK из Inspector.get_foreign_keys().

        Returns:
            Operation | None: Operation(kind='add_foreign_key') или None, если
            недостаточно данных для построения корректного SQL.
        """
        ref_table = fk.get('referred_table')
        if not ref_table:
            return None

        name = fk.get('name') or self._make_fk_name(table_name, fk)

        cols = fk.get('constrained_columns') or []
        ref_cols = fk.get('referred_columns') or []
        ref_schema = fk.get('referred_schema') or self.schema

        if not cols or not ref_cols:
            return None

        cols_sql = ', '.join(self._q(c) for c in cols)
        ref_cols_sql = ', '.join(self._q(c) for c in ref_cols)

        if ref_schema:
            ref_table_sql = f'{self._q(ref_schema)}.{self._q(ref_table)}'
        else:
            ref_table_sql = self._q(ref_table)

        sql = (
            f'ALTER TABLE {self._qt(table_name)} '
            f'ADD CONSTRAINT {self._q(name)} '
            f'FOREIGN KEY ({cols_sql}) '
            f'REFERENCES {ref_table_sql} ({ref_cols_sql})'
        )

        opts = fk.get('options') or {}
        if opts.get('ondelete'):
            sql += f' ON DELETE {opts["ondelete"]}'
        if opts.get('onupdate'):
            sql += f' ON UPDATE {opts["onupdate"]}'

        if self.target_engine.dialect.name == 'postgresql':
            sql += ' NOT VALID'

        sql += ';'

        return Operation(
            kind='add_foreign_key',
            sql=sql,
            comment=f'Add foreign key {table_name}.{name}',
        )

    def _fk_signature(self, fk: dict) -> tuple:
        """Возвращает сигнатуру FK для сравнения source vs target."""
        opts = fk.get('options') or {}
        return (
            tuple(fk.get('constrained_columns') or []),
            fk.get('referred_schema') or self.schema,
            fk.get('referred_table'),
            tuple(fk.get('referred_columns') or []),
            opts.get('ondelete'),
            opts.get('onupdate'),
        )

    def _make_fk_name(self, table_name: str, fk: dict) -> str:
        """Генерирует детерминированное имя FK (если инспектор не дал name)."""
        cols = '_'.join(fk.get('constrained_columns') or [])
        ref = fk.get('referred_table') or 'ref'
        name = f'fk_{table_name}_{cols}_{ref}'
        return name[:60]

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

        out: dict[str, dict[str, object]] = {}
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

    def _is_sqlite(self) -> bool:
        """Возвращает True, если target-диалект SQLite."""
        return self.target_engine.dialect.name == 'sqlite'
