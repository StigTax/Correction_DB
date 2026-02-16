# Correction_DB

Инструмент для безопасной синхронизации схемы `target`-БД по эталонной
`source`-БД.

Ключевая идея: `target` уже содержит данные → автоматически выполняются только аддитивные изменения (без удаления/переписывания данных).

## Что делает

- строит план изменений через `SchemaCorrector.diff()`;
- применяет план через `SchemaCorrector.apply()`:
  - в режиме dry-run печатает SQL и ничего не меняет;
  - в режиме apply выполняет SQL в транзакции;
- операции `kind="report"` никогда не выполняются, только печатаются/логируются.

### Какие операции считаются безопасными

Автоматически планируются и применяются только:
  - `create_table` — создание отсутствующей таблицы;
  - `add_column` — добавление отсутствующей колонки;
  - `create_index` — создание отсутствующего индекса;
  - `add_foreign_key` — добавление FK только там, где это безопасно/поддерживается (см. ниже).

### Что не делает (и почему)

- не удаляет таблицы/колонки/индексы из `target`;
- не меняет типы колонок;
- не ужесточает `NULL/NOT NULL` автоматически;
- рискованные различия возвращает как `Operation(kind="report")` и логирует `WARNING`.

### Особенности по внешним ключам (FK)

- PostgreSQL: FK добавляются через `ALTER TABLE ... ADD CONSTRAINT ...` и помечаются `NOT VALID` (чтобы не валидировать constraint на больших данных автоматически).

- SQLite:
  - для новых таблиц FK включаются в `CREATE TABLE`;
  - для существующих таблиц SQLite не умеет `ALTER TABLE ADD CONSTRAINT`, поэтому возвращается `report` (если есть различия по FK).

Если в `target` уже есть FK на те же колонки, но на другую сущность/таблицу — это считается конфликтом и должно уходить в `report` (без попыток “добавить ещё один FK”).

## Установка

Требования: Python 3.12+.

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

Если используете PostgreSQL, установите драйвер:

```bash
pip install psycopg2-binary
```

## CLI запуск

В проекте добавлена консольная точка входа: `main.py`.

### 1. Dry-run (по умолчанию)

Команда только печатает план SQL-операций и ничего не меняет в `target`.

```bash
python main.py \
  --source-url "postgresql+psycopg2://user:pass@host:5432/source_db" \
  --target-url "postgresql+psycopg2://user:pass@host:5432/target_db" \
  --schema public
```

### 2. Реальное применение изменений

Добавьте флаг `--apply`, чтобы выполнить SQL в `target`.

```bash
python main.py \
  --source-url "postgresql+psycopg2://user:pass@host:5432/source_db" \
  --target-url "postgresql+psycopg2://user:pass@host:5432/target_db" \
  --schema public \
  --lock-timeout 10 \
  --statement-timeout 30 \
  --log-level INFO \
  --apply
```

### Параметры CLI

- `--source-url` (обязательный): URL эталонной БД;
- `--target-url` (обязательный): URL целевой БД;
- `--schema` (опциональный): схема, например `public`;
- `--lock-timeout` (опциональный, по умолчанию `10`): timeout блокировок в секундах;
- `--statement-timeout` (опциональный, по умолчанию `0`): timeout SQL в секундах (`0` = без лимита);
- `--log-level` (опциональный, по умолчанию `INFO`): `DEBUG|INFO|WARNING|ERROR|CRITICAL`;
- `--apply`: выполнить изменения (без флага остаётся dry-run).

## Использование как Python API

```python
from src.corrector import SchemaCorrector

corrector = SchemaCorrector(
    source_url="postgresql+psycopg2://user:pass@host:5432/source_db",
    target_url="postgresql+psycopg2://user:pass@host:5432/target_db",
    schema="public",
    lock_timeout_seconds=10,
    statement_timeout_seconds=30,
)

ops = corrector.diff()

# 1) Просмотр плана (ничего не применяет)
corrector.apply(ops, dry_run=True)

# 2) Применение только безопасных операций
safe_ops = [op for op in ops if op.kind != "report"]
corrector.apply(safe_ops, dry_run=False)
```

## Логирование

CLI импортирует `src/log_conf.py`, где настраивается базовый `logging`.

Типовые уровни:

- `INFO`: этапы работы и выполняемые операции;
- `WARNING`: рискованные различия и лишние объекты в `target`;
- `ERROR`: ошибка применения SQL (с traceback);
- `CRITICAL`: процесс коррекции прерван.

Уровень логирования в CLI можно переопределить через `--log-level`.

## Безопасность данных

По умолчанию потенциально опасные изменения не применяются автоматически
(например, изменение типов и ужесточение ограничений).

Перед запуском с `--apply` на боевой базе рекомендуется:

- сделать резервную копию;
- сначала прогнать dry-run;
- проверить выполнение на копии боевой БД.

## Тесты

В `pytest.ini` настроены:

- `pythonpath = src`;
- маркеры для `unit` и `integration`-сценариев.

### Маркеры

- `unit`: быстрые тесты без реальной БД;
- `integration`: тесты с реальной БД/движком;
- `sqlite`: интеграционные тесты SQLite;
- `postgres`: интеграционные тесты PostgreSQL;
- `dry_run`: тесты режима dry-run;
- `logging`: тесты логирования и обработки ошибок.

### Запуск

```bash
# Все тесты
pytest -q

# Только unit
pytest -m unit -q

# Интеграция SQLite
pytest -m "integration and sqlite" -q

# Интеграция PostgreSQL
pytest -m "integration and postgres" -q
```

Для PostgreSQL-интеграции задайте переменные окружения:

- `POSTGRES_SOURCE_URL`;
- `POSTGRES_TARGET_URL`.

Если переменные не заданы, PostgreSQL-тесты будут пропущены.

## CI

Workflow: `.github/workflows/tests.yml`.
