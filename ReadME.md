# Correction_DB

Инструмент для безопасной синхронизации схемы боевой базы данных
на основе тестовой или эталонной базы.

## Назначение

Есть две базы данных:

- `source` — эталонная (обычно тестовая), в неё внесены изменения схемы.
- `target` — боевая, уже содержит данные, которые нельзя повредить.

Цель: подтянуть схему `target` к `source` максимально безопасно.

## Установка

Требования: Python 3.12 и выше.

```bash
python -m venv venv

# Для Windows
venv\Scripts\activate

# Для Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

## Быстрый старт

Пример использования (`dry-run` + применение только безопасных операций):

```python
from src.corrector import SchemaCorrector

corrector = SchemaCorrector(
    source_url="postgresql+psycopg2://user:pass@host:5432/source_db",
    target_url="postgresql+psycopg2://user:pass@host:5432/target_db",
    schema="public",  # Опционально.
    lock_timeout_seconds=10,
    statement_timeout_seconds=30,
)

ops = corrector.diff()

# 1. Посмотреть план (ничего не применяет).
corrector.apply(ops, dry_run=True)

# 2. Применить только безопасные операции.
safe_ops = [op for op in ops if op.kind != "report"]
corrector.apply(safe_ops, dry_run=False)
```

## Логирование

В проекте используется стандартный модуль `logging`. Конфигурацию можно
держать в `src/log_conf.py` (например, через `logging.basicConfig(...)`), а
затем импортировать в точке входа.

Типовые уровни:

- `INFO` — этапы работы и выполняемые операции;
- `WARNING` — рискованные различия и "лишние" объекты в `target`;
- `ERROR` — ошибка применения SQL (с `traceback`);
- `CRITICAL` — процесс коррекции прерван.

Минимальная настройка:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
```

## Безопасность данных

По умолчанию не выполняются операции, которые могут повредить данные
(изменение типов, `NOT NULL`, `UNIQUE` и т. д.).

Перед применением на боевой базе рекомендуется:

- сделать резервную копию (или снимок);
- прогнать инструмент на копии боевой базы.

## Тесты

В `pytest.ini` настроено:

- `pythonpath = src` (поэтому импорты в тестах: `from corrector import ...`);
- маркеры для `unit`/`integration` и отдельных наборов тестов.

### Маркеры

- `unit` — быстрые тесты без реальной БД;
- `integration` — тесты с реальной БД или движком;
- `sqlite` — интеграционные тесты на SQLite;
- `postgres` — интеграционные тесты на PostgreSQL;
- `dry_run` — тесты режима `dry-run`;
- `logging` — тесты логирования и обработки ошибок.

### Запуск тестов

```bash
# Все тесты
pytest -q

# Только модульные (`unit`)
pytest -m unit -q

# Интеграция SQLite
pytest -m "integration and sqlite" -q

# Интеграция PostgreSQL
pytest -m "integration and postgres" -q
```

Для PostgreSQL-тестов нужны переменные окружения:

- `POSTGRES_SOURCE_URL`;
- `POSTGRES_TARGET_URL`.

Если переменные не заданы, соответствующие тесты будут пропущены.

## Непрерывная интеграция (GitHub Actions)

Файл workflow: `.github/workflows/tests.yml`.

При отправке изменений (`push`) в ветку `main` запускаются:

- модульные тесты (`unit`);
- интеграционные тесты на SQLite;
- интеграционные тесты на PostgreSQL (с поднятием контейнера PostgreSQL).
