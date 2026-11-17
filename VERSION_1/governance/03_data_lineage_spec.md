# Data Lineage Specification (OpenLineage)

**Версия:** 1.0 (Draft)  
**Область:** Lakehouse Dota 2 (RAW → STAGE → MART → Trino → Superset)  
**Регуляторный драйвер:** 152-ФЗ, аудит, BCBS 239  
**Инструмент:** Apache OpenLineage → Marquez  
**Статус:** Проект документа

---

## 1. Назначение

Линейность данных (lineage) обеспечивает прослеживаемость:
- из каких таблиц получены данные;
- какие фильтры применялись;
- какие преобразования выполнялись;
- какие бизнес-метрики рассчитывались;
- в какой итоговой витрине сохранён результат.

Трассировка требуется для аудита, отладки, инцидентов и соблюдения регулятивных требований.

---

## 2. Архитектура линейности

```
Источник (PG/Mongo) → Airflow DAG → Spark Job → Iceberg RAW → Iceberg STAGE → Iceberg MART → Trino → Superset
      │                                                                          │
      ▼                                                                          ▼
Marquez (OpenLineage) ←──────────────────────────────────────────────────────────┘
```

- **Ноутбуки:** эмитят события lineage через кастомный менеджер контекста `ctx.lineage()`.
- **Airflow DAGs:** эмитят события lineage при запуске/завершении задач (на платформе).
- **Marquez:** хранит граф зависимостей наборов данных и задач.

---

## 3. Формат событий lineage

Каждое событие содержит:

| Поле | Описание | Пример |
|------|----------|--------|
| `job.name` | Имя задания | `practice-case-03-raw-matches` |
| `job.namespace` | Пространство имён | `bootcamp2026.case03` |
| `run.runId` | Уникальный ID запуска | UUID |
| `inputs` | Список входных наборов данных | `raw.pg__matches` |
| `outputs` | Список выходных наборов данных | `raw.practice_case_03_matches` |
| `description` | Описание преобразования | `Загрузка matches для кейса 3` |
| `producer` | Инструмент, создавший событие | `bootcamp2026-notebooks` |
| `timestamp` | Время события | ISO 8601 |

---

## 4. Реализация в ноутбуках

Линейность реализуется через менеджер контекста `ctx.lineage()` в `lakehouse_spark`:

```python
with ctx.lineage(
    "practice-case-03-raw-matches",
    inputs=["raw.pg__matches"],
    output="raw.practice_case_03_matches",
    description="Загрузка matches для кейса 3 из истории озера",
    dataset_descriptions={
        "raw.practice_case_03_matches": "Накопительная RAW-таблица матчей (история из озера)."
    }
):
    upsert_iceberg(spark, frame, output, keys)
```

**Правила:**
1. Каждое чтение из источника и запись в слой должны быть обёрнуты в `ctx.lineage()`.
2. `inputs` — все таблицы, из которых читаются данные.
3. `outputs` — все таблицы, в которые пишутся данные.
4. `dataset_descriptions` — описание каждого выходного набора.

---

## 5. Карта наборов данных (Datasets)

| Набор данных | Слой | Источники | Преобразования | Потребители |
|--------------|------|-----------|----------------|-------------|
| `raw.pg__matches` | RAW | PostgreSQL `dota.matches` | Копирование (идемпотентно) | STAGE, MART |
| `raw.pg__match_players` | RAW | PostgreSQL `dota.match_players` | Копирование | STAGE, MART |
| `raw.mongo__cs_events` | RAW | MongoDB `dota_events.cs_events` | Копирование (filter npcId=133) | STAGE, MART |
| `raw.mongo__gold_events` | RAW | MongoDB `dota_events.gold_events` | Копирование (filter reason='ROSHAN') | STAGE, MART |
| `stage.practice_case_03_clean_matches` | STAGE | `raw.practice_case_03_matches` | Дедупликация, валидация, фильтрация | MART |
| `stage.practice_case_03_clean_match_players` | STAGE | `raw.practice_case_03_match_players` + `stage.practice_case_03_clean_matches` | Дедупликация, ссылочная целостность | MART |
| `mart.practice_case_03_match_roshan_mart` | MART | 5 STAGE таблиц | Каскадная привязка стороны, башни, агрегаты | Trino, Superset, Analytics |
| `mart.practice_case_03_roshan_time_effect` | MART | `mart.practice_case_03_match_roshan_mart` | Агрегация по интервалам, квантили | Trino, Superset |
| `mart.practice_case_22_match_draft_mart` | MART | `stage.practice_case_22_clean_matches`, `stage.practice_case_22_clean_match_players` | 5×5 пары героев, target variable | ML обучение, Superset |
| `mart.practice_case_22_hero_matchups` | MART | `stage.practice_case_22_clean_match_players`, `mart.practice_case_22_match_draft_mart` | 25 пар на матч | ML анализ контр-пиков |
| `stage.nick_classification` | STAGE | `raw.pg__players` + LLM API | Классификация ников | Анализ сегментов (№21) |

---

## 6. Проверка целостности lineage

Обязательные проверки перед публикацией витрины:

| Проверка | Описание |
|----------|----------|
| **Все входы отражены** | Каждый входной набор в ноутбуке указан в `ctx.lineage(inputs=...)` |
| **Все выходы отражены** | Каждая записанная таблица указана в `ctx.lineage(output=...)` |
| **Нет разорванных связей** | Каждый MART набор имеет цепочку lineage до RAW |
| **Описания заполнены** | `dataset_descriptions` заполнены для всех выходов |

---

## 7. Связь с DQ и инцидентами

При обнаружении проблемы в витрине:
1. Найти витрину в Marquez.
2. Построить обратный граф (до источника).
3. Определить, какое преобразование или источник стало причиной.
4. Зафиксировать в инциденте (см. Incident Response Runbook).

---

## 8. Ответственный

Владелец: Data Engineeringチーム.  
Обновление: при добавлении новых таблиц, изменении схемы или появлении новых кейсов.