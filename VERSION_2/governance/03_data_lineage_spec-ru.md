# Data Lineage Specification (OpenLineage) — VERSION_2

**Версия:** 2.0 (Production)  
**Область:** Lakehouse Dota 2 (RAW → STAGE → MART → Trino → Superset)  
**Регуляторный драйвер:** Отсутствует (учебный проект). Архитектура спроектирована с оглядкой на лучшие практики (аналоги 152-ФЗ, BCBS 239) исключительно для формирования правильных инженерных привычек. Реализация полного соответствия регуляторным требованиям не выполнена.  
**Инструмент:** Apache OpenLineage → Marquez  
**Актуальность:** Документ описывает lineage **только для реализованного пайплайна Кейса №3**.

---

## 1. Назначение

Линейность данных (lineage) обеспечивает полную прослеживаемость:
- из каких таблиц получены данные;
- какие фильтры применялись;
- какие преобразования выполнялись;
- какие бизнес-метрики рассчитывались;
- в какой итоговой витрине сохранён результат.

Трассировка требуется для аудита, отладки, расследования инцидентов и соблюдения регулятивных требований.

---

## 2. Архитектура линейности

```text
Источник (PG/Mongo) → Airflow DAG → Spark Job → Iceberg RAW → Iceberg STAGE → Iceberg MART → Trino → Superset
      │                                                                          │
      ▼                                                                          ▼
Marquez (OpenLineage) ←──────────────────────────────────────────────────────────┘
```

- **PySpark Jobs:** эмитят события lineage через кастомный менеджер контекста `ctx.lineage()` из `src/common/lakehouse_spark.py`.
- **Airflow DAGs:** автоматически эмитят события lineage при запуске/завершении задач.
- **Marquez:** хранит и визуализирует граф зависимостей наборов данных и задач.

---

## 3. Формат событий lineage

Каждое событие OpenLineage содержит:

| Поле | Описание | Пример |
|------|----------|--------|
| `job.name` | Имя задания | `practice-case-03-stage-clean-matches` |
| `job.namespace` | Пространство имён | `bootcamp2026.case03` |
| `run.runId` | Уникальный ID запуска | UUID |
| `inputs` | Список входных наборов данных | `["lakehouse.raw.pg__matches"]` |
| `outputs` | Список выходных наборов данных | `["lakehouse.stage.case03_pg__matches"]` |
| `description` | Описание преобразования | `Дедупликация и очистка матчей` |
| `producer` | Инструмент, создавший событие | `bootcamp2026-notebooks/VERSION_2` |
| `timestamp` | Время события | ISO 8601 |

---

## 4. Реализация в PySpark

Линейность реализуется через менеджер контекста `ctx.lineage()`:

```python
with ctx.lineage(
    job_name="practice-case-03-stage-clean-matches",
    inputs=["lakehouse.raw.pg__matches"],
    output="lakehouse.stage.case03_pg__matches",
    description="Дедупликация и очистка матчей (без бизнес-агрегаций)",
    dataset_descriptions={
        "lakehouse.stage.case03_pg__matches": "Чистая таблица матчей >= 10 мин и 10 игроков."
    }
):
    clean_matches.writeTo("lakehouse.stage.case03_pg__matches").using("iceberg").createOrReplace()
```

**Правила:**
1. Каждое чтение из источника и запись в слой должны быть обёрнуты в `ctx.lineage()`.
2. `inputs` — все таблицы, из которых читаются данные.
3. `outputs` — все таблицы, в которые пишутся данные.
4. `dataset_descriptions` — краткое описание каждого выходного набора.

---

## 5. Карта наборов данных (Datasets) для Кейса №3

| Набор данных | Слой | Источники | Преобразования | Потребители |
|--------------|------|-----------|----------------|-------------|
| `lakehouse.raw.pg__matches` | RAW | PostgreSQL `dota.matches` | Идемпотентный MERGE INTO | STAGE |
| `lakehouse.raw.pg__match_players` | RAW | PostgreSQL `dota.match_players` | Идемпотентный MERGE INTO | STAGE |
| `lakehouse.raw.mongo__cs_events` | RAW | MongoDB `dota_events.cs_events` | Фильтр `npcId=133`, MERGE INTO | STAGE |
| `lakehouse.raw.mongo__gold_events` | RAW | MongoDB `dota_events.gold_events` | Фильтр `reason='ROSHAN'`, MERGE INTO | STAGE |
| `lakehouse.raw.mongo__tower_death_events` | RAW | MongoDB `dota_events.tower_death_events` | Идемпотентный MERGE INTO | STAGE |
| `lakehouse.stage.case03_pg__matches` | STAGE | `raw.pg__matches` | Дедупликация, валидация, фильтрация | MART |
| `lakehouse.stage.case03_pg__match_players` | STAGE | `raw.pg__match_players` + `stage.case03_pg__matches` | Дедупликация, ссылочная целостность | MART |
| `lakehouse.stage.case03_mongo__cs_events` | STAGE | `raw.mongo__cs_events` + `stage.case03_pg__matches` | Валидация времени, дедупликация | MART |
| `lakehouse.stage.case03_mongo__gold_events` | STAGE | `raw.mongo__gold_events` + `stage.case03_pg__matches` | Валидация времени, дедупликация | MART |
| `lakehouse.stage.case03_mongo__tower_death_events` | STAGE | `raw.mongo__tower_death_events` + `stage.case03_pg__matches` | Валидация времени, дедупликация | MART |
| `lakehouse.mart.case03_pg__match_roshan_mart` | MART | 5 STAGE таблиц | Каскадная привязка стороны, башни, агрегаты | Trino, Superset, DQ |
| `lakehouse.mart.case03_pg__roshan_time_effect` | MART | `mart.case03_pg__match_roshan_mart` | Агрегация по интервалам, квантили | Trino, Superset, DQ |
| `lakehouse.mart.case03_pg__roshan_control_effect` | MART | `mart.case03_pg__match_roshan_mart` | Агрегация по контролю и темпу матча | Trino, Superset, DQ |

---

## 6. Проверка целостности lineage

Обязательные проверки перед публикацией витрины:
- [ ] **Все входы отражены:** Каждый входной набор в коде указан в `ctx.lineage(inputs=...)`.
- [ ] **Все выходы отражены:** Каждая записанная таблица указана в `ctx.lineage(output=...)`.
- [ ] **Нет разорванных связей:** Каждый MART набор имеет непрерывную цепочку lineage до RAW.
- [ ] **Описания заполнены:** `dataset_descriptions` заполнены для всех выходов.

---

## 7. Связь с DQ и инцидентами

При обнаружении проблемы в витрине:
1. Найти витрину в интерфейсе Marquez.
2. Построить обратный граф (Upstream) до источника.
3. Определить, какое преобразование или источник стал причиной аномалии.
4. Зафиксировать находку в инциденте (см. `04_incident_response_runbook-ru.md`).

---

## 8. Ответственный

**Владелец:** команда Data Engineering.  
**Обновление:** при добавлении новых таблиц, изменении схемы или появлении новых кейсов.  
**Проверка актуальности:** ежемесячно.