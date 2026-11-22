# Data Dictionary — Lakehouse Dota 2 (VERSION_2)

**Версия:** 2.0 (Production)  
**Область:** PostgreSQL `dota`, MongoDB `dota_events`, Iceberg слои RAW/STAGE/MART  
**Актуальность:** Документ описывает **только те таблицы и поля, которые реально задействованы** в реализованном ETL-пайплайне (Кейс №3: Рошан и победа).

---

## 1. ОПРЕДЕЛЕНИЯ

| Термин | Определение |
|--------|-------------|
| **RAW** | Слой исходных данных. Идемпотентная загрузка через `MERGE INTO` с использованием watermark. |
| **STAGE** | Слой очищенных, типизированных, дедуплицированных сущностей без бизнес-агрегаций. |
| **MART** | Аналитические витрины с бизнес-логикой, метриками и признаками для конкретных кейсов. |
| **PII** | Персональные данные (никнеймы, account_id) — требуют защиты и минимизации. |
| **DQ** | Data Quality — проверки полноты, корректности, согласованности, уникальности. |

---

## 2. PostgreSQL — схема `dota`

### Таблица `matches` — Основная таблица матчей

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `match_id` | BIGINT PK | LONG | Уникальный ID матча (Valve) | Нет | Да |
| `start_time` | INTEGER | INT | Unix timestamp начала матча | Нет | Да |
| `duration` | INTEGER | INT | Длительность (сек) | Нет | Да |
| `radiant_win` | BOOLEAN | BOOLEAN | true если Radiant победил | Нет | Да |
| `human_players` | INTEGER | INT | Количество человек | Нет | Да |
| `tower_status_radiant` | INTEGER | INT | Битовая маска башен Radiant | Нет | Да |
| `tower_status_dire` | INTEGER | INT | Битовая маска башен Dire | Нет | Да |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время вставки в БД (используется как watermark) | Нет | Да |

**ПК:** `match_id`  
**Валидация:** `duration >= 600 AND duration <= 18000`, `human_players = 10`

---

### Таблица `match_players` — Статистика игроков в матче

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `match_id` | BIGINT FK | LONG | ID матча (FK на `matches`) | Нет | Да |
| `player_slot` | INTEGER | INT | Слот (0-127 Radiant, 128-255 Dire) | Нет | Да |
| `account_id` | BIGINT | LONG | Steam account ID | **Да (псевдоним)** | Да |
| `hero_id` | INTEGER | INT | ID героя (FK на `heroes`) | Нет | Да |
| `is_radiant` | BOOLEAN | BOOLEAN | true если в Radiant | Нет | Да |
| `win` | BOOLEAN | BOOLEAN | true если выиграл матч | Нет | Да |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время последнего upsert строки | Нет | Да |

**ПК:** composite (`match_id`, `player_slot`)  
**Валидация:** `player_slot IN (0,1,2,3,4,128,129,130,131,132)`, `hero_id > 0`

---

### Таблица `players` — Справочник имён игроков

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `account_id` | BIGINT PK | LONG | Steam account ID | **Да (псевдоним)** | Да |
| `personaname` | TEXT | STRING | Отображаемое имя в Steam | **Да** | Да |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время последнего upsert строки | Нет | Да |

**ПК:** `account_id`  
**PII:** `personaname` — может содержать токсичные/личные данные. Фильтрация: `account_id != 4294967295` (аноним).

---

### Таблица `heroes` — Справочник героев Dota 2

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `hero_id` | INTEGER PK | INT | Уникальный ID героя (Valve) | Нет | Да |
| `name` | TEXT | STRING | Internal name (например `npc_dota_hero_antimage`) | Нет | Да |
| `localized_name` | TEXT | STRING | Отображаемое имя (например `Anti-Mage`) | Нет | Да |

**ПК:** `hero_id`

---

### Таблица `npcs` — Справочник NPC из Stratz

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `npc_id` | INTEGER PK | INT | ID NPC | Нет | Да |
| `name` | TEXT | STRING | Имя (например `npc_dota_roshan`) | Нет | Да |
| `is_ancient` | BOOLEAN | BOOLEAN | Древний | Нет | Да |

**ПК:** `npc_id`  
**Использование:** `npc_id = 133` для фильтрации событий Рошана.

---

## 3. MongoDB — база `dota_events`

### Общие поля для всех коллекций

| Поле | Тип Mongo | Тип Iceberg | Описание |
|------|-----------|-------------|----------|
| `_id` | ObjectId | STRING | Уникальный ID документа |
| `match_id` | long | LONG | ID матча |
| `created_at` | datetime | TIMESTAMP | Время записи в UTC |

### Коллекция `cs_events` (Player-level)
Добывание крипов (last hits / denies), включая Рошана.

| Поле | Тип Mongo | Тип Iceberg | Описание |
|------|-----------|-------------|----------|
| `steam_account_id` | long | LONG | Steam account ID |
| `time` | int | INT | Время события (сек) |
| `attacker` | int | INT | steamAccountId игрока |
| `npcId` | int | INT | ID убитого NPC (133 для Рошана) |
| `isFromIllusion` | bool | BOOLEAN | Добил иллюзией |

---

### Коллекция `gold_events` (Player-level)
Получение золота.

| Поле | Тип Mongo | Тип Iceberg | Описание |
|------|-----------|-------------|----------|
| `steam_account_id` | long | LONG | Steam account ID |
| `time` | int | INT | Время получения (сек) |
| `amount` | int | INT | Количество золота |
| `reason` | string | STRING | Причина: `ROSHAN`, `Kill`, `Tower` и т.д. |

---

### Коллекция `tower_death_events` (Match-level)
Моменты уничтожения башен.

| Поле | Тип Mongo | Тип Iceberg | Описание |
|------|-----------|-------------|----------|
| `time` | int | INT | Время уничтожения (сек) |
| `radiant` | int | INT | Битовая маска башен Radiant |
| `dire` | int | INT | Битовая маска башен Dire |

---

## 4. Связи между таблицами (Lineage)

```text
matches.match_id ← match_players.match_id
matches.match_id ← cs_events.match_id, gold_events.match_id, tower_death_events.match_id
match_players.account_id ← players.account_id
match_players.hero_id ← heroes.hero_id
cs_events.npcId ← npcs.npc_id (133 = Roshan)
```

**PII-флаги:** `players.personaname`, `players.account_id`, `match_players.account_id` — псевдонимизированы, требуют защиты при экспорте.

---

## 5. Ответственный за поддержку

**Владелец документа:** команда Data Engineering.  
**Обновление:** при изменении схемы источников или добавлении новых таблиц в пайплайн.  
**Проверка актуальности:** ежемесячно.