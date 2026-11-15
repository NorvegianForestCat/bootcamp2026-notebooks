# Data Dictionary — Lakehouse Dota 2

**Версия:** 1.0 (Draft)  
**Источник:** `.temp/Источники. Описание полей таблиц.md`, `sla.md`, `data_quality_pyspark.md`, ноутбуки `learn/`  
**Область:** PostgreSQL `dota`, MongoDB `dota_events`, Iceberg слои RAW/STAGE/MART  
**Регуляторный драйвер:** 152-ФЗ ст. 19, CBR 657-P, аудит

---

## 1. ОПРЕДЕЛЕНИЯ

| Термин | Определение |
|--------|-------------|
| **RAW** | Слой исходных данных, максимально близкий к источникам. Идемпотентная загрузка через MERGE INTO Iceberg. |
| **STAGE** | Слой очищенных, типизированных, дедуплицированных сущностей без бизнес-агрегаций. |
| **MART** | Аналитические витрины с бизнес-логикой, метриками и признаками для конкретных кейсов. |
| **PII** | Персональные данные (никнеймы, account_id) — требуют защиты и минимизации. |
| **DQ** | Data Quality — проверки полноты, корректности, согласованности, уникальности. |

---

## 2. PostgreSQL — схема `dota`

### Таблица `heroes` — Справочник героев Dota 2

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `hero_id` | INTEGER PK | INT | Уникальный ID героя (Valve) | Нет | Да |
| `name` | TEXT | STRING | Internal name (например `npc_dota_hero_antimage`) | Нет | Да |
| `localized_name` | TEXT | STRING | Отображаемое имя (например `Anti-Mage`) | Нет | Да |
| `primary_attr` | TEXT | STRING | Основной атрибут: `str`, `agi`, `int`, `all` | Нет | Да |
| `attack_type` | TEXT | STRING | Тип атаки: `Melee` или `Ranged` | Нет | Да |
| `roles` | array<string> | ARRAY<STRING> | Список ролей героя (например `["Carry", "Escape"]`) | Нет | Да |

**ПК:** `hero_id`  
**Источник:** Stratz GraphQL / Valve API  
**Слой:** RAW → STAGE

---

### Таблица `items` — Предметы Dota 2

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `item_id` | INTEGER PK | INT | Уникальный ID предмета | Нет | Да |
| `name` | TEXT | STRING | Short name (например `bfury`) | Нет | Да |
| `localized_name` | TEXT | STRING | Отображаемое имя (например `Battle Fury`) | Нет | Да |
| `cost` | INTEGER | INT | Стоимость в золоте | Нет | Да |
| `qual` | TEXT | STRING | Качество: `component`, `rare`, `epic`, `legendary`, `consumable`, `secret_shop` | Нет | Да |
| `components` | array<string> | ARRAY<STRING> | Список short name компонентов (null если нет) | Нет | Нет |
| `is_created` | BOOLEAN | BOOLEAN | true если создаётся из компонентов | Нет | Да |
| `lore` | TEXT | STRING | Лор-описание | Нет | Нет |
| `description` | TEXT | STRING | Описание способности (актив/пассив) | Нет | Нет |

**ПК:** `item_id`  
**Слой:** RAW → STAGE

---

### Таблица `neutral_items` — Нейтральные предметы

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `item_id` | INTEGER PK | INT | Уникальный ID предмета | Нет | Да |
| `name` | TEXT | STRING | Short name (например `keen_optic`) | Нет | Да |
| `localized_name` | TEXT | STRING | Отображаемое имя (например `Keen Optic`) | Нет | Да |
| `tier` | INTEGER | INT | Тир (1-5) | Нет | Да |
| `lore` | TEXT | STRING | Лор-описание | Нет | Нет |
| `description` | TEXT | STRING | Описание способности | Нет | Нет |

**ПК:** `item_id`  
**Валидация:** `tier BETWEEN 1 AND 5` (DQ-STG-02)

---

### Таблица `game_modes` — Режимы игры

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `mode_id` | INTEGER PK | INT | ID режима игры | Нет | Да |
| `name` | TEXT | STRING | Название (например `All Pick`) | Нет | Да |
| `balanced` | BOOLEAN | BOOLEAN | true если сбалансирован для рейтинга | Нет | Да |

**ПК:** `mode_id`  
**Использование:** Filtрация стандартных режимов в кейсе №22 (`game_mode IN (1, 2, 3, 22)`)

---

### Таблица `lobby_types` — Типы лобби

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `lobby_id` | INTEGER PK | INT | ID типа лобби | Нет | Да |
| `name` | TEXT | STRING | Название (например `Ranked`) | Нет | Да |
| `balanced` | BOOLEAN | BOOLEAN | true если лобби сбалансированное | Нет | Да |

**ПК:** `lobby_id`

---

### Таблица `abilities` — Способности героев

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `ability_id` | INTEGER PK | INT | Уникальный ID способности | Нет | Да |
| `name` | TEXT | STRING | Internal name (например `antimage_mana_break`) | Нет | Да |
| `display_name` | TEXT | STRING | Отображаемое имя | Нет | Да |
| `description` | TEXT | STRING | Описание способности | Нет | Нет |
| `aghanim_desc` | TEXT | STRING | Описание Aghanim's Scepter | Нет | Нет |
| `shard_desc` | TEXT | STRING | Описание Aghanim's Shard | Нет | Нет |
| `lore` | TEXT | STRING | Лор способности | Нет | Нет |
| `attributes` | array<string> | ARRAY<STRING> | Численные атрибуты (урон, радиус и т.п.) | Нет | Нет |
| `notes` | array<string> | ARRAY<STRING> | Дополнительные заметки | Нет | Нет |
| `is_talent` | BOOLEAN | BOOLEAN | true если это талант | Нет | Да |

**ПК:** `ability_id`  
**Источник:** Stratz GraphQL `constants.abilities`

---

### Таблица `hero_abilities` — Привязка способностей к героям

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `hero_id` | INTEGER FK | INT | ID героя (FK на `heroes`) | Нет | Да |
| `ability_id` | INTEGER FK | INT | ID способности (FK на `abilities`) | Нет | Да |
| `slot` | INTEGER | INT | Номер слота способности | Нет | Да |

**ПК:** composite (`hero_id`, `ability_id`)

---

### Таблица `hero_talents` — Таланты героев

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `hero_id` | INTEGER FK | INT | ID героя (FK на `heroes`) | Нет | Да |
| `ability_id` | INTEGER FK | INT | ID таланта (FK на `abilities`) | Нет | Да |
| `slot` | INTEGER | INT | Сырой слот от Stratz (0-7) | Нет | Да |
| `tier` | INTEGER | INT | Тир таланта 1-4 (`slot / 2 + 1`) | Нет | Да |
| `talent_level` | INTEGER | INT | Уровень героя: 10/15/20/25 | Нет | Да |

**ПК:** composite (`hero_id`, `ability_id`)

---

### Таблица `discover_runs` — Журнал запусков discover (Steam API → PG)

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `run_id` | SERIAL PK | INT | Уникальный ID запуска | Нет | Да |
| `started_at` | TIMESTAMPTZ | TIMESTAMP | Время начала | Нет | Да |
| `finished_at` | TIMESTAMPTZ | TIMESTAMP | Время завершения | Нет | Нет |
| `duration_sec` | REAL | FLOAT | Длительность (сек) | Нет | Нет |
| `count` | INTEGER | INT | Параметр `--count` | Нет | Да |
| `days_ago` | INTEGER | INT | Параметр `--days-ago` | Нет | Да |
| `status` | TEXT | STRING | Статус: `running`, `success`, `failed` | Нет | Да |
| `matches_discovered` | INTEGER | INT | Новых матчей через Steam | Нет | Да |
| `players_fetched` | INTEGER | INT | Обновлённых записей игроков | Нет | Да |
| `error_message` | TEXT | STRING | Ошибка (если статус = failed) | Нет | Нет |

**ПК:** `run_id`

---

### Таблица `enrich_runs` — Журнал запусков enrich (Stratz → MongoDB)

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `run_id` | SERIAL PK | INT | Уникальный ID запуска | Нет | Да |
| `started_at` | TIMESTAMPTZ | TIMESTAMP | Время начала | Нет | Да |
| `finished_at` | TIMESTAMPTZ | TIMESTAMP | Время завершения | Нет | Нет |
| `duration_sec` | REAL | FLOAT | Длительность (сек) | Нет | Нет |
| `limit_requested` | INTEGER | INT | Параметр `--limit` | Нет | Да |
| `status` | TEXT | STRING | Статус: `running`, `success`, `failed` | Нет | Да |
| `matches_enriched` | INTEGER | INT | Успешно обогащено | Нет | Да |
| `enriched_failed` | INTEGER | INT | Неудачных попыток | Нет | Да |
| `error_message` | TEXT | STRING | Ошибка (если статус = failed) | Нет | Нет |

**ПК:** `run_id`

---

### Таблица `cleanup_runs` — Журнал очистки (retention, 4 часа)

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `run_id` | SERIAL PK | INT | Уникальный ID запуска | Нет | Да |
| `started_at` | TIMESTAMPTZ | TIMESTAMP | Время начала | Нет | Да |
| `finished_at` | TIMESTAMPTZ | TIMESTAMP | Время завершения | Нет | Нет |
| `duration_sec` | REAL | FLOAT | Длительность (сек) | Нет | Нет |
| `status` | TEXT | STRING | Статус: `running`, `success`, `failed` | Нет | Да |
| `cutoff_dt` | TIMESTAMPTZ | TIMESTAMP | Время cutoff (удаляются данные до него) | Нет | Да |
| `matches_deleted` | INTEGER | INT | Матчей удалено из PG | Нет | Да |
| `players_deleted` | INTEGER | INT | Игроков удалено (включая orphans) | Нет | Да |
| `mongodb_docs_deleted` | INTEGER | INT | Документов удалено из MongoDB | Нет | Да |
| `mongodb_compacted` | BOOLEAN | BOOLEAN | Выполнен ли compact | Нет | Да |
| `error_message` | TEXT | STRING | Ошибка (если статус = failed) | Нет | Нет |

**ПК:** `run_id`  
**Режим:** Запускается каждые 4 часа, удаляет матчи старше 4 часов (по `loaded_at`)

---

### Таблица `matches` — Основная таблица матчей

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `match_id` | BIGINT PK | LONG | Уникальный ID матча (Valve) | Нет | Да |
| `match_seq_num` | BIGINT | LONG | Последовательный номер для пагинации | Нет | Нет |
| `start_time` | INTEGER | INT | Unix timestamp начала матча | Нет | Да |
| `duration` | INTEGER | INT | Длительность (сек) | Нет | Да |
| `game_mode` | INTEGER | INT | ID режима игры (FK на `game_modes`) | Нет | Да |
| `lobby_type` | INTEGER | INT | ID типа лобби (FK на `lobby_types`) | Нет | Да |
| `radiant_win` | BOOLEAN | BOOLEAN | true если Radiant победил | Нет | Да |
| `radiant_score` | INTEGER | INT | Убийств Radiant | Нет | Да |
| `dire_score` | INTEGER | INT | Убийств Dire | Нет | Да |
| `tower_status_radiant` | INTEGER | INT | Битовая маска башен Radiant | Нет | Да |
| `tower_status_dire` | INTEGER | INT | Битовая маска башен Dire | Нет | Да |
| `barracks_status_radiant` | INTEGER | INT | Битовая маска казарм Radiant | Нет | Да |
| `barracks_status_dire` | INTEGER | INT | Битовая маска казарм Dire | Нет | Да |
| `cluster` | INTEGER | INT | ID кластера (регион) | Нет | Нет |
| `first_blood_time` | INTEGER | INT | Время первого убийства (сек) | Нет | Нет |
| `human_players` | INTEGER | INT | Количество человек | Нет | Да |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время вставки в БД | Нет | Да |
| `stratz_parsed_at` | TIMESTAMPTZ | TIMESTAMP | Время обогащения Stratz | Нет | Нет |
| `discover_run_id` | INTEGER FK | INT | ID discover-запуска | Нет | Нет |
| `enrich_run_id` | INTEGER FK | INT | ID enrich-запуска | Нет | Нет |
| `enrichment_attempts` | INTEGER | INT | Попыток обогащения (макс. 5) | Нет | Да |

**ПК:** `match_id`  
**Валидация:** `duration >= 600 AND duration <= 18000` (10 мин – 5 ч), `human_players = 10` (DQ-STG-05)

---

### Таблица `match_players` — Статистика игроков в матче

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `match_id` | BIGINT FK | LONG | ID матча (FK на `matches`) | Нет | Да |
| `player_slot` | INTEGER | INT | Слот (0-127 Radiant, 128-255 Dire) | Нет | Да |
| `account_id` | BIGINT | LONG | Steam account ID | **Да (псевдоним)** | Да |
| `hero_id` | INTEGER | INT | ID героя (FK на `heroes`) | Нет | Да |
| `item_0` … `item_5` | INTEGER | INT | ID предмета в слоте 0-5 | Нет | Нет |
| `backpack_0` … `backpack_2` | INTEGER | INT | ID предмета в рюкзаке 0-2 | Нет | Нет |
| `item_neutral` | INTEGER | INT | ID нейтрального предмета | Нет | Нет |
| `kills` | INTEGER | INT | Убийства | Нет | Да |
| `deaths` | INTEGER | INT | Смерти | Нет | Да |
| `assists` | INTEGER | INT | Ассисты | Нет | Да |
| `kda` | REAL | FLOAT | KDA: (kills + assists) / max(deaths, 1) | Нет | Да |
| `gold` | INTEGER | INT | Золото на конец матча | Нет | Да |
| `gold_per_min` | INTEGER | INT | GPM | Нет | Да |
| `xp_per_min` | INTEGER | INT | XPM | Нет | Да |
| `gold_spent` | INTEGER | INT | Потраченное золото | Нет | Нет |
| `net_worth` | INTEGER | INT | Net worth | Нет | Нет |
| `hero_damage` | INTEGER | INT | Урон героям | Нет | Нет |
| `tower_damage` | INTEGER | INT | Урон башням | Нет | Нет |
| `hero_healing` | INTEGER | INT | Лечение героев | Нет | Нет |
| `level` | INTEGER | INT | Уровень героя | Нет | Да |
| `last_hits` | INTEGER | INT | Last hits | Нет | Да |
| `denies` | INTEGER | INT | Denies | Нет | Да |
| `is_radiant` | BOOLEAN | BOOLEAN | true если в Radiant | Нет | Да |
| `win` | BOOLEAN | BOOLEAN | true если выиграл матч | Нет | Да |
| `aghanims_scepter` | BOOLEAN | BOOLEAN | true если куплен Scepter | Нет | Нет |
| `aghanims_shard` | BOOLEAN | BOOLEAN | true если куплен Shard | Нет | Нет |
| `rank` | INTEGER | INT | Ранг (1-80: 1-10=Herald … 71-80=Immortal) | Нет | Нет |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время последнего upsert строки | Нет | Да |

**ПК:** composite (`match_id`, `player_slot`)  
**Валидация:** `player_slot IN (0,1,2,3,4,128,129,130,131,132)`, `hero_id > 0`, `kills >= 0`, `deaths >= 0`, `assists >= 0` (DQ-STG-06)

---

### Таблица `players` — Справочник имён игроков

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `account_id` | BIGINT PK | LONG | Steam account ID | **Да (псевдоним)** | Да |
| `personaname` | TEXT | STRING | Отображаемое имя в Steam | **Да** | Да |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Время последнего upsert строки | Нет | Да |

**ПК:** `account_id`  
**PII:** `personaname` — может содержать токсичные/личные данные. Фильтрация: `account_id != 4294967295` (аноним), `TRIM(personaname) <> ''` (DQ-CASE21-01)

---

### Таблица `npcs` — Справочник NPC из Stratz

| Поле | Тип PG | Тип Iceberg | Описание | PII | Обязательное |
|------|--------|-------------|----------|-----|--------------|
| `npc_id` | INTEGER PK | INT | ID NPC | Нет | Да |
| `name` | TEXT | STRING | Имя (например `npc_dota_creep_badguys_ranged`) | Нет | Да |
| `status_health` | REAL | FLOAT | Здоровье | Нет | Да |
| `status_mana` | REAL | FLOAT | Мана | Нет | Да |
| `movement_speed` | REAL | FLOAT | Скорость передвижения | Нет | Да |
| `attack_range` | REAL | FLOAT | Дальность атаки | Нет | Да |
| `attack_damage_min` | REAL | FLOAT | Мин. урон | Нет | Да |
| `attack_damage_max` | REAL | FLOAT | Макс. урон | Нет | Да |
| `attack_rate` | REAL | FLOAT | Скорость атаки | Нет | Да |
| `projectile_speed` | REAL | FLOAT | Скорость снаряда | Нет | Да |
| `team_name` | TEXT | STRING | Команда (Radiant/Dire/Neutrals) | Нет | Да |
| `is_neutral_unit` | BOOLEAN | BOOLEAN | Нейтральный юнит | Нет | Да |
| `is_ancient` | BOOLEAN | BOOLEAN | Древний | Нет | Да |
| `can_be_dominated` | BOOLEAN | BOOLEAN | Можно захватить (Chen/Helm) | Нет | Да |
| `has_inventory` | BOOLEAN | BOOLEAN | Есть инвентарь | Нет | Да |
| `level` | REAL | FLOAT | Уровень | Нет | Да |

**ПК:** `npc_id`  
**Использование:** `npcId = 133` для событий Рошана (케이с №3)

---

## 3. MongoDB — база `dota_events`

### Общие поля для всех коллекций

| Поле | Тип Mongo | Тип Iceberg | Описание |
|------|-----------|-------------|----------|
| `_id` | ObjectId | STRING | Уникальный ID документа |
| `match_id` | long | LONG | ID матча |
| `created_at` | datetime | TIMESTAMP | Время записи в UTC |

### Player-level kolekции (имеют `steam_account_id`)

| Колекция | Ключевые поля | Назначение |
|----------|-------------|------------|
| `cs_events` | `steam_account_id`, `time`, `attacker`, `npcId`, `isFromIllusion`, `gold`, `xp`, `isCreep`, `isNeutral`, `isAncient` | Last hits / denies, включая Рошана (npcId=133) |
| `gold_events` | `steam_account_id`, `time`, `amount`, `reason`, `npcId` | Начисление золота; `reason = 'ROSHAN'` для кейса №3 |
| `kill_events` | `steam_account_id`, `time`, `attacker`, `target`, `byAbility`, `byItem`, `isSolo`, `isGank`, `isSmoke` | Убийства героев |
| `death_events` | `steam_account_id`, `time`, `attacker`, `target`, `goldFed`, `xpFed`, `assist` (array), `isFromIllusion` | Смерти героев |
| `buyback_events` | `steam_account_id`, `time`, `heroId`, `cost` | Байбэки |
| `heal_events` | `steam_account_id`, `time`, `value`, `byAbility`, `byItem`, `attacker`, `target` | Лечение |
| `hero_damage_events` | `steam_account_id`, `time`, `value`, `damageType`, `attacker`, `target`, `fromNpc`, `toNpc` | Урон героям |
| `ability_used_events` | `steam_account_id`, `time`, `abilityId`, `attacker`, `target` | Использование способностей |
| `ability_learn_events` | `steam_account_id`, `time`, `abilityId`, `levelObtained`, `isUltimate`, `isTalent` | Изучение способностей |
| `rune_events` | `steam_account_id`, `time`, `rune`, `action`, `gold` | Сбор рун |
| `streak_events` | `steam_account_id`, `time`, `heroId`, `type`, `value` | Серии убийств |
| `assist_events` | `steam_account_id`, `time`, `attacker`, `target`, `gold`, `xp` | Ассисты |
| `player_update_level_events` | `steam_account_id`, `time`, `level` | Повышение уровня |
| `purchase_events` | `steam_account_id`, `time`, `itemId` | Покупки предметов |

### Match-level kolekции (без `steam_account_id`)

| Колекция | Ключевые поля | Назначение |
|----------|-------------|------------|
| `tower_damage_events` | `time`, `attacker`, `npcId`, `damage`, `byAbility`, `byItem` | Урон по башням |
| `tower_death_events` | `time`, `radiant` (mask), `dire` (mask) | Уничтожение башен |
| `ward_events` | `time`, `indexId`, `fromPlayer`, `wardType`, `action`, `playerDestroyed` | Варды |

**Валидация времени событий:** `time >= -90` и `time <= 18060` (DQ-STG-09)  
**Валидация уникальности:** строковое представление `_id` (ObjectId) уникально во всех коллекциях событий (DQ-STG-08)

---

## 4. Связи между таблицами (Lineage)

```
matches.match_id ← match_players.match_id
matches.match_id ← cs_events.match_id, gold_events.match_id, kill_events.match_id, ...
match_players.account_id ← players.account_id
match_players.hero_id ← heroes.hero_id
cs_events.npcId ← npcs.npc_id (133 = Roshan)
```

**PII-флаги:** `players.personaname`, `players.account_id`, `match_players.account_id` — псевдонимизированы, требуют защиты при экспорте.

---

## 5. PII-регистр

| Поле | Таблица | Классификация | Обработка |
|------|---------|---------------|-----------|
| `personaname` | `players` | PII (имя) | Не выгружать без同意, минимизировать в выгрузках |
| `account_id` | `players`, `match_players` | PII (псевдоним) | Шифровать при передаче вне платформы |
| `steam_account_id` | Mongo коллекции | PII (псевдоним) | Не хранить в открытом виде вне Iceberg |

---

## 6. Ответственный за поддержку

Владелец документа: команда аналитики Data Engineering.  
Обновление: при изменении схемы источников или добавлении новых таблиц.  
Проверка актуальности: ежемесячно.