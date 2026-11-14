# Спецификация Data Quality (DQ) для Lakehouse Dota 2

Документ описывает правила контроля качества данных для архитектуры **Medallion (`RAW` → `STAGE` → `MART`)** на базе **Apache Iceberg**, **Spark** и **Airflow**, а также специализированные проверки под аналитические и ML-задачи (Кейсы №3, №20, №21).

---

## 1. Базовые требования по слоям Lakehouse

### Слой 1: Источники → `RAW` (Контроль доставки и окна очистки)

Поскольку джоба `cleanup_runs` удаляет данные старше 4 часов из PostgreSQL и MongoDB, критически важно проверять полноту и своевременность перелива.

#### Актуальность / Timeliness (`DQ-RAW-01`)
* **Правило:** Максимальное значение `loaded_at` в `raw.matches` должно отставать от времени запуска DAG не более чем на 2 часа (буфер безопасности до 4-часового `cutoff_dt` в `cleanup_runs`).
* **Реакция:** *Hard Fail* (остановка пайплайна и алерт).

#### Сверка объёмов / Reconciliation (`DQ-RAW-02`)
* **Правило:** Количество выгруженных строк в `raw.matches` за батч должно совпадать с суммой `matches_discovered` по успешным запускам в таблице `discover_runs`.
* **Реакция:** *Hard Fail*.

#### Полнота обогащения / Enrichment Completeness (`DQ-RAW-03`)
* **Правило:** Для 100% матчей из `raw.matches`, у которых `stratz_parsed_at IS NOT NULL`, в `raw.death_events`, `raw.cs_events` или `raw.gold_events` должен присутствовать минимум 1 документ с соответствующим `match_id`.
* **Реакция:** *Soft Fail / Warning* (логирование в `PostgreSQL Metadata`).

---

### Слой 2: `RAW` → `STAGE` (Очистка, фильтрация и валидация сущностей)

#### Уникальность и полнота справочников (`DQ-STG-01`)
* **Правило:** Первичные ключи `hero_id`, `item_id`, `ability_id`, `npc_id` в таблицах `heroes`, `items`, `neutral_items`, `abilities`, `npcs` уникальны и не содержат `NULL`.
* **Реакция:** *Hard Fail*.

#### Валидность атрибутов справочников (`DQ-STG-02`)
* **Правило:** В `heroes` поле `primary_attr IN ('str', 'agi', 'int', 'all')`, `attack_type IN ('Melee', 'Ranged')`. В `neutral_items` поле `tier BETWEEN 1 AND 5`.
* **Реакция:** *Hard Fail*.

#### Уникальность матчей и слотов игроков (`DQ-STG-03`)
* **Правило:** `match_id` уникален в `stage.matches`; составной ключ `(match_id, player_slot)` уникален в `stage.match_players`.
* **Реакция:** *Hard Fail* (после дедупликации по `loaded_at DESC`).

#### Обязательные поля матча (`DQ-STG-04`)
* **Правило:** Поля `match_id`, `start_time`, `duration`, `radiant_win`, `hero_id` строго `NOT NULL`.
* **Реакция:** *Hard Fail*.

#### Фильтрация длительности матчей и валидность счёта (`DQ-STG-05`)
* **Правило:** Отсекаются несостоявшиеся/быстрые матчи длительностью менее 10 минут и зависшие лобби: `duration >= 600 AND duration <= 18000` (от 10 минут до 5 часов). Счёт убийств неотрицательный: `radiant_score >= 0 AND dire_score >= 0`.
* **Реакция:** *Quarantine* (отправка в `stage.quarantine_matches` / отсев из витрин).

#### Валидность слотов, рангов и базовой статистики (`DQ-STG-06`)
* **Правило:** `player_slot IN (0, 1, 2, 3, 4, 128, 129, 130, 131, 132)`; `rank IS NULL OR (rank BETWEEN 1 AND 80)`; `kills >= 0 AND deaths >= 0 AND assists >= 0 AND gold_per_min >= 0 AND xp_per_min >= 0`.
* **Реакция:** *Quarantine*.

#### Комплектность составов 5v5 (`DQ-STG-07`)
* **Правило:** Для каждого валидного `match_id` в `stage.match_players` присутствует ровно 10 записей (5 игроков за Radiant с `player_slot < 128` и 5 игроков за Dire с `player_slot >= 128`), при этом `human_players = 10`.
* **Реакция:** *Quarantine*.

#### Уникальность событий MongoDB (`DQ-STG-08`)
* **Правило:** Строковое представление `_id` (ObjectId) уникально во всех коллекциях событий (`cs_events`, `gold_events`, `death_events` и др.).
* **Реакция:** *Hard Fail*.

#### Валидность игрового времени событий (`DQ-STG-09`)
* **Правило:** Поле `time >= -90` (учитываем прегейм-стадию до выхода крипов на `0:00`) и `time <= 18060`.
* **Реакция:** *Quarantine*.

---

### Слой 3: `STAGE` → `MART` (Ссылочная целостность)

#### Целостность внешних ключей (`DQ-MRT-01`)
* **Правило:** 100% `hero_id` из `stage.match_players` присутствуют в `stage.heroes`; 100% `match_id` из событийных таблиц существуют в `stage.matches`.
* **Реакция:** *Hard Fail* перед сборкой витрин.

---

## 2. Специфические требования Data Quality по кейсам

### Кейс №3: Корреляция между временем убийства Рошана и победой

**Используемые источники:** `cs_events` (`npcId = 133`), `gold_events` (`reason = 'ROSHAN'`), `matches` (`radiant_win`, `duration >= 600`).

#### Согласованность событий убийства Рошана (`DQ-CASE3-01`)
* **Правило:** На каждое событие добивания Рошана в `cs_events` (`npcId = 133`, `time > 0`) в том же матче и в окне `±5 секунд` должно приходиться ровно 5 записей начисления командного золота в `gold_events` (`reason = 'ROSHAN'`) для игроков победившей Рошана стороны.
* **Реакция:** *Quarantine* (исключение матча с рассинхронизированными логами из расчёта).

#### Адекватность таймингов возрождения Рошана (`DQ-CASE3-02`)
* **Правило:** Первый Рошан не может быть убит до выхода крипов (`time >= 0`). Интервал между последовательными убийствами Рошана внутри одного `match_id` должен составлять не менее 480 секунд (минимальный таймер респавна Рошана — 8 минут).
* **Реакция:** *Quarantine*.

#### Полнота истории событий для длинных матчей (`DQ-CASE3-03`)
* **Правило:** Так как `cleanup_runs` очищает MongoDB каждые 4 часа, в витрину для Кейса №3 допускаются только матчи, у которых `stratz_parsed_at IS NOT NULL` и не было пропусков батчей (`DQ-RAW-01`), иначе матчи без убийств Рошана нельзя отличить от матчей с потерянными логами.
* **Реакция:** *Hard Fail* при нарушении непрерывности истории.

---

### Кейс №20: Сегментация героев по пику и контр-пику (ALS Matrix Factorization)

**Используемые источники:** `match_players` (`match_id`, `hero_id`, `player_slot`, `is_radiant`, `win`), `matches` (`duration >= 600`), `heroes`.

#### Валидность индикатора команды и исхода для ALS (`DQ-CASE20-01`)
* **Правило:** В матрице взаимодействий `match × hero` (или `team_match × hero` для ALS) значения `hero_id` строго входят в актуальный справочник `stage.heroes`, а в одном матче все 10 `hero_id` уникальны (нет дублирующихся героев в рамках одного матча, отсекаем кастомные режимы вроде Ability Draft / All Random Deathmatch, проверяя `game_mode IN (1, 2, 3, 4, 22)`).
* **Реакция:** *Quarantine* (исключение нестандартных режимов из обучающей выборки ALS).

#### Баланс выборки и достаточность покрытий (`DQ-CASE20-02`)
* **Правило:** Каждый активный герой из `stage.heroes` должен встречаться в обучающем батче не менее `N` раз (например, `>= 30` матчей), а доля побед `AVG(CAST(win AS INT))` по всему датасету должна находиться строго в диапазоне `[0.49, 0.51]` (проверка отсутствия перекоса при фильтрации матчей `< 10 мин`).
* **Реакция:** *Hard Fail* (если выборка перекошена или есть «холодные» герои с 0 матчей, модель ALS выдаст нерелевантные эмбеддинги).

#### Отсутствие пропусков в признаках матрицы (`DQ-CASE20-03`)
* **Правило:** В финальном датафрейме для `pyspark.ml.recommendation.ALS` колонки `match_idx` (int), `hero_id` (int) и целевой рейтинг/индикатор (`rating` / `team_indicator`) не содержат `NULL` и `NaN`.
* **Реакция:** *Hard Fail*.

---

### Кейс №21: Классификация ников игроков (LLM via API)

**Используемые источники:** `players` (`account_id`, `personaname`, `loaded_at`), `match_players` (`account_id`, `win`, `kills`, `deaths`, `assists`).

#### Фильтрация анонимных и пустых ников (`DQ-CASE21-01`)
* **Правило:** Из потока на отправку в LLM исключаются записи с `account_id = 4294967295` (анонимный профиль Steam), `personaname IS NULL`, а также пустые строки после очистки пробелов (`LENGTH(TRIM(personaname)) = 0`).
* **Реакция:** *Quarantine / Filter* (экономия токенов LLM API).

#### Дедупликация и нормализация перед батчингом (`DQ-CASE21-02`)
* **Правило:** Отправка в LLM происходит строго по уникальным значениям `TRIM(personaname)`. Повторные запросы по уже классифицированным никам (сверка с целевой таблицей `stage.nick_classification`) запрещены.
* **Реакция:** *Hard Fail* (защита бюджета API от зацикливания).

#### Валидация JSON-схемы ответа от LLM (`DQ-CASE21-03`)
* **Правило:** Ответ модели для каждого ника должен парситься как валидный JSON и содержать обязательные ключи:
  * `script` из множества `{'latin', 'cyrillic', 'chinese', 'mixed', 'emoji_only'}`
  * `type` из множества `{'normal', 'pro_team', 'clan_tag', 'bot_like', 'streamer', 'toxic', 'numbers_only'}`
  * `toxic` булевого типа (`true` / `false`)
* **Реакция:** *Quarantine* (невалидные ответы LLM складываются в `stage.quarantine_nick_classification` для повторного ретрая, не роняя весь батч из 10 000 ников).

#### Контроль доли ошибок API (`DQ-CASE21-04`)
* **Правило:** Доля упавших или нераспарсенных ответов LLM в одном запуске не должна превышать `5%` от размера батча.
* **Реакция:** *Hard Fail* (сигнал о проблемах с токеном, лимитами или деградацией провайдера LLM).

---

## 3. Код проверок на PySpark

Ниже приведён полный модуль на PySpark, реализующий базовую очистку (`duration >= 600` сек) и специфические DQ-пайплайны для Кейсов №3, №20 (ALS) и №21.

```python
import json
from dataclasses import dataclass
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T


@dataclass
class DQRule:
    rule_id: str
    description: str
    sql_condition: str
    severity: str  # "HARD" или "QUARANTINE"


class DotaLakehouseDQ:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def apply_rules(
        self,
        df: DataFrame,
        table_name: str,
        rules: List[DQRule],
        run_id: str,
        pk_cols: Optional[List[str]] = None,
        order_col: str = "loaded_at"
    ) -> DataFrame:
        """Базовый движок проверки DQ: дедупликация, HARD-ассерты и изоляция в карантин."""
        if pk_cols:
            w = Window.partitionBy(*pk_cols).orderBy(F.col(order_col).desc())
            df = df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")

        total_rows = df.count()
        if total_rows == 0:
            raise ValueError(f"[DQ-EMPTY] {table_name}: пустой датафрейм (run_id={run_id})")

        checked_df = df
        for r in rules:
            checked_df = checked_df.withColumn(
                f"__flag_{r.rule_id}",
                F.expr(f"COALESCE(({r.sql_condition}), false)")
            )

        checked_df.cache()

        # Проверка HARD-правил
        for r in rules:
            if r.severity == "HARD":
                bad_cnt = checked_df.filter(~F.col(f"__flag_{r.rule_id}")).count()
                if bad_cnt > 0:
                    checked_df.unpersist()
                    raise RuntimeError(
                        f"[DQ HARD FAIL] {table_name} | {r.rule_id} ({r.description}): "
                        f"нарушено в {bad_cnt}/{total_rows} строк."
                    )

        # Обработка QUARANTINE-правил
        q_rules = [r for r in rules if r.severity == "QUARANTINE"]
        flag_cols = [c for c in checked_df.columns if c.startswith("__flag_")]

        if q_rules:
            valid_expr = " AND ".join([f"__flag_{r.rule_id}" for r in q_rules])
            reasons_expr = F.array_compact(
                F.array(*[F.when(~F.col(f"__flag_{r.rule_id}"), F.lit(r.rule_id)) for r in q_rules])
            )

            quarantine_df = (
                checked_df.filter(f"NOT ({valid_expr})")
                .withColumn("dq_failed_rules", reasons_expr)
                .withColumn("dq_run_id", F.lit(run_id))
                .withColumn("dq_quarantined_at", F.current_timestamp())
                .drop(*flag_cols)
            )

            if quarantine_df.count() > 0:
                quarantine_df.writeTo(f"iceberg.stage.quarantine_{table_name}").append()

            clean_df = checked_df.filter(valid_expr).drop(*flag_cols)
        else:
            clean_df = checked_df.drop(*flag_cols)

        checked_df.unpersist()
        return clean_df

    # ==========================================================================
    # БАЗОВАЯ ОЧИСТКА МАТЧЕЙ (ОТСЕКАЕМ МАТЧИ < 10 МИНУТ)
    # ==========================================================================
    def process_base_matches_and_players(self, run_id: str, watermark_ts: str):
        match_rules = [
            DQRule("DQ-STG-04", "Обязательные поля заполнены",
                   "match_id IS NOT NULL AND start_time IS NOT NULL AND duration IS NOT NULL AND radiant_win IS NOT NULL",
                   "HARD"),
            DQRule("DQ-STG-05", "Длительность матча от 10 минут (600с) до 5 часов (18000с)",
                   "duration >= 600 AND duration <= 18000",
                   "QUARANTINE"),
            DQRule("DQ-STG-05B", "Неотрицательный счёт убийств и 10 человек",
                   "radiant_score >= 0 AND dire_score >= 0 AND human_players = 10",
                   "QUARANTINE"),
        ]

        raw_matches = self.spark.table("iceberg.raw.matches").filter(f"loaded_at > TIMESTAMP '{watermark_ts}'")
        clean_matches = self.apply_rules(raw_matches, "matches", match_rules, run_id, pk_cols=["match_id"])
        clean_matches.writeTo("iceberg.stage.matches").append()

        mp_rules = [
            DQRule("DQ-STG-03", "ПК не NULL", "match_id IS NOT NULL AND player_slot IS NOT NULL", "HARD"),
            DQRule("DQ-STG-06A", "Валидный слот", "player_slot IN (0,1,2,3,4,128,129,130,131,132)", "HARD"),
            DQRule("DQ-STG-04B", "Валидный герой", "hero_id IS NOT NULL AND hero_id > 0", "QUARANTINE"),
            DQRule("DQ-STG-06B", "Неотрицательные метрики",
                   "kills >= 0 AND deaths >= 0 AND assists >= 0 AND gold_per_min >= 0", "QUARANTINE"),
        ]

        raw_mp = self.spark.table("iceberg.raw.match_players").filter(f"loaded_at > TIMESTAMP '{watermark_ts}'")
        clean_mp = self.apply_rules(raw_mp, "match_players", mp_rules, run_id, pk_cols=["match_id", "player_slot"])

        # Оставляем игроков только из валидных матчей (>= 10 мин) и с полным составом 5v5 (DQ-STG-07)
        valid_5v5 = (
            clean_mp.join(clean_matches.select("match_id"), on="match_id", how="inner")
            .groupBy("match_id")
            .agg(
                F.count("*").alias("cnt"),
                F.sum(F.when(F.col("player_slot") < 128, 1).otherwise(0)).alias("rad_cnt")
            )
            .filter("cnt = 10 AND rad_cnt = 5")
            .select("match_id")
        )

        final_mp = clean_mp.join(valid_5v5, on="match_id", how="inner")
        final_mp.writeTo("iceberg.stage.match_players").append()

    # ==========================================================================
    # КЕЙС №3: DQ ДЛЯ АНАЛИЗА УБИЙСТВ РОШАНА
    # ==========================================================================
    def validate_case3_roshan_data(self, run_id: str) -> DataFrame:
        matches_df = self.spark.table("iceberg.stage.matches").filter("duration >= 600")
        cs_df = self.spark.table("iceberg.stage.cs_events").filter("npcId = 133")
        gold_df = self.spark.table("iceberg.stage.gold_events").filter("reason = 'ROSHAN'")

        # DQ-CASE3-02: Проверка тайминга первого Рошана (>= 0) и интервала респавна (>= 480 сек)
        w_roshan = Window.partitionBy("match_id").orderBy("time")
        roshan_kills = (
            cs_df.withColumn("prev_kill_time", F.lag("time").over(w_roshan))
            .withColumn("respawn_diff", F.col("time") - F.col("prev_kill_time"))
            .withColumn("roshan_num", F.row_number().over(w_roshan))
        )

        roshan_rules = [
            DQRule("DQ-CASE3-02A", "Рошан убит после выхода крипов (time >= 0)", "time >= 0", "QUARANTINE"),
            DQRule("DQ-CASE3-02B", "Интервал между Рошанами >= 480 сек",
                   "respawn_diff IS NULL OR respawn_diff >= 480", "QUARANTINE"),
        ]
        valid_roshan_kills = self.apply_rules(roshan_kills, "case3_roshan_kills", roshan_rules, run_id)

        # DQ-CASE3-01: Сверка с командным золотом (ровно 5 игроков получили золото в окне +-5 сек)
        gold_grouped = (
            valid_roshan_kills.alias("rk")
            .join(
                gold_df.alias("g"),
                (F.col("rk.match_id") == F.col("g.match_id")) &
                (F.abs(F.col("rk.time") - F.col("g.time")) <= 5),
                how="left"
            )
            .groupBy("rk.match_id", "rk.roshan_num", "rk.time", "rk.attacker")
            .agg(F.count("g._id").alias("gold_recipients_cnt"))
        )

        consistency_rules = [
            DQRule("DQ-CASE3-01", "Ровно 5 игроков получили золото за Рошана",
                   "gold_recipients_cnt = 5", "QUARANTINE")
        ]
        clean_roshan_events = self.apply_rules(gold_grouped, "case3_roshan_consistency", consistency_rules, run_id)

        # Объединяем с исходом матча (только матчи >= 10 мин)
        return clean_roshan_events.join(matches_df.select("match_id", "radiant_win", "duration"), on="match_id", how="inner")

    # ==========================================================================
    # КЕЙС №20: DQ ДЛЯ ALS (ПИКИ И КОНТР-ПИКИ)
    # ==========================================================================
    def validate_case20_als_dataset(self, run_id: str) -> DataFrame:
        matches_df = self.spark.table("iceberg.stage.matches").filter("duration >= 600")
        mp_df = self.spark.table("iceberg.stage.match_players")
        heroes_df = self.spark.table("iceberg.stage.heroes")

        # Оставляем только стандартные режимы (All Pick, Captains Mode, Random Draft, Ranked All Pick)
        std_matches = matches_df.filter("game_mode IN (1, 2, 3, 22)")

        joined_df = (
            mp_df.join(std_matches.select("match_id", "radiant_win"), on="match_id", how="inner")
            .join(heroes_df.select("hero_id"), on="hero_id", how="inner")
        )

        # DQ-CASE20-01: В одном матче должно быть ровно 10 уникальных героев
        match_hero_counts = (
            joined_df.groupBy("match_id")
            .agg(F.countDistinct("hero_id").alias("unique_heroes"))
            .filter("unique_heroes = 10")
            .select("match_id")
        )

        als_input_df = (
            joined_df.join(match_hero_counts, on="match_id", how="inner")
            .withColumn("is_radiant_team", F.when(F.col("player_slot") < 128, 1).otherwise(0))
            # Формируем rating для ALS: 1.0 если герой в победившей команде, 0.0 если в проигравшей
            .withColumn(
                "rating",
                F.when(
                    ((F.col("player_slot") < 128) & F.col("radiant_win")) |
                    ((F.col("player_slot") >= 128) & (~F.col("radiant_win"))),
                    F.lit(1.0)
                ).otherwise(F.lit(0.0))
            )
            .select(
                F.col("match_id").cast("long"),
                F.col("hero_id").cast("int"),
                F.col("is_radiant_team").cast("int"),
                F.col("rating").cast("float")
            )
        )

        # DQ-CASE20-03: Проверка отсутствия NULL/NaN
        als_rules = [
            DQRule("DQ-CASE20-03", "Отсутствие NULL в признаках ALS",
                   "match_id IS NOT NULL AND hero_id IS NOT NULL AND rating IS NOT NULL AND NOT isnan(rating)",
                   "HARD")
        ]
        als_input_df = self.apply_rules(als_input_df, "case20_als_matrix", als_rules, run_id)

        # DQ-CASE20-02: Проверка баланса винрейта выборки [0.49, 0.51]
        stats = als_input_df.agg(
            F.avg("rating").alias("avg_winrate"),
            F.count("*").alias("total_rows")
        ).collect()[0]

        if stats["total_rows"] == 0 or not (0.49 <= stats["avg_winrate"] <= 0.51):
            raise RuntimeError(
                f"[DQ HARD FAIL] DQ-CASE20-02: Перекос выборки для ALS! "
                f"Средний winrate = {stats['avg_winrate']:.4f} (ожидается 0.49..0.51)."
            )

        return als_input_df

    # ==========================================================================
    # КЕЙС №21: DQ ДЛЯ КЛАССИФИКАЦИИ НИКОВ (LLM VIA API)
    # ==========================================================================
    def prepare_case21_nicks_for_llm(self, run_id: str) -> DataFrame:
        """Фильтрация и дедупликация ников перед отправкой в LLM API (DQ-CASE21-01, DQ-CASE21-02)."""
        players_df = self.spark.table("iceberg.stage.players")

        # Исключаем анонимные профили (4294967295) и пустые имена
        clean_nicks = (
            players_df
            .filter("account_id IS NOT NULL AND account_id != 4294967295")
            .filter("personaname IS NOT NULL AND LENGTH(TRIM(personaname)) > 0")
            .withColumn("clean_nick", F.trim(F.col("personaname")))
            .select("clean_nick")
            .distinct()
        )

        # Исключаем ники, которые уже были классифицированы ранее
        if self.spark.catalog.tableExists("iceberg.stage.nick_classification"):
            already_classified = self.spark.table("iceberg.stage.nick_classification").select("clean_nick")
            clean_nicks = clean_nicks.join(already_classified, on="clean_nick", how="left_anti")

        return clean_nicks

    def validate_case21_llm_responses(self, raw_llm_df: DataFrame, run_id: str) -> DataFrame:
        """Валидация JSON-ответов от LLM API (DQ-CASE21-03, DQ-CASE21-04)."""
        llm_schema = T.StructType([
            T.StructField("script", T.StringType(), True),
            T.StructField("type", T.StringType(), True),
            T.StructField("toxic", T.BooleanType(), True),
            T.StructField("description", T.StringType(), True),
        ])

        parsed_df = raw_llm_df.withColumn("parsed", F.from_json(F.col("raw_json_response"), llm_schema))

        total_batch = parsed_df.count()
        if total_batch == 0:
            raise ValueError("[DQ-EMPTY] Пустой батч ответов от LLM.")

        llm_rules = [
            DQRule("DQ-CASE21-03A", "Валидный JSON и поле script",
                   "parsed.script IN ('latin', 'cyrillic', 'chinese', 'mixed', 'emoji_only')",
                   "QUARANTINE"),
            DQRule("DQ-CASE21-03B", "Валидное поле type",
                   "parsed.type IN ('normal', 'pro_team', 'clan_tag', 'bot_like', 'streamer', 'toxic', 'numbers_only')",
                   "QUARANTINE"),
            DQRule("DQ-CASE21-03C", "Булево поле toxic заполнено",
                   "parsed.toxic IS NOT NULL",
                   "QUARANTINE"),
        ]

        valid_df = self.apply_rules(parsed_df, "case21_llm_responses", llm_rules, run_id)

        # DQ-CASE21-04: Проверяем, что доля брака от LLM не превысила 5%
        valid_count = valid_df.count()
        error_rate = 1.0 - (valid_count / total_batch)
        if error_rate > 0.05:
            raise RuntimeError(
                f"[DQ HARD FAIL] DQ-CASE21-04: Превышен порог ошибок LLM API! "
                f"Брак составил {error_rate:.2%} (максимум 5%)."
            )

        return valid_df.select(
            "clean_nick",
            F.col("parsed.script").alias("script"),
            F.col("parsed.type").alias("nick_type"),
            F.col("parsed.toxic").alias("is_toxic"),
            F.col("parsed.description").alias("description"),
            F.current_timestamp().alias("classified_at")
        )
```