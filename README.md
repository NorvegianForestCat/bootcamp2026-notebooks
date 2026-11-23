# Bootcamp 2026: Dota 2 Data Engineering & Analytics

> **Кратко:** Этот репозиторий содержит материалы и реализацию аналитических кейсов по данным матчей Dota 2, созданных в рамках учебного буткемпа 2026 года.  
> **In short:** This repository contains materials and implementation of analytical cases based on Dota 2 match data, developed as part of the 2026 educational bootcamp.

---

## 📂 Структура репозитория / Repository Structure

Репозиторий разделён на две основные версии, отражающие эволюцию проекта от исследовательского прототипа к production-ready решению.  
The repository is divided into two main versions, reflecting the project's evolution from a research prototype to a production-ready solution.

### 📁 [`VERSION_1/`](./VERSION_1/) — Исходные материалы буткемпа (Bootcamp Research & Prototypes)
Содержит исходные исследовательские материалы, созданные участниками в рамках учебного процесса:  
*Contains original research materials created by participants during the educational process:*
- **Jupyter Notebooks:** EDA, статистические проверки гипотез, прототипы кода и черновики преобразований.  
  *(EDA, statistical hypothesis testing, code prototypes, and draft transformations.)*
- **Governance-документация:** Data Dictionary, MRM Policy, Data Lineage Spec, Incident Response Runbook, Access Control Matrix, SLA.  
  *(Governance documentation.)*
- **Спецификации:** Data Quality (PySpark) и описания источников данных.  
  *(Data Quality specifications and data source descriptions.)*

> ⚠️ **Важно / Important:** Эта версия служит источником бизнес-требований и логики, но **не предназначена** для прямого развёртывания или промышленной эксплуатации.  
> *This version serves as a source of business requirements and logic, but is **not intended** for direct deployment or production use.*

### 📁 [`VERSION_2/`](./VERSION_2/) — Production-реализация (Production-Ready Engineering)
Технически улучшенная, модульная и полностью автоматизированная версия проекта, доведённая до "презентабельного ума". Реализует принципы Medallion Architecture (RAW → STAGE → MART) на базе современного стека DataOps.  
*A technically improved, modular, and fully automated version of the project, refined to a "presentable state". It implements Medallion Architecture principles (RAW → STAGE → MART) using a modern DataOps stack.*

- **Реализованный кейс:** Полностью реализован **Кейс №3: Влияние убийства Рошана на исход матча** (включая генерацию данных, ETL-пайплайн, DQ-проверки и оркестрацию). Остальные кейсы из `VERSION_1` запланированы к переносу по аналогичному шаблону.  
  *(Implemented case: **Case #3: Roshan Kill Impact on Match Outcome** is fully implemented, including data generation, ETL pipeline, DQ checks, and orchestration. Other cases from `VERSION_1` are planned for migration using the same template.)*
- **Технический стек / Tech Stack:**
  - **Оркестрация / Orchestration:** Apache Airflow (DAGs с TaskGroup по слоям).
  - **Обработка / Processing:** Apache Spark (PySpark) с интеграцией OpenLineage.
  - **Хранение / Storage:** Apache Iceberg поверх RustFS (S3-compatible object storage).
  - **Источники / Sources:** PostgreSQL (структурированные данные), MongoDB (события).
  - **Аналитика / Analytics:** Trino (SQL-движок) + Apache Superset (BI-визуализация).
  - **Наблюдаемость / Observability:** Marquez (Data Lineage).

---

## 🚀 Быстрый старт (VERSION_2) / Quick Start (VERSION_2)

Для развёртывания всей инфраструктуры и запуска пайплайна:  
*To deploy the entire infrastructure and run the pipeline:*

1. Перейдите в директорию `VERSION_2`:  
   *Navigate to the `VERSION_2` directory:*
   ```bash
   cd VERSION_2
   ```

2. Скопируйте переменные окружения (при необходимости отредактируйте `.env`):  
   *Copy environment variables (edit `.env` if necessary):*
   ```bash
   cp .env.example .env
   ```

3. Запустите инфраструктуру:  
   *Start the infrastructure:*
   ```bash
   docker compose --profile base --profile etl --profile compute --profile bi --profile lineage up -d
   ```

4. Дождитесь инициализации сервисов (1–2 минуты) и проверьте статус:  
   *Wait for services to initialize (1–2 mins) and check status:*
   ```bash
   docker compose ps
   ```

5. Откройте веб-интерфейсы / Open web interfaces:
   - **Airflow:** http://localhost:8081 *(admin / admin)*
   - **Superset:** http://localhost:8088 *(admin / admin)*
   - **Marquez:** http://localhost:5000

---

## 🛡️ Комплаенс и качество данных / Compliance & Data Quality

Проект строго следует принципам Data Governance. Подробные правила, политики и спецификации описаны в отдельных документах:  
*The project strictly follows Data Governance principles. Detailed rules, policies, and specifications are described in separate documents:*

- 🇷🇺 [`COMPLIANCE-ru.md`](./COMPLIANCE-ru.md) — Описание мер по обеспечению качества, безопасности и наблюдаемости данных (на русском).
- 🇬🇧 [`COMPLIANCE-en.md`](./COMPLIANCE-en.md) — Compliance and Data Governance overview (in English).

> ⚠️ **Честность реализации / Implementation Honesty:**  
> Документация по комплаенсу отражает **только реально реализованные** в `VERSION_2` механизмы.  
> *Compliance documentation reflects **only actually implemented** mechanisms in `VERSION_2`.*
> - Инкрементальная загрузка (`MERGE INTO`) реализована только на слое RAW; слои STAGE и MART используют полный пересчёт (`createOrReplace`) для гарантии целостности агрегатов.  
>   *(Incremental loading (`MERGE INTO`) is implemented only at the RAW layer; STAGE and MART layers use full recomputation (`createOrReplace`) to guarantee aggregate integrity.)*
> - Механизмы криптографического шифрования данных в покое **не применяются** в рамках данного учебного проекта, что прямо указано в соответствующих разделах документации.  
>   *(Cryptographic encryption mechanisms for data at rest are **not applied** in this educational project, which is explicitly stated in the relevant documentation sections.)*
> - Регуляторные упоминания (152-ФЗ, CBR, Basel) приведены исключительно как примеры лучших практик, к которым стремится архитектура; проект является учебным и не подпадает под реальное регулирование.  
>   *(Regulatory mentions (152-FZ, CBR, Basel) are provided solely as examples of best practices the architecture aims for; the project is educational and not subject to actual regulation.)*

---

## 📝 Лицензия / License

Этот проект распространяется под лицензией MIT. См. файл `LICENSE` для подробностей.  
*This project is distributed under the MIT License. See the `LICENSE` file for details.*