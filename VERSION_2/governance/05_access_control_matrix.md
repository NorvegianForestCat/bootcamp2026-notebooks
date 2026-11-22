# Access Control Matrix (RBAC)

**Версия:** 1.0 (Draft)  
**Область:** Lakehouse (RAW/STAGE/MART), Airflow, Trino, Superset, LLM API  
**Принцип:** Минимальные привилегии (least privilege)  
**Статус:** Проект документа

---

## 1. Роли

| Роль | Описание | Состав |
|------|----------|--------|
| **Data Analyst** | Аналитика, построение витрин, исследования | Участники буткемпа, аналитики |
| **Data Engineer** | Разработка ETL, поддержка pipeline | Команда Data Engineering |
| **Data Scientist** | Обучение ML-моделей, эксперименты | Команда ML |
| **ML Engineer** | Внедрение и мониторинг моделей | Команда ML Ops |
| **BI Consumer** | Просмотр дашбордов Superset | Бизнес-пользователи, заказчики |
| **Portal Admin** | Администрирование портала, пользователи | Администраторы платформы |
| **Audit / Compliance** | Аудит, проверка соответствия | Служба безопасности, риск-менеджмент |

---

## 2. Матрица доступа к данным (Lakehouse)

| Ресурс | Data Analyst | Data Engineer | Data Scientist | ML Engineer | BI Consumer | Portal Admin | Audit |
|--------|--------------|---------------|----------------|-------------|-------------|--------------|-------|
| **RAW** (Iceberg) | READ | READ/WRITE | READ | READ | — | — | READ |
| **STAGE** (Iceberg) | READ | READ/WRITE | READ | READ | — | — | READ |
| **MART** (Iceberg) | READ | READ/WRITE | READ | READ | READ | — | READ |
| **Quarantine tables** | — | READ/WRITE | — | — | — | — | READ |
| **Airflow DAGs** | — | READ/WRITE | — | READ | — | — | READ |
| **Airflow Variables/Connections** | — | WRITE (секреты) | — | READ (LLM ключ) | — | WRITE | READ |
| **Trino** | READ (MART) | READ/WRITE | READ | READ | READ (MART) | — | READ |
| **Superset** | READ | READ | READ | READ | READ/WRITE (дашборды) | — | READ |
| **Marquez (lineage)** | READ | READ/WRITE | READ | READ | READ | — | READ |
| **LLM API ключ** | — | WRITE | — | READ | — | WRITE | READ |
| **Iceberg snapshots** | READ | READ/WRITE | READ | READ | READ | — | READ |

**Легенда:** READ — чтение, WRITE — запись/изменение, — — нет доступа.

---

## 3. Матрица доступа к операциям

| Операция | Data Analyst | Data Engineer | Data Scientist | ML Engineer | BI Consumer | Portal Admin | Audit |
|----------|--------------|---------------|----------------|-------------|-------------|--------------|-------|
| Запуск DAG (ручной) | — | Да | — | Да | — | Да | — |
| Откат Iceberg snapshot | — | Да | — | — | — | — | — |
| Создание MART витрины | Да | Да | — | — | — | — | — |
| Обучение ML модели | — | — | Да | Да | — | — | — |
| Инференс LLM API | — | — | — | Да | — | — | — |
| Экспорт данных в CSV | Да (MART) | Да | Да (MART) | Да (MART) | Да (MART) | — | Да |
| Изменение схемы таблицы | — | Да | — | — | — | — | — |
| Управление пользователями | — | — | — | — | — | Да | — |
| Просмотр логов инцидентов | Да | Да | Да | Да | — | Да | Да |

---

## 4. Поля с PII — ограничения

| Поле | Таблица | Кто имеет доступ | Ограничения |
|------|---------|------------------|-------------|
| `personaname` | `players` | Data Engineer, Data Scientist, Audit | Не выгружать в открытом виде вне платформы |
| `account_id` | `players`, `match_players` | Все роли с доступом к данным | Псевдонимизировано, не связывать с реальными лицами без согласия |
| `steam_account_id` | Mongo коллекции | Data Engineer, Data Scientist, Audit | Только внутри Iceberg/Spark |

---

## 5. Процедура предоставления доступа

1. Запрос доступа через Portal или трекер задач.
2. Одобрение владельцем ресурса (Data Engineering Lead для данных, Portal Admin для пользователей).
3. Предоставление минимально необходимого доступа.
4. Ежемесячный ревью прав доступа (access review).
5. Отзыв доступа при увольнении или изменении роли (не позднее 1 рабочего дня).

---

## 6. Мониторинг доступа

- Логирование всех попыток доступа к RAW и PII-полям.
- Еженедельный обзор аномальных запросов (mass download, доступ вне рабочего времени).
- Алерт при попытке доступа без прав.

---

## 7. Исключения

Любое исключение из матрицы требует:
- Документированного обоснования.
- Одобрения Portal Admin и Data Engineering Lead.
- Ограниченного срока действия (максимум 30 дней).
- Фиксации в журнале исключений.