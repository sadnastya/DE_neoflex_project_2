# DE_neoflex_project_2

Учебный проект в рамках курса **Data Engineering** от Neoflex.  
Стек: **PostgreSQL / PL/pgSQL**, **Apache Airflow**, **Python (pandas)**.

---

## Структура репозитория

```
├── task_1/
│   └── task2_1.sql
├── task_2/
│   ├── script_loan_holiday.sql
│   └── dag_upload_data_loan_holiday.py
└── task_3/
    ├── airflow_dag/
    │   ├── dag_balance_turnover.py
    │   ├── update_account_balance.sql
    │   └── update_dm_turnover.sql
    └── full_script/
        └── full_script.sql
```

---

## Task 1 — Поиск и удаление дубликатов

SQL-скрипт для работы с таблицей `dm.client`:

- Запрос для выявления дублирующихся записей по полям `client_rk` и `effective_from_date`.
- Удаление дубликатов с сохранением одной записи по минимальному `ctid`.

---

## Task 2 — Витрина кредитных каникул (`dm.loan_holiday_info`)

### SQL-логика (`script_loan_holiday.sql`)

- Дедупликация таблицы `rd.product` по полям `product_rk`, `product_name`, `effective_from_date`.
- Хранимая процедура `dm.fill_loan_holiday_info()` — заполняет витрину данными о кредитных каникулах через JOIN трёх источников:
  - `rd.deal_info` — информация о сделках,
  - `rd.loan_holiday` — данные о кредитных каникулах,
  - `rd.product` — справочник продуктов.

### Airflow DAG (`dag_upload_data_loan_holiday.py`)

DAG `data_from_csv_loan_holiday` выполняет полный пайплайн загрузки данных:

1. **Параллельная загрузка CSV** → PostgreSQL (pandas + SQLAlchemy):
   - `product_info.csv` → `rd.product`
   - `deal_info.csv` → `rd.deal_info`
2. **Запуск SQL-скрипта** — удаление дубликатов и вызов процедуры заполнения витрины.

```
start → [load_product | load_deal] → delete_duplicates + fill_dm → end
```

---

## Task 3 — Витрина оборотов по счетам (`dm.account_balance_turnover`)

### SQL-логика

**`update_account_balance.sql`** — корректировка остатков в `rd.account_balance`:  
`account_in_sum` каждого дня приравнивается к `account_out_sum` предыдущего дня (через CTE + UPDATE).

**`update_dm_turnover.sql`** — хранимая процедура `dm.fill_account_balance_turnover()`:  
заполняет витрину `dm.account_balance_turnover` путём JOIN:
- `rd.account` — реестр счетов,
- `rd.account_balance` — остатки по счетам,
- `dm.dict_currency` — справочник валют.

**`full_script/full_script.sql`** — полный аналитический скрипт с проверочными запросами (корректность `account_in_sum` / `account_out_sum`) и ручным вызовом процедуры.

### Airflow DAG (`dag_balance_turnover.py`)

DAG `update_dm_balance_turnovers`:

1. **Очистка** таблицы `dm.dict_currency`.
2. **Загрузка** `dict_currency.csv` → `dm.dict_currency` (pandas).
3. **Обновление** `rd.account_balance` (исправление остатков).
4. **Заполнение** витрины `dm.account_balance_turnover`.

```
start → truncate_currency → load_currency → update_rd → update_dm → end
```

---

---

## Видеокомментарии к заданиям

[Смотреть на Google Drive](https://drive.google.com/drive/folders/1glNt1bb_xEqrfdMuU8Leq6GKSG40cMpM?usp=sharing)
