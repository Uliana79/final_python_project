# final_python_project

##  Описание

Проект реализует ETL-пайплайн обработки данных по доставкам.

Функциональность:

* загрузка денормализованных parquet-файлов
* нормализация данных (core слой)
* загрузка в PostgreSQL
* построение аналитических витрин

---

##  Архитектура

```text
Parquet (data/)
        ↓
load_core_entities (Airflow DAG)
        ↓
Core tables (users, orders, items, ...)
        ↓
build_datamarts (Airflow DAG)
        ↓
mart_orders / mart_items
```

---

##  Структура проекта

```text
final_python_project/
│
├── airflow/
│   ├── dags/
│   │   ├── load_core_entities.py
│   │   ├── build_datamarts.py
│   │   └── build_datamarts_spark.py
│   │
│   └── logs/
│
├── data/                      # parquet файлы (chunk_*.parquet)
│
├── init/
│   └── de-postgres/
│       └── ddl.sql            # создание таблиц
│
├── pgadmin/
│   └── servers.json
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Локальный запуск

### 1. Клонирование

```bash
git clone <repo_url>
cd final_python_project
```

---

### 2. Запуск

```bash
docker-compose up --build
```

##  Доступы

### Airflow

```text
http://localhost:8080
```

```text
login: admin
password: admin
```

---

### pgAdmin

```text
http://localhost:5050
```

* master password: 123
* пароль пользователя airflow: `airflow`

---

##  База данных

Рабочая база:

```text
team_3_store
```

---

## Просмотр данных

### Через pgAdmin:

```text
team_3_store → Schemas → public → Tables
```

##  Порядок запуска

```text
1. load_core_entities
2. проверка данных
3. build_datamarts_spark
```

---

## Перезапуск DAG

1. Обновить страницу Airflow
2. Нажать **Clear task**
3. Нажать **Trigger DAG**

