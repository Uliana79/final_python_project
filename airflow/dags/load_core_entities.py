from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import re
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

DAG_ID = "load_core_entities"
POSTGRES_CONN_ID = "postgres_default"

# Для локального теста  заменить на абсолютный путь:
# DATA_DIR = Path("/Users/kataacmeneva/final_python_project/data")
DATA_DIR = Path("/opt/airflow/data")

def get_parquet_files():
    return sorted(DATA_DIR.glob("chunk_*.parquet"))
def get_engine():
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)

    port = conn.port or 5432
    database = conn.schema

    uri = (
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{port}/{database}"
    )
    return create_engine(uri)


def get_parquet_files() -> list[Path]:
    files = sorted(DATA_DIR.glob("chunk_*.parquet"))
    if not files:
        raise FileNotFoundError(f"Не найдены parquet-файлы в папке: {DATA_DIR}")
    return files


def truncate_core_tables():
    engine = get_engine()

    sql = """
          TRUNCATE TABLE
              order_drivers,
        order_items,
        orders,
        users,
        drivers,
        stores,
        items
    CASCADE; \
          """

    with engine.begin() as connection:
        connection.execute(text(sql))

def normalize_phone(phone):
    if pd.isna(phone):
        return None
    
    digits = re.sub(r'\D', '', str(phone))
    
    if not digits:
        return None
    
    if digits[0] == '8':
        digits = '7' + digits[1:]
    return f"+{digits}"

def load_users():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        users_df = df[["user_id", "user_phone"]].copy()
        users_df = users_df.dropna(subset=["user_id"])
        users_df = users_df.drop_duplicates()

        users_df["user_phone"] = users_df["user_phone"].apply(normalize_phone)
        
        all_parts.append(users_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["user_id"])

    engine = get_engine()
    result.to_sql("users", engine, if_exists="append", index=False)


def load_drivers():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        drivers_df = df[["driver_id", "driver_phone"]].copy()
        drivers_df = drivers_df.dropna(subset=["driver_id"])
        drivers_df = drivers_df.drop_duplicates()

        drivers_df["driver_phone"] = drivers_df["driver_phone"].apply(normalize_phone)
        
        all_parts.append(drivers_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["driver_id"])

    engine = get_engine()
    result.to_sql("drivers", engine, if_exists="append", index=False)


def load_stores():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        stores_df = df[["store_id", "store_address"]].copy()
        stores_df = stores_df.dropna(subset=["store_id"])
        stores_df = stores_df.drop_duplicates()
        
        all_parts.append(stores_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["store_id"])

    result["store_name"] = result["store_address"].apply(
        lambda x: x.split(',')[0].strip() if pd.notna(x) and ',' in str(x) else None
    )
    
    result["store_city"] = result["store_address"].apply(
        lambda x: x.split(',')[1].strip() if pd.notna(x) and ',' in str(x) and len(x.split(',')) >= 2 else None
    )
    # result["store_name"] = None
    # result["store_city"] = None

    result = result[["store_id", "store_name", "store_city", "store_address"]]

    engine = get_engine()
    result.to_sql("stores", engine, if_exists="append", index=False)


def load_items():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        items_df = df[["item_id", "item_title", "item_category"]].copy()
        items_df = items_df.dropna(subset=["item_id"])
        items_df = items_df.drop_duplicates()

        all_parts.append(items_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["item_id"])

    engine = get_engine()
    result.to_sql("items", engine, if_exists="append", index=False)


def load_orders():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        orders_df = df[
            [
                "order_id",
                "user_id",
                "store_id",
                "address_text",
                "created_at",
                "paid_at",
                "delivery_started_at",
                "delivered_at",
                "canceled_at",
                "payment_type",
                "order_discount",
                "order_cancellation_reason",
                "delivery_cost",
            ]
        ].copy()

        orders_df = orders_df.dropna(subset=["order_id"])
        orders_df = orders_df.drop_duplicates(subset=["order_id"])
        
        orders_df["delivery_city"] = orders_df["address_text"].apply(
            lambda x: x.split(',')[0].strip() if pd.notna(x) and ',' in str(x) else None
        )
        
        all_parts.append(orders_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["order_id"])

    result = result[
        [
            "order_id",
            "user_id",
            "store_id",
            "address_text",
            "delivery_city",
            "created_at",
            "paid_at",
            "delivery_started_at",
            "delivered_at",
            "canceled_at",
            "payment_type",
            "order_discount",
            "order_cancellation_reason",
            "delivery_cost",
        ]
    ]

    engine = get_engine()
    result.to_sql("orders", engine, if_exists="append", index=False)


def load_order_items():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        oi_df = df[
            [
                "order_id",
                "item_id",
                "item_quantity",
                "item_price",
                "item_canceled_quantity",
                "item_replaced_id",
                "item_discount",
            ]
        ].copy()

        oi_df = oi_df.dropna(subset=["order_id", "item_id"])

        # Делаем суррогатный id как text.
        oi_df["id"] = (
                oi_df["order_id"].astype("Int64").astype(str)
                + "_"
                + oi_df["item_id"].astype("Int64").astype(str)
                + "_"
                + oi_df.groupby(["order_id", "item_id"]).cumcount().astype(str)
        )

        # item_replaced_id в parquet float64 из-за NaN; приводим к nullable int
        oi_df["item_replaced_id"] = oi_df["item_replaced_id"].astype("Int64")
        oi_df["item_quantity"] = oi_df["item_quantity"].fillna(0).astype("Int64")
        oi_df["item_price"] = oi_df["item_price"].fillna(0).astype("Int64")
        oi_df["item_canceled_quantity"] = oi_df["item_canceled_quantity"].fillna(0).astype("Int64")
        oi_df["item_discount"] = oi_df["item_discount"].fillna(0).astype("Int64")

        oi_df = oi_df[
            [
                "id",
                "order_id",
                "item_id",
                "item_quantity",
                "item_price",
                "item_canceled_quantity",
                "item_replaced_id",
                "item_discount",
            ]
        ]

        all_parts.append(oi_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["id"])

    engine = get_engine()
    result.to_sql("order_items", engine, if_exists="append", index=False)


def load_order_drivers():
    all_parts = []

    for file in get_parquet_files():
        df = pd.read_parquet(file)

        od_df = df[["order_id", "driver_id"]].copy()
        od_df = od_df.dropna(subset=["order_id", "driver_id"])
        od_df = od_df.drop_duplicates()

        od_df["is_final"] = True

        # Суррогатный id как text
        od_df["id"] = (
                od_df["order_id"].astype("Int64").astype(str)
                + "_"
                + od_df["driver_id"].astype("Int64").astype(str)
                + "_"
                + od_df.groupby(["order_id", "driver_id"]).cumcount().astype(str)
        )

        od_df = od_df[["id", "order_id", "driver_id", "is_final"]]

        all_parts.append(od_df)

    result = pd.concat(all_parts, ignore_index=True)
    result = result.drop_duplicates(subset=["id"])

    engine = get_engine()
    result.to_sql("order_drivers", engine, if_exists="append", index=False)


with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2026, 3, 23),
        schedule_interval=None,
        catchup=False,
        tags=["final_project", "core"],
) as dag:

    task_truncate_core_tables = PythonOperator(
        task_id="truncate_core_tables",
        python_callable=truncate_core_tables,
    )

    task_load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_users,
    )

    task_load_drivers = PythonOperator(
        task_id="load_drivers",
        python_callable=load_drivers,
    )

    task_load_stores = PythonOperator(
        task_id="load_stores",
        python_callable=load_stores,
    )

    task_load_items = PythonOperator(
        task_id="load_items",
        python_callable=load_items,
    )

    task_load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    task_load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_order_items,
    )

    task_load_order_drivers = PythonOperator(
        task_id="load_order_drivers",
        python_callable=load_order_drivers,
    )

    task_truncate_core_tables >> [
        task_load_users,
        task_load_drivers,
        task_load_stores,
        task_load_items,
    ]

    [task_load_users, task_load_stores] >> task_load_orders
    [task_load_orders, task_load_items] >> task_load_order_items
    [task_load_orders, task_load_drivers] >> task_load_order_drivers