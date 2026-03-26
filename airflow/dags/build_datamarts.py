from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


DAG_ID = "build_datamarts"
POSTGRES_CONN_ID = "postgres_default"


CREATE_MART_ORDERS_SQL = """
                         CREATE TABLE IF NOT EXISTS mart_orders (
                                                                    year_num int,
                                                                    month_num int,
                                                                    day_num int,
                                                                    city text,
                                                                    store_id int,
                                                                    store_name text,

                                                                    turnover numeric,
                                                                    revenue numeric,
                                                                    profit numeric,

                                                                    created_orders_cnt int,
                                                                    delivered_orders_cnt int,
                                                                    canceled_orders_cnt int,
                                                                    cancel_after_delivery_cnt int,
                                                                    service_error_cancel_cnt int,

                                                                    buyers_cnt int,
                                                                    avg_check numeric,
                                                                    orders_per_buyer numeric,
                                                                    revenue_per_buyer numeric,

                                                                    driver_changes_cnt int,
                                                                    active_drivers_cnt int
                         ); \
                         """


TRUNCATE_MART_ORDERS_SQL = """
                           TRUNCATE TABLE mart_orders; \
                           """


INSERT_MART_ORDERS_SQL = """
                         INSERT INTO mart_orders (
                             year_num,
                             month_num,
                             day_num,
                             city,
                             store_id,
                             store_name,
                             turnover,
                             revenue,
                             profit,
                             created_orders_cnt,
                             delivered_orders_cnt,
                             canceled_orders_cnt,
                             cancel_after_delivery_cnt,
                             service_error_cancel_cnt,
                             buyers_cnt,
                             avg_check,
                             orders_per_buyer,
                             revenue_per_buyer,
                             driver_changes_cnt,
                             active_drivers_cnt
                         )
                         WITH order_items_agg AS (
                             SELECT
                                 oi.order_id,
                                 SUM(
                                         oi.item_quantity * oi.item_price - COALESCE(oi.item_discount, 0)
                                 ) AS turnover_order,
                                 SUM(
                                         (oi.item_quantity - COALESCE(oi.item_canceled_quantity, 0)) * oi.item_price
                                             - COALESCE(oi.item_discount, 0)
                                 ) AS revenue_order
                             FROM order_items oi
                             GROUP BY oi.order_id
                         ),
                              driver_stats AS (
                                  SELECT
                                      od.order_id,
                                      CASE WHEN COUNT(DISTINCT od.driver_id) > 1 THEN 1 ELSE 0 END AS has_driver_change,
                                      COUNT(DISTINCT od.driver_id) AS active_drivers_per_order
                                  FROM order_drivers od
                                  GROUP BY od.order_id
                              )
                         SELECT
                             EXTRACT(YEAR FROM o.created_at)::int AS year_num,
                             EXTRACT(MONTH FROM o.created_at)::int AS month_num,
                             EXTRACT(DAY FROM o.created_at)::int AS day_num,
                             COALESCE(o.delivery_city, s.store_city, 'UNKNOWN') AS city,
                             o.store_id,
                             s.store_name,

                             COALESCE(SUM(oia.turnover_order), 0) AS turnover,
                             COALESCE(SUM(oia.revenue_order), 0) AS revenue,
                             COALESCE(SUM(oia.revenue_order), 0) - COALESCE(SUM(o.delivery_cost), 0) AS profit,

                             COUNT(DISTINCT o.order_id)::int AS created_orders_cnt,
                             COUNT(DISTINCT CASE WHEN o.delivered_at IS NOT NULL THEN o.order_id END)::int AS delivered_orders_cnt,
                             COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL THEN o.order_id END)::int AS canceled_orders_cnt,
                             COUNT(DISTINCT CASE
                                                WHEN o.delivered_at IS NOT NULL AND o.canceled_at IS NOT NULL THEN o.order_id
                                 END)::int AS cancel_after_delivery_cnt,
                             COUNT(DISTINCT CASE
                                                WHEN o.order_cancellation_reason IN ('Ошибка приложения', 'Проблемы с оплатой')
                                                    THEN o.order_id
                                 END)::int AS service_error_cancel_cnt,

                             COUNT(DISTINCT o.user_id)::int AS buyers_cnt,

                             ROUND(
                                     COALESCE(SUM(oia.revenue_order), 0)::numeric
        / NULLIF(COUNT(DISTINCT o.order_id), 0),
                                     2
                             ) AS avg_check,

                             ROUND(
                                     COUNT(DISTINCT o.order_id)::numeric
        / NULLIF(COUNT(DISTINCT o.user_id), 0),
                                     2
                             ) AS orders_per_buyer,

                             ROUND(
                                     COALESCE(SUM(oia.revenue_order), 0)::numeric
        / NULLIF(COUNT(DISTINCT o.user_id), 0),
                                     2
                             ) AS revenue_per_buyer,

                             COALESCE(SUM(ds.has_driver_change), 0)::int AS driver_changes_cnt,
                             COUNT(DISTINCT od.driver_id)::int AS active_drivers_cnt

                         FROM orders o
                                  LEFT JOIN stores s
                                            ON o.store_id = s.store_id
                                  LEFT JOIN order_items_agg oia
                                            ON o.order_id = oia.order_id
                                  LEFT JOIN driver_stats ds
                                            ON o.order_id = ds.order_id
                                  LEFT JOIN order_drivers od
                                            ON o.order_id = od.order_id
                         GROUP BY
                             EXTRACT(YEAR FROM o.created_at),
                             EXTRACT(MONTH FROM o.created_at),
                             EXTRACT(DAY FROM o.created_at),
                             COALESCE(o.delivery_city, s.store_city, 'UNKNOWN'),
                             o.store_id,
                             s.store_name; \
                         """


CREATE_MART_ITEMS_SQL = """
                        CREATE TABLE IF NOT EXISTS mart_items (
                                                                  year_num int,
                                                                  month_num int,
                                                                  day_num int,
                                                                  city text,
                                                                  store_id int,
                                                                  store_name text,
                                                                  item_category text,
                                                                  item_id int,
                                                                  item_title text,

                                                                  item_turnover numeric,
                                                                  ordered_qty int,
                                                                  canceled_qty int,
                                                                  orders_with_item_cnt int,
                                                                  orders_with_item_cancel_cnt int
                        ); \
                        """


TRUNCATE_MART_ITEMS_SQL = """
                          TRUNCATE TABLE mart_items; \
                          """


INSERT_MART_ITEMS_SQL = """
                        INSERT INTO mart_items (
                            year_num,
                            month_num,
                            day_num,
                            city,
                            store_id,
                            store_name,
                            item_category,
                            item_id,
                            item_title,
                            item_turnover,
                            ordered_qty,
                            canceled_qty,
                            orders_with_item_cnt,
                            orders_with_item_cancel_cnt
                        )
                        SELECT
                            EXTRACT(YEAR FROM o.created_at)::int AS year_num,
                            EXTRACT(MONTH FROM o.created_at)::int AS month_num,
                            EXTRACT(DAY FROM o.created_at)::int AS day_num,
                            COALESCE(o.delivery_city, s.store_city, 'UNKNOWN') AS city,
                            o.store_id,
                            s.store_name,
                            i.item_category,
                            i.item_id,
                            i.item_title,

                            SUM(
                                    oi.item_quantity * oi.item_price - COALESCE(oi.item_discount, 0)
                            ) AS item_turnover,

                            SUM(oi.item_quantity)::int AS ordered_qty,
                            SUM(COALESCE(oi.item_canceled_quantity, 0))::int AS canceled_qty,
                            COUNT(DISTINCT o.order_id)::int AS orders_with_item_cnt,
                            COUNT(DISTINCT CASE
                                               WHEN COALESCE(oi.item_canceled_quantity, 0) > 0 THEN o.order_id
                                END)::int AS orders_with_item_cancel_cnt

                        FROM order_items oi
                                 JOIN orders o
                                      ON oi.order_id = o.order_id
                                 LEFT JOIN items i
                                           ON oi.item_id = i.item_id
                                 LEFT JOIN stores s
                                           ON o.store_id = s.store_id
                        GROUP BY
                            EXTRACT(YEAR FROM o.created_at),
                            EXTRACT(MONTH FROM o.created_at),
                            EXTRACT(DAY FROM o.created_at),
                            COALESCE(o.delivery_city, s.store_city, 'UNKNOWN'),
                            o.store_id,
                            s.store_name,
                            i.item_category,
                            i.item_id,
                            i.item_title; \
                        """


with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2026, 3, 23),
        schedule_interval=None,
        catchup=False,
        tags=["final_project", "marts"],
) as dag:

    create_mart_orders = PostgresOperator(
        task_id="create_mart_orders",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_MART_ORDERS_SQL,
    )

    truncate_mart_orders = PostgresOperator(
        task_id="truncate_mart_orders",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=TRUNCATE_MART_ORDERS_SQL,
    )

    insert_mart_orders = PostgresOperator(
        task_id="insert_mart_orders",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=INSERT_MART_ORDERS_SQL,
    )

    create_mart_items = PostgresOperator(
        task_id="create_mart_items",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_MART_ITEMS_SQL,
    )

    truncate_mart_items = PostgresOperator(
        task_id="truncate_mart_items",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=TRUNCATE_MART_ITEMS_SQL,
    )

    insert_mart_items = PostgresOperator(
        task_id="insert_mart_items",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=INSERT_MART_ITEMS_SQL,
    )

    create_mart_orders >> truncate_mart_orders >> insert_mart_orders
    create_mart_items >> truncate_mart_items >> insert_mart_items