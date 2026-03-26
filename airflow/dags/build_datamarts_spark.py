from datetime import datetime
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

DAG_ID = "build_datamarts_spark"
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/team_3_store"
POSTGRES_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def get_spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("build_mart") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.3.jar") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


INSERT_MART_ORDERS_SQL = """
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
                             CAST(EXTRACT(YEAR FROM o.created_at) AS INT) AS year_num,
                             CAST(EXTRACT(MONTH FROM o.created_at) AS INT) AS month_num,
                             CAST(EXTRACT(DAY FROM o.created_at) AS INT) AS day_num,
                             COALESCE(o.delivery_city, s.store_city, 'UNKNOWN') AS city,
                             o.store_id,
                             s.store_name,

                             COALESCE(SUM(oia.turnover_order), 0) AS turnover,
                             COALESCE(SUM(oia.revenue_order), 0) AS revenue,
                             COALESCE(SUM(oia.revenue_order), 0) - COALESCE(SUM(o.delivery_cost), 0) AS profit,

                             COUNT(DISTINCT o.order_id) AS created_orders_cnt,
                             COUNT(DISTINCT CASE WHEN o.delivered_at IS NOT NULL THEN o.order_id END) AS delivered_orders_cnt,
                             COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL THEN o.order_id END) AS canceled_orders_cnt,
                             COUNT(DISTINCT CASE
                                                WHEN o.delivered_at IS NOT NULL AND o.canceled_at IS NOT NULL THEN o.order_id
                                 END) AS cancel_after_delivery_cnt,
                             COUNT(DISTINCT CASE
                                                WHEN o.order_cancellation_reason IN ('Ошибка приложения', 'Проблемы с оплатой')
                                                    THEN o.order_id
                                 END) AS service_error_cancel_cnt,

                             COUNT(DISTINCT o.user_id) AS buyers_cnt,

                             ROUND(
                                     COALESCE(SUM(oia.revenue_order), 0)
        / NULLIF(COUNT(DISTINCT o.order_id), 0),
                                     2
                             ) AS avg_check,

                             ROUND(
                                     COUNT(DISTINCT o.order_id)
        / NULLIF(COUNT(DISTINCT o.user_id), 0),
                                     2
                             ) AS orders_per_buyer,

                             ROUND(
                                     COALESCE(SUM(oia.revenue_order), 0)
        / NULLIF(COUNT(DISTINCT o.user_id), 0),
                                     2
                             ) AS revenue_per_buyer,

                             COALESCE(SUM(ds.has_driver_change), 0) AS driver_changes_cnt,
                             COUNT(DISTINCT od.driver_id) AS active_drivers_cnt

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
                             CAST(EXTRACT(YEAR FROM o.created_at) AS INT),
                             CAST(EXTRACT(MONTH FROM o.created_at) AS INT),
                             CAST(EXTRACT(DAY FROM o.created_at) AS INT),
                             COALESCE(o.delivery_city, s.store_city, 'UNKNOWN'),
                             o.store_id,
                             s.store_name;
                         """

INSERT_MART_ITEMS_SQL = """
                        SELECT
                            CAST(EXTRACT(YEAR FROM o.created_at) AS INT) AS year_num,
                            CAST(EXTRACT(MONTH FROM o.created_at) AS INT) AS month_num,
                            CAST(EXTRACT(DAY FROM o.created_at) AS INT) AS day_num,
                            COALESCE(o.delivery_city, s.store_city, 'UNKNOWN') AS city,
                            o.store_id,
                            s.store_name,
                            i.item_category,
                            i.item_id,
                            i.item_title,

                            SUM(
                                    oi.item_quantity * oi.item_price - COALESCE(oi.item_discount, 0)
                            ) AS item_turnover,

                            SUM(oi.item_quantity) AS ordered_qty,
                            SUM(COALESCE(oi.item_canceled_quantity, 0)) AS canceled_qty,
                            COUNT(DISTINCT o.order_id) AS orders_with_item_cnt,
                            COUNT(DISTINCT CASE
                                               WHEN COALESCE(oi.item_canceled_quantity, 0) > 0 THEN o.order_id
                                END) AS orders_with_item_cancel_cnt

                        FROM order_items oi
                                 JOIN orders o
                                      ON oi.order_id = o.order_id
                                 LEFT JOIN items i
                                           ON oi.item_id = i.item_id
                                 LEFT JOIN stores s
                                           ON o.store_id = s.store_id
                        GROUP BY
                            CAST(EXTRACT(YEAR FROM o.created_at) AS INT),
                            CAST(EXTRACT(MONTH FROM o.created_at) AS INT),
                            CAST(EXTRACT(DAY FROM o.created_at) AS INT),
                            COALESCE(o.delivery_city, s.store_city, 'UNKNOWN'),
                            o.store_id,
                            s.store_name,
                            i.item_category,
                            i.item_id,
                            i.item_title 
                        """


def build_mart():
    spark = get_spark()
    orders = spark.read.jdbc(POSTGRES_URL, "orders", properties=POSTGRES_PROPS)
    stores = spark.read.jdbc(POSTGRES_URL, "stores", properties=POSTGRES_PROPS)
    order_items = spark.read.jdbc(POSTGRES_URL, "order_items", properties=POSTGRES_PROPS)
    order_drivers = spark.read.jdbc(POSTGRES_URL, "order_drivers", properties=POSTGRES_PROPS)
    items = spark.read.jdbc(POSTGRES_URL, "items", properties=POSTGRES_PROPS)
        
    orders.createOrReplaceTempView("orders")
    stores.createOrReplaceTempView("stores")
    order_items.createOrReplaceTempView("order_items")
    order_drivers.createOrReplaceTempView("order_drivers")
    items.createOrReplaceTempView("items")
        
   
    result_orders = spark.sql(INSERT_MART_ORDERS_SQL)
    result_orders.write.jdbc(POSTGRES_URL, "mart_orders", mode="overwrite", properties=POSTGRES_PROPS)
           
    result_items = spark.sql(INSERT_MART_ITEMS_SQL)
    result_items.write.jdbc(POSTGRES_URL, "mart_items", mode="overwrite", properties=POSTGRES_PROPS)
    
    spark.stop()


with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2026, 3, 23),
        schedule_interval=None,
        catchup=False,
        tags=["final_project", "marts", "spark"],
) as dag:

    insert_mart = PythonOperator(
        task_id="insert_mart_orders",
        python_callable=build_mart,
    )

    insert_mart