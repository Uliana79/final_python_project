CREATE TABLE IF NOT EXISTS mart_orders (
    id                      SERIAL PRIMARY KEY,
    year                    INT NOT NULL,
    month                   INT NOT NULL,
    day                     INT NOT NULL,
    city                    VARCHAR(100) NOT NULL,
    store_id                INT NOT NULL,
    -- Метрики
    turnover                NUMERIC(15,2),
    revenue                 NUMERIC(15,2),
    profit                  NUMERIC(15,2),
    orders_created          INT,
    orders_delivered        INT,
    orders_canceled         INT,
    orders_canceled_after_delivery INT,
    orders_canceled_service_error INT,
    customers_count         INT,
    avg_check               NUMERIC(15,2),
    orders_per_customer     NUMERIC(10,2),
    revenue_per_customer    NUMERIC(15,2),
    driver_changes_count    INT,
    active_drivers_count    INT
);

CREATE TABLE IF NOT EXISTS mart_items (
    id                      SERIAL PRIMARY KEY,
    year                    INT NOT NULL,
    month                   INT NOT NULL,
    day                     INT NOT NULL,
    city                    VARCHAR(100) NOT NULL,
    store_id                INT NOT NULL,
    category                VARCHAR(100) NOT NULL,
    item_id                 INT NOT NULL,
    -- Метрики
    item_turnover           NUMERIC(15,2),
    items_ordered           INT,
    items_canceled          INT,
    orders_with_item        INT,
    orders_with_item_cancel INT
);