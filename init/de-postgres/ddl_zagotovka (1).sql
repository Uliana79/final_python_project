
CREATE TABLE IF NOT EXISTS "users" (
	"user_id" int,
	"user_phone" varchar(20),
	PRIMARY KEY("user_id")
);


CREATE TABLE IF NOT EXISTS "drivers" (
	"driver_id" int,
	"driver_phone" varchar(20),
	PRIMARY KEY("driver_id")
);


CREATE TABLE IF NOT EXISTS "stores" (
	"store_id" int,
	"store_name" text,
	"store_city" text,
	"store_address" text,
	PRIMARY KEY("store_id")
);


CREATE TABLE IF NOT EXISTS "items" (
	"item_id" int,
	"item_title" text,
	"item_category" text,
	PRIMARY KEY("item_id")
);


CREATE TABLE IF NOT EXISTS "orders" (
	"order_id" int,
	"user_id" int,
	"store_id" int,
	"address_text" text,
	"delivery_city" text,
	"created_at" TIMESTAMPTZ,
	"paid_at" TIMESTAMPTZ,
	"delivery_started_at" TIMESTAMPTZ,
	"delivered_at" TIMESTAMPTZ,
	"canceled_at" TIMESTAMPTZ,
	"payment_type" varchar(50),
	"order_discount" smallint DEFAULT 0,
	"order_cancellation_reason" text,
	"delivery_cost" int DEFAULT 0,
	PRIMARY KEY("order_id")
);


CREATE TABLE IF NOT EXISTS "order_items" (
	"id" blob,
	"order_id" int,
	"item_id" int,
	"item_quantity" smallint DEFAULT 0,
	"item_price" int DEFAULT 0,
	"item_canceled_quantity" smallint DEFAULT 0,
	"item_replaced_id" int,
	"item_discount" smallint DEFAULT 0,
	PRIMARY KEY("id")
);


CREATE TABLE IF NOT EXISTS "order_drivers" (
	"id" blob,
	"order_id" int,
	"driver_id" int,
	"is_final" boolean DEFAULT true,
	PRIMARY KEY("id")
);


ALTER TABLE "orders"
ADD FOREIGN KEY("user_id") REFERENCES "users"("user_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "orders"
ADD FOREIGN KEY("store_id") REFERENCES "stores"("store_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "order_items"
ADD FOREIGN KEY("order_id") REFERENCES "orders"("order_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "order_items"
ADD FOREIGN KEY("item_id") REFERENCES "items"("item_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "order_items"
ADD FOREIGN KEY("item_replaced_id") REFERENCES "items"("item_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "order_drivers"
ADD FOREIGN KEY("order_id") REFERENCES "orders"("order_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "order_drivers"
ADD FOREIGN KEY("driver_id") REFERENCES "drivers"("driver_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;