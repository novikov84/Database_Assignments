CREATE TABLE "customer" (
  "customer_id" integer PRIMARY KEY,
  "first_name" varchar,
  "last_name" varchar,
  "gender" varchar,
  "DOB" date,
  "job_title" varchar,
  "job_industry_category" varchar,
  "wealth_segment" varchar,
  "deceased_indicator" varchar,
  "owns_car" varchar,
  "address" varchar,
  "postcode" integer,
  "state" varchar,
  "country" varchar,
  "property_valuation" integer,
  UNIQUE("customer_id")
);

CREATE TABLE "transaction" (
  "transaction_id" integer PRIMARY KEY,
  "product_key_id" integer,
  "customer_id" integer, 
  "transaction_date" date,
  "online_order" varchar,
  "order_status" varchar,
  UNIQUE("transaction_id")
);

CREATE TABLE "product" (
  "product_key_id" integer PRIMARY KEY,
  "product_id" integer,
  "brand" varchar,
  "product_line" varchar,
  "product_class" varchar,
  "product_size" varchar,
  "list_price" float,
  "standard_cost" float,
  UNIQUE("product_key_id")
);

ALTER TABLE "transaction" ADD FOREIGN KEY ("customer_id") REFERENCES "customer" ("customer_id");
ALTER TABLE "transaction" ADD FOREIGN KEY ("product_key_id") REFERENCES "product" ("product_key_id");
