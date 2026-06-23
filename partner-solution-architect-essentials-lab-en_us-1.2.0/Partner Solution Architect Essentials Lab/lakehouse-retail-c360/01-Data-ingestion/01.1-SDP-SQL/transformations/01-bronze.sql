-- ----------------------------------
-- Ingest raw app events stream in incremental mode
-- Application events and sessions from CSV files
-- Contains user activity data: page views, clicks, session information
-- ----------------------------------
CREATE STREAMING TABLE churn_app_events (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Application events and sessions"
AS SELECT * FROM cloud_files("/Volumes/dbacademy/${schema}/c360/events", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- ----------------------------------
-- Ingest raw orders from ERP system
-- Order history from CSV files containing customer purchases
-- Contains transaction data: amounts, items, dates, customer IDs
-- ----------------------------------
CREATE STREAMING TABLE churn_orders_bronze (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Spending score from raw data"
AS SELECT * FROM cloud_files("/Volumes/dbacademy/${schema}/c360/orders", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- ----------------------------------
-- Ingest raw user data
-- Customer profile data from CSV files
-- Contains demographic information: name, age, address, gender, age group
-- ----------------------------------
CREATE STREAMING TABLE churn_users_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from CSV files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/Volumes/dbacademy/${schema}/c360/users", "csv", map("cloudFiles.inferColumnTypes", "true"))
