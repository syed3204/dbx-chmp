# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 of 6: AI-BI Data Warehousing
# MAGIC
# MAGIC Review the notebooks in `lakehouse-retail-c360/03-AI-BI-data-warehousing/` to understand the concepts, then complete the activities below. You will work with the SQL Editor, create alerts, build metric views, and publish dashboards with Genie.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. SQL Editor: Queries and Alerts
# MAGIC
# MAGIC ### A1. (Optional) Use AI to Create a Query
# MAGIC 1. Go to the **SQL Editor** in the left sidebar.
# MAGIC 2. Write a query referencing one of the `churn_*` tables, or use the AI assistant.
# MAGIC    - Examples: data drift over time, predictions by platform or country, churn patterns.
# MAGIC
# MAGIC ### A2. Create, Format, and Save a Query
# MAGIC
# MAGIC 1. **Create a New Query** in the SQL Editor.
# MAGIC 2. Paste the SQL below, replacing `your_schema` with your schema name (e.g., `labuser15359237_1780317248`).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 12px 0; border: 1px solid #e0e0e0; border-radius: 8px;">
# MAGIC <summary style="cursor: pointer; padding: 12px 18px; background: #F9F7F4; font-weight: 600; font-size: 12pt; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
# MAGIC <span>&#9654; Click to expand: Data Quality Query SQL</span>
# MAGIC <button onclick="event.stopPropagation();var t=document.getElementById('sql-dlt-expectations');var b=this;navigator.clipboard.writeText(t.value).then(function(){b.textContent='Copied!';setTimeout(function(){b.textContent='Copy to Clipboard'},2000)}).catch(function(){t.select();document.execCommand('copy');b.textContent='Copied!';setTimeout(function(){b.textContent='Copy to Clipboard'},2000)})" style="padding: 6px 16px; border-radius: 6px; border: 1px solid #2574B5; background: #2574B5; color: white; cursor: pointer; font-size: 12px; font-weight: 600; flex-shrink: 0; margin-left: 12px;">Copy to Clipboard</button>
# MAGIC </summary>
# MAGIC <div style="padding: 0 18px 14px 18px;">
# MAGIC <p style="font-size: 12pt; margin: 10px 0;">Replace <code>your_schema</code> with your schema name after pasting.</p>
# MAGIC <textarea id="sql-dlt-expectations" style="width:100%; height:140px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 0.85rem; line-height: 1.4; padding: 12px; border: 1px solid #e5e7eb; border-radius: 6px; background: #f8fafc; resize: vertical;">SELECT dataset, name AS validation_rule, SUM(passed_records) AS passed_records, SUM(failed_records) AS failed_records, SUM(dropped_records) AS dropped_records, SUM(failed_records) / SUM(output_records) * 100 AS failure_rate FROM dbacademy.your_schema.dlt_expectations GROUP BY dataset, validation_rule ORDER BY dataset, failure_rate DESC</textarea>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC 3. **Name your query** -- give it a descriptive name (e.g., "Data Quality Expectations").
# MAGIC 4. **Format the query** -- click the **kebab menu (three dots)** > **Edit** > **Format Query** to auto-format the SQL.
# MAGIC 5. **Save your query** by selecting **Save** in the toolbar. Save it in your Partner Solution Architect Essentials Lab folder.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">This is the same query you created a notebook visual for in Lab 1 (Data Ingestion).</div>
# MAGIC </div>
# MAGIC
# MAGIC ### A3. Create an Alert
# MAGIC
# MAGIC 1. From the saved query, click the **kebab menu (three dots)** > **File** > **Create Alert**.
# MAGIC 2. Configure the alert conditions -- choose a column, operator, and threshold that makes sense for your data (e.g., trigger when a `failure_rate` value exceeds a percentage you choose).
# MAGIC 3. **Save** a screenshot of the completed alert configuration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A4. Verification: Alert Screenshot
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Proof of Execution</strong>
# MAGIC <div style="color:#333;">Upload a screenshot of your completed alert configuration. Then run the <code>show_evidence</code> cell to display it.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Images/Image_Display_Function

# COMMAND ----------

upload_evidence("evidence-03-alert.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows your completed alert configuration.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-03-alert.png", width=600)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A5. Add the Query to the Dashboard
# MAGIC
# MAGIC 1. Return to your saved query in the SQL Editor.
# MAGIC 2. Click the **kebab menu (three dots)** > **File** > **Add to Dashboard**.
# MAGIC 3. Select **Add to Existing Dashboard**.
# MAGIC 4. Find the **Retail - Customer Churn - Universal** dashboard (it ends with your labuser username, e.g., `labuser_12345`).
# MAGIC 5. Select it and confirm.
# MAGIC
# MAGIC ### A6. Add a Visualization
# MAGIC
# MAGIC 1. In the dashboard, navigate from the **Data** tab to the **New Page** tab.
# MAGIC 2. Click **Add a visualization** and place it on the page.
# MAGIC 3. **Title:** "Validation Rule Performance by Dataset and Status"
# MAGIC 4. **Description:** Evaluates validation rule effectiveness across datasets and identifies consistently failing rules.
# MAGIC 5. In the visualization config, choose the **Dataset** from the dropdown -- select the dataset from your SQL query (likely the last one in the list).
# MAGIC 6. Choose a chart type:
# MAGIC
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 12px; margin: 16px 0;">
# MAGIC   <div style="flex: 1; min-width: 250px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #2574B5; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option A: Bar Chart</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.8;">
# MAGIC       <strong>X-axis:</strong> <code>dataset</code><br>
# MAGIC       <strong>Y-axis:</strong> <code>failure_rate</code><br>
# MAGIC       <strong>Color:</strong> <code>validation_rule</code>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 250px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #02A36F; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option B: Heatmap</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.8;">
# MAGIC       <strong>X-axis:</strong> <code>dataset</code><br>
# MAGIC       <strong>Y-axis:</strong> <code>validation_rule</code><br>
# MAGIC       <strong>Color:</strong> <code>failure_rate</code>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Create a Metric View and Add It to the Dashboard
# MAGIC
# MAGIC Metric views in Unity Catalog provide business-friendly definitions for KPIs. Instead of writing complex SQL for every query, you define dimensions and measures once and reuse them across dashboards, Genie, and SQL queries.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 1: Review the metric view reference</strong>
# MAGIC <div style="color:#333;">Open <code>lakehouse-retail-c360/03-AI-BI-data-warehousing/03.3-Metric-Views</code> and read through it. It explains metric view concepts (dimensions, measures, filters, joins, window functions) and shows how to create one via the UI or SQL.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 2: Create the metric view</strong>
# MAGIC <div style="color:#333;">
# MAGIC <p>Run the code cell below to create a metric view called <code>churn_users_metric_view</code>. It automatically uses your schema -- no copy/paste needed.</p>
# MAGIC <p>This metric view defines business-friendly dimensions (Age Group, Gender, Canal, Country) and measures (Total Users, Churned Users, Churn Rate, Active Users, Total Order Amount) with a join to the orders table.</p>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

import re
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
schema = re.sub(r'[^a-zA-Z0-9_]', '_', username.split('@')[0])

spark.sql(f"""
CREATE OR REPLACE VIEW dbacademy.{schema}.churn_users_metric_view
WITH METRICS
LANGUAGE YAML
COMMENT 'Churn analysis metric view with business-friendly dimensions and measures'
AS $$
version: 0.1
source: dbacademy.{schema}.churn_users
filter: last_activity_date >= '2020-01-01'
joins:
  - name: churn_orders
    source: dbacademy.{schema}.churn_orders
    using: ["user_id"]
dimensions:
  - name: Age Group
    expr: age_group
  - name: Canal
    expr: canal
  - name: Country
    expr: country
  - name: Order Creation Date
    expr: churn_orders.creation_date
  - name: Last Activity Date
    expr: date_trunc('day', last_activity_date)
  - name: Gender
    expr: CASE WHEN gender = 0 THEN 'Female' WHEN gender = 1 THEN 'Male' ELSE 'Other' END
measures:
  - name: Total Users
    expr: COUNT(DISTINCT user_id)
  - name: Active Users
    expr: COUNT(DISTINCT user_id) FILTER (WHERE last_activity_date > CURRENT_DATE - INTERVAL 3 YEARS)
  - name: Churned Users
    expr: COUNT(DISTINCT user_id) FILTER (WHERE churn = 1)
  - name: Churn Rate Percentage
    expr: (MEASURE(`Churned Users`) / MEASURE(`Total Users`)) * 100
  - name: Total Order Amount
    expr: SUM(churn_orders.amount)
  - name: Trailing 30-Day Active Users
    expr: COUNT(DISTINCT user_id)
    window:
      - order: Last Activity Date
        range: trailing 30 day
        semiadditive: last
$$
""")
print(f"Created metric view: dbacademy.{schema}.churn_users_metric_view")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 3: Verify in Catalog Explorer</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>Open <strong>Catalog</strong> from the left sidebar.</li>
# MAGIC <li>Navigate to <strong>dbacademy</strong> > <strong>your_schema</strong> and find <code>churn_users_metric_view</code>.</li>
# MAGIC <li>Review the <strong>Measures</strong> and <strong>Dimensions</strong> tabs to confirm the view was created correctly.</li>
# MAGIC <li>(Optional) Certify the metric view by clicking the edit icon below the name and setting a certification.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 4: Add the metric view as a dataset in the dashboard</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>Open the <strong>Retail - Customer Churn - Universal</strong> dashboard (ending with your labuser username).</li>
# MAGIC <li>Click the <strong>Data</strong> tab at the top of the dashboard editor.</li>
# MAGIC <li>Click <strong>Add dataset</strong>.</li>
# MAGIC <li>Navigate to <strong>All catalogs</strong> > <strong>dbacademy</strong> > <strong>your_schema</strong> (e.g., <code>labuser15359237_1780317248</code>).</li>
# MAGIC <li>Select <code>churn_users_metric_view</code> (it has a checkmark icon indicating it is a metric view).</li>
# MAGIC <li>Click <strong>Add</strong>.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 5: Add a visualization using Genie Code</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>Click <strong>Add a visualization</strong> and place it on the page.</li>
# MAGIC <li>In the Genie Code prompt, type: <code>Churn rate percentage by country using metric view</code> and click Submit.</li>
# MAGIC <li>Accept the generated visualization.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">Including "using metric view" in the prompt is not usually necessary -- Genie Code selects the right dataset automatically. We include it here to be explicit about which data source to use for this exercise.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 6 (Optional): Add a manual visualization</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>Click <strong>Add a visualization</strong> and place it on the page.</li>
# MAGIC <li>Select <strong>Pie</strong> as the chart type.</li>
# MAGIC <li>Set <strong>Angle</strong> to <code>Churn Rate Percentage</code> (MEASURE is added automatically).</li>
# MAGIC <li>Set <strong>Color</strong> to <code>Country</code>.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Publish Your Dashboard with Genie
# MAGIC
# MAGIC ### C1. Enable Genie and Publish
# MAGIC
# MAGIC 1. Open the **Retail - Customer Churn - Universal** dashboard (ending with your labuser username).
# MAGIC 2. Click the **kebab menu (three dots)** and choose **Settings and themes** > **General** tab.
# MAGIC 3. Verify that **Auto-generate Genie space** is selected.
# MAGIC 4. Click **Publish**.
# MAGIC 5. Click **Publish** again to confirm, then close the Sharing menu.
# MAGIC 6. In the upper center of the page, click **View Published** to see the live dashboard.
# MAGIC
# MAGIC ### C2. Explore Genie with Metric View Data
# MAGIC
# MAGIC 1. From the published dashboard, open the Genie Space.
# MAGIC 2. Ask questions using business-friendly metric view dimensions and measures:
# MAGIC    - _"What is the churn rate by age group?"_
# MAGIC    - _"Show active user distribution by canal"_
# MAGIC    - _"What is the total order amount by country?"_
# MAGIC 3. After Genie answers, explore how it works:
# MAGIC    - Click on **Genie's thinking** to see the reasoning.
# MAGIC    - Click **Executed query** to see the SQL that Genie generated.
# MAGIC    - Click **Show code** to view the full query source. Depending on the question, the source will reference the `churn_users_metric_view` -- Genie translates your business-language question into the metric view's measures and dimensions automatically.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Tip: View the Genie Space configuration</strong>
# MAGIC <div style="color:#333;">Click the kebab menu (three dots) in the Genie Space and select <strong>Open Genie Space</strong>. Go to <strong>Configure</strong> to see the data sources, including the <code>churn_users_metric_view</code> that was automatically added from the dashboard.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Verification: Published Dashboard with Genie
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Proof of Execution</strong>
# MAGIC <div style="color:#333;">
# MAGIC <p><strong>Save</strong> a screenshot of the published dashboard showing:</p>
# MAGIC <ul>
# MAGIC <li>The metric view visualization(s) you added</li>
# MAGIC <li>The dlt_expectations visualization from section A</li>
# MAGIC <li>Genie answering a relevant question (e.g., churn rate by age group)</li>
# MAGIC </ul>
# MAGIC <p>Upload using the widget below, then run the <code>show_evidence</code> cell.</p>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

upload_evidence("evidence-03-dashboard.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows the published dashboard with metric view visualizations and Genie enabled.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-03-dashboard.png", width=600)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C4. (Optional) Extend the Metric View and Test with Genie
# MAGIC
# MAGIC <div style="border-left: 4px solid #7b1fa2; background: #f3e5f5; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#7b1fa2; margin-bottom:6px; font-size: 1.1em;">Bonus Task: Add a Custom KPI</strong>
# MAGIC <div style="color:#333;">Extend the metric view with new measures, dimensions, or filters -- then ask Genie questions about them.</div>
# MAGIC </div>
# MAGIC
# MAGIC 1. **Edit the metric view** -- open `churn_users_metric_view` in Catalog Explorer, click **Edit**, and modify the YAML. Use **Genie Code** to help write new definitions. Ideas:
# MAGIC    - Add a custom KPI measure (e.g., "Revenue Per User" = `SUM(churn_orders.amount) / COUNT(DISTINCT user_id)`)
# MAGIC    - Add a new dimension (e.g., "Churn Status" = `CASE WHEN churn = 1 THEN 'Churned' ELSE 'Active' END`)
# MAGIC    - Add a filter (e.g., `country = 'USA'`) to create a region-specific view
# MAGIC 2. **Save** the updated metric view.
# MAGIC 3. **Test with Genie** -- return to the Genie Space and ask questions that use your new KPI or dimension (e.g., "What is the revenue per user by canal?" or "Show churned vs active users by age group").
# MAGIC 4. Verify that Genie picks up the new definitions by checking the **Executed query** and **Show code** to confirm it references your custom measures.