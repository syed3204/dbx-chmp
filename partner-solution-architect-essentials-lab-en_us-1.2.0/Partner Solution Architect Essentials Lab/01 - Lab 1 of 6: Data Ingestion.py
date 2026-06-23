# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 of 6: Data Ingestion
# MAGIC
# MAGIC This section covers data ingestion. Follow the steps below to review the SDP pipeline, add a new transformation, and explore data quality expectations.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Ingest Data
# MAGIC
# MAGIC ### A1. Review the SDP Pipeline
# MAGIC
# MAGIC 1. Open **Jobs & Pipelines** from the left sidebar and find your pipeline (`psae_sdp_churn_<your_username>`). Click into it and review the DAG (Graph) to understand the data flow.
# MAGIC
# MAGIC 2. Navigate to `lakehouse-retail-c360/01-Data-ingestion/01.1-SDP-SQL`. This is the SQL-based pipeline your SDP pipeline uses. (The `01.2-SDP-python` folder contains the same logic in Python as an alternative reference, but the pipeline runs the SQL version.)
# MAGIC
# MAGIC 3. Start with **`01.1-SDP-churn-SQL`**. Open it and read through the SQL to see how it references the transformation files, defines expectations, and orchestrates the pipeline stages. Then explore the other files in the folder using the reference below.
# MAGIC
# MAGIC <div style="margin: 16px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
# MAGIC <div style="background: #1B3139; color: white; padding: 12px 18px; border-radius: 8px 8px 0 0; font-size: 15pt; font-weight: 600;">01.1-SDP-SQL File Reference</div>
# MAGIC
# MAGIC <details style="border: 1px solid #e0e0e0; border-top: none;">
# MAGIC <summary style="cursor: pointer; padding: 12px 18px; background: #F9F7F4; font-weight: 600; font-size: 12pt; border-bottom: 1px solid #e0e0e0;">Pipeline Overview Files</summary>
# MAGIC <div style="padding: 14px 18px; font-size: 12pt; line-height: 1.6;">
# MAGIC <p><code style="background: #eef; padding: 2px 6px; border-radius: 3px;">01.1-SDP-churn-SQL</code><br/>
# MAGIC Start here. Documents the end-to-end pipeline architecture: how data flows from raw ingestion through bronze, silver, and gold layers. References the transformation files below and defines the pipeline expectations.</p>
# MAGIC <p><code style="background: #eef; padding: 2px 6px; border-radius: 3px;">01.2-SDP-churn-expectation-dashboard-data-prep</code><br/>
# MAGIC Queries the pipeline event log to extract data quality expectation results (passed, failed, dropped records). These results populate the dashboards used in section C of this notebook.</p>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <details style="border: 1px solid #e0e0e0; border-top: none;">
# MAGIC <summary style="cursor: pointer; padding: 12px 18px; background: #F9F7F4; font-weight: 600; font-size: 12pt; border-bottom: 1px solid #e0e0e0;">transformations/ -- Pipeline Stages (Bronze > Silver > Gold)</summary>
# MAGIC <div style="padding: 14px 18px; font-size: 12pt; line-height: 1.6;">
# MAGIC <p>These files are executed by the SDP pipeline engine in order. Each file defines one or more streaming tables or materialized views.</p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 12px; margin-top: 8px;">
# MAGIC   <div style="flex: 1; min-width: 220px; border-left: 4px solid #2574B5; background: #f8fafc; padding: 10px 14px; border-radius: 0 6px 6px 0;">
# MAGIC     <strong style="color: #2574B5;">01-bronze.sql</strong><br/>
# MAGIC     Ingests raw data from the UC Volume using <code>cloud_files</code> (Auto Loader). Creates streaming tables: <code>churn_app_events</code> (CSV), <code>churn_orders_bronze</code> (JSON), <code>churn_users_bronze</code> (JSON).
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 220px; border-left: 4px solid #02A36F; background: #f8fafc; padding: 10px 14px; border-radius: 0 6px 6px 0;">
# MAGIC     <strong style="color: #02A36F;">02-silver.sql</strong><br/>
# MAGIC     Cleans and joins the raw tables into enriched views. Applies data quality expectations (valid IDs, non-null fields). Produces <code>churn_orders</code>, <code>churn_users</code>.
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 220px; border-left: 4px solid #FFAB00; background: #f8fafc; padding: 10px 14px; border-radius: 0 6px 6px 0;">
# MAGIC     <strong style="color: #B8860B;">03-gold.sql</strong><br/>
# MAGIC     Aggregates user behavior into <code>churn_features</code>, then calls the <code>predict_churn</code> UDF to produce <code>churn_prediction</code>.
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 220px; border-left: 4px solid #FF5F46; background: #f8fafc; padding: 10px 14px; border-radius: 0 6px 6px 0;">
# MAGIC     <strong style="color: #FF5F46;">04-churn-UDF.py</strong><br/>
# MAGIC     Python file that loads the churn model from MLflow and registers it as a Spark SQL UDF (<code>predict_churn</code>) for use in the gold layer.
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <details style="border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 8px 8px;">
# MAGIC <summary style="cursor: pointer; padding: 12px 18px; background: #F9F7F4; font-weight: 600; font-size: 12pt;">explorations/ -- Optional Reference Queries</summary>
# MAGIC <div style="padding: 14px 18px; font-size: 12pt; line-height: 1.6;">
# MAGIC <p><code style="background: #eef; padding: 2px 6px; border-radius: 3px;">sample_exploration</code><br/>
# MAGIC Sample SQL queries to explore the raw data directly (read from the UC Volume). Not executed by the pipeline. Use as a reference for ad-hoc data exploration.</p>
# MAGIC </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">SDP Pipeline Note</strong>
# MAGIC <div style="color:#333;">The files in <code>transformations/</code> are executed by the SDP pipeline engine, not interactively. You can view and edit them in the workspace, but to test your changes you must re-run the pipeline from <strong>Jobs & Pipelines</strong>. See <a href="https://docs.databricks.com/aws/en/ldp/index">Spark Declarative Pipelines documentation</a> for details.</div>
# MAGIC </div>
# MAGIC
# MAGIC Once you have reviewed the data ingestion and transformation steps, proceed to the next section.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Enhance Your SDP Pipeline with LLM-Based Insights
# MAGIC
# MAGIC Add a new transformation step **after** the churn predictions.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 1: Navigate to the transformations folder</strong>
# MAGIC <div style="color:#333;">
# MAGIC <p>Open <code>lakehouse-retail-c360/01-Data-ingestion/01.1-SDP-SQL/transformations/</code>.</p>
# MAGIC <p>This folder contains the SQL and Python files that the pipeline executes. The pipeline uses a glob pattern (<code>transformations/**</code>) to discover all files in this folder, so any new <code>.sql</code> file you add here is automatically included in the next pipeline run.</p>
# MAGIC <p>Review <code>03-gold.sql</code> to see how the existing <code>churn_prediction</code> materialized view is created. Your new step will build on that output.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 2: Add the churn insights SQL</strong>
# MAGIC <div style="color:#333;">
# MAGIC <p>The sample SQL below creates a materialized view called <code>churn_insights</code> that aggregates daily churn data from <code>churn_prediction</code> and uses <code>AI_QUERY</code> with an LLM to generate a summary for each day. You can use it as-is or adapt it to your own use case.</p>
# MAGIC <p>Choose one of these approaches to add it to the pipeline:</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 12px; margin: 12px 0;">
# MAGIC   <div style="flex: 1; min-width: 280px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #2574B5; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option A: Add to the existing gold file</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.6;">
# MAGIC       <ol style="margin: 0; padding-left: 20px;">
# MAGIC         <li>Open <code>transformations/03-gold.sql</code>.</li>
# MAGIC         <li>Scroll to the bottom of the file.</li>
# MAGIC         <li>Copy and paste the sample SQL below (the separator is included).</li>
# MAGIC       </ol>
# MAGIC       <p style="margin-top: 8px; color: #555;">This keeps all gold-layer views in one file. The new view runs after <code>churn_prediction</code> since it reads from that table.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 280px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #02A36F; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option B: Create a new file</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.6;">
# MAGIC       <ol style="margin: 0; padding-left: 20px;">
# MAGIC         <li>In the <code>transformations/</code> folder, create a new file named <code>05-churn-insights.sql</code>.</li>
# MAGIC         <li>Paste the sample SQL into the new file.</li>
# MAGIC       </ol>
# MAGIC       <p style="margin-top: 8px; color: #555;">The pipeline uses a glob pattern (<code>transformations/**</code>) to discover files, so new files are automatically included in the next run.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 12px 0; border: 1px solid #e0e0e0; border-radius: 8px;">
# MAGIC <summary style="cursor: pointer; padding: 12px 18px; background: #F9F7F4; font-weight: 600; font-size: 12pt; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
# MAGIC <span>&#9654; Click to expand: Sample SQL for <code>05-churn-insights.sql</code></span>
# MAGIC <button onclick="event.stopPropagation();var d=this.closest('details');d.open=true;setTimeout(function(){var t=document.getElementById('sql-churn-insights');t.select();document.execCommand('copy');},100);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy to Clipboard'},2000)" style="padding: 6px 16px; border-radius: 6px; border: 1px solid #2574B5; background: #2574B5; color: white; cursor: pointer; font-size: 12px; font-weight: 600; flex-shrink: 0; margin-left: 12px;">Copy to Clipboard</button>
# MAGIC </summary>
# MAGIC <div style="padding: 0 18px 14px 18px;">
# MAGIC <p style="font-size: 12pt; margin: 10px 0;">Paste into <code>03-gold.sql</code> (Option A) or a new <code>05-churn-insights.sql</code> file (Option B). The cell separator is included at the top.</p>
# MAGIC <textarea id="sql-churn-insights" style="width:100%; height:520px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 0.85rem; line-height: 1.4; padding: 12px; border: 1px solid #e5e7eb; border-radius: 6px; background: #f8fafc; resize: vertical;">-- COMMAND ----------
# MAGIC -- Create daily churn insights with embedded JSON data passed to an LLM
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW churn_insights
# MAGIC COMMENT "Daily AI-generated insights for high-risk churn segments based on real user data."
# MAGIC AS
# MAGIC WITH aggregated_data AS (
# MAGIC     SELECT
# MAGIC         date(last_activity_date) AS churn_date,
# MAGIC         COALESCE(SUM(session_count), 0) AS total_sessions,
# MAGIC         COALESCE(SUM(event_count), 0) AS total_events,
# MAGIC         COALESCE(ROUND(AVG(days_since_last_activity), 1), 0) AS avg_days_since_last_activity,
# MAGIC         COALESCE(ROUND(AVG(churn) * 100, 2), 0) AS churn_rate,
# MAGIC         COALESCE(COUNT(DISTINCT platform), 0) AS platform_breakdown,
# MAGIC         COALESCE(COUNT(DISTINCT age_group), 0) AS age_group_breakdown,
# MAGIC         COALESCE(MAX(canal), 'Unknown') AS top_canal,
# MAGIC         COALESCE(MAX(age_group), 'Unknown') AS top_segment_churn
# MAGIC     FROM churn_prediction
# MAGIC     WHERE churn = 1
# MAGIC     GROUP BY date(last_activity_date)
# MAGIC )
# MAGIC SELECT
# MAGIC     churn_date,
# MAGIC     AI_QUERY(
# MAGIC         'databricks-claude-sonnet-4-6',
# MAGIC         CONCAT(
# MAGIC             'Use the provided data to summarize the daily churn insights. ',
# MAGIC             'Here is the data for ', churn_date, ': ',
# MAGIC             '{',
# MAGIC                 '"total_sessions": "', CAST(total_sessions AS STRING), '", ',
# MAGIC                 '"total_events": "', CAST(total_events AS STRING), '", ',
# MAGIC                 '"avg_days_since_last_activity": "', CAST(avg_days_since_last_activity AS STRING), '", ',
# MAGIC                 '"churn_rate": "', CAST(churn_rate AS STRING), '", ',
# MAGIC                 '"platform_breakdown": "', CAST(platform_breakdown AS STRING), '", ',
# MAGIC                 '"age_group_breakdown": "', CAST(age_group_breakdown AS STRING), '", ',
# MAGIC                 '"top_canal": "', top_canal, '", ',
# MAGIC                 '"top_segment_churn": "', CAST(top_segment_churn AS STRING), '"',
# MAGIC             '}'
# MAGIC         ),
# MAGIC         TRUE
# MAGIC     ) AS churn_insight_summary,
# MAGIC     total_sessions,
# MAGIC     total_events,
# MAGIC     avg_days_since_last_activity,
# MAGIC     churn_rate
# MAGIC FROM aggregated_data;</textarea>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 3: Re-run the SDP Pipeline</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>Go to <strong>Jobs & Pipelines</strong> from the left sidebar.</li>
# MAGIC <li>Find your pipeline (<code>psae_sdp_churn_&lt;your_username&gt;</code>) and click <strong>Run Pipeline</strong>.</li>
# MAGIC <li>Wait for the pipeline to complete and verify all steps succeed, including your new <code>churn_insights</code> step.</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Verification
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Verification: Two pieces of evidence required</strong>
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC <strong>Evidence 1: Pipeline DAG Screenshot</strong>
# MAGIC <ol>
# MAGIC <li>Open <strong>Jobs & Pipelines</strong> from the left sidebar.</li>
# MAGIC <li>Click into your pipeline (<code>psae_sdp_churn_&lt;your_username&gt;</code>).</li>
# MAGIC <li>After the pipeline run completes, <strong>save</strong> a screenshot of the DAG that shows:
# MAGIC   <ul>
# MAGIC     <li>Your new <code>churn_insights</code> materialized view visible as a node in the graph</li>
# MAGIC     <li>All pipeline steps completed successfully (green checkmarks)</li>
# MAGIC   </ul>
# MAGIC </li>
# MAGIC <li>Click the <strong>Upload Screenshot</strong> button in the upload widget below, navigate to your saved screenshot, and select <strong>Open</strong>.</li>
# MAGIC <li>Run the <code>show_evidence</code> code cell to display the uploaded image in the notebook output.</li>
# MAGIC </ol>
# MAGIC
# MAGIC <strong>Evidence 2: Table Data</strong>
# MAGIC <ol>
# MAGIC <li>Run the code cell at the bottom of this section to query and display the <code>churn_insights</code> table.</li>
# MAGIC <li>The cell output confirms the table was created and contains LLM-generated insights.</li>
# MAGIC </ol>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Images/Image_Display_Function

# COMMAND ----------

upload_evidence("evidence-01-pipeline.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows your pipeline DAG with the <code>churn_insights</code> node and all steps green.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-01-pipeline.png", width=600)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidence 2: Display the churn_insights Table

# COMMAND ----------

import re
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
schema = re.sub(r'[^a-zA-Z0-9_]', '_', username.split('@')[0])

spark.read.table(f"dbacademy.{schema}.churn_insights").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Create a Data Quality Visualization
# MAGIC
# MAGIC The SDP pipeline records data quality expectation results (passed, failed, dropped records) in the `dlt_expectations` table. In this activity, you will query that data and build a chart directly in the notebook.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 1: Run the query</strong>
# MAGIC <div style="color:#333;">Run the code cell below. It returns a summary of passed, failed, and dropped records grouped by dataset and validation rule, along with each rule's failure rate.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 2: Create a visualization</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>In the cell output, click the <strong>+</strong> icon next to the "Table" tab.</li>
# MAGIC <li>Select <strong>Visualization</strong> to open the chart editor.</li>
# MAGIC <li>Configure the chart using one of the options below, or design your own.</li>
# MAGIC <li>Click <strong>Save</strong>. The chart appears as a new tab next to "Table". Both the table and chart are captured when you export this notebook as HTML.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 12px; margin: 16px 0;">
# MAGIC   <div style="flex: 1; min-width: 250px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #2574B5; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option A: Bar Chart</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.8;">
# MAGIC       <strong>Type:</strong> Bar<br/>
# MAGIC       <strong>X-axis:</strong> <code>dataset</code><br/>
# MAGIC       <strong>Y-axis:</strong> <code>failure_rate</code><br/>
# MAGIC       <strong>Group by / Color:</strong> <code>validation_rule</code>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 250px; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
# MAGIC     <div style="background: #02A36F; color: white; padding: 10px 14px; font-weight: 600; font-size: 12pt;">Option B: Heatmap</div>
# MAGIC     <div style="padding: 12px 14px; font-size: 12pt; line-height: 1.8;">
# MAGIC       <strong>Type:</strong> Heatmap<br/>
# MAGIC       <strong>X-axis:</strong> <code>dataset</code><br/>
# MAGIC       <strong>Y-axis:</strong> <code>validation_rule</code><br/>
# MAGIC       <strong>Color:</strong> <code>failure_rate</code>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">What to look for</strong>
# MAGIC <div style="color:#333;">The chart shows which datasets and validation rules have the highest failure rates. Look for rules where <code>failure_rate</code> spikes -- these indicate data quality issues in specific pipeline stages that may need attention.</div>
# MAGIC </div>

# COMMAND ----------

import re
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
schema = re.sub(r'[^a-zA-Z0-9_]', '_', username.split('@')[0])

spark.sql(f"""
SELECT
    dataset,
    name AS validation_rule,
    SUM(passed_records) AS passed_records,
    SUM(failed_records) AS failed_records,
    SUM(dropped_records) AS dropped_records,
    SUM(failed_records) / SUM(output_records) * 100 AS failure_rate
FROM dbacademy.{schema}.dlt_expectations
GROUP BY dataset, validation_rule
ORDER BY dataset, failure_rate DESC
""").display()
