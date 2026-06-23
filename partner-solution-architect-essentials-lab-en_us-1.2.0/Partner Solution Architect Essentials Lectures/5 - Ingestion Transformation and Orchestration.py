# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 Lecture - Ingestion, Transformation, and Orchestration
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Every data-driven initiative begins with the same challenge: getting the right data, in the right shape, to the right place, on time. Organizations depend on dozens of source systems, from enterprise SaaS applications and relational databases to streaming message buses and flat files in cloud storage. Stitching these sources together has traditionally required a patchwork of specialized tools, each with its own security model, scheduling system, and operational overhead. The result is fragile pipelines, redundant data copies, and engineering bottlenecks that slow every downstream workload.
# MAGIC
# MAGIC Databricks addresses this complexity with **LakeFlow**, a unified data engineering solution that consolidates ingestion, transformation, and orchestration into a single platform. LakeFlow consists of three components: **LakeFlow Connect** for managed ingestion, **Lakeflow Declarative Pipelines** (powered by Spark Declarative Pipelines) for transformation and data quality, and **Lakeflow Jobs** for orchestration. Together, they provide a low floor for simple use cases and a high ceiling for sub-second streaming, all governed by Unity Catalog.
# MAGIC
# MAGIC This lecture covers 6 sections that build on each other:
# MAGIC
# MAGIC - **A. The Ingestion Challenge** - Why managed ingestion matters and the cost of fragmented tooling
# MAGIC - **B. Ingestion Methods** - LakeFlow Connect, Auto Loader, and Structured Streaming
# MAGIC - **C. Orchestration with Lakeflow Jobs** - Task types, triggers, compute options, and monitoring
# MAGIC - **D. Transformation with Declarative Pipelines** - Medallion architecture, streaming tables, and materialized views
# MAGIC - **E. Data Quality with Expectations** - Rules, violation policies, and monitoring
# MAGIC - **F. Change Data Capture** - APPLY CHANGES INTO, SCD patterns, and CDC processing
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC <br/>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div>
# MAGIC     <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Compare batch, streaming, and managed connector ingestion methods and identify when to use each (Auto Loader, LakeFlow Connect, Structured Streaming)</li>
# MAGIC         <li>Explain how LakeFlow unifies ingestion, transformation, and orchestration into a single data engineering platform with Connect, Pipelines, and Jobs</li>
# MAGIC         <li>Describe the building blocks of Lakeflow Jobs, including task types, control flows, triggers, and compute options</li>
# MAGIC         <li>Build a multi-hop ETL pipeline using the medallion architecture with streaming tables and materialized views</li>
# MAGIC         <li>Implement data quality controls using Expectations with appropriate violation policies (warn, drop, fail)</li>
# MAGIC         <li>Apply change data capture patterns using AUTO CDC to process incremental inserts, updates, and deletes</li>
# MAGIC       </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The Ingestion Challenge

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Why Managed Ingestion Matters
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Data lies at the core of virtually every modern initiative: customer experience, product innovation, operational efficiency, and revenue growth all depend on timely, governed data. Yet most organizations face a <strong>fragmented data delivery landscape</strong> where diverse ingestion tools, processing frameworks, and analytics platforms create operational complexity, pipeline fragility, and high onboarding overhead.</p>
# MAGIC
# MAGIC <strong>The problem:</strong>
# MAGIC - Diverse and fragmented tooling (Fivetran, Qlik, AWS Glue, Airflow, Azure Data Factory) each requiring specialized skills
# MAGIC - Redundant ETL jobs across multiple systems with no central governance or lineage tracking
# MAGIC - Turning SQL queries into production ETL pipelines involves tedious, complicated operational work
# MAGIC
# MAGIC <strong>The Databricks answer: LakeFlow</strong>
# MAGIC - One data engineering solution covering ingestion, transformation, and orchestration
# MAGIC - Three pillars: <strong>Connect</strong> (Ingestion), <strong>Pipelines</strong> (Transformation), <strong>Jobs</strong> (Orchestration)
# MAGIC - Low floor (point-and-click for business users) and high ceiling (sub-second streaming for power users)
# MAGIC - <strong>Lakeflow Designer</strong> (Public Preview April 2026, with GA updates in May 2026) adds a visual drag-and-drop canvas for building data transformation workflows, providing a no-code option alongside SQL and Python pipeline definitions
# MAGIC
# MAGIC <!-- ── Visual: a1-lakeflow-three-pillars ── -->
# MAGIC <style>
# MAGIC .a1-lf-wrapper { width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .a1-lf-header {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC   padding: 16px 20px;
# MAGIC   border-radius: 12px 12px 0 0;
# MAGIC   font-size: 16pt;
# MAGIC   font-weight: 700;
# MAGIC   letter-spacing: 0.5px;
# MAGIC }
# MAGIC .a1-lf-pillars {
# MAGIC   display: flex;
# MAGIC   gap: 0;
# MAGIC   border: 2px solid #1B3139;
# MAGIC   border-top: none;
# MAGIC   border-radius: 0 0 12px 12px;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .a1-lf-pillar {
# MAGIC   flex: 1;
# MAGIC   padding: 20px;
# MAGIC   text-align: center;
# MAGIC   border-right: 1px solid #EEEDE9;
# MAGIC }
# MAGIC .a1-lf-pillar:last-child { border-right: none; }
# MAGIC .a1-lf-pillar { transition: transform 0.2s, box-shadow 0.2s, background 0.15s; }
# MAGIC .a1-lf-pillar:hover { background: transparent; transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); z-index: 1; position: relative; }
# MAGIC .a1-lf-pillar-icon { font-size: 28px; margin-bottom: 8px; }
# MAGIC .a1-lf-pillar-name {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B5162;
# MAGIC   margin-bottom: 6px;
# MAGIC }
# MAGIC .a1-lf-pillar-role {
# MAGIC   font-size: 14pt;
# MAGIC   color: #618794;
# MAGIC   font-weight: 600;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .a1-lf-pillar-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .a1-lf-footer {
# MAGIC   display: flex;
# MAGIC   gap: 0;
# MAGIC   margin-top: 2px;
# MAGIC }
# MAGIC .a1-lf-footer-item {
# MAGIC   flex: 1;
# MAGIC   background: #F9F7F4;
# MAGIC   text-align: center;
# MAGIC   padding: 10px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #618794;
# MAGIC   border: 1px solid #EEEDE9;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="a1-lf-wrapper">
# MAGIC   <div class="a1-lf-header">LakeFlow: One Data Engineering Solution</div>
# MAGIC   <div class="a1-lf-pillars">
# MAGIC     <div class="a1-lf-pillar">
# MAGIC       <div class="a1-lf-pillar-dot" style="width:12px;height:12px;border-radius:50%;background:#00A972;margin:0 auto 6px;"></div>
# MAGIC       <div class="a1-lf-pillar-name">LakeFlow Connect</div>
# MAGIC       <div class="a1-lf-pillar-role">Ingestion</div>
# MAGIC       <div class="a1-lf-pillar-desc">Managed connectors for SaaS apps, databases, cloud storage, and message buses</div>
# MAGIC     </div>
# MAGIC     <div class="a1-lf-pillar">
# MAGIC       <div class="a1-lf-pillar-dot" style="width:12px;height:12px;border-radius:50%;background:#1B5162;margin:0 auto 6px;"></div>
# MAGIC       <div class="a1-lf-pillar-name">Lakeflow Pipelines</div>
# MAGIC       <div class="a1-lf-pillar-role">Transformation</div>
# MAGIC       <div class="a1-lf-pillar-desc">Declarative ETL with streaming tables, materialized views, and data quality</div>
# MAGIC     </div>
# MAGIC     <div class="a1-lf-pillar">
# MAGIC       <div class="a1-lf-pillar-dot" style="width:12px;height:12px;border-radius:50%;background:#FFAB00;margin:0 auto 6px;"></div>
# MAGIC       <div class="a1-lf-pillar-name">Lakeflow Jobs</div>
# MAGIC       <div class="a1-lf-pillar-role">Orchestration</div>
# MAGIC       <div class="a1-lf-pillar-desc">built-in task orchestration with DAGs, triggers, serverless compute, and monitoring</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#1B3139;color:#a8c9d6;text-align:center;padding:12px 20px;border-radius:0 0 12px 12px;font-size:14pt;margin-top:2px;">
# MAGIC     <strong style="color:#fff;">Shared foundation across all three:</strong> Unity Catalog Governance &bull; Delta Lake Storage &bull; Serverless Compute
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">The Fragmented Tooling Problem</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Diverse tooling landscape:</strong> Organizations commonly use Fivetran or Qlik for ingestion, AWS Glue or Azure Data Factory for processing, Airflow for orchestration, and separate platforms for analytics and ML. Each tool has its own security model, scheduling system, and operational overhead.</li>
# MAGIC           <li><strong>Operational complexity:</strong> Data engineers must master ETL, streaming, CDC, software engineering, and infrastructure management. This specialization requirement slows hiring and creates knowledge silos in teams.</li>
# MAGIC           <li><strong>Pipeline fragility:</strong> When a connector update breaks an upstream tool, the downstream cascade is invisible to the orchestrator. Teams spend more time diagnosing cross-tool failures than building new pipelines.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">How LakeFlow Solves It</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Single platform:</strong> LakeFlow consolidates ingestion (Connect), transformation (Pipelines), and orchestration (Jobs) into a single interface. One security model, one lineage graph, one monitoring surface.</li>
# MAGIC           <li><strong>Declarative approach:</strong> Engineers define "what" they want (table definitions, quality rules, schedules) rather than "how" to achieve it. The platform handles DAG construction, retries, scaling, and incremental processing automatically.</li>
# MAGIC           <li><strong>Compatible with existing workloads:</strong> LakeFlow works alongside existing tools. Teams can migrate incrementally, starting with the component (Connect, Pipelines, or Jobs) that delivers the most immediate value.</li>
# MAGIC           <li><strong>LakeFlow Designer:</strong> a visual, drag-and-drop canvas (Public Preview April 2026, with GA updates in May 2026) for building transformation workflows. Features include AI-powered operator search, bidirectional AI descriptions, N-way Combine operators, and configurable multi-modal outputs. This gives teams a no-code option alongside SQL and Python pipeline definitions.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/the-rank-group-plc/lakeflow-jobs" style="color: #2574B5;">The Rank Group</a> consolidated data from 50+ systems and 7 data warehouses onto Databricks, saving 1.2 million GBP and boosting productivity by 30%. Daily reports now arrive approximately 4 hours earlier. &#x25C6;</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/national-australian-bank-nab" style="color: #2574B5;">National Australian Bank</a> is standardizing all 1,800 pipelines on Lakeflow, improving job success rates from 86% to 99.6% and reducing transformation complexity by 80%. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Ingestion Methods

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. LakeFlow Connect: Managed Connectors
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>LakeFlow Connect</strong> provides fully managed connectors for ingesting data from enterprise SaaS applications and databases into the Databricks Lakehouse. It is designed for low-maintenance, plug-and-play ingestion where the platform handles authentication, scheduling, schema management, and CDC automatically.</p>
# MAGIC
# MAGIC LakeFlow Connect offers three connector tiers:
# MAGIC
# MAGIC 1. <strong>Managed Connectors</strong>
# MAGIC Fully managed by Databricks with out-of-the-box functionality. Includes Salesforce, Workday, SQL Server, ServiceNow, Google Analytics, SharePoint, NetSuite, MySQL, PostgreSQL, and more. The ingestion process itself cannot be customized.
# MAGIC 1. <strong>Standard Connectors</strong>
# MAGIC Cloud storage and message bus connectors (S3, ADLS, GCS, Kafka, Kinesis) with customization options via Structured Streaming or Declarative Pipelines.
# MAGIC 1. <strong>Community Connectors</strong>
# MAGIC Open-source, community-maintained connectors for unsupported sources. No Databricks SLA.
# MAGIC
# MAGIC All managed connectors use <strong>incremental ingestion</strong>: the first run loads all selected data while simultaneously tracking changes. Subsequent runs capture only modified data.
# MAGIC
# MAGIC <!-- ── Visual: b1-saas-vs-db-architecture ── -->
# MAGIC <style>
# MAGIC .b1-arch-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .b1-arch-row {
# MAGIC   display: flex;
# MAGIC   gap: 20px;
# MAGIC   margin-bottom: 16px;
# MAGIC }
# MAGIC .b1-arch-card {
# MAGIC   flex: 1;
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   transition: box-shadow 0.2s;
# MAGIC }
# MAGIC .b1-arch-card:hover {
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .b1-arch-card-header {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   padding: 12px 16px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .b1-arch-card-body {
# MAGIC   padding: 16px;
# MAGIC   background: #F9F7F4;
# MAGIC }
# MAGIC .b1-arch-flow {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   gap: 8px;
# MAGIC   flex-wrap: wrap;
# MAGIC }
# MAGIC .b1-arch-step {
# MAGIC   background: #fff;
# MAGIC   border: 1px solid #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 8px 12px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   text-align: center;
# MAGIC   min-width: 100px;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .b1-arch-step:hover {
# MAGIC   transform: translateY(-2px);
# MAGIC   box-shadow: 0 4px 10px rgba(27,49,57,0.10);
# MAGIC }
# MAGIC .b1-arch-arrow {
# MAGIC   font-size: 16pt;
# MAGIC   color: #618794;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="b1-arch-wrapper">
# MAGIC   <div class="b1-arch-row">
# MAGIC     <div class="b1-arch-card">
# MAGIC       <div class="b1-arch-card-header">SaaS Connectors (Simpler)</div>
# MAGIC       <div class="b1-arch-card-body">
# MAGIC         <div class="b1-arch-flow">
# MAGIC           <div class="b1-arch-step"><strong>Source</strong><br/>Salesforce, Workday, etc.</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>UC Credentials</strong><br/>Authentication</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Serverless Pipeline</strong><br/>Managed Ingestion</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Streaming Delta Tables</strong><br/>Unity Catalog</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-arch-row">
# MAGIC     <div class="b1-arch-card">
# MAGIC       <div class="b1-arch-card-header">Database Connectors (CDC-Enabled)</div>
# MAGIC       <div class="b1-arch-card-body">
# MAGIC         <div class="b1-arch-flow">
# MAGIC           <div class="b1-arch-step"><strong>Source DB</strong><br/>SQL Server, MySQL, etc.</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>UC Credentials</strong></div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Ingestion Gateway</strong><br/>Classic Compute</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Internal State</strong><br/>UC Volumes</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Managed Ingestion</strong><br/>Serverless + AUTO CDC</div>
# MAGIC           <div class="b1-arch-arrow">&#x2192;</div>
# MAGIC           <div class="b1-arch-step"><strong>Streaming Delta Tables</strong></div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">SaaS Connectors vs. Database Connectors</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SaaS connectors</strong> use a 3-step flow: credential retrieval from Unity Catalog, API-based connection to the source, and transformation into streaming Delta tables. Completely serverless, no gateway needed.</li>
# MAGIC           <li><strong>Database connectors</strong> add a 4th component: an ingestion gateway running on classic compute that captures snapshots and change logs from the source database. Internal state (metadata, CDC JSON, snapshots) is managed on a UC Volume.</li>
# MAGIC           <li><strong>Key difference:</strong> SaaS connectors can run in serverless-only workspaces. Database connectors require classic compute for the ingestion gateway.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">LakeFlow Connect vs. Alternatives</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Lakehouse Federation:</strong> queries external sources without moving data. Best for ad hoc reporting and proof-of-concept work where data residency matters.</li>
# MAGIC           <li><strong>Delta Sharing:</strong> securely shares live data across platforms, clouds, and regions. Best when minimizing data duplication and enabling real-time access.</li>
# MAGIC           <li><strong>Auto Loader:</strong> handles cloud object storage files (S3, ADLS, GCS). Not a replacement for managed connectors, which handle SaaS and database sources with fully managed pipelines.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/panasonic/lakeflow-connect" style="color: #2574B5;">Panasonic</a> standardized on Auto Loader and LakeFlow Connect for SAP ingestion. Their largest table dropped from hours to 2 minutes, with end-to-end silver processing completing in roughly 30 minutes. &#x25C6;</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/toyota/lakeflow-connect" style="color: #2574B5;">Toyota</a> achieved a 98% reduction in data latency (4.5 seconds to 0.1 seconds) using LakeFlow for real-time factory telemetry. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Auto Loader: Incremental File Ingestion
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Auto Loader</strong> incrementally and efficiently processes new data files as they arrive in cloud storage. It provides a Structured Streaming source called <code style="font-size:14pt;">cloudFiles</code> (Python) or the <code style="font-size:14pt;">read_files()</code> table-valued function (SQL). Auto Loader scales to millions of files per hour, handles schema inference and evolution automatically, and guarantees exactly-once processing via a RocksDB checkpoint.</p>
# MAGIC
# MAGIC <!-- ── Visual: source-process-output (Auto Loader pipeline) ── -->
# MAGIC <style>
# MAGIC .b2-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; color: #0b2026; }
# MAGIC /* Pipeline flow */
# MAGIC .b2-v-flow { display: flex; align-items: stretch; gap: 0; margin-bottom: 14px; }
# MAGIC .b2-v-node { flex: 1; border-radius: 10px; padding: 16px 14px; text-align: center; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b2-v-node:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .b2-v-arrow { display: flex; align-items: center; font-size: 28pt; color: #94b3be; padding: 0 6px; flex-shrink: 0; }
# MAGIC /* Node styles */
# MAGIC .b2-v-src { background: #F9F7F4; border: 2px solid #DCE0E2; }
# MAGIC .b2-v-proc { background: linear-gradient(135deg, #1B3139, #1B5162); color: #fff; border: 2px solid #1B5162; }
# MAGIC .b2-v-out { background: #F9F7F4; border: 2px solid #00A972; }
# MAGIC .b2-v-node-icon { width: 48px; height: 48px; border-radius: 50%; margin: 0 auto 8px; display: flex; align-items: center; justify-content: center; font-size: 20pt; font-weight: 700; color: #fff; }
# MAGIC .b2-v-node-title { font-size: 15pt; font-weight: 700; margin-bottom: 6px; }
# MAGIC .b2-v-src .b2-v-node-title { color: #1B3139; }
# MAGIC .b2-v-out .b2-v-node-title { color: #00A972; }
# MAGIC /* Pills inside nodes */
# MAGIC .b2-v-pills { display: flex; flex-wrap: wrap; gap: 4px; justify-content: center; margin-top: 8px; }
# MAGIC .b2-v-pill { font-size: 14pt; font-weight: 600; padding: 3px 8px; border-radius: 10px; }
# MAGIC .b2-v-pill-src { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC .b2-v-pill-proc { background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3); }
# MAGIC .b2-v-pill-out { background: rgba(0,169,114,0.10); color: #007a53; }
# MAGIC /* Detection modes comparison */
# MAGIC .b2-v-modes { display: flex; gap: 12px; margin-bottom: 14px; }
# MAGIC .b2-v-mode { flex: 1; border-radius: 10px; padding: 14px; border: 1.5px solid #DCE0E2; background: #fff; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b2-v-mode:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.10); }
# MAGIC .b2-v-mode-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .b2-v-mode-desc { font-size: 14pt; color: #555; line-height: 1.4; }
# MAGIC .b2-v-mode-fit { font-size: 14pt; font-weight: 600; margin-top: 6px; }
# MAGIC .b2-v-mode-fit-simple { color: #618794; }
# MAGIC .b2-v-mode-fit-prod { color: #00A972; }
# MAGIC /* Schema evolution bar */
# MAGIC .b2-v-schema { display: flex; gap: 0; border-radius: 10px; overflow: hidden; border: 1.5px solid #DCE0E2; }
# MAGIC .b2-v-schema-item { flex: 1; padding: 10px 12px; font-size: 14pt; font-weight: 600; color: #1B3139; background: #F9F7F4; border-right: 1px solid #DCE0E2; text-align: center; }
# MAGIC .b2-v-schema-item:last-child { border-right: none; }
# MAGIC .b2-v-schema-hdr { background: #EEEDE9; font-weight: 700; color: #5A6F77; text-transform: uppercase; letter-spacing: 0.5px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b2-v-wrap">
# MAGIC   <!-- Pipeline flow: Source → Auto Loader → Streaming Table -->
# MAGIC   <div class="b2-v-flow">
# MAGIC     <div class="b2-v-node b2-v-src">
# MAGIC       <div class="b2-v-node-icon" style="background:#618794;">&#x279C;</div>
# MAGIC       <div class="b2-v-node-title">Cloud Storage</div>
# MAGIC       <div style="font-size:14pt;color:#555;">Files land in S3, ADLS, GCS, or UC Volumes</div>
# MAGIC       <div class="b2-v-pills">
# MAGIC         <span class="b2-v-pill b2-v-pill-src">JSON</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-src">CSV</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-src">Parquet</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-src">Avro</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-src">ORC</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-src">XML</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-v-arrow">&#x279C;</div>
# MAGIC     <div class="b2-v-node b2-v-proc">
# MAGIC       <div class="b2-v-node-icon" style="background:rgba(255,255,255,0.2);border:2px solid rgba(255,255,255,0.4);"><span style="font-size:16pt;">AL</span></div>
# MAGIC       <div class="b2-v-node-title">Auto Loader</div>
# MAGIC       <div style="font-size:14pt;color:#a8c9d6;">Detects new files, infers schema, processes exactly once</div>
# MAGIC       <div class="b2-v-pills">
# MAGIC         <span class="b2-v-pill b2-v-pill-proc">Exactly-Once</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-proc">Schema Evolution</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-proc">Millions/hr</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-v-arrow">&#x279C;</div>
# MAGIC     <div class="b2-v-node b2-v-out">
# MAGIC       <div class="b2-v-node-icon" style="background:#00A972;">&#x2714;</div>
# MAGIC       <div class="b2-v-node-title">Streaming Table</div>
# MAGIC       <div style="font-size:14pt;color:#555;">Delta table in Unity Catalog, refreshed on schedule or trigger</div>
# MAGIC       <div class="b2-v-pills">
# MAGIC         <span class="b2-v-pill b2-v-pill-out">Governed</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-out">Incremental</span>
# MAGIC         <span class="b2-v-pill b2-v-pill-out">Append-Only</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Two detection modes -->
# MAGIC   <div class="b2-v-modes">
# MAGIC     <div class="b2-v-mode">
# MAGIC       <div class="b2-v-mode-title">Directory Listing Mode</div>
# MAGIC       <div class="b2-v-mode-desc">Scans the directory to find new files. Simple setup, no cloud configuration needed.</div>
# MAGIC       <div class="b2-v-mode-fit b2-v-mode-fit-simple">Best for: development, small-to-medium file volumes</div>
# MAGIC     </div>
# MAGIC     <div class="b2-v-mode">
# MAGIC       <div class="b2-v-mode-title">File Notification Mode</div>
# MAGIC       <div class="b2-v-mode-desc">Uses cloud-native events (S3 SQS, ADLS Event Grid, GCS Pub/Sub) for lower-cost, lower-latency detection.</div>
# MAGIC       <div class="b2-v-mode-fit b2-v-mode-fit-prod">Best for: production, high-volume ingestion at scale</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Schema handling bar -->
# MAGIC   <div class="b2-v-schema">
# MAGIC     <div class="b2-v-schema-item b2-v-schema-hdr">Schema Handling</div>
# MAGIC     <div class="b2-v-schema-item">Auto-infer from first 50 GB or 1,000 files</div>
# MAGIC     <div class="b2-v-schema-item">5 evolution modes (addNewColumns default)</div>
# MAGIC     <div class="b2-v-schema-item">_rescued_data column for mismatches</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Schema Inference and Evolution</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Schema inference</strong> samples the first 50 GB or 1,000 files (whichever comes first). JSON and CSV default to STRING types unless <code>cloudFiles.inferColumnTypes</code> is set to true.</li>
# MAGIC           <li><strong>Five schema evolution modes:</strong> <code>addNewColumns</code> (default, restarts stream with new columns), <code>addNewColumnsWithTypeWidening</code> (also widens types like int to long), <code>rescue</code> (never evolves, all unknowns go to _rescued_data), <code>failOnNewColumns</code> (hard failure), and <code>none</code> (ignores new columns).</li>
# MAGIC           <li><strong>The _rescued_data column</strong> captures data that does not match the schema, including type mismatches and case mismatches. Think of it as a "lost and found" bin for unexpected fields.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Production Configuration</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>File notification mode</strong> is recommended for production. It uses cloud-native notification services (S3 SQS, ADLS Event Grid, GCS Pub/Sub) for lower-cost, lower-latency detection compared to directory scanning.</li>
# MAGIC           <li><strong>Trigger.AvailableNow</strong> provides batch-style processing without a continuous cluster, ideal for cost-sensitive scheduled ingestion.</li>
# MAGIC           <li><strong>File archiving:</strong> use <code>cloudFiles.cleanSource = "MOVE"</code> to automatically archive processed files with configurable retention.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Auto Loader vs. COPY INTO</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Scale:</strong> COPY INTO suits thousands of files. Auto Loader scales to millions or billions.</li>
# MAGIC           <li><strong>Schema evolution:</strong> Auto Loader provides automatic inference and evolution. COPY INTO requires manual <code>mergeSchema</code>.</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/ahold-delhaize" style="color: #2574B5;">Ahold Delhaize</a> replaced Azure Data Factory with Auto Loader for ingestion, running approximately 1,165 ingestion jobs daily across their self-service pipeline platform. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Structured Streaming: Message Bus Ingestion
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">For real-time data from message buses, Databricks provides native Structured Streaming connectors for <strong>Apache Kafka, Amazon Kinesis, Azure Event Hubs, Google Pub/Sub, and Apache Pulsar</strong>. The same engine that powers Auto Loader also processes event streams, providing exactly-once semantics, checkpointing, and integration with the medallion architecture. Two processing modes serve different latency needs.</p>
# MAGIC
# MAGIC <!-- ── Visual: source-process-output (Structured Streaming pipeline) ── -->
# MAGIC <style>
# MAGIC .b3-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; color: #0b2026; }
# MAGIC /* Two processing modes as tabs */
# MAGIC .b3-v-radio { display: none; }
# MAGIC .b3-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .b3-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .b3-v-tab-title { font-size: 15pt; font-weight: 700; display: block; }
# MAGIC .b3-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .b3-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC #b3-t1:checked ~ .b3-v-tabs .b3-v-tab1 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #b3-t1:checked ~ .b3-v-tabs .b3-v-tab1 .b3-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #b3-t1:checked ~ .b3-v-p1 { display: block; border-color: #1B5162; }
# MAGIC #b3-t2:checked ~ .b3-v-tabs .b3-v-tab2 { background: #E5A100; color: #fff; border-color: #E5A100; }
# MAGIC #b3-t2:checked ~ .b3-v-tabs .b3-v-tab2 .b3-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #b3-t2:checked ~ .b3-v-p2 { display: block; border-color: #E5A100; }
# MAGIC /* Shared flow styles */
# MAGIC .b3-v-flow { display: flex; align-items: stretch; gap: 0; margin-bottom: 14px; }
# MAGIC .b3-v-node { flex: 1; border-radius: 10px; padding: 16px 14px; text-align: center; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b3-v-node:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .b3-v-arrow { display: flex; align-items: center; font-size: 28pt; color: #94b3be; padding: 0 6px; flex-shrink: 0; }
# MAGIC .b3-v-src { background: #F9F7F4; border: 2px solid #DCE0E2; }
# MAGIC .b3-v-proc { border: 2px solid #1B5162; }
# MAGIC .b3-v-proc-mb { background: linear-gradient(135deg, #1B3139, #1B5162); color: #fff; }
# MAGIC .b3-v-proc-rt { background: linear-gradient(135deg, #8a6200, #E5A100); color: #fff; }
# MAGIC .b3-v-out { background: #F9F7F4; border: 2px solid #00A972; }
# MAGIC .b3-v-node-icon { width: 48px; height: 48px; border-radius: 50%; margin: 0 auto 8px; display: flex; align-items: center; justify-content: center; font-size: 20pt; font-weight: 700; color: #fff; }
# MAGIC .b3-v-node-title { font-size: 15pt; font-weight: 700; margin-bottom: 6px; }
# MAGIC .b3-v-src .b3-v-node-title { color: #1B3139; }
# MAGIC .b3-v-out .b3-v-node-title { color: #00A972; }
# MAGIC .b3-v-pills { display: flex; flex-wrap: wrap; gap: 4px; justify-content: center; margin-top: 8px; }
# MAGIC .b3-v-pill { font-size: 14pt; font-weight: 600; padding: 3px 8px; border-radius: 10px; }
# MAGIC .b3-v-pill-src { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC .b3-v-pill-proc { background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3); }
# MAGIC .b3-v-pill-out { background: rgba(0,169,114,0.10); color: #007a53; }
# MAGIC /* Key metrics */
# MAGIC .b3-v-metric { text-align: center; padding: 12px; border-radius: 8px; margin-bottom: 14px; }
# MAGIC .b3-v-metric-val { font-size: 20pt; font-weight: 800; color: #fff; }
# MAGIC .b3-v-metric-label { font-size: 14pt; color: rgba(255,255,255,0.85); }
# MAGIC /* Characteristics */
# MAGIC .b3-v-chars { display: flex; gap: 8px; flex-wrap: wrap; }
# MAGIC .b3-v-char { flex: 1; min-width: 140px; border-radius: 8px; padding: 10px; border: 1.5px solid #DCE0E2; background: #F9F7F4; text-align: center; }
# MAGIC .b3-v-char-title { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .b3-v-char-val { font-size: 14pt; color: #555; margin-top: 2px; }
# MAGIC /* SDP callout */
# MAGIC .b3-v-sdp { border-left: 4px solid #1B5162; background: #F9F7F4; padding: 12px 16px; border-radius: 4px; font-size: 14pt; color: #333; margin-top: 14px; }
# MAGIC .b3-v-sdp strong { color: #1B5162; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b3-v-wrap">
# MAGIC <input type="radio" name="b3tabs" id="b3-t1" class="b3-v-radio" checked>
# MAGIC <input type="radio" name="b3tabs" id="b3-t2" class="b3-v-radio">
# MAGIC
# MAGIC <div class="b3-v-tabs">
# MAGIC   <label for="b3-t1" class="b3-v-tab b3-v-tab1"><span class="b3-v-tab-title">Micro-Batch Mode</span><span class="b3-v-tab-sub">Default: seconds to minutes latency</span></label>
# MAGIC   <label for="b3-t2" class="b3-v-tab b3-v-tab2"><span class="b3-v-tab-title">Real-Time Mode</span><span class="b3-v-tab-sub">GA: as low as 5 ms latency</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tab 1: Micro-Batch -->
# MAGIC <div class="b3-v-panel b3-v-p1">
# MAGIC   <div class="b3-v-metric" style="background:linear-gradient(135deg,#1B5162,#2a7a94);">
# MAGIC     <div class="b3-v-metric-val">All Sources &#x279C; Delta Table</div>
# MAGIC     <div class="b3-v-metric-label">Periodic micro-batches on serverless, dedicated, or standard compute</div>
# MAGIC   </div>
# MAGIC   <div class="b3-v-flow">
# MAGIC     <div class="b3-v-node b3-v-src">
# MAGIC       <div class="b3-v-node-icon" style="background:#618794;">&#x279C;</div>
# MAGIC       <div class="b3-v-node-title">Message Bus Sources</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Apache Kafka</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Amazon Kinesis</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Azure Event Hubs</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Google Pub/Sub</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Apache Pulsar</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b3-v-arrow">&#x279C;</div>
# MAGIC     <div class="b3-v-node b3-v-proc b3-v-proc-mb">
# MAGIC       <div class="b3-v-node-icon" style="background:rgba(255,255,255,0.2);border:2px solid rgba(255,255,255,0.4);"><span style="font-size:16pt;">SS</span></div>
# MAGIC       <div class="b3-v-node-title">Structured Streaming</div>
# MAGIC       <div style="font-size:14pt;color:#a8c9d6;">Processes data in periodic micro-batches</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">Exactly-Once</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">Checkpointed</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">Serverless OK</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b3-v-arrow">&#x279C;</div>
# MAGIC     <div class="b3-v-node b3-v-out">
# MAGIC       <div class="b3-v-node-icon" style="background:#00A972;">&#x2714;</div>
# MAGIC       <div class="b3-v-node-title">Delta Table</div>
# MAGIC       <div style="font-size:14pt;color:#555;">Streaming table in Unity Catalog</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill b3-v-pill-out">Governed</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-out">ACID</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b3-v-chars">
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Latency</div><div class="b3-v-char-val">Seconds to minutes</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Compute</div><div class="b3-v-char-val">Serverless, dedicated, or standard</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Sinks</div><div class="b3-v-char-val">Delta tables (append, merge)</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Best For</div><div class="b3-v-char-val">ETL, analytics, cost-sensitive pipelines</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tab 2: Real-Time Mode -->
# MAGIC <div class="b3-v-panel b3-v-p2">
# MAGIC   <div class="b3-v-metric" style="background:linear-gradient(135deg,#8a6200,#E5A100);">
# MAGIC     <div class="b3-v-metric-val">Kafka Sources &#x279C; Kafka Sinks</div>
# MAGIC     <div class="b3-v-metric-label">Continuous event-by-event processing, no micro-batching</div>
# MAGIC   </div>
# MAGIC   <div class="b3-v-flow">
# MAGIC     <div class="b3-v-node b3-v-src">
# MAGIC       <div class="b3-v-node-icon" style="background:#E5A100;">&#x279C;</div>
# MAGIC       <div class="b3-v-node-title">Supported Sources</div>
# MAGIC       <div style="font-size:14pt;color:#555;">Subset of connectors only</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Apache Kafka</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Event Hubs (Kafka)</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">Kinesis (EFO)</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-src">AWS MSK</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b3-v-arrow">&#x279C;</div>
# MAGIC     <div class="b3-v-node b3-v-proc b3-v-proc-rt" style="border-color:#E5A100;">
# MAGIC       <div class="b3-v-node-icon" style="background:rgba(255,255,255,0.2);border:2px solid rgba(255,255,255,0.4);"><span style="font-size:16pt;">RT</span></div>
# MAGIC       <div class="b3-v-node-title">Real-Time Mode</div>
# MAGIC       <div style="font-size:14pt;color:rgba(255,255,255,0.85);">Processes each event as it arrives</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">5 ms Latency</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">Update Mode</span>
# MAGIC         <span class="b3-v-pill b3-v-pill-proc">No Serverless</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b3-v-arrow">&#x279C;</div>
# MAGIC     <div class="b3-v-node b3-v-out" style="border-color:#E5A100;">
# MAGIC       <div class="b3-v-node-icon" style="background:#E5A100;">&#x279C;</div>
# MAGIC       <div class="b3-v-node-title">Kafka Sink</div>
# MAGIC       <div style="font-size:14pt;color:#555;">Kafka, Event Hubs, or forEachWriter</div>
# MAGIC       <div class="b3-v-pills">
# MAGIC         <span class="b3-v-pill" style="background:rgba(229,161,0,0.12);color:#8a6200;">No Delta Sink</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b3-v-chars">
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Latency</div><div class="b3-v-char-val">As low as 5 ms end-to-end</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Compute</div><div class="b3-v-char-val">Dedicated or standard only</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Sinks</div><div class="b3-v-char-val">Kafka, Event Hubs, forEachWriter</div></div>
# MAGIC     <div class="b3-v-char"><div class="b3-v-char-title">Best For</div><div class="b3-v-char-val">Fraud detection, real-time personalization</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- SDP callout -->
# MAGIC <div class="b3-v-sdp"><strong>Lakeflow Declarative Pipelines</strong> extends Structured Streaming with declarative SQL/Python, automatic orchestration, and incremental processing. LDP powers both streaming tables and materialized views (Section D). Real-time mode in LDP is in Public Preview as of May 2026.</div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Choosing an Ingestion Method</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>LakeFlow Connect:</strong> use when ingesting from enterprise SaaS apps (Salesforce, Workday) or databases (SQL Server, MySQL) where you want fully managed, zero-code ingestion.</li>
# MAGIC           <li><strong>Auto Loader:</strong> use when ingesting files from cloud object storage (S3, ADLS, GCS). Ideal for landing zones, log files, and data lake patterns.</li>
# MAGIC           <li><strong>Structured Streaming:</strong> use when consuming from message buses (Kafka, Kinesis, Pub/Sub) for event-driven architectures.</li>
# MAGIC           <li><strong>Lakehouse Federation:</strong> use when you want to query external sources without moving data (ad hoc reporting, proof-of-concept).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Micro-Batch vs. Real-Time Mode</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Micro-batch (default):</strong> streaming tables process on <strong>REFRESH</strong> (manual, scheduled, or triggered). They are not continuously running streams. The serverless pipeline starts, processes new data, and shuts down. Supports all sources (Kafka, Kinesis, Event Hubs, Pub/Sub, Pulsar) and writes to Delta tables.</li>
# MAGIC           <li><strong>Real-time mode (GA):</strong> processes each event as it arrives with end-to-end latency as low as 5 ms. Supports only Kafka-compatible sources (Kafka, Event Hubs via Kafka connector, Kinesis EFO, AWS MSK). Sinks are Kafka, Event Hubs, or forEachWriter. <strong>Does not write to Delta tables.</strong> Requires dedicated or standard compute (no serverless). Uses update output mode only.</li>
# MAGIC           <li><strong>Real-time mode in LDP (Public Preview May 2026):</strong> extends RTM to Lakeflow Declarative Pipelines using the <code>update_flow</code> decorator. Supports stateful aggregations without requiring a watermark.</li>
# MAGIC           <li>File ingestion streaming tables do <strong>not</strong> require watermarks. Watermarks are only needed for stateful operations like windowed aggregations or stream-stream joins.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/navy-federal/lakeflow-declarative-pipelines" style="color: #2574B5;">Navy Federal Credit Union</a> processed approximately 9 billion application events over 9 months of continuous streaming with near-zero maintenance. Proof of concept in 1 week, production deployment in 3 weeks. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Orchestration with Lakeflow Jobs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Why built-in Orchestration Matters
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Lakeflow Jobs</strong> (formerly Databricks Lakeflow Jobs) is the built-in, fully managed orchestration service for data, analytics, and AI workloads on the Data Intelligence Platform. Unlike external orchestrators such as Airflow, Jenkins, or AWS Step Functions, Lakeflow Jobs requires no separate infrastructure, offers a 99.95% uptime SLA, and charges no orchestration surcharge: you pay only for compute used by tasks.</p>
# MAGIC
# MAGIC <!-- ── Visual: before-after-split (External vs Lakeflow Jobs) ── -->
# MAGIC <style>
# MAGIC .c1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC /* Before/After panels */
# MAGIC .c1-v-panels { display: flex; border-radius: 10px; overflow: hidden; border: 2px solid #DCE0E2; }
# MAGIC .c1-v-panel { flex: 1; }
# MAGIC .c1-v-panel-before { background: #fff; border-right: 3px solid #DCE0E2; position: relative; }
# MAGIC .c1-v-panel-after { background: #fff; }
# MAGIC .c1-v-panel-hdr { padding: 12px 16px; font-size: 15pt; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 8px; }
# MAGIC .c1-v-hdr-before { background: #98102A; }
# MAGIC .c1-v-hdr-after { background: #00A972; }
# MAGIC /* Dimension rows */
# MAGIC .c1-v-row { display: flex; border-bottom: 1px solid #EEEDE9; }
# MAGIC .c1-v-row:last-child { border-bottom: none; }
# MAGIC .c1-v-dim { width: 130px; flex-shrink: 0; white-space: nowrap; padding: 10px 12px; font-size: 14pt; font-weight: 700; color: #5A6F77; background: #F9F7F4; display: flex; align-items: center; border-right: 1px solid #EEEDE9; }
# MAGIC .c1-v-val { flex: 1; padding: 10px 14px; font-size: 14pt; color: #333; }
# MAGIC .c1-v-panel-before .c1-v-val { color: #6b4a47; }
# MAGIC .c1-v-panel-after .c1-v-val { color: #2d5a47; }
# MAGIC /* Arrow divider */
# MAGIC .c1-v-divider { position: absolute; right: -24px; top: 50%; transform: translateY(-50%); background: #1B3139; color: #fff; width: 46px; height: 46px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20pt; font-weight: 700; z-index: 2; box-shadow: 0 2px 8px rgba(27,49,57,0.2); }
# MAGIC /* Metrics bar */
# MAGIC .c1-v-metrics { display: flex; gap: 0; border-radius: 10px; overflow: hidden; border: 2px solid #1B5162; margin-top: 14px; }
# MAGIC .c1-v-metric { flex: 1; padding: 12px; text-align: center; background: linear-gradient(135deg, #1B3139, #1B5162); color: #fff; border-right: 1px solid rgba(255,255,255,0.15); }
# MAGIC .c1-v-metric:last-child { border-right: none; }
# MAGIC .c1-v-metric-val { font-size: 18pt; font-weight: 800; }
# MAGIC .c1-v-metric-label { font-size: 14pt; color: #a8c9d6; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="c1-v-wrap">
# MAGIC   <div class="c1-v-panels">
# MAGIC     <!-- Before: External Orchestrators -->
# MAGIC     <div class="c1-v-panel c1-v-panel-before">
# MAGIC       <div class="c1-v-panel-hdr c1-v-hdr-before">&#x2717; External Orchestrators</div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Reliability</div><div class="c1-v-val">Difficult to patch; single point of failure</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Complexity</div><div class="c1-v-val">Steep learning curve; code-only Python DAGs</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Cost</div><div class="c1-v-val">Separate infrastructure and operational overhead</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Observability</div><div class="c1-v-val">Siloed from the data platform; no lineage</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Streaming</div><div class="c1-v-val">Batch only</div></div>
# MAGIC       <div class="c1-v-divider">&#x279C;</div>
# MAGIC     </div>
# MAGIC     <!-- After: Lakeflow Jobs -->
# MAGIC     <div class="c1-v-panel c1-v-panel-after">
# MAGIC       <div class="c1-v-panel-hdr c1-v-hdr-after">&#x2714; Lakeflow Jobs</div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Reliability</div><div class="c1-v-val">Fully managed; 99.95% uptime SLA</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Complexity</div><div class="c1-v-val">UI, CLI, IDE, or Declarative Automation Bundles (YAML)</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Cost</div><div class="c1-v-val">No orchestration surcharge; pay only for compute</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Observability</div><div class="c1-v-val">UC lineage, system tables, AI error diagnosis</div></div>
# MAGIC       <div class="c1-v-row"><div class="c1-v-dim">Streaming</div><div class="c1-v-val">Batch and streaming in one platform</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Key metrics -->
# MAGIC   <div class="c1-v-metrics">
# MAGIC     <div class="c1-v-metric"><div class="c1-v-metric-val">99.95%</div><div class="c1-v-metric-label">Uptime SLA</div></div>
# MAGIC     <div class="c1-v-metric"><div class="c1-v-metric-val">14,600+</div><div class="c1-v-metric-label">Customers</div></div>
# MAGIC     <div class="c1-v-metric"><div class="c1-v-metric-val">100M+</div><div class="c1-v-metric-label">Jobs per week</div></div>
# MAGIC     <div class="c1-v-metric"><div class="c1-v-metric-val">$0</div><div class="c1-v-metric-label">Orchestration surcharge</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">What a DAG Is</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>A <strong>Directed Acyclic Graph (DAG)</strong> is a graph where edges have direction (task A must complete before task B) and no cycles exist. In Lakeflow Jobs, tasks are nodes and dependencies are directed edges.</li>
# MAGIC           <li>The DAG determines execution order while <strong>maximizing parallelization</strong>: independent tasks run simultaneously. Think of it like a project management Gantt chart where tasks on separate tracks run in parallel.</li>
# MAGIC           <li>Job success is determined by <strong>leaf tasks</strong> (tasks with no downstream dependencies).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Declarative Automation Bundles and Authoring</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Declarative Automation Bundles (DABs):</strong> define jobs, pipelines, and infrastructure as YAML files for version-controlled, atomic deployment across dev/staging/production. DABs replace the need for Terraform or custom CI/CD scripts for many use cases.</li>
# MAGIC           <li><strong>Multiple authoring modes:</strong> UI (drag-and-drop task graph), CLI (<code style="font-size:14pt;">databricks bundle deploy</code>), IDE (VS Code with Databricks extension), or YAML-first with DABs. This is the key complexity differentiator over external orchestrators that require code-only Python DAGs.</li>
# MAGIC           <li><strong>Scale:</strong> 14,600+ customers, 187,000 weekly users, and 100M+ jobs run per week on the platform.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Third-Party Integrations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Azure Data Factory:</strong> built-in ADF Activities or Lakeflow Jobs API for hybrid orchestration.</li>
# MAGIC           <li><strong>Apache Airflow:</strong> DatabricksRunNowOperator for triggering Lakeflow Jobs from Airflow. YipitData achieved 60% lower database costs and 90% reduction in processing time after migrating.</li>
# MAGIC           <li><strong>dbt:</strong> dbt-databricks adapter for declarative transformations in Jobs.</li>
# MAGIC           <li><strong>Terraform:</strong> Databricks Terraform provider with a Jobs Resource for infrastructure-as-code deployment.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/coxautomotive/lakeflow-jobs" style="color: #2574B5;">Cox Automotive</a> orchestrates approximately 300 jobs with Lakeflow Jobs, roughly 120 scheduled regularly, processing about 720GB per day across diverse source types. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Building Blocks: Task Types, Control Flows, and Triggers
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Lakeflow Jobs is built on three foundational building blocks: <strong>Jobs</strong> (the workflow container with a DAG, triggers, and parameters), <strong>Tasks</strong> (individual units of work), and <strong>Triggers</strong> (mechanisms that initiate runs). Together, these blocks support everything from simple single-task notebooks to complex multi-hundred-task pipelines with conditional branching and looping.</p>
# MAGIC
# MAGIC <!-- ── Visual: c2-building-blocks-grid ── -->
# MAGIC <style>
# MAGIC .c2-bb-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .c2-bb-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr 1fr;
# MAGIC   gap: 16px;
# MAGIC }
# MAGIC .c2-bb-card {
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .c2-bb-card:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .c2-bb-card-header {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   padding: 10px 14px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .c2-bb-card-body {
# MAGIC   padding: 12px 14px;
# MAGIC   background: #F9F7F4;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC }
# MAGIC .c2-bb-tag {
# MAGIC   display: inline-block;
# MAGIC   background: #fff;
# MAGIC   border: 1px solid #EEEDE9;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 4px 10px;
# MAGIC   margin: 3px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   transition: background 0.2s, transform 0.15s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .c2-bb-tag:hover {
# MAGIC   background: #e3ecef;
# MAGIC   transform: scale(1.05);
# MAGIC }
# MAGIC </style>
# MAGIC <div class="c2-bb-wrapper">
# MAGIC   <div class="c2-bb-grid">
# MAGIC     <div class="c2-bb-card">
# MAGIC       <div class="c2-bb-card-header">Task Types</div>
# MAGIC       <div class="c2-bb-card-body">
# MAGIC         <span class="c2-bb-tag">Notebook</span>
# MAGIC         <span class="c2-bb-tag">Python Script</span>
# MAGIC         <span class="c2-bb-tag">SQL File/Query</span>
# MAGIC         <span class="c2-bb-tag">Pipeline (SDP)</span>
# MAGIC         <span class="c2-bb-tag">dbt</span>
# MAGIC         <span class="c2-bb-tag">AI/BI Dashboard</span>
# MAGIC         <span class="c2-bb-tag">Power BI</span>
# MAGIC         <span class="c2-bb-tag">Python Wheel</span>
# MAGIC         <span class="c2-bb-tag">Java JAR</span>
# MAGIC         <span class="c2-bb-tag">Spark Submit</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="c2-bb-card">
# MAGIC       <div class="c2-bb-card-header">Control Flows</div>
# MAGIC       <div class="c2-bb-card-body">
# MAGIC         <span class="c2-bb-tag">Sequential</span>
# MAGIC         <span class="c2-bb-tag">Parallel</span>
# MAGIC         <span class="c2-bb-tag">If/Else Conditionals</span>
# MAGIC         <span class="c2-bb-tag">For Each Loop</span>
# MAGIC         <span class="c2-bb-tag">Run Job (Modular)</span>
# MAGIC         <br/><br/>
# MAGIC         <strong style="font-size: 14pt;">6 Run-If Conditions:</strong><br/>
# MAGIC         All Succeeded, At Least 1 Succeeded, None Failed, All Done, At Least 1 Failed, All Failed
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="c2-bb-card">
# MAGIC       <div class="c2-bb-card-header">Triggers</div>
# MAGIC       <div class="c2-bb-card-body">
# MAGIC         <span class="c2-bb-tag">Manual</span>
# MAGIC         <span class="c2-bb-tag">Scheduled (Cron)</span>
# MAGIC         <span class="c2-bb-tag">API Trigger</span>
# MAGIC         <span class="c2-bb-tag">File Arrival</span>
# MAGIC         <span class="c2-bb-tag">Table Update</span>
# MAGIC         <span class="c2-bb-tag">Continuous</span>
# MAGIC         <br/><br/>
# MAGIC         <strong style="font-size: 14pt;">Event-Driven:</strong> File arrival and table update triggers fire jobs when data changes, not on a schedule.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Data Triggers (Event-Driven)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>File arrival trigger:</strong> automatically initiates job runs when new files appear in Unity Catalog external locations or volumes. Best-effort check every minute. Only new files trigger runs; overwriting an existing file with the same name does not trigger.</li>
# MAGIC           <li><strong>Table update trigger:</strong> fires when specified Delta tables are updated. Supports UC Delta/Iceberg tables, streaming tables, and materialized views. Up to 10 tables per trigger with "any updated" or "all updated" logic.</li>
# MAGIC           <li>Moving from cron-based scheduling to event-driven triggers is like going from checking your mailbox every hour to getting a push notification when a letter arrives.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Task Type Details</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SQL tasks</strong> run exclusively on SQL warehouses (serverless or pro), not on clusters. They support parameterized queries using <code>:parameter_name</code> syntax.</li>
# MAGIC           <li><strong>Pipeline tasks</strong> run Lakeflow Spark Declarative Pipelines. Pipelines are faster and cheaper with automatic incremental processing.</li>
# MAGIC           <li><strong>For Each loops</strong> run a nested task in a loop with configurable concurrency. Use <code>{{tasks.&lt;name&gt;.output.rows}}</code> to iterate over SQL query results (max 1,000 rows, 48 KB).</li>
# MAGIC           <li><strong>AI/BI Dashboard and Power BI tasks</strong> refresh dashboards as part of a pipeline, keeping BI outputs in sync with upstream data processing without manual intervention.</li>
# MAGIC           <li><strong>Spark Submit tasks</strong> run arbitrary Spark applications (JAR or Python) for workloads that need full Spark API control outside of notebooks.</li>
# MAGIC           <li><strong>Disabling tasks:</strong> individual tasks can now be disabled at run time without removing them from the job definition (GA April 2026), preserving configuration while skipping specific steps during debugging or partial runs.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Compute Options</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Serverless Compute</strong> (recommended default): auto-scales, zero infrastructure management, two modes. Standard mode is cost-optimized (4-6 min startup). Performance Optimized mode is faster but consumes more DBUs.</li>
# MAGIC           <li><strong>Job Clusters:</strong> provisioned on-demand and terminated at job end. Up to 50% cheaper than long-running clusters. Best for production pipelines.</li>
# MAGIC           <li><strong>Interactive Clusters:</strong> multi-user, best for ad hoc analysis and prototyping. Not recommended for production.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Monitoring and Observability
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Lakeflow Jobs provides a comprehensive monitoring surface with real-time metrics, drill-down troubleshooting, and system tables for long-term analysis. The monitoring experience includes <strong>Graph View</strong> (DAG visualization), <strong>Timeline View</strong> (Gantt-style execution analysis), and <strong>Matrix View</strong> (historical run grid with color-coded status).</p>
# MAGIC
# MAGIC <!-- ── Visual: tabs-css (3 monitoring views + debugging flow) ── -->
# MAGIC <style>
# MAGIC .c3-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .c3-v-radio { display: none; }
# MAGIC /* Tabs */
# MAGIC .c3-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .c3-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .c3-v-tab-title { font-size: 15pt; font-weight: 700; display: block; }
# MAGIC .c3-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .c3-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC #c3-t1:checked ~ .c3-v-tabs .c3-v-tab1 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #c3-t1:checked ~ .c3-v-tabs .c3-v-tab1 .c3-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c3-t1:checked ~ .c3-v-p1 { display: block; border-color: #1B5162; }
# MAGIC #c3-t2:checked ~ .c3-v-tabs .c3-v-tab2 { background: #2574B5; color: #fff; border-color: #2574B5; }
# MAGIC #c3-t2:checked ~ .c3-v-tabs .c3-v-tab2 .c3-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c3-t2:checked ~ .c3-v-p2 { display: block; border-color: #2574B5; }
# MAGIC #c3-t3:checked ~ .c3-v-tabs .c3-v-tab3 { background: #618794; color: #fff; border-color: #618794; }
# MAGIC #c3-t3:checked ~ .c3-v-tabs .c3-v-tab3 .c3-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c3-t3:checked ~ .c3-v-p3 { display: block; border-color: #618794; }
# MAGIC /* Panel content */
# MAGIC .c3-v-desc { font-size: 14pt; color: #333; line-height: 1.5; margin-bottom: 14px; }
# MAGIC .c3-v-features { display: flex; gap: 10px; flex-wrap: wrap; }
# MAGIC .c3-v-feat { flex: 1; min-width: 180px; border-radius: 8px; padding: 12px; border: 1.5px solid #DCE0E2; background: #F9F7F4; text-align: center; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .c3-v-feat:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.10); }
# MAGIC .c3-v-feat-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .c3-v-feat-desc { font-size: 14pt; color: #555; }
# MAGIC /* Debug flow */
# MAGIC .c3-v-debug { display: flex; align-items: stretch; gap: 0; margin-top: 16px; }
# MAGIC .c3-v-dstep { flex: 1; border-radius: 10px; padding: 12px 10px; text-align: center; border: 1.5px solid #DCE0E2; background: #fff; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .c3-v-dstep:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.10); }
# MAGIC .c3-v-dstep-num { display: inline-flex; align-items: center; justify-content: center; width: 32px; height: 32px; border-radius: 50%; font-size: 14pt; font-weight: 700; color: #fff; background: #1B5162; margin-bottom: 4px; }
# MAGIC .c3-v-dstep-title { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .c3-v-dstep-desc { font-size: 14pt; color: #555; }
# MAGIC .c3-v-darrow { display: flex; align-items: center; font-size: 22pt; color: #94b3be; padding: 0 4px; flex-shrink: 0; }
# MAGIC .c3-v-debug-label { text-align: center; font-size: 14pt; font-weight: 700; color: #5A6F77; text-transform: uppercase; letter-spacing: 0.8px; margin-top: 16px; margin-bottom: 6px; }
# MAGIC /* System tables bar */
# MAGIC .c3-v-systbl { display: flex; gap: 0; border-radius: 8px; overflow: hidden; border: 1.5px solid #DCE0E2; margin-top: 14px; }
# MAGIC .c3-v-systbl-hdr { padding: 10px 14px; font-size: 14pt; font-weight: 700; color: #fff; background: #1B3139; white-space: nowrap; display: flex; align-items: center; }
# MAGIC .c3-v-systbl-items { flex: 1; display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 12px; align-items: center; background: #F9F7F4; }
# MAGIC .c3-v-systbl-pill { font-size: 14pt; font-weight: 600; padding: 3px 10px; border-radius: 10px; background: #fff; border: 1px solid #DCE0E2; color: #1B3139; font-family: 'Menlo','Consolas',monospace; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="c3-v-wrap">
# MAGIC <input type="radio" name="c3tabs" id="c3-t1" class="c3-v-radio" checked>
# MAGIC <input type="radio" name="c3tabs" id="c3-t2" class="c3-v-radio">
# MAGIC <input type="radio" name="c3tabs" id="c3-t3" class="c3-v-radio">
# MAGIC
# MAGIC <div class="c3-v-tabs">
# MAGIC   <label for="c3-t1" class="c3-v-tab c3-v-tab1"><span class="c3-v-tab-title">Graph View</span><span class="c3-v-tab-sub">DAG visualization</span></label>
# MAGIC   <label for="c3-t2" class="c3-v-tab c3-v-tab2"><span class="c3-v-tab-title">Timeline View</span><span class="c3-v-tab-sub">Gantt-style execution</span></label>
# MAGIC   <label for="c3-t3" class="c3-v-tab c3-v-tab3"><span class="c3-v-tab-title">Matrix View</span><span class="c3-v-tab-sub">Historical run grid</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Graph View -->
# MAGIC <div class="c3-v-panel c3-v-p1">
# MAGIC   <div class="c3-v-desc">Click task nodes to see run details, source code, cluster info, and metrics. Failed tasks are highlighted in red for quick identification.</div>
# MAGIC   <div class="c3-v-features">
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Task Status</div><div class="c3-v-feat-desc">Color-coded nodes: green (success), red (failed), grey (pending)</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Drill-Down</div><div class="c3-v-feat-desc">Click any task for logs, error messages, and AI diagnosis</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Repair Run</div><div class="c3-v-feat-desc">Re-run only failed tasks and their dependents</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Timeline View -->
# MAGIC <div class="c3-v-panel c3-v-p2">
# MAGIC   <div class="c3-v-desc">Gantt-style chart showing task dependencies, durations, and the critical path. Identifies bottleneck tasks and scheduling delays.</div>
# MAGIC   <div class="c3-v-features">
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Critical Path</div><div class="c3-v-feat-desc">Longest chain of dependent tasks determining total run time</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Parallelization</div><div class="c3-v-feat-desc">See which tasks ran concurrently vs. sequentially</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Duration Tracking</div><div class="c3-v-feat-desc">Compare task durations across runs to spot regressions</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Matrix View -->
# MAGIC <div class="c3-v-panel c3-v-p3">
# MAGIC   <div class="c3-v-desc">Grid of runs (rows) by tasks (columns) with color-coded cells. Bar heights indicate run duration. Quickly spot patterns in failures.</div>
# MAGIC   <div class="c3-v-features">
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Color Coding</div><div class="c3-v-feat-desc">Green=success, red=failure, pink=skipped, yellow=retry, grey=pending</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">Pattern Detection</div><div class="c3-v-feat-desc">Spot recurring failures across runs at a glance</div></div>
# MAGIC     <div class="c3-v-feat"><div class="c3-v-feat-title">60-Day History</div><div class="c3-v-feat-desc">UI retains 60 days; system tables extend retention</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Debugging workflow -->
# MAGIC <div class="c3-v-debug-label">Debugging Workflow</div>
# MAGIC <div class="c3-v-debug">
# MAGIC   <div class="c3-v-dstep"><div class="c3-v-dstep-num">1</div><div class="c3-v-dstep-title">Identify</div><div class="c3-v-dstep-desc">Red cells in Matrix View</div></div>
# MAGIC   <div class="c3-v-darrow">&#x279C;</div>
# MAGIC   <div class="c3-v-dstep"><div class="c3-v-dstep-num">2</div><div class="c3-v-dstep-title">Diagnose</div><div class="c3-v-dstep-desc">Click node in Graph View; AI error analysis</div></div>
# MAGIC   <div class="c3-v-darrow">&#x279C;</div>
# MAGIC   <div class="c3-v-dstep"><div class="c3-v-dstep-num">3</div><div class="c3-v-dstep-title">Fix</div><div class="c3-v-dstep-desc">Edit SQL, update params, check warehouse</div></div>
# MAGIC   <div class="c3-v-darrow">&#x279C;</div>
# MAGIC   <div class="c3-v-dstep"><div class="c3-v-dstep-num">4</div><div class="c3-v-dstep-title">Repair</div><div class="c3-v-dstep-desc">Re-run only failed tasks</div></div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- System tables -->
# MAGIC <div class="c3-v-systbl">
# MAGIC   <div class="c3-v-systbl-hdr">System Tables</div>
# MAGIC   <div class="c3-v-systbl-items">
# MAGIC     <span class="c3-v-systbl-pill">system.lakeflow.jobs</span>
# MAGIC     <span class="c3-v-systbl-pill">system.lakeflow.job_tasks</span>
# MAGIC     <span class="c3-v-systbl-pill">system.lakeflow.job_run_timeline</span>
# MAGIC     <span class="c3-v-systbl-pill">system.lakeflow.job_task_run_timeline</span>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Three Monitoring Views</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Graph View:</strong> click task nodes to see run details, source code, cluster info, and metrics. Failed tasks are highlighted for quick identification.</li>
# MAGIC           <li><strong>Timeline View:</strong> Gantt-style chart showing task dependencies, durations, and the critical path. Helps identify bottleneck tasks and scheduling delays.</li>
# MAGIC           <li><strong>Matrix View:</strong> a grid of runs by tasks with color-coded cells (green=success, red=failure, pink=skipped, yellow=retry, grey=pending). Bar heights indicate run duration.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Debugging Workflow</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Identify:</strong> find the failed run in the Matrix View (scan for red cells).</li>
# MAGIC           <li><strong>Diagnose:</strong> click the red task node in Graph View. Read the error message. Use "Diagnose Error" for AI-powered analysis.</li>
# MAGIC           <li><strong>Fix:</strong> address the root cause (edit SQL file, update parameters, confirm warehouse status).</li>
# MAGIC           <li><strong>Repair:</strong> use Repair Run to re-run only failed tasks. Successful tasks are not re-executed. The Repair Run feature is like a save point in a video game: you resume from the last checkpoint, not the beginning.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">System Tables and Retention</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>UI retention:</strong> the Jobs UI retains run history for 60 days. For longer retention, use system tables to build custom dashboards and alerts.</li>
# MAGIC           <li><strong>system.lakeflow.jobs:</strong> job definitions, owners, schedules, and configuration metadata.</li>
# MAGIC           <li><strong>system.lakeflow.job_tasks:</strong> task definitions within each job, including task type, compute config, and dependencies.</li>
# MAGIC           <li><strong>system.lakeflow.job_run_timeline:</strong> run-level execution data: start/end times, result states, durations.</li>
# MAGIC           <li><strong>system.lakeflow.job_task_run_timeline:</strong> task-level execution data: per-task start/end, retries, cluster IDs.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Cost Monitoring</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Join <code>system.lakeflow.job_run_timeline</code> with <code>system.billing.usage</code> to track job costs by run, task, and SKU.</li>
# MAGIC           <li>Pricing for Lakeflow Jobs is tied to the compute used to run tasks, with no additional orchestration surcharge.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Transformation with Declarative Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Lakeflow Declarative Pipelines Overview
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Lakeflow Declarative Pipelines</strong> (powered by Spark Declarative Pipelines, or SDP) is the best way to do ETL on the lakehouse. It is a declarative framework where you define tables and transformations in SQL or Python, and the platform handles DAG construction, retries, scaling, incremental processing, and data quality automatically.</p>
# MAGIC
# MAGIC <!-- ── Visual: four-card-grid (LDP 4 pillars + enhancements) ── -->
# MAGIC <style>
# MAGIC .d1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .d1-v-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 14px; }
# MAGIC .d1-v-card { border-radius: 10px; padding: 16px; background: #fff; border: 2px solid #DCE0E2; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .d1-v-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .d1-v-card-accent { height: 5px; border-radius: 3px; margin-bottom: 10px; background: #1B5162; }
# MAGIC .d1-v-card-title { font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 6px; }
# MAGIC .d1-v-card-desc { font-size: 14pt; color: #555; line-height: 1.4; }
# MAGIC .d1-v-card-pills { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 8px; }
# MAGIC .d1-v-pill { font-size: 14pt; font-weight: 600; padding: 3px 10px; border-radius: 10px; background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC /* Enhancements bar */
# MAGIC .d1-v-enh { display: flex; gap: 10px; flex-wrap: wrap; }
# MAGIC .d1-v-enh-card { flex: 1; min-width: 200px; border-radius: 10px; padding: 12px 14px; background: #F9F7F4; border: 1.5px solid #DCE0E2; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .d1-v-enh-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.10); }
# MAGIC .d1-v-enh-badge { display: inline-block; font-size: 14pt; font-weight: 700; padding: 2px 8px; border-radius: 8px; color: #fff; margin-bottom: 4px; }
# MAGIC .d1-v-badge-ga { background: #00A972; }
# MAGIC .d1-v-badge-pp { background: #E5A100; }
# MAGIC .d1-v-badge-beta { background: #618794; }
# MAGIC .d1-v-enh-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 2px; }
# MAGIC .d1-v-enh-desc { font-size: 14pt; color: #555; }
# MAGIC .d1-v-enh-label { font-size: 14pt; font-weight: 700; color: #5A6F77; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="d1-v-wrap">
# MAGIC   <!-- 4 Pillars -->
# MAGIC   <div class="d1-v-grid">
# MAGIC     <div class="d1-v-card">
# MAGIC       <div class="d1-v-card-accent"></div>
# MAGIC       <div class="d1-v-card-title">Accelerate ETL Development</div>
# MAGIC       <div class="d1-v-card-desc">Declare transformations in SQL or Python. SDP compiles into an optimized DAG with automatic retries and change management.</div>
# MAGIC       <div class="d1-v-card-pills"><span class="d1-v-pill">SQL + Python</span><span class="d1-v-pill">Auto DAG</span><span class="d1-v-pill">No Plumbing</span></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-card">
# MAGIC       <div class="d1-v-card-accent" style="background:#2574B5;"></div>
# MAGIC       <div class="d1-v-card-title">Automatically Manage Infrastructure</div>
# MAGIC       <div class="d1-v-card-desc">Auto-scaling, checkpointing, performance tuning, and serverless compute are built in. Engineers focus on logic, not clusters.</div>
# MAGIC       <div class="d1-v-card-pills"><span class="d1-v-pill">Serverless</span><span class="d1-v-pill">Auto-Scale</span><span class="d1-v-pill">Checkpoint</span></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-card">
# MAGIC       <div class="d1-v-card-accent" style="background:#00A972;"></div>
# MAGIC       <div class="d1-v-card-title">Ensure High Data Quality</div>
# MAGIC       <div class="d1-v-card-desc">Expectations provide declarative quality rules with configurable violation policies: warn, drop, or fail. Quality scorecards and lineage graphs.</div>
# MAGIC       <div class="d1-v-card-pills"><span class="d1-v-pill">Expectations</span><span class="d1-v-pill">Warn / Drop / Fail</span><span class="d1-v-pill">Scorecards</span></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-card">
# MAGIC       <div class="d1-v-card-accent" style="background:#E5A100;"></div>
# MAGIC       <div class="d1-v-card-title">Unify Batch and Streaming</div>
# MAGIC       <div class="d1-v-card-desc">One API for both. Streaming tables process each row once (append). Materialized views maintain full computed results (batch).</div>
# MAGIC       <div class="d1-v-card-pills"><span class="d1-v-pill">Streaming Tables</span><span class="d1-v-pill">Materialized Views</span><span class="d1-v-pill">One API</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Recent enhancements -->
# MAGIC   <div class="d1-v-enh-label">Recent Enhancements (2026)</div>
# MAGIC   <div class="d1-v-enh">
# MAGIC     <div class="d1-v-enh-card">
# MAGIC       <span class="d1-v-enh-badge d1-v-badge-ga">GA May 2026</span>
# MAGIC       <div class="d1-v-enh-title">LDP Sinks</div>
# MAGIC       <div class="d1-v-enh-desc">Write to Kafka, Event Hubs, and custom Python sinks beyond Delta tables</div>
# MAGIC       <div style="margin-top:6px;font-size:14pt;"><a href="https://docs.databricks.com/aws/en/ldp/sinks" style="color:#2574B5;text-decoration:none;font-weight:600;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/sinks" style="color:#2574B5;text-decoration:none;font-weight:600;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/sinks" style="color:#2574B5;text-decoration:none;font-weight:600;">GCP</a></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-enh-card">
# MAGIC       <span class="d1-v-enh-badge d1-v-badge-pp">Preview May 2026</span>
# MAGIC       <div class="d1-v-enh-title">Real-Time Mode in LDP</div>
# MAGIC       <div class="d1-v-enh-desc">5 ms latency via <code style="font-size:14pt;">update_flow</code> decorator for operational streaming</div>
# MAGIC       <div style="margin-top:6px;font-size:14pt;"><a href="https://docs.databricks.com/aws/en/ldp/pipeline-mode" style="color:#2574B5;text-decoration:none;font-weight:600;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/pipeline-mode" style="color:#2574B5;text-decoration:none;font-weight:600;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/pipeline-mode" style="color:#2574B5;text-decoration:none;font-weight:600;">GCP</a></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-enh-card">
# MAGIC       <span class="d1-v-enh-badge d1-v-badge-beta">Beta May 2026</span>
# MAGIC       <div class="d1-v-enh-title">Standalone Pipelines</div>
# MAGIC       <div class="d1-v-enh-desc">Create MVs and STs from serverless notebooks without a full pipeline definition</div>
# MAGIC       <div style="margin-top:6px;font-size:14pt;"><a href="https://docs.databricks.com/aws/en/ldp/dbsql/dbsql-for-ldp" style="color:#2574B5;text-decoration:none;font-weight:600;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/dbsql/dbsql-for-ldp" style="color:#2574B5;text-decoration:none;font-weight:600;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/dbsql/dbsql-for-ldp" style="color:#2574B5;text-decoration:none;font-weight:600;">GCP</a></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">What SDP Handles Automatically</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Dependency inference:</strong> SDP analyzes table/view references in your SQL and Python code to construct the processing graph automatically. You can define tables in any order across any number of files.</li>
# MAGIC           <li><strong>Incremental processing:</strong> streaming tables process new data only. Materialized views use the Enzyme engine to process only changed data when possible.</li>
# MAGIC           <li><strong>Serverless compute:</strong> all SDP pipelines run on serverless compute by default. The SQL warehouse only coordinates; the serverless pipeline does the actual processing.</li>
# MAGIC           <li><strong>LDP Sinks (GA May 2026):</strong> pipelines can now write to external destinations such as Kafka, Event Hubs, and custom Python sinks, extending Declarative Pipelines beyond Delta table outputs.</li>
# MAGIC           <li><strong>Real-Time Mode (Public Preview May 2026):</strong> a new <code>update_flow</code> decorator enables end-to-end latency as low as five milliseconds for operational streaming workloads. This positions Declarative Pipelines for use cases that previously required custom Structured Streaming code.</li>
# MAGIC           <li><strong>Standalone Pipelines (Beta May 2026):</strong> create materialized views and streaming tables from serverless notebooks without a full pipeline definition, simplifying ad hoc transformation workflows.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Key Terminology</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Streaming Table (ST):</strong> a Unity Catalog managed Delta table with streaming semantics. Each row is processed exactly once. Best for ingestion and low-latency streaming transforms.</li>
# MAGIC           <li><strong>Materialized View (MV):</strong> a Unity Catalog managed Delta table that physically stores pre-computed query results. Uses batch semantics. Best for aggregations, joins, and BI dashboards.</li>
# MAGIC           <li><strong>Flow:</strong> the fundamental processing unit. Streaming flows (Append, AUTO CDC) for STs; batch flows for MVs.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/ifood/dlt" style="color: #2574B5;">iFood</a> achieved a 67% cost reduction and 70% reduction in pipeline maintenance after migrating to Declarative Pipelines, consolidating from nearly 4,000 tables to just 100. &#x25C6;</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/blog/2022/04/27/how-uplift-built-cdc-and-multiplexing-data-pipelines-with-databricks-delta-live-tables.html" style="color: #2574B5;">Uplift</a> built a 100+ table pipeline in one managed job with time and money savings using CDC and multiplexing. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Medallion Architecture with Streaming Tables and Materialized Views
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The <strong>medallion architecture</strong> organizes data into three layers: <strong>Bronze</strong> (raw ingestion), <strong>Silver</strong> (filtered, cleaned, augmented), and <strong>Gold</strong> (business-level aggregation). Lakeflow Declarative Pipelines maps naturally to this pattern using streaming tables for ingestion and materialized views for transformation.</p>
# MAGIC
# MAGIC <!-- ── Visual: d2-medallion-tabs ── -->
# MAGIC <style>
# MAGIC .d2-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .d2-v-radio { display: none; }
# MAGIC /* Flow bar (always visible) */
# MAGIC .d2-v-flowbar { display: flex; gap: 0; margin-bottom: 0; }
# MAGIC .d2-v-flabel { flex: 1; text-align: center; padding: 14px 10px; cursor: pointer; transition: all 0.15s; border: 2px solid #DCE0E2; border-bottom: none; }
# MAGIC .d2-v-flabel:first-child { border-radius: 10px 0 0 0; }
# MAGIC .d2-v-flabel:last-child { border-radius: 0 10px 0 0; }
# MAGIC .d2-v-flabel-title { font-size: 16pt; font-weight: 700; display: block; }
# MAGIC .d2-v-flabel-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .d2-v-flabel-type { font-size: 14pt; font-weight: 600; display: block; margin-top: 4px; }
# MAGIC .d2-v-arrow-sep { display: flex; align-items: center; font-size: 24pt; padding: 0 2px; color: #94b3be; }
# MAGIC /* Inactive state */
# MAGIC .d2-v-fl-bronze { background: #f5ebe0; color: #8a6320; border-color: #dcc9a8; }
# MAGIC .d2-v-fl-silver { background: #eef1f3; color: #5A6F77; border-color: #c8d0d5; }
# MAGIC .d2-v-fl-gold { background: #fff8e6; color: #8a6200; border-color: #e5d49a; }
# MAGIC /* Active states */
# MAGIC #d2-t1:checked ~ .d2-v-flowbar .d2-v-fl-bronze { background: #CD7F32; color: #fff; border-color: #CD7F32; }
# MAGIC #d2-t1:checked ~ .d2-v-flowbar .d2-v-fl-bronze .d2-v-flabel-sub { color: rgba(255,255,255,0.8); }
# MAGIC #d2-t1:checked ~ .d2-v-p1 { display: block; border-color: #CD7F32; }
# MAGIC #d2-t2:checked ~ .d2-v-flowbar .d2-v-fl-silver { background: #90A5B1; color: #fff; border-color: #90A5B1; }
# MAGIC #d2-t2:checked ~ .d2-v-flowbar .d2-v-fl-silver .d2-v-flabel-sub { color: rgba(255,255,255,0.8); }
# MAGIC #d2-t2:checked ~ .d2-v-p2 { display: block; border-color: #90A5B1; }
# MAGIC #d2-t3:checked ~ .d2-v-flowbar .d2-v-fl-gold { background: #E5A100; color: #fff; border-color: #E5A100; }
# MAGIC #d2-t3:checked ~ .d2-v-flowbar .d2-v-fl-gold .d2-v-flabel-sub { color: rgba(255,255,255,0.8); }
# MAGIC #d2-t3:checked ~ .d2-v-p3 { display: block; border-color: #E5A100; }
# MAGIC /* Panels */
# MAGIC .d2-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 20px; }
# MAGIC /* Panel content */
# MAGIC .d2-v-pcols { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .d2-v-pcol { flex: 1; min-width: 260px; }
# MAGIC .d2-v-pcol-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px; }
# MAGIC .d2-v-pcol-desc { font-size: 14pt; color: #555; line-height: 1.5; margin-bottom: 10px; }
# MAGIC .d2-v-pills { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 10px; }
# MAGIC .d2-v-pill { font-size: 14pt; font-weight: 600; padding: 3px 10px; border-radius: 10px; }
# MAGIC .d2-v-pill-b { background: rgba(205,127,50,0.12); color: #8a5a1e; }
# MAGIC .d2-v-pill-s { background: rgba(144,165,177,0.15); color: #4a6a76; }
# MAGIC .d2-v-pill-g { background: rgba(229,161,0,0.12); color: #8a6200; }
# MAGIC /* Code block */
# MAGIC .d2-v-code { background: #272822; border-radius: 6px; padding: 14px 16px; font-family: 'Menlo','Consolas',monospace; font-size: 14pt; color: #f8f8f2; line-height: 1.6; overflow-x: auto; }
# MAGIC .d2-v-kw { color: #66d9ef; font-style: italic; }
# MAGIC .d2-v-fn { color: #a6e22e; }
# MAGIC .d2-v-str { color: #e6db74; }
# MAGIC .d2-v-cmt { color: #75715e; font-style: italic; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="d2-v-wrap">
# MAGIC <input type="radio" name="d2tabs" id="d2-t1" class="d2-v-radio" checked>
# MAGIC <input type="radio" name="d2tabs" id="d2-t2" class="d2-v-radio">
# MAGIC <input type="radio" name="d2tabs" id="d2-t3" class="d2-v-radio">
# MAGIC
# MAGIC <!-- Flow bar: Bronze → Silver → Gold -->
# MAGIC <div class="d2-v-flowbar">
# MAGIC   <label for="d2-t1" class="d2-v-flabel d2-v-fl-bronze"><span class="d2-v-flabel-title">Bronze</span><span class="d2-v-flabel-sub">Raw Ingestion</span><span class="d2-v-flabel-type">Streaming Tables</span></label>
# MAGIC   <div class="d2-v-arrow-sep">&#x279C;</div>
# MAGIC   <label for="d2-t2" class="d2-v-flabel d2-v-fl-silver"><span class="d2-v-flabel-title">Silver</span><span class="d2-v-flabel-sub">Cleaned + Enriched</span><span class="d2-v-flabel-type">ST or MV</span></label>
# MAGIC   <div class="d2-v-arrow-sep">&#x279C;</div>
# MAGIC   <label for="d2-t3" class="d2-v-flabel d2-v-fl-gold"><span class="d2-v-flabel-title">Gold</span><span class="d2-v-flabel-sub">Business Aggregation</span><span class="d2-v-flabel-type">Materialized Views</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Bronze panel -->
# MAGIC <div class="d2-v-panel d2-v-p1">
# MAGIC   <div class="d2-v-pcols">
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">What happens here</div>
# MAGIC       <div class="d2-v-pcol-desc">Raw data lands exactly as received from sources. No transformations, no filtering. Each row processed exactly once (append-only).</div>
# MAGIC       <div class="d2-v-pills">
# MAGIC         <span class="d2-v-pill d2-v-pill-b">Auto Loader</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-b">Kafka / Kinesis</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-b">LakeFlow Connect</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-b">Exactly-Once</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">SQL pattern</div>
# MAGIC       <div class="d2-v-code"><span class="d2-v-cmt">-- Bronze: ingest raw files</span><br><span class="d2-v-kw">CREATE OR REFRESH STREAMING TABLE</span> <span class="d2-v-fn">raw_orders</span><br><span class="d2-v-kw">AS SELECT</span> * <span class="d2-v-kw">FROM STREAM</span> <span class="d2-v-fn">read_files</span>(<br>&nbsp;&nbsp;<span class="d2-v-str">'/volumes/landing/orders'</span>,<br>&nbsp;&nbsp;format => <span class="d2-v-str">'json'</span>);</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Silver panel -->
# MAGIC <div class="d2-v-panel d2-v-p2">
# MAGIC   <div class="d2-v-pcols">
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">What happens here</div>
# MAGIC       <div class="d2-v-pcol-desc">Data is filtered, cleaned, enriched with joins, and validated with Expectations. Use a streaming table for incremental append-only flows, or a materialized view for complex joins and recomputations.</div>
# MAGIC       <div class="d2-v-pills">
# MAGIC         <span class="d2-v-pill d2-v-pill-s">Filtering</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-s">Joins</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-s">Expectations</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-s">Deduplication</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">SQL pattern (with quality rules)</div>
# MAGIC       <div class="d2-v-code"><span class="d2-v-cmt">-- Silver: clean + validate</span><br><span class="d2-v-kw">CREATE OR REFRESH STREAMING TABLE</span> <span class="d2-v-fn">clean_orders</span> (<br>&nbsp;&nbsp;<span class="d2-v-kw">CONSTRAINT</span> valid_amt <span class="d2-v-kw">EXPECT</span> (amount > 0)<br>&nbsp;&nbsp;&nbsp;&nbsp;<span class="d2-v-kw">ON VIOLATION DROP ROW</span>,<br>&nbsp;&nbsp;<span class="d2-v-kw">CONSTRAINT</span> valid_id <span class="d2-v-kw">EXPECT</span> (order_id <span class="d2-v-kw">IS NOT NULL</span>)<br>&nbsp;&nbsp;&nbsp;&nbsp;<span class="d2-v-kw">ON VIOLATION FAIL UPDATE</span><br>) <span class="d2-v-kw">AS SELECT</span> * <span class="d2-v-kw">FROM STREAM</span> <span class="d2-v-fn">raw_orders</span>;</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Gold panel -->
# MAGIC <div class="d2-v-panel d2-v-p3">
# MAGIC   <div class="d2-v-pcols">
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">What happens here</div>
# MAGIC       <div class="d2-v-pcol-desc">Business-level aggregations, KPIs, and BI-ready summaries. Materialized views maintain pre-computed results that refresh incrementally via the Enzyme engine.</div>
# MAGIC       <div class="d2-v-pills">
# MAGIC         <span class="d2-v-pill d2-v-pill-g">Aggregations</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-g">KPIs</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-g">BI Dashboards</span>
# MAGIC         <span class="d2-v-pill d2-v-pill-g">Enzyme Refresh</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d2-v-pcol">
# MAGIC       <div class="d2-v-pcol-title">SQL pattern</div>
# MAGIC       <div class="d2-v-code"><span class="d2-v-cmt">-- Gold: business aggregation</span><br><span class="d2-v-kw">CREATE OR REFRESH MATERIALIZED VIEW</span><br>&nbsp;&nbsp;<span class="d2-v-fn">daily_order_summary</span><br><span class="d2-v-kw">AS SELECT</span><br>&nbsp;&nbsp;<span class="d2-v-fn">date_trunc</span>(<span class="d2-v-str">'day'</span>, order_date) <span class="d2-v-kw">AS</span> day,<br>&nbsp;&nbsp;region,<br>&nbsp;&nbsp;<span class="d2-v-fn">COUNT</span>(*) <span class="d2-v-kw">AS</span> order_count,<br>&nbsp;&nbsp;<span class="d2-v-fn">SUM</span>(amount) <span class="d2-v-kw">AS</span> total_revenue<br><span class="d2-v-kw">FROM</span> <span class="d2-v-fn">clean_orders</span><br><span class="d2-v-kw">GROUP BY</span> day, region;</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Streaming Tables vs. Materialized Views</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Streaming Tables</strong> use streaming semantics: each row is processed exactly once, append-only by default. Best for ingestion, event streams, and logs. Think of it as a conveyor belt: items go on once and move through in order.</li>
# MAGIC           <li><strong>Materialized Views</strong> use batch semantics: the entire query result is maintained and updated as source data changes. Best for aggregations, joins, and BI dashboards. Think of it as a whiteboard summary that gets erased and rewritten whenever the underlying data changes.</li>
# MAGIC           <li><strong>Medallion mapping:</strong> Bronze = ST (ingestion), Silver = ST or MV (depends on complexity), Gold = MV (aggregation/reporting).</li>
# MAGIC           <li><strong>MV refresh policies (GA May 2026):</strong> materialized view refresh policies allow you to configure refresh schedules and cost optimization strategies directly in the MV definition.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Incremental Refresh with Enzyme</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The <strong>Enzyme engine</strong> powers incremental refresh for materialized views. Instead of recomputing the entire query from scratch, Enzyme tracks how new data affects results and processes only changes.</li>
# MAGIC           <li>Enzyme selects from multiple refresh techniques (ROW_BASED, PARTITION_OVERWRITE, GROUP_AGGREGATE, APPEND_ONLY, etc.) based on an internal cost model.</li>
# MAGIC           <li>For optimal incremental refresh, enable row tracking and change data feed on source tables: <code>ALTER TABLE source SET TBLPROPERTIES (delta.enableRowTracking = true, delta.enableChangeDataFeed = true)</code>.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">ETL Development Lifecycle</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Parameterization:</strong> SQL uses <code>${parameter_name}</code> syntax; Python uses <code>spark.conf.get("parameter_name")</code>. Inject environment-specific values (catalogs, schemas) at deploy time.</li>
# MAGIC           <li><strong>Materialized view refresh policies (GA May 2026):</strong> you can now configure refresh schedules and cost optimization strategies directly in the MV definition, giving fine-grained control over when and how materialized views are updated.</li>
# MAGIC           <li><strong>Lakeflow Pipelines Editor (GA May 2026):</strong> an agent-first development experience with code, chat, pipeline graph, and metrics displayed side by side. This is the authoring surface for the Bronze/Silver/Gold pipeline definitions shown in the tabs above.</li>
# MAGIC           <li><strong>CI/CD with Declarative Automation Bundles:</strong> package code, jobs, and pipelines as YAML-defined units for atomic deployment across dev, staging, and production.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Data Quality with Expectations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Expectations: Declarative Data Quality Rules
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Expectations</strong> are optional clauses in streaming table, materialized view, or view creation statements that apply data quality checks on each record. They consist of three elements: a <strong>name</strong> (unique identifier for tracking), a <strong>constraint</strong> (SQL Boolean expression), and an <strong>action on violation</strong> (what happens when validation fails).</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-expectations-tabs ── -->
# MAGIC <style>
# MAGIC .e1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .e1-v-radio { display: none; }
# MAGIC /* Policy tabs */
# MAGIC .e1-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .e1-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .e1-v-tab-title { font-size: 16pt; font-weight: 700; display: block; }
# MAGIC .e1-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .e1-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC /* Activation */
# MAGIC #e1-t1:checked ~ .e1-v-tabs .e1-v-tab1 { background: #E5A100; color: #fff; border-color: #E5A100; }
# MAGIC #e1-t1:checked ~ .e1-v-tabs .e1-v-tab1 .e1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #e1-t1:checked ~ .e1-v-p1 { display: block; border-color: #E5A100; }
# MAGIC #e1-t2:checked ~ .e1-v-tabs .e1-v-tab2 { background: #618794; color: #fff; border-color: #618794; }
# MAGIC #e1-t2:checked ~ .e1-v-tabs .e1-v-tab2 .e1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #e1-t2:checked ~ .e1-v-p2 { display: block; border-color: #618794; }
# MAGIC #e1-t3:checked ~ .e1-v-tabs .e1-v-tab3 { background: #98102A; color: #fff; border-color: #98102A; }
# MAGIC #e1-t3:checked ~ .e1-v-tabs .e1-v-tab3 .e1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #e1-t3:checked ~ .e1-v-p3 { display: block; border-color: #98102A; }
# MAGIC /* Panel layout */
# MAGIC .e1-v-cols { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .e1-v-col { flex: 1; min-width: 260px; }
# MAGIC /* Data flow mini-diagram */
# MAGIC .e1-v-flow { display: flex; align-items: center; gap: 0; margin-bottom: 14px; }
# MAGIC .e1-v-fbox { border-radius: 8px; padding: 10px 14px; text-align: center; font-size: 14pt; font-weight: 600; }
# MAGIC .e1-v-fbox-in { background: #F9F7F4; border: 1.5px solid #DCE0E2; color: #1B3139; }
# MAGIC .e1-v-fbox-gate { padding: 10px 16px; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .e1-v-fbox-out { border-radius: 8px; padding: 10px 14px; font-size: 14pt; font-weight: 600; }
# MAGIC .e1-v-farrow { font-size: 22pt; color: #94b3be; padding: 0 6px; }
# MAGIC /* Outcome description */
# MAGIC .e1-v-outcome { font-size: 14pt; color: #333; line-height: 1.5; margin-bottom: 12px; }
# MAGIC .e1-v-outcome strong { color: #1B3139; }
# MAGIC /* Code block */
# MAGIC .e1-v-code { background: #272822; border-radius: 6px; padding: 12px 14px; font-family: 'Menlo','Consolas',monospace; font-size: 14pt; color: #f8f8f2; line-height: 1.5; overflow-x: auto; }
# MAGIC .e1-v-kw { color: #66d9ef; font-style: italic; }
# MAGIC .e1-v-fn { color: #a6e22e; }
# MAGIC .e1-v-str { color: #e6db74; }
# MAGIC .e1-v-cmt { color: #75715e; font-style: italic; }
# MAGIC .e1-v-code-label { font-size: 14pt; font-weight: 700; color: #5A6F77; margin-bottom: 6px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="e1-v-wrap">
# MAGIC <input type="radio" name="e1tabs" id="e1-t1" class="e1-v-radio" checked>
# MAGIC <input type="radio" name="e1tabs" id="e1-t2" class="e1-v-radio">
# MAGIC <input type="radio" name="e1tabs" id="e1-t3" class="e1-v-radio">
# MAGIC
# MAGIC <div class="e1-v-tabs">
# MAGIC   <label for="e1-t1" class="e1-v-tab e1-v-tab1"><span class="e1-v-tab-title">Warn</span><span class="e1-v-tab-sub">Default: log + pass through</span></label>
# MAGIC   <label for="e1-t2" class="e1-v-tab e1-v-tab2"><span class="e1-v-tab-title">Drop Row</span><span class="e1-v-tab-sub">Remove invalid records</span></label>
# MAGIC   <label for="e1-t3" class="e1-v-tab e1-v-tab3"><span class="e1-v-tab-title">Fail Update</span><span class="e1-v-tab-sub">Halt the pipeline</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- WARN -->
# MAGIC <div class="e1-v-panel e1-v-p1">
# MAGIC   <div class="e1-v-flow">
# MAGIC     <div class="e1-v-fbox e1-v-fbox-in">5 records in</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-gate" style="background:#E5A100;">EXPECT (year >= 2020)</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-out" style="background:rgba(229,161,0,0.10);border:1.5px solid #E5A100;color:#8a6200;"><strong style="font-size:14pt;">5 records written</strong><br>1 flagged in metrics</div>
# MAGIC   </div>
# MAGIC   <div class="e1-v-cols">
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-outcome"><strong>Behavior:</strong> all records pass through to the target table, including invalid ones. The system logs which records failed validation and collects pass/fail metrics for monitoring dashboards. No data is lost or blocked.</div>
# MAGIC       <div class="e1-v-outcome"><strong>Use when:</strong> you want visibility into data quality issues without disrupting the pipeline. Good for initial monitoring before enforcing stricter policies.</div>
# MAGIC     </div>
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-code-label">SQL</div>
# MAGIC       <div class="e1-v-code"><span class="e1-v-kw">CONSTRAINT</span> valid_date <span class="e1-v-kw">EXPECT</span> (<span class="e1-v-fn">year</span>(order_date) >= <span class="e1-v-str">2020</span>)</div>
# MAGIC       <div class="e1-v-code-label" style="margin-top:10px;">Python</div>
# MAGIC       <div class="e1-v-code">@dp.<span class="e1-v-fn">expect</span>(<span class="e1-v-str">"valid_date"</span>, <span class="e1-v-str">"year(order_date) >= 2020"</span>)</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- DROP ROW -->
# MAGIC <div class="e1-v-panel e1-v-p2">
# MAGIC   <div class="e1-v-flow">
# MAGIC     <div class="e1-v-fbox e1-v-fbox-in">5 records in</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-gate" style="background:#618794;">EXPECT (amount > 0)</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-out" style="background:rgba(97,135,148,0.10);border:1.5px solid #618794;color:#4a6a76;"><strong style="font-size:14pt;">4 records written</strong><br>1 dropped silently</div>
# MAGIC   </div>
# MAGIC   <div class="e1-v-cols">
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-outcome"><strong>Behavior:</strong> records that fail validation are removed before writing. Valid records pass through normally. Metrics track how many records were dropped per batch.</div>
# MAGIC       <div class="e1-v-outcome"><strong>Use when:</strong> bad data should never reach downstream consumers. Common for null checks, range validations, and deduplication filters at the Silver layer.</div>
# MAGIC     </div>
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-code-label">SQL</div>
# MAGIC       <div class="e1-v-code"><span class="e1-v-kw">CONSTRAINT</span> positive_amount <span class="e1-v-kw">EXPECT</span> (amount > <span class="e1-v-str">0</span>)<br>&nbsp;&nbsp;<span class="e1-v-kw">ON VIOLATION DROP ROW</span></div>
# MAGIC       <div class="e1-v-code-label" style="margin-top:10px;">Python</div>
# MAGIC       <div class="e1-v-code">@dp.<span class="e1-v-fn">expect_or_drop</span>(<span class="e1-v-str">"positive_amount"</span>, <span class="e1-v-str">"amount > 0"</span>)</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- FAIL UPDATE -->
# MAGIC <div class="e1-v-panel e1-v-p3">
# MAGIC   <div class="e1-v-flow">
# MAGIC     <div class="e1-v-fbox e1-v-fbox-in">5 records in</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-gate" style="background:#98102A;">EXPECT (id IS NOT NULL)</div>
# MAGIC     <span class="e1-v-farrow">&#x279C;</span>
# MAGIC     <div class="e1-v-fbox e1-v-fbox-out" style="background:rgba(152,16,42,0.08);border:1.5px solid #98102A;color:#98102A;"><strong style="font-size:14pt;">0 records written</strong><br>Pipeline rolls back</div>
# MAGIC   </div>
# MAGIC   <div class="e1-v-cols">
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-outcome"><strong>Behavior:</strong> if any record fails validation, the entire pipeline update rolls back atomically. No records are written and no metrics are recorded for the failed batch. The pipeline stops.</div>
# MAGIC       <div class="e1-v-outcome"><strong>Use when:</strong> data integrity is non-negotiable. Required for primary key constraints, foreign key relationships, or regulatory compliance where partial writes are unacceptable.</div>
# MAGIC     </div>
# MAGIC     <div class="e1-v-col">
# MAGIC       <div class="e1-v-code-label">SQL</div>
# MAGIC       <div class="e1-v-code"><span class="e1-v-kw">CONSTRAINT</span> required_id <span class="e1-v-kw">EXPECT</span> (order_id <span class="e1-v-kw">IS NOT NULL</span>)<br>&nbsp;&nbsp;<span class="e1-v-kw">ON VIOLATION FAIL UPDATE</span></div>
# MAGIC       <div class="e1-v-code-label" style="margin-top:10px;">Python</div>
# MAGIC       <div class="e1-v-code">@dp.<span class="e1-v-fn">expect_or_fail</span>(<span class="e1-v-str">"required_id"</span>, <span class="e1-v-str">"order_id IS NOT NULL"</span>)</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">Advanced Patterns</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>Grouped expectations (Python):</strong> apply multiple rules via a dictionary using <code>@dp.expect_all(rules)</code>, <code>@dp.expect_all_or_drop(rules)</code>, or <code>@dp.expect_all_or_fail(rules)</code>. Useful for reusable validation rule sets.</li>
# MAGIC           <li><strong>Quarantine pattern:</strong> partition data on a validation flag, then route valid and invalid records to separate views. This preserves dropped records for investigation rather than silently discarding them.</li>
# MAGIC           <li><strong>Complex expressions:</strong> constraints support CASE statements, multi-condition AND/OR logic, and date range validations. However, custom Python functions, external service calls, and subqueries are not supported.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Monitoring Expectations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>Go to Jobs and Pipelines, click the pipeline name, select the target dataset, and view the <strong>Data quality tab</strong> in the sidebar for a visual scorecard.</li>
# MAGIC           <li>Metrics are also queryable via the pipeline event log, enabling custom dashboards and alerting on expectation pass/failure rates.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/block/lakeflow-declarative-pipelines" style="color: #2574B5;">Block</a> uses expectations at the silver layer to check or verify higher-quality data in their Kafka CDC streaming pipelines. Quality checks run automatically alongside CDC processing. &#x25C6;</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/the-rank-group-plc/lakeflow-jobs" style="color: #2574B5;">The Rank Group</a> achieved data quality pass rates exceeding 97.2% across 20 million daily transactions using automated quality checks. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Change Data Capture

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. AUTO CDC: Replacing Hand-Coded MERGE Logic
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Change Data Capture (CDC)</strong> is the process of capturing row-level changes (inserts, updates, deletes) from source systems. Traditionally, implementing CDC required complex MERGE INTO logic that grows increasingly fragile as requirements accumulate: deduplication, out-of-order handling, delete processing, and SCD Type 2 history tracking. The AUTO CDC API replaces approximately 150 lines of procedural merge logic with approximately 10 lines of declarative SQL.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-auto-cdc-tabs ── -->
# MAGIC <style>
# MAGIC .f1c-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .f1c-v-radio { display: none; }
# MAGIC .f1c-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .f1c-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .f1c-v-tab-title { font-size: 16pt; font-weight: 700; display: block; }
# MAGIC .f1c-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .f1c-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC #f1c-t1:checked ~ .f1c-v-tabs .f1c-v-tab1 { background: #98102A; color: #fff; border-color: #98102A; }
# MAGIC #f1c-t1:checked ~ .f1c-v-tabs .f1c-v-tab1 .f1c-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1c-t1:checked ~ .f1c-v-p1 { display: block; border-color: #98102A; }
# MAGIC #f1c-t2:checked ~ .f1c-v-tabs .f1c-v-tab2 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #f1c-t2:checked ~ .f1c-v-tabs .f1c-v-tab2 .f1c-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1c-t2:checked ~ .f1c-v-p2 { display: block; border-color: #1B5162; }
# MAGIC #f1c-t3:checked ~ .f1c-v-tabs .f1c-v-tab3 { background: #00A972; color: #fff; border-color: #00A972; }
# MAGIC #f1c-t3:checked ~ .f1c-v-tabs .f1c-v-tab3 .f1c-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1c-t3:checked ~ .f1c-v-p3 { display: block; border-color: #00A972; }
# MAGIC /* Shared */
# MAGIC .f1c-v-cols { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .f1c-v-col { flex: 1; min-width: 260px; }
# MAGIC .f1c-v-metric { text-align: center; padding: 14px; border-radius: 10px; margin-bottom: 16px; }
# MAGIC .f1c-v-metric-val { font-size: 20pt; font-weight: 800; color: #fff; }
# MAGIC .f1c-v-metric-label { font-size: 14pt; color: rgba(255,255,255,0.85); }
# MAGIC .f1c-v-desc { font-size: 14pt; color: #333; line-height: 1.5; margin-bottom: 10px; }
# MAGIC .f1c-v-desc strong { color: #1B3139; }
# MAGIC .f1c-v-pills { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 10px; }
# MAGIC .f1c-v-pill { font-size: 14pt; font-weight: 600; padding: 3px 10px; border-radius: 10px; }
# MAGIC .f1c-v-pill-r { background: rgba(152,16,42,0.10); color: #98102A; }
# MAGIC .f1c-v-pill-t { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC .f1c-v-pill-g { background: rgba(0,169,114,0.10); color: #007a53; }
# MAGIC .f1c-v-code { background: #272822; border-radius: 6px; padding: 12px 14px; font-family: 'Menlo','Consolas',monospace; font-size: 14pt; color: #f8f8f2; line-height: 1.5; overflow-x: auto; }
# MAGIC .f1c-v-kw { color: #66d9ef; font-style: italic; }
# MAGIC .f1c-v-fn { color: #a6e22e; }
# MAGIC .f1c-v-str { color: #e6db74; }
# MAGIC .f1c-v-cmt { color: #75715e; font-style: italic; }
# MAGIC .f1c-v-clause { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }
# MAGIC .f1c-v-cl { flex: 1; min-width: 180px; border-radius: 8px; padding: 10px; border: 1.5px solid #DCE0E2; background: #F9F7F4; }
# MAGIC .f1c-v-cl-title { font-size: 14pt; font-weight: 700; color: #1B3139; font-family: 'Menlo','Consolas',monospace; }
# MAGIC .f1c-v-cl-desc { font-size: 14pt; color: #555; margin-top: 2px; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="f1c-v-wrap">
# MAGIC <input type="radio" name="f1ctabs" id="f1c-t1" class="f1c-v-radio" checked>
# MAGIC <input type="radio" name="f1ctabs" id="f1c-t2" class="f1c-v-radio">
# MAGIC <input type="radio" name="f1ctabs" id="f1c-t3" class="f1c-v-radio">
# MAGIC
# MAGIC <div class="f1c-v-tabs">
# MAGIC   <label for="f1c-t1" class="f1c-v-tab f1c-v-tab1"><span class="f1c-v-tab-title">The Problem</span><span class="f1c-v-tab-sub">Manual MERGE is fragile</span></label>
# MAGIC   <label for="f1c-t2" class="f1c-v-tab f1c-v-tab2"><span class="f1c-v-tab-title">SCD Type 1</span><span class="f1c-v-tab-sub">Current state only</span></label>
# MAGIC   <label for="f1c-t3" class="f1c-v-tab f1c-v-tab3"><span class="f1c-v-tab-title">SCD Type 2</span><span class="f1c-v-tab-sub">Full change history</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- The Problem -->
# MAGIC <div class="f1c-v-panel f1c-v-p1">
# MAGIC   <div class="f1c-v-metric" style="background:linear-gradient(135deg,#6b1020,#98102A);">
# MAGIC     <div class="f1c-v-metric-val">~150 Lines of MERGE vs. ~10 Lines of AUTO CDC</div>
# MAGIC     <div class="f1c-v-metric-label">Every new requirement adds fragility to hand-coded MERGE logic</div>
# MAGIC   </div>
# MAGIC   <div class="f1c-v-pills">
# MAGIC     <span class="f1c-v-pill f1c-v-pill-r">Duplicate updates in one batch</span>
# MAGIC     <span class="f1c-v-pill f1c-v-pill-r">Out-of-order events across batches</span>
# MAGIC     <span class="f1c-v-pill f1c-v-pill-r">Soft-deletes + tombstones + cleanup</span>
# MAGIC     <span class="f1c-v-pill f1c-v-pill-r">SCD Type 2 checksums + window functions</span>
# MAGIC   </div>
# MAGIC   <div class="f1c-v-desc"><strong>AUTO CDC solves all four:</strong> the SEQUENCE BY clause resolves ordering. KEYS handles dedup. APPLY AS DELETE processes deletes. STORED AS SCD TYPE 2 tracks history. The platform handles the MERGE logic internally.</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- SCD Type 1 -->
# MAGIC <div class="f1c-v-panel f1c-v-p2">
# MAGIC   <div class="f1c-v-metric" style="background:linear-gradient(135deg,#1B3139,#1B5162);">
# MAGIC     <div class="f1c-v-metric-val">Current State Only</div>
# MAGIC     <div class="f1c-v-metric-label">Each key has one row reflecting the latest value</div>
# MAGIC   </div>
# MAGIC   <div class="f1c-v-code"><span class="f1c-v-cmt">-- SCD Type 1: keep only the latest record</span><br><span class="f1c-v-kw">CREATE OR REFRESH STREAMING TABLE</span> <span class="f1c-v-fn">customers</span><br><span class="f1c-v-kw">FLOW AUTO CDC</span><br><span class="f1c-v-kw">FROM STREAM</span> <span class="f1c-v-fn">customer_updates</span><br><span class="f1c-v-kw">KEYS</span> (customer_id)<br><span class="f1c-v-kw">SEQUENCE BY</span> updated_at<br><span class="f1c-v-kw">COLUMNS</span> * <span class="f1c-v-kw">EXCEPT</span> (operation)<br><span class="f1c-v-kw">APPLY AS DELETE WHEN</span> operation = <span class="f1c-v-str">"DELETE"</span><br><span class="f1c-v-kw">STORED AS SCD TYPE 1</span>;</div>
# MAGIC   <div class="f1c-v-clause">
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">KEYS</div><div class="f1c-v-cl-desc">Business key for matching (not a PK constraint)</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">SEQUENCE BY</div><div class="f1c-v-cl-desc">Resolves ordering: latest timestamp wins</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">APPLY AS DELETE</div><div class="f1c-v-cl-desc">Interprets matching events as deletes</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">SCD TYPE 1</div><div class="f1c-v-cl-desc">Overwrites previous value (default)</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- SCD Type 2 -->
# MAGIC <div class="f1c-v-panel f1c-v-p3">
# MAGIC   <div class="f1c-v-metric" style="background:linear-gradient(135deg,#007a53,#00A972);">
# MAGIC     <div class="f1c-v-metric-val">Full Change History</div>
# MAGIC     <div class="f1c-v-metric-label">Every version of a record is preserved with __START_AT and __END_AT timestamps</div>
# MAGIC   </div>
# MAGIC   <div class="f1c-v-code"><span class="f1c-v-cmt">-- SCD Type 2: preserve full history</span><br><span class="f1c-v-kw">CREATE OR REFRESH STREAMING TABLE</span> <span class="f1c-v-fn">customers_history</span><br><span class="f1c-v-kw">FLOW AUTO CDC</span><br><span class="f1c-v-kw">FROM STREAM</span> <span class="f1c-v-fn">customer_updates</span><br><span class="f1c-v-kw">KEYS</span> (customer_id)<br><span class="f1c-v-kw">SEQUENCE BY</span> updated_at<br><span class="f1c-v-kw">COLUMNS</span> * <span class="f1c-v-kw">EXCEPT</span> (operation)<br><span class="f1c-v-kw">APPLY AS DELETE WHEN</span> operation = <span class="f1c-v-str">"DELETE"</span><br><span class="f1c-v-kw">STORED AS SCD TYPE 2</span>;</div>
# MAGIC   <div class="f1c-v-clause">
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">__START_AT</div><div class="f1c-v-cl-desc">When this version became active</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">__END_AT</div><div class="f1c-v-cl-desc">When this version was superseded (NULL = current)</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">SCD TYPE 2</div><div class="f1c-v-cl-desc">New row per change; old rows closed with end timestamp</div></div>
# MAGIC     <div class="f1c-v-cl"><div class="f1c-v-cl-title">TRACK HISTORY ON</div><div class="f1c-v-cl-desc">Optional: limit which columns trigger a new version</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162;">SCD Type 1 vs. SCD Type 2</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li><strong>SCD Type 1</strong> maintains only the current state. Updates overwrite existing values, no history retained. Think of it as a whiteboard: you erase and rewrite when something changes.</li>
# MAGIC           <li><strong>SCD Type 2</strong> preserves a complete history of changes by creating new rows for each version. AUTO CDC automatically manages <code>__START_AT</code> and <code>__END_AT</code> columns. Currently active records have <code>__END_AT = NULL</code>. Think of it as a photo album: every version is dated and preserved.</li>
# MAGIC           <li><strong>TRACK HISTORY ON:</strong> for SCD Type 2, you can specify which columns trigger new versions. Changes to excluded columns update the current version in place. Example: <code>TRACK HISTORY ON * EXCEPT (city)</code>.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">SCD Type 2 Syntax</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7;">
# MAGIC           <li>The only syntax change from Type 1 to Type 2 is <code>STORED AS SCD TYPE 2</code>. AUTO CDC handles the rest: effective dating, tombstones for deleted records, and version management.</li>
# MAGIC           <li>For SCD Type 2, deleted rows are temporarily retained as <strong>tombstones</strong> with configurable retention via <code>pipelines.cdc.tombstoneGCThresholdInSeconds</code>.</li>
# MAGIC           <li>Query current records: <code>SELECT * FROM customers_history WHERE __END_AT IS NULL</code>.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162;">Customer Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7;">
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/navy-federal/lakeflow-declarative-pipelines" style="color: #2574B5;">Navy Federal Credit Union</a> used AUTO CDC (APPLY CHANGES) to remove duplicates from 9 billion application events over 9 months with near-zero maintenance. Proof of concept in 1 week, production in 3 weeks. &#x25C6;</li>
# MAGIC           <li>&#x25C6; <a href="https://www.databricks.com/customers/block/lakeflow-declarative-pipelines" style="color: #2574B5;">Block</a> applies CDC from Kafka events into Bronze tables in real time, then merges into Silver tables with data quality expectations, demonstrating all three capabilities (CDC, expectations, streaming) together. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child {
# MAGIC   transition: transform 0.2s ease;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC details[open] summary span:first-child {
# MAGIC   transform: rotate(90deg);
# MAGIC }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">LakeFlow provides a unified, declarative approach to data engineering that eliminates the fragmented tooling landscape most organizations struggle with. <strong>LakeFlow Connect</strong> handles managed ingestion from enterprise sources. <strong>Lakeflow Declarative Pipelines</strong> provides declarative transformation with automatic incremental processing, data quality expectations, and CDC support. <strong>Lakeflow Jobs</strong> orchestrates everything with built-in DAG execution, event-driven triggers, and comprehensive monitoring. Together, these components enable organizations to build reliable, governed, and cost-effective data pipelines at any scale.</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>LakeFlow Connect (<a href="https://docs.databricks.com/aws/en/ingestion/overview">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ingestion/overview">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ingestion/overview">GCP</a>): managed connectors for enterprise SaaS apps and databases</li>
# MAGIC         <li>Auto Loader (<a href="https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/">GCP</a>): incremental file ingestion from cloud storage</li>
# MAGIC         <li>Lakeflow Jobs (<a href="https://docs.databricks.com/aws/en/jobs/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/jobs/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/jobs/">GCP</a>): built-in workflow orchestration with DAGs and triggers</li>
# MAGIC         <li>Lakeflow Declarative Pipelines (<a href="https://docs.databricks.com/aws/en/ldp/concepts">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/concepts">GCP</a>): streaming tables, materialized views, and data quality</li>
# MAGIC         <li>Expectations (<a href="https://docs.databricks.com/aws/en/ldp/expectations">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/expectations">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/expectations">GCP</a>): declarative data quality rules with violation policies</li>
# MAGIC         <li>AUTO CDC (<a href="https://docs.databricks.com/aws/en/ldp/cdc">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ldp/cdc">GCP</a>): change data capture with SCD Type 1 and Type 2 support</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year">2026</span> Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/">Support</a>
# MAGIC
# MAGIC <script>document.getElementById("dbx-year").textContent = new Date().getFullYear();</script>
