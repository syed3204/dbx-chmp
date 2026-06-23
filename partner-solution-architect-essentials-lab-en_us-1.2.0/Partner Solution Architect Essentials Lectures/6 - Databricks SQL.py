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
# MAGIC # 6 Lecture - Databricks SQL
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Data warehousing has evolved over more than 40 years, yet organizations still face the same core challenges: specialized skills required to build and maintain warehouses, complex infrastructure that drains engineering time, and performance that degrades as data volumes grow. Cloud data warehouses addressed some of these limitations but introduced new ones, including data silos, vendor lock-in, and runaway compute costs.
# MAGIC
# MAGIC Databricks SQL takes a different approach. Rather than building a separate data warehouse that requires copying data out of the lake, it provides a complete data warehousing experience directly on the lakehouse. The result is data warehouse performance with data lake economics, unified governance through Unity Catalog, and AI-infused capabilities that automate optimization and make analytics accessible to more users.
# MAGIC
# MAGIC This lecture covers 8 sections that build on each other:
# MAGIC
# MAGIC - **A. Data Warehousing Fundamentals** -- ETL vs ELT patterns, OLAP workloads, and the challenges that traditional warehousing has not solved
# MAGIC - **B. Databricks SQL Overview** -- How the lakehouse architecture delivers a complete data warehousing solution without data movement
# MAGIC - **C. SQL Warehouse Architecture** -- Classic vs Serverless compute, Photon engine, and the technology stack under the hood
# MAGIC - **D. Intelligent Data Warehousing** -- Databricks AI, natural language access, predictive optimization, and strong price/performance
# MAGIC - **E. AI-Infused Engine** -- Intelligent Workload Management, Automatic Data Layout, and Predictive I/O
# MAGIC - **F. Performance and Benchmarks** -- TPC-DS results, competitive positioning, and the Classic/Pro/Serverless feature matrix
# MAGIC - **G. SQL Programming and Stored Procedures** -- SQL scripting, stored procedures, and the migration story from legacy warehouses
# MAGIC - **H. Semantic Models with Metric Views** -- The governed semantic layer for consistent business metrics across dashboards, Genie, and BI tools
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain the difference between ETL and ELT patterns and the challenges of traditional data warehousing
# MAGIC - Describe how Databricks SQL provides a complete data warehousing solution built on the lakehouse architecture
# MAGIC - Identify the three pillars of intelligent data warehousing: natural language access, predictive optimization, and competitive TCO
# MAGIC - Explain how AI is infused into the Databricks SQL engine through Intelligent Workload Management, Automatic Data Layout, and Predictive I/O
# MAGIC - Distinguish between Classic, Pro, and Serverless SQL Warehouse architectures and their capabilities
# MAGIC - Describe how SQL scripting and stored procedures enable warehouse migration and procedural logic on Databricks
# MAGIC - Explain how Unity Catalog metric views provide a governed semantic layer for consistent business metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Data Warehousing Fundamentals

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. ETL vs ELT: Two Approaches to Data Warehousing
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Data warehousing serves a specific purpose: <strong>ETL/ELT plus query serving</strong>, optimized for OLAP workloads (read-heavy, structured queries). Two dominant patterns have emerged for loading data into a warehouse, each with distinct trade-offs.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>ETL (Extract, Transform, Load)</strong> transforms data before loading it into the warehouse. Data quality is high on arrival, but the transformation step adds latency and requires specialized tooling outside the warehouse.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>ELT (Extract, Load, Transform)</strong> loads raw data first and transforms it inside the warehouse using SQL. Pipelines are simpler, but lineage can be harder to trace when transformations happen after landing.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-etl-elt-comparison ── -->
# MAGIC <style>
# MAGIC .a1-etl-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .a1-etl-row {
# MAGIC   width: 100%; overflow: hidden;
# MAGIC   margin-bottom: 16px;
# MAGIC }
# MAGIC .a1-etl-pane {
# MAGIC   float: left; width: 48%; margin: 0 1%;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .a1-etl-pane:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .a1-etl-header {
# MAGIC   padding: 14px 20px;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .a1-etl-header-left { background: #1B5162; }
# MAGIC .a1-etl-header-right { background: #618794; }
# MAGIC .a1-etl-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 18px 20px;
# MAGIC }
# MAGIC .a1-etl-flow {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   gap: 8px;
# MAGIC   margin: 12px 0 16px 0;
# MAGIC }
# MAGIC .a1-etl-step {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   padding: 10px 16px;
# MAGIC   border-radius: 6px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   text-align: center;
# MAGIC   min-width: 90px;
# MAGIC }
# MAGIC .a1-etl-arrow {
# MAGIC   font-size: 18pt;
# MAGIC   color: #618794;
# MAGIC   font-weight: bold;
# MAGIC }
# MAGIC .a1-etl-traits {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   margin: 8px 0 0 0;
# MAGIC   padding-left: 20px;
# MAGIC }
# MAGIC .a1-etl-sources {
# MAGIC   text-align: center;
# MAGIC   padding: 14px 20px;
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC   margin-top: 8px;
# MAGIC }
# MAGIC .a1-etl-src-label {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #1B5162;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .a1-etl-src-items {
# MAGIC   display: flex;
# MAGIC   justify-content: center;
# MAGIC   gap: 16px;
# MAGIC   flex-wrap: wrap;
# MAGIC }
# MAGIC .a1-etl-src-tag {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 6px 14px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #303F47;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="a1-etl-wrapper">
# MAGIC   <div class="a1-etl-row">
# MAGIC     <div class="a1-etl-pane">
# MAGIC       <div class="a1-etl-header a1-etl-header-left">ETL: Extract, Transform, Load</div>
# MAGIC       <div class="a1-etl-body">
# MAGIC         <div class="a1-etl-flow">
# MAGIC           <div class="a1-etl-step">Extract</div>
# MAGIC           <div class="a1-etl-arrow">&#x2192;</div>
# MAGIC           <div class="a1-etl-step">Transform</div>
# MAGIC           <div class="a1-etl-arrow">&#x2192;</div>
# MAGIC           <div class="a1-etl-step">Load</div>
# MAGIC         </div>
# MAGIC         <ul class="a1-etl-traits">
# MAGIC           <li>Transformation happens <strong>before</strong> loading</li>
# MAGIC           <li>High data quality on arrival</li>
# MAGIC           <li>Adds latency to the pipeline</li>
# MAGIC           <li>Requires external transformation tooling</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a1-etl-pane">
# MAGIC       <div class="a1-etl-header a1-etl-header-right">ELT: Extract, Load, Transform</div>
# MAGIC       <div class="a1-etl-body">
# MAGIC         <div class="a1-etl-flow">
# MAGIC           <div class="a1-etl-step">Extract</div>
# MAGIC           <div class="a1-etl-arrow">&#x2192;</div>
# MAGIC           <div class="a1-etl-step">Load</div>
# MAGIC           <div class="a1-etl-arrow">&#x2192;</div>
# MAGIC           <div class="a1-etl-step">Transform</div>
# MAGIC         </div>
# MAGIC         <ul class="a1-etl-traits">
# MAGIC           <li>Raw data loaded <strong>first</strong>, then transformed</li>
# MAGIC           <li>Transformation uses SQL inside the warehouse</li>
# MAGIC           <li>Simpler ingestion pipelines</li>
# MAGIC           <li>Lineage can be harder to trace</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="a1-etl-sources">
# MAGIC     <div class="a1-etl-src-label">Common Source Types</div>
# MAGIC     <div class="a1-etl-src-items">
# MAGIC       <div class="a1-etl-src-tag">OLTP Databases</div>
# MAGIC       <div class="a1-etl-src-tag">Flat Files (CSV, TXT)</div>
# MAGIC       <div class="a1-etl-src-tag">Semi-structured (JSON, XML)</div>
# MAGIC       <div class="a1-etl-src-tag">Streaming Sources</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ETL: When Transformation Comes First</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Control before commitment:</strong> ETL validates, cleans, and reshapes data before it reaches the warehouse. This means the warehouse contains data that has already been quality-checked.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>The cost of upfront transformation:</strong> building and maintaining transformation logic outside the warehouse requires additional tools (Informatica, Talend, custom scripts) and dedicated engineering time. Changes to source schemas can break pipelines.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Latency trade-off:</strong> transformation adds processing time between extraction and availability. For real-time or near-real-time use cases, this delay can be a problem.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ELT: When the Warehouse Does the Work</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Load first, ask questions later:</strong> raw data lands in the warehouse quickly, and analysts write SQL to transform it. This pattern has grown in popularity with cloud warehouses that offer elastic compute.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lineage challenge:</strong> when transformations happen after loading, tracing how a final report connects back to its raw sources requires careful documentation. Tools like dbt have emerged to address this gap.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lakehouse alignment:</strong> the Databricks lakehouse supports both patterns, but its architecture particularly suits ELT. Raw data lands in the bronze layer, transformations happen in silver, and analytics-ready data lives in gold.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">OLAP vs OLTP: Why This Matters</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>OLTP (Online Transaction Processing):</strong> optimized for high-frequency writes, small row-level operations, and low-latency transactions. Think point-of-sale systems and order processing.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>OLAP (Online Analytical Processing):</strong> optimized for read-heavy analytical queries that scan large volumes of data. Data warehousing sits in this category, supporting aggregations, joins, and complex reporting.</li>
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
# MAGIC ### A2. Traditional Data Warehousing Challenges
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">After more than 40 years of evolution, data warehousing remains <strong>complex and expensive</strong>. Cloud data warehouses improved some aspects, but three recurring challenges persist across generations of technology.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-three-challenges ── -->
# MAGIC <style>
# MAGIC .a2-chal-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   display: flex;
# MAGIC   gap: 20px;
# MAGIC   margin: 24px 0;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .a2-chal-card {
# MAGIC   float: left; width: 31.33%; margin: 0 1%;
# MAGIC   background: #fff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 6px rgba(0,0,0,0.06);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .a2-chal-card:hover {
# MAGIC   transform: translateY(-4px);
# MAGIC   box-shadow: 0 8px 20px rgba(27,49,57,0.12);
# MAGIC }
# MAGIC .a2-chal-header {
# MAGIC   padding: 20px 20px 16px 20px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   gap: 14px;
# MAGIC }
# MAGIC .a2-chal-num {
# MAGIC   width: 40px;
# MAGIC   height: 40px;
# MAGIC   border-radius: 50%;
# MAGIC   background: #98102A;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   flex-shrink: 0;
# MAGIC }
# MAGIC .a2-chal-title {
# MAGIC   font-size: 16pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC   line-height: 1.3;
# MAGIC }
# MAGIC .a2-chal-bullets {
# MAGIC   padding: 0 20px 16px 20px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .a2-chal-bullets ul {
# MAGIC   margin: 0;
# MAGIC   padding-left: 18px;
# MAGIC   list-style: disc;
# MAGIC }
# MAGIC .a2-chal-bullets li {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC   margin-bottom: 6px;
# MAGIC }
# MAGIC .a2-chal-impact {
# MAGIC   padding: 12px 20px;
# MAGIC   border-top: 1px solid #EDE9E4;
# MAGIC }
# MAGIC .a2-chal-impact-label {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #98102A;
# MAGIC   margin-bottom: 6px;
# MAGIC }
# MAGIC .a2-chal-bar-track {
# MAGIC   height: 8px;
# MAGIC   background: #F0ECEA;
# MAGIC   border-radius: 4px;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .a2-chal-bar-fill {
# MAGIC   height: 100%;
# MAGIC   border-radius: 4px;
# MAGIC   background: linear-gradient(90deg, #98102A, #E24B4A);
# MAGIC   width: 0%;
# MAGIC   transition: width 1.2s ease;
# MAGIC }
# MAGIC .a2-chal-card:hover .a2-chal-bar-fill,
# MAGIC .a2-chal-wrapper .a2-chal-bar-fill {
# MAGIC   width: var(--a2-bar-w);
# MAGIC }
# MAGIC .a2-chal-summary {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC   padding: 14px 24px;
# MAGIC   border-radius: 8px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   margin-top: 16px;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="a2-chal-wrapper">
# MAGIC   <div class="a2-chal-card">
# MAGIC     <div class="a2-chal-header">
# MAGIC       <div class="a2-chal-num">01</div>
# MAGIC       <div class="a2-chal-title">Specialized Technical Skills</div>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-bullets">
# MAGIC       <ul>
# MAGIC         <li>Deep SQL, data modeling, and tuning expertise required</li>
# MAGIC         <li>Insights locked behind a small team of specialists</li>
# MAGIC         <li>Business users wait days for answers</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-impact">
# MAGIC       <div class="a2-chal-impact-label">Access limited to the few</div>
# MAGIC       <div class="a2-chal-bar-track"><div class="a2-chal-bar-fill" style="--a2-bar-w: 33%;"></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="a2-chal-card">
# MAGIC     <div class="a2-chal-header">
# MAGIC       <div class="a2-chal-num">02</div>
# MAGIC       <div class="a2-chal-title">Complex Infrastructure</div>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-bullets">
# MAGIC       <ul>
# MAGIC         <li>Manual cluster sizing, scaling policies, patching</li>
# MAGIC         <li>Scheduling OPTIMIZE, VACUUM, index maintenance</li>
# MAGIC         <li>Engineering hours spent on plumbing, not analytics</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-impact">
# MAGIC       <div class="a2-chal-impact-label">Maintenance consumes engineering</div>
# MAGIC       <div class="a2-chal-bar-track"><div class="a2-chal-bar-fill" style="--a2-bar-w: 50%;"></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="a2-chal-card">
# MAGIC     <div class="a2-chal-header">
# MAGIC       <div class="a2-chal-num">03</div>
# MAGIC       <div class="a2-chal-title">Slow Performance at Scale</div>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-bullets">
# MAGIC       <ul>
# MAGIC         <li>Query latency grows as data volumes increase</li>
# MAGIC         <li>Scaling = spending more on larger warehouse configs</li>
# MAGIC         <li>Performance and cost pull in opposite directions</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div class="a2-chal-impact">
# MAGIC       <div class="a2-chal-impact-label">Costs grow faster than value</div>
# MAGIC       <div class="a2-chal-bar-track"><div class="a2-chal-bar-fill" style="--a2-bar-w: 80%;"></div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="a2-chal-summary">40+ years of evolution, same three problems. Databricks SQL addresses all three.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Skills Bottleneck</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Analyst dependency:</strong> business users who need insights often cannot get them without filing a request to a data engineering team. The backlog of requests grows faster than the team can address them.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Modeling expertise:</strong> designing star schemas, slowly changing dimensions, and aggregate tables requires domain knowledge that takes years to develop. Mistakes in the model affect every downstream report.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Infrastructure Complexity</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Manual tuning cycle:</strong> DBAs schedule OPTIMIZE, VACUUM, and ANALYZE operations. They decide which columns to index, how to partition tables, and when to rebuild statistics. Each decision depends on workload patterns that change over time.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> managing a traditional warehouse is like maintaining a car yourself. You need to know when to change the oil, rotate the tires, and tune the engine. Miss a step and performance suffers.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Scale-Cost Problem</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Linear cost growth:</strong> when query volumes double, traditional warehouses require proportionally more compute. Cloud warehouses improved elasticity, but idle compute and oversized clusters remain common.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/lumen-technologies/dbsql" style="color: #2574B5; font-size: 14pt;">Lumen Technologies</a> experienced these challenges with legacy infrastructure: queries took hours and compute costs were substantial. After migrating to Databricks SQL, they reduced compute costs by up to 40% and saw 90% faster query performance. &#x25C6;</li>
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
# MAGIC ## B. Databricks SQL Overview

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. A Complete Data Warehousing Solution
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks SQL is a cloud data warehouse built on lakehouse architecture. It runs directly on your data lake, supports ANSI SQL with Delta Lake extensions, and provides tools to build <strong>highly performant, cost-effective data warehouses without moving your data</strong>. The entire analytics lifecycle is covered: from ingestion through transformation, querying, visualization, and serving to external applications. Note: the workspace sidebar section previously labeled "SQL" was renamed to "Lakehouse" in May 2026, reflecting the broader scope of capabilities available through SQL Warehouses and the lakehouse platform.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-v-progress-tracker ── -->
# MAGIC <div style="width:100%;margin:20px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <div style="text-align:center;color:#1B3139;font-size:16pt;font-weight:700;margin:0 0 20px 0;">Databricks SQL: Complete Data Warehousing Lifecycle</div>
# MAGIC   <table style="width:100%;border-spacing:0;border-collapse:collapse;table-layout:fixed;">
# MAGIC   <tr>
# MAGIC     <td style="text-align:center;vertical-align:top;padding:0 4px;cursor:pointer;" onclick="b1Step(0)">
# MAGIC       <div id="b1d0" style="width:44px;height:44px;border-radius:50%;background:#1B5162;border:4px solid #fff;box-shadow:0 0 0 3px #1B5162,0 0 0 8px rgba(27,81,98,0.2);color:#fff;font-size:14pt;font-weight:700;display:inline-flex;align-items:center;justify-content:center;transform:scale(1.15);">1</div>
# MAGIC       <div style="margin-top:8px;font-size:14pt;font-weight:600;color:#1B3139;">Ingest</div>
# MAGIC       <div style="margin-top:2px;font-size:14pt;color:#618794;line-height:1.3;">Streaming tables, COPY INTO, connectors</div>
# MAGIC     </td>
# MAGIC     <td style="text-align:center;vertical-align:top;padding:0 4px;cursor:pointer;" onclick="b1Step(1)">
# MAGIC       <div id="b1d1" style="width:44px;height:44px;border-radius:50%;background:#1B5162;border:4px solid #fff;box-shadow:0 0 0 3px #1B5162;color:#fff;font-size:14pt;font-weight:700;display:inline-flex;align-items:center;justify-content:center;">2</div>
# MAGIC       <div style="margin-top:8px;font-size:14pt;font-weight:600;color:#1B3139;">Transform</div>
# MAGIC       <div style="margin-top:2px;font-size:14pt;color:#618794;line-height:1.3;">Materialized views, medallion architecture</div>
# MAGIC     </td>
# MAGIC     <td style="text-align:center;vertical-align:top;padding:0 4px;cursor:pointer;" onclick="b1Step(2)">
# MAGIC       <div id="b1d2" style="width:44px;height:44px;border-radius:50%;background:#1B5162;border:4px solid #fff;box-shadow:0 0 0 3px #1B5162;color:#fff;font-size:14pt;font-weight:700;display:inline-flex;align-items:center;justify-content:center;">3</div>
# MAGIC       <div style="margin-top:8px;font-size:14pt;font-weight:600;color:#1B3139;">Query</div>
# MAGIC       <div style="margin-top:2px;font-size:14pt;color:#618794;line-height:1.3;">SQL Editor, Genie Code, AI functions</div>
# MAGIC     </td>
# MAGIC     <td style="text-align:center;vertical-align:top;padding:0 4px;cursor:pointer;" onclick="b1Step(3)">
# MAGIC       <div id="b1d3" style="width:44px;height:44px;border-radius:50%;background:#1B5162;border:4px solid #fff;box-shadow:0 0 0 3px #1B5162;color:#fff;font-size:14pt;font-weight:700;display:inline-flex;align-items:center;justify-content:center;">4</div>
# MAGIC       <div style="margin-top:8px;font-size:14pt;font-weight:600;color:#1B3139;">Visualize</div>
# MAGIC       <div style="margin-top:2px;font-size:14pt;color:#618794;line-height:1.3;">AI/BI Dashboards, Genie, metric views</div>
# MAGIC     </td>
# MAGIC     <td style="text-align:center;vertical-align:top;padding:0 4px;cursor:pointer;" onclick="b1Step(4)">
# MAGIC       <div id="b1d4" style="width:44px;height:44px;border-radius:50%;background:#1B5162;border:4px solid #fff;box-shadow:0 0 0 3px #1B5162;color:#fff;font-size:14pt;font-weight:700;display:inline-flex;align-items:center;justify-content:center;">5</div>
# MAGIC       <div style="margin-top:8px;font-size:14pt;font-weight:600;color:#1B3139;">Serve</div>
# MAGIC       <div style="margin-top:2px;font-size:14pt;color:#618794;line-height:1.3;">Tableau, Power BI, Looker, Delta Sharing</div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC   <div id="b1detail" style="margin-top:16px;padding:16px 20px;background:#F9F7F4;border:1px solid #DCE0E2;border-radius:8px;font-size:14pt;color:#333;line-height:1.6;border-left:4px solid #1B5162;">
# MAGIC     <div id="b1dtitle" style="font-size:15pt;font-weight:700;color:#1B3139;margin-bottom:6px;">1. Ingest</div>
# MAGIC     <div id="b1dtext" style="font-size:14pt;">Bring data from any source into the lakehouse. Streaming tables provide incremental ingestion from cloud storage. COPY INTO handles batch loads. Lakeflow Connect offers managed connectors for SaaS applications and databases. All data lands as governed Delta tables in Unity Catalog.</div>
# MAGIC   </div>
# MAGIC   <table style="width:100%;border-spacing:8px 0;border-collapse:separate;margin-top:14px;table-layout:fixed;">
# MAGIC   <tr>
# MAGIC     <td style="width:50%;background:#1B5162;color:#fff;padding:16px 20px;font-size:15pt;font-weight:600;border-radius:8px;text-align:center;">Performance: Photon + Predictive I/O</td>
# MAGIC     <td style="width:50%;background:#618794;color:#fff;padding:16px 20px;font-size:15pt;font-weight:600;border-radius:8px;text-align:center;">Governance: Unity Catalog + Delta Sharing + Marketplace</td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC </div>
# MAGIC <script>
# MAGIC function b1Step(n) {
# MAGIC   var titles = ['1. Ingest','2. Transform','3. Query','4. Visualize','5. Serve'];
# MAGIC   var texts = [
# MAGIC     'Bring data from any source into the lakehouse. Streaming tables provide incremental ingestion from cloud storage. COPY INTO handles batch loads. Lakeflow Connect offers managed connectors for SaaS applications and databases. All data lands as governed Delta tables in Unity Catalog.',
# MAGIC     'Shape raw data into business-ready tables. Materialized views define transformations declaratively and refresh automatically. The medallion architecture (bronze, silver, gold) organizes data quality tiers. Stored procedures handle complex procedural logic when needed.',
# MAGIC     'Run analytics directly on the lakehouse. The SQL Editor with Genie Code provides AI-assisted query authoring. 13+ AI functions (ai_query, ai_classify, ai_extract, ai_gen) embed LLM capabilities directly in SQL. Serverless SQL Warehouses start in seconds.',
# MAGIC     'Present insights to business users. AI/BI Dashboards support cross-filtering, parameters, and embedded credentials for sharing. AI/BI Genie answers questions in natural language. Unity Catalog metric views provide governed, consistent business calculations.',
# MAGIC     'Deliver data to external consumers. Built-in connectors for Tableau, Power BI, Looker, Qlik, and dbt. Delta Sharing enables governed, zero-copy data exchange across organizations. Marketplace lets you publish and consume shared datasets.'
# MAGIC   ];
# MAGIC   for (var i = 0; i < 5; i++) {
# MAGIC     var dot = document.getElementById('b1d' + i);
# MAGIC     if (i === n) { dot.className = 'b1-v-dot b1-v-dot-active'; }
# MAGIC     else { dot.className = 'b1-v-dot'; }
# MAGIC   }
# MAGIC   document.getElementById('b1dtitle').textContent = titles[n];
# MAGIC   document.getElementById('b1dtext').textContent = texts[n];
# MAGIC }
# MAGIC </script>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">End-to-End Lifecycle</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Ingest:</strong> streaming tables and COPY INTO bring data from cloud storage, message queues, and external databases into Delta Lake with minimal code.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Transform:</strong> materialized views and the medallion architecture (bronze, silver, gold) refine raw data into analytics-ready tables, all expressed in standard SQL.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Query + Visualize:</strong> the SQL editor provides an interactive workspace; AI/BI Dashboards and metric views deliver governed, reusable visualizations without requiring a separate BI tool.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Serve:</strong> built-in BI connectors (Tableau, Power BI, Looker) and Delta Sharing push results to downstream consumers, including external organizations, with no data copying.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">No Data Movement Required</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> traditional data warehousing is like maintaining a separate library branch with photocopies of every book from the main library. Databricks SQL is like adding a reading room with comfortable chairs and fast search to the main library, using the original books.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>How it works:</strong> every stage in the pipeline runs directly on the same Delta Lake data in your cloud storage. There is no separate ETL pipeline copying data into a proprietary warehouse format.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Why it matters:</strong> eliminating data movement reduces latency, lowers storage costs, and removes an entire class of consistency bugs caused by stale copies.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Real-World Impact</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/getyourguide/dbsql" style="color: #2574B5; font-size: 14pt;">GetYourGuide</a> migrated from Snowflake to Databricks SQL in 4 months with just 2 full-time engineers. They now run 100% of their Looker workloads directly on Databricks SQL, achieving approximately 20% lower BI serving costs. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/nab/dbsql" style="color: #2574B5; font-size: 14pt;">National Australia Bank</a> decommissioned their legacy data warehouse and rebuilt reporting suites as DBSQL pipelines, delivering Power BI dashboards with fresher data. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Lakehouse Architecture Benefits
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Because Databricks SQL is built on the lakehouse rather than as a standalone warehouse, it delivers four architectural advantages that traditional cloud data warehouses cannot match.</p>
# MAGIC
# MAGIC <!-- ── Visual: b2-four-benefits ── -->
# MAGIC <style>
# MAGIC .b2-ben-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .b2-ben-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 16px;
# MAGIC }
# MAGIC .b2-ben-card {
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .b2-ben-card:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .b2-ben-bar {
# MAGIC   height: 6px;
# MAGIC }
# MAGIC .b2-ben-bar-1 { background: #00A972; }
# MAGIC .b2-ben-bar-2 { background: #1B5162; }
# MAGIC .b2-ben-bar-3 { background: #4299E0; }
# MAGIC .b2-ben-bar-4 { background: #618794; }
# MAGIC .b2-ben-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 18px 20px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .b2-ben-title {
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .b2-ben-text {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="b2-ben-wrapper">
# MAGIC   <div class="b2-ben-grid">
# MAGIC     <div class="b2-ben-card">
# MAGIC       <div class="b2-ben-bar b2-ben-bar-1"></div>
# MAGIC       <div class="b2-ben-body">
# MAGIC         <div class="b2-ben-title">Unified Architecture</div>
# MAGIC         <div class="b2-ben-text">No new data silo. DBSQL queries the same Delta Lake data used by data engineering and data science workloads. One copy of data serves all use cases.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-ben-card">
# MAGIC       <div class="b2-ben-bar b2-ben-bar-2"></div>
# MAGIC       <div class="b2-ben-body">
# MAGIC         <div class="b2-ben-title">Unified Governance</div>
# MAGIC         <div class="b2-ben-text">Unity Catalog provides a single source of truth for access control, audit logging, and data lineage across SQL, Python, and ML workloads.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-ben-card">
# MAGIC       <div class="b2-ben-bar b2-ben-bar-3"></div>
# MAGIC       <div class="b2-ben-body">
# MAGIC         <div class="b2-ben-title">Unified Data Teams</div>
# MAGIC         <div class="b2-ben-text">SQL analysts and Python engineers work on the same platform, accessing the same governed data. No handoffs between separate warehouse and lake environments.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-ben-card">
# MAGIC       <div class="b2-ben-bar b2-ben-bar-4"></div>
# MAGIC       <div class="b2-ben-body">
# MAGIC         <div class="b2-ben-title">Open Data Sharing</div>
# MAGIC         <div class="b2-ben-text">Delta Sharing and Marketplace enable governed data exchange with external partners and vendors without requiring recipients to use Databricks.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unified Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>No data copies to maintain:</strong> traditional CDWs require ETL pipelines to copy data from the lake into the warehouse. Every copy can drift out of sync. Databricks SQL eliminates this by querying data in place.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Medallion architecture integration:</strong> the data warehouse is modeled in the silver layer (3NF or Data Vault), feeding specialized data marts in the gold layer (star schemas, dimensional models). The silver layer serves as the single source of truth for all data marts.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> a traditional CDW is like running a separate cafeteria that sources ingredients from the same farm but cooks them in a different kitchen. The lakehouse is a single kitchen that serves every dining room in the building.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unified Governance</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>One security model:</strong> when a data engineer writes a pipeline and an analyst queries the output, both operations are governed by the same Unity Catalog policies. There is no gap at the boundary between systems.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lineage across workloads:</strong> Unity Catalog tracks data flow from ingestion through transformation to dashboard consumption, providing end-to-end visibility.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unified Data Teams</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Shared platform, different languages:</strong> SQL analysts and Python engineers work on the same platform, accessing the same governed data. No handoffs between separate warehouse and lake environments are needed.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Collaboration benefits:</strong> a data engineer builds a pipeline in Python and publishes a gold table. An analyst queries that table in SQL five minutes later, using the same Unity Catalog permissions. No data export, no separate access request.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Open Data Sharing</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Two sharing mechanisms:</strong> Delta Sharing enables governed, real-time data exchange with external partners over an open protocol. Marketplace provides a catalog of shared data sets and AI models from providers across industries. Neither requires the recipient to use Databricks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>BI tool connectivity:</strong> DirectQuery (real-time, small result sets, sub-5-second latency) and Import/Extract (batch, large result sets) are both supported through built-in connectors for Tableau, Power BI, Looker, dbt, Qlik, and others.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/eqt/lakehouse" style="color: #2574B5; font-size: 14pt;">EQT</a> migrated from Azure Synapse to Databricks SQL in six months without external consultants, achieving 3.5x faster performance on large telemetry workloads and self-service analytics with governed access through Unity Catalog. &#x25C6;</li>
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
# MAGIC
# MAGIC D ----------
# MAGIC ## C. SQL Warehouse Architecture
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #555; margin-top: -4px;">Section B introduced Databricks SQL as a complete warehousing solution. This section examines the technology stack that powers it: from Delta Lake storage through Photon execution to the analyst experience.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Under the Hood: The Technology Stack
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks SQL is built on a layered technology stack. At the foundation, <strong>Delta Lake</strong> provides reliable, open storage. <strong>Unity Catalog</strong> adds governance. <strong>Photon</strong>, a built-in vectorized query engine written in C++, handles execution. SQL Warehouses provide the compute layer, and the analyst and admin experiences sit on top.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-architecture-stack ── -->
# MAGIC <style>
# MAGIC .e1-acc-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC   width: 100%;
# MAGIC }
# MAGIC .e1-acc-bar {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: space-between;
# MAGIC   padding: 16px 20px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #fff;
# MAGIC   cursor: pointer;
# MAGIC   border-bottom: 2px solid rgba(255,255,255,0.2);
# MAGIC   user-select: none;
# MAGIC }
# MAGIC .e1-acc-bar:first-child {
# MAGIC   border-radius: 12px 12px 0 0;
# MAGIC }
# MAGIC .e1-acc-left {
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .e1-acc-sub {
# MAGIC   font-weight: 400;
# MAGIC   font-size: 14pt;
# MAGIC   opacity: 0.85;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .e1-acc-chev {
# MAGIC   font-size: 16pt;
# MAGIC   transition: transform 0.25s ease;
# MAGIC   flex-shrink: 0;
# MAGIC   margin-left: 16px;
# MAGIC }
# MAGIC .e1-acc-detail {
# MAGIC   max-height: 0px;
# MAGIC   overflow: hidden;
# MAGIC   opacity: 0;
# MAGIC   background: #fff;
# MAGIC   border-left: 4px solid #ccc;
# MAGIC   padding: 0 20px;
# MAGIC   transition: max-height 0.3s ease, padding 0.3s ease, opacity 0.25s ease;
# MAGIC }
# MAGIC .e1-acc-detail ul {
# MAGIC   margin: 4px 0 4px 0;
# MAGIC   padding-left: 20px;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   font-size: 14pt;
# MAGIC }
# MAGIC .e1-acc-detail li {
# MAGIC   font-size: 14pt;
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC .e1-acc-bi {
# MAGIC   display: flex;
# MAGIC   justify-content: center;
# MAGIC   gap: 12px;
# MAGIC   flex-wrap: wrap;
# MAGIC   margin-top: 14px;
# MAGIC   width: 100%;
# MAGIC }
# MAGIC .e1-acc-bi-tag {
# MAGIC   background: #EEEDE9;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 6px 14px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #303F47;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="e1-acc-wrapper">
# MAGIC   <!-- Layer 0: Analyst + Admin Experience -->
# MAGIC   <div class="e1-acc-bar" style="background:#618794;" onclick="e1Acc(0)">
# MAGIC     <div class="e1-acc-left">
# MAGIC       <span>Analyst + Admin Experience</span>
# MAGIC       <span class="e1-acc-sub">SQL Editor, Dashboards, Alerts, Query History, Warehouse Management</span>
# MAGIC     </div>
# MAGIC     <span class="e1-acc-chev" id="e1-acc-c-0">&#x25B6;</span>
# MAGIC   </div>
# MAGIC   <div class="e1-acc-detail" id="e1-acc-d-0" style="border-left-color:#618794;">
# MAGIC     <ul>
# MAGIC       <li>SQL Editor with intelligent autocomplete and Genie Code assistance</li>
# MAGIC       <li>AI/BI Dashboards with cross-filtering, parameters, and embedded credentials</li>
# MAGIC       <li>Query History and profiling for performance debugging</li>
# MAGIC       <li>Admin console for warehouse sizing, cost policies, and user provisioning</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <!-- Layer 1: SQL Warehouses -->
# MAGIC   <div class="e1-acc-bar" style="background:#1B5162;" onclick="e1Acc(1)">
# MAGIC     <div class="e1-acc-left">
# MAGIC       <span>SQL Warehouses (Compute)</span>
# MAGIC       <span class="e1-acc-sub">Serverless, Pro, or Classic with auto-scaling</span>
# MAGIC     </div>
# MAGIC     <span class="e1-acc-chev" id="e1-acc-c-1">&#x25B6;</span>
# MAGIC   </div>
# MAGIC   <div class="e1-acc-detail" id="e1-acc-d-1" style="border-left-color:#1B5162;">
# MAGIC     <ul>
# MAGIC       <li>Serverless: instant startup from warm pool, pay per query, no configuration</li>
# MAGIC       <li>Pro: customer-managed compute with Photon and auto-scaling</li>
# MAGIC       <li>Classic: entry-level with Photon, manual scaling</li>
# MAGIC       <li>All types connect via JDBC/ODBC, REST API, and Partner Connect</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <!-- Layer 2: Photon Engine -->
# MAGIC   <div class="e1-acc-bar" style="background:#E24B4A;" onclick="e1Acc(2)">
# MAGIC     <div class="e1-acc-left">
# MAGIC       <span>Photon Engine</span>
# MAGIC       <span class="e1-acc-sub">Native C++ vectorized engine, columnar batch processing</span>
# MAGIC     </div>
# MAGIC     <span class="e1-acc-chev" id="e1-acc-c-2">&#x25B6;</span>
# MAGIC   </div>
# MAGIC   <div class="e1-acc-detail" id="e1-acc-d-2" style="border-left-color:#E24B4A;">
# MAGIC     <ul>
# MAGIC       <li>Replaces JVM-based Spark SQL execution with native C++ runtime</li>
# MAGIC       <li>Processes data in columnar batches using SIMD instructions</li>
# MAGIC       <li>Adaptive Query Execution: dynamically adjusts joins, partitions, and skew handling</li>
# MAGIC       <li>Runtime filter generation: up to 18x faster for star schema workloads</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <!-- Layer 3: Unity Catalog -->
# MAGIC   <div class="e1-acc-bar" style="background:#00A972;" onclick="e1Acc(3)">
# MAGIC     <div class="e1-acc-left">
# MAGIC       <span>Unity Catalog (Governance)</span>
# MAGIC       <span class="e1-acc-sub">Access control, audit, lineage, data discovery</span>
# MAGIC     </div>
# MAGIC     <span class="e1-acc-chev" id="e1-acc-c-3">&#x25B6;</span>
# MAGIC   </div>
# MAGIC   <div class="e1-acc-detail" id="e1-acc-d-3" style="border-left-color:#00A972;">
# MAGIC     <ul>
# MAGIC       <li>Fine-grained privileges on tables, views, functions, and models</li>
# MAGIC       <li>Automatic lineage tracking across SQL and Python workloads</li>
# MAGIC       <li>Data discovery with search, tagging, and AI-generated comments</li>
# MAGIC       <li>Row-level security and column masking enforced at query time</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <!-- Layer 4: Delta Lake -->
# MAGIC   <div class="e1-acc-bar" style="background:#1B3139; border-radius: 0 0 12px 12px; border-bottom: none;" onclick="e1Acc(4)">
# MAGIC     <div class="e1-acc-left">
# MAGIC       <span>Delta Lake (Storage Foundation)</span>
# MAGIC       <span class="e1-acc-sub">ACID transactions, schema evolution, time travel, UniForm</span>
# MAGIC     </div>
# MAGIC     <span class="e1-acc-chev" id="e1-acc-c-4">&#x25B6;</span>
# MAGIC   </div>
# MAGIC   <div class="e1-acc-detail" id="e1-acc-d-4" style="border-left-color:#1B3139;">
# MAGIC     <ul>
# MAGIC       <li>Open format on customer cloud storage (S3, ADLS, GCS)</li>
# MAGIC       <li>ACID transactions with schema enforcement and evolution</li>
# MAGIC       <li>Time travel for auditing and rollback (DESCRIBE HISTORY)</li>
# MAGIC       <li>UniForm: read Delta tables as Iceberg without data duplication</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function e1Acc(n) {
# MAGIC   for (var i = 0; i < 5; i++) {
# MAGIC     var panel = document.getElementById('e1-acc-d-' + i);
# MAGIC     var chev = document.getElementById('e1-acc-c-' + i);
# MAGIC     if (i === n && panel.style.maxHeight === '0px') {
# MAGIC       panel.style.maxHeight = '200px';
# MAGIC       panel.style.padding = '14px 20px';
# MAGIC       panel.style.opacity = '1';
# MAGIC       chev.style.transform = 'rotate(90deg)';
# MAGIC     } else {
# MAGIC       panel.style.maxHeight = '0px';
# MAGIC       panel.style.padding = '0 20px';
# MAGIC       panel.style.opacity = '0';
# MAGIC       chev.style.transform = 'rotate(0deg)';
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC </script>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Analyst + Admin Experience</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>SQL Editor with AI:</strong> intelligent autocomplete, Genie Code assistance, and inline query profiling. The editor supports multi-statement scripts, parameterized queries, and result visualization.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Dashboards, alerts, and query history:</strong> AI/BI Dashboards provide cross-filtering and embedded credentials. Alerts trigger on metric thresholds. Query History logs every execution for performance debugging and audit.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> the analyst experience is the cockpit of the airplane. All the instruments (editor, dashboards, history) are at your fingertips, but you do not need to open the engine cover to fly.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">SQL Warehouses (Compute)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Three tiers:</strong> Serverless (instant start from warm pool, pay per query, no configuration), Pro (customer-managed compute with Photon and auto-scaling), and Classic (entry-level with Photon, manual scaling).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Connectivity:</strong> all types connect via JDBC/ODBC, REST API, and Partner Connect for BI tool integration.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Photon Engine</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Native C++ vectorized execution:</strong> replaces JVM-based Spark SQL execution. Processes data in columnar batches using SIMD instructions. No garbage collection pauses, no JIT warmup delays.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Catalyst still plans, Photon executes:</strong> Spark's optimizer handles parsing and planning. Photon takes over at execution. Unsupported operations fall back to Spark transparently.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Always on:</strong> Photon is enabled on all SQL warehouse types and cannot be turned off. Runtime filter generation delivers up to 18x faster performance on star schema workloads.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unity Catalog (Governance)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Fine-grained access control:</strong> privileges on tables, views, functions, and models. Row-level security and column masking enforced at query time.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lineage and discovery:</strong> automatic lineage tracking across SQL and Python workloads. Data discovery with search, tagging, and AI-generated comments.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Delta Lake (Storage Foundation)</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Open format on customer storage:</strong> ACID transactions with schema enforcement and evolution on S3, ADLS, or GCS. Time travel for auditing and rollback.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>UniForm:</strong> read Delta tables as Iceberg without data duplication, enabling interoperability with non-Databricks tools that support Iceberg.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/block" style="color: #2574B5; font-size: 14pt;">Block (Square)</a> runs analytics on Delta Lake with Unity Catalog governance, providing their data teams a single platform for engineering and SQL analytics workloads. &#x25C6;</li>
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
# MAGIC ### C2. Classic vs Serverless SQL Warehouses
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">SQL warehouses are the compute layer for Databricks SQL. The two primary architecture models differ in where compute runs and how scaling works. <strong>Classic/Pro warehouses</strong> run in the customer's cloud account with VM-based clusters. <strong>Serverless warehouses</strong> run in Databricks-managed infrastructure with a warm pool that enables startup in seconds.</p>
# MAGIC
# MAGIC <!-- ── Visual: e2-architecture-comparison ── -->
# MAGIC <style>
# MAGIC .e2-arch-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .e2-arch-row {
# MAGIC   width: 100%; overflow: hidden;
# MAGIC   gap: 24px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .e2-arch-pane {
# MAGIC   float: left; width: 48%; margin: 0 1%;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .e2-arch-pane:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .e2-arch-header {
# MAGIC   padding: 14px 20px;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .e2-arch-header-classic { background: #618794; }
# MAGIC .e2-arch-header-serverless { background: #1B5162; }
# MAGIC .e2-arch-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 18px 20px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .e2-arch-section {
# MAGIC   margin-bottom: 12px;
# MAGIC }
# MAGIC .e2-arch-label {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #1B5162;
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC .e2-arch-detail {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC }
# MAGIC .e2-arch-startup {
# MAGIC   display: flex;
# MAGIC   gap: 24px;
# MAGIC   margin-top: 16px;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .e2-arch-time {
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 14px 24px;
# MAGIC   text-align: center;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .e2-arch-time-num {
# MAGIC   font-size: 22pt;
# MAGIC   font-weight: 800;
# MAGIC }
# MAGIC .e2-arch-time-slow { color: #98102A; }
# MAGIC .e2-arch-time-fast { color: #00A972; }
# MAGIC .e2-arch-time-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="e2-arch-wrapper">
# MAGIC   <div class="e2-arch-row">
# MAGIC     <div class="e2-arch-pane">
# MAGIC       <div class="e2-arch-header e2-arch-header-classic">Classic/Pro: Customer Account</div>
# MAGIC       <div class="e2-arch-body">
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Control Plane</div>
# MAGIC           <div class="e2-arch-detail">Databricks AI, Genie Code, Unity Catalog, Platform Services (Databricks-managed)</div>
# MAGIC         </div>
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Data Plane</div>
# MAGIC           <div class="e2-arch-detail">Photon-powered clusters run in the customer's cloud account as VMs. Customer controls network configuration and VPC settings.</div>
# MAGIC         </div>
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Scaling</div>
# MAGIC           <div class="e2-arch-detail">Reactive: threshold-based autoscaling. VMs must be provisioned on demand. Approximately 4 minutes to start.</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="e2-arch-pane">
# MAGIC       <div class="e2-arch-header e2-arch-header-serverless">Serverless: Databricks Account</div>
# MAGIC       <div class="e2-arch-body">
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Control Plane</div>
# MAGIC           <div class="e2-arch-detail">Same as Classic/Pro: Databricks AI, Genie Code, Unity Catalog, Platform Services</div>
# MAGIC         </div>
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Serverless Compute</div>
# MAGIC           <div class="e2-arch-detail">Photon-powered clusters run in Databricks-managed infrastructure. Warm pool of pre-provisioned instances enables instant assignment. Tenant isolation at VM, network, and container levels.</div>
# MAGIC         </div>
# MAGIC         <div class="e2-arch-section">
# MAGIC           <div class="e2-arch-label">Scaling</div>
# MAGIC           <div class="e2-arch-detail">Proactive: IWM-driven ML prediction. Clusters assigned from warm pool. 2-6 seconds to start.</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="e2-arch-startup">
# MAGIC     <div class="e2-arch-time">
# MAGIC       <div class="e2-arch-time-num e2-arch-time-slow">~4 min</div>
# MAGIC       <div class="e2-arch-time-label">Classic/Pro startup time</div>
# MAGIC     </div>
# MAGIC     <div class="e2-arch-time">
# MAGIC       <div class="e2-arch-time-num e2-arch-time-fast">2-6 sec</div>
# MAGIC       <div class="e2-arch-time-label">Serverless startup time</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Warm Pool: Why Serverless Starts in Seconds</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> the warm pool is like a taxi stand at a busy hotel. Cars are always waiting, so you get one in seconds instead of calling one from across town.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>How it works:</strong> Databricks maintains a pool of pre-provisioned compute instances. When a serverless warehouse starts or scales, it draws from this pool rather than provisioning new VMs from the cloud provider.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Tenant isolation:</strong> serverless compute provides isolation at the VM, network, and container levels. Each customer's workload runs in an isolated environment in Databricks' infrastructure.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When to Choose Each Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Serverless (recommended for most workloads):</strong> variable workloads, fast response time requirements, minimal infrastructure management. This is the default when creating a new warehouse in serverless-enabled regions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Pro:</strong> regions where serverless is not yet available, custom VPC networking requirements, on-premises database connectivity, or compliance requirements for compute in the customer's account.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Classic:</strong> entry-level tier for basic data exploration. Lacks Predictive I/O and IWM. Positioned as the legacy option.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Analogy: Ownership vs Ride-Share</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Classic:</strong> like owning a car. You pay for it even when it is parked in the driveway.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Pro:</strong> like a long-term rental. More features, but still sitting in your driveway when not in use.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Serverless:</strong> like a ride-share. A car appears in seconds when you need it, and you pay only while riding.</li>
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
# MAGIC
# MAGIC D ----------
# MAGIC ## D. Intelligent Data Warehousing
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #555; margin-top: -4px;">With the architecture stack in place, this section introduces the intelligence layered on top: how Databricks AI powers natural language access, intelligent execution, and self-maintaining operations.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Three Pillars of Intelligent Data Warehousing
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Traditional warehouses are fast but dumb: they run queries but do not understand your data, optimize themselves, or help non-technical users. Databricks SQL adds intelligence at every layer. <strong>Databricks AI</strong>, the platform's compound AI system, indexes your organization's data semantics and powers three pillars of intelligence.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1pb Three Pillars (Option B) ── -->
# MAGIC <style>
# MAGIC .c1pb-wrap { width: 100% !important;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .c1pb-tab {
# MAGIC   text-align: center;
# MAGIC   padding: 16px 12px;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   cursor: pointer;
# MAGIC   border-radius: 10px 10px 0 0;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-bottom: none;
# MAGIC   background: #e8ebed;
# MAGIC   color: #666;
# MAGIC }
# MAGIC .c1pb-tab-sub {
# MAGIC   display: block;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 400;
# MAGIC   margin-top: 2px;
# MAGIC   text-transform: uppercase;
# MAGIC   letter-spacing: 1px;
# MAGIC }
# MAGIC .c1pb-panel {
# MAGIC   display: none;
# MAGIC   width: 100% !important;
# MAGIC   background: #fff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 0 0 10px 10px;
# MAGIC   padding: 20px;
# MAGIC   min-height: 300px;
# MAGIC   box-shadow: 0 4px 20px rgba(0,0,0,0.06);
# MAGIC }
# MAGIC .c1pb-metric {
# MAGIC   text-align: center;
# MAGIC   padding: 12px;
# MAGIC   border-radius: 10px;
# MAGIC   margin-bottom: 14px;
# MAGIC }
# MAGIC .c1pb-metric-value {
# MAGIC   font-size: 22pt;
# MAGIC   font-weight: 800;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .c1pb-metric-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: rgba(255,255,255,0.85);
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .c1pb-km-green { background: linear-gradient(135deg, #00A972, #00c987); }
# MAGIC .c1pb-km-teal { background: linear-gradient(135deg, #1B5162, #2a7a94); }
# MAGIC .c1pb-km-amber { background: linear-gradient(135deg, #C48E00, #e0a800); }
# MAGIC /* card */ .c1pb-card {
# MAGIC   padding: 14px;
# MAGIC   border-radius: 8px;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   background: #fff;
# MAGIC   vertical-align: top;
# MAGIC }
# MAGIC .c1pb-card h3 {
# MAGIC   font-size: 14pt;
# MAGIC   margin-bottom: 6px;
# MAGIC }
# MAGIC .c1pb-card p {
# MAGIC   font-size: 14pt;
# MAGIC   line-height: 1.5;
# MAGIC   color: #555;
# MAGIC }
# MAGIC .c1pb-badge {
# MAGIC   display: inline-block;
# MAGIC   padding: 3px 10px;
# MAGIC   border-radius: 20px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC   margin-bottom: 10px;
# MAGIC }
# MAGIC .c1pb-badge-green { background: #00A972; }
# MAGIC .c1pb-badge-teal { background: #1B5162; }
# MAGIC .c1pb-badge-amber { background: #C48E00; }
# MAGIC .c1pb-green-card { border-left: 4px solid #00A972; }
# MAGIC .c1pb-green-card h3 { color: #00734d; }
# MAGIC .c1pb-teal-card { border-left: 4px solid #1B5162; }
# MAGIC .c1pb-teal-card h3 { color: #1B5162; }
# MAGIC .c1pb-amber-card { border-left: 4px solid #C48E00; }
# MAGIC .c1pb-amber-card h3 { color: #8a6300; }
# MAGIC .c1pb-comp-before h4 { font-size: 14pt; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 2px solid #ccc; color: #999; }
# MAGIC .c1pb-comp-after h4 { font-size: 14pt; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 2px solid; }
# MAGIC .c1pb-comp-after-green h4 { border-color: #00A972; color: #00A972; }
# MAGIC .c1pb-comp-after-teal h4 { border-color: #1B5162; color: #1B5162; }
# MAGIC .c1pb-comp-after-amber h4 { border-color: #C48E00; color: #C48E00; }
# MAGIC .c1pb-comp-list { list-style: none; padding: 0; margin: 0; }
# MAGIC .c1pb-comp-list li { font-size: 14pt; padding: 6px 0; line-height: 1.45; }
# MAGIC .c1pb-before-item::before { content: "x  "; color: #c44; font-weight: 700; }
# MAGIC .c1pb-after-item-green::before { content: "+  "; color: #00A972; font-weight: 700; }
# MAGIC .c1pb-after-item-teal::before { content: "+  "; color: #1B5162; font-weight: 700; }
# MAGIC .c1pb-after-item-amber::before { content: "+  "; color: #C48E00; font-weight: 700; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="c1pb-wrap">
# MAGIC
# MAGIC <!-- Foundation Banner -->
# MAGIC <div style="width:100%;background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%);color:#fff;padding:14px 28px;border-radius:10px;box-shadow:0 4px 16px rgba(27,49,57,0.15);margin-bottom:8px;font-size:14pt;">
# MAGIC <span style="font-size:14pt;text-transform:uppercase;letter-spacing:2px;opacity:0.6;padding-right:16px;border-right:1px solid rgba(255,255,255,0.3);">Foundation</span>
# MAGIC <span style="font-size:18pt;font-weight:700;padding-left:16px;">Databricks AI</span>
# MAGIC <span style="font-size:14pt;opacity:0.8;padding-left:16px;">Data Intelligence Engine powering all three pillars</span>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tabs Bar -->
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click each pillar tab to explore its intelligence features</p>
# MAGIC <div style="width:100%;margin:0;padding:0;overflow:hidden;">
# MAGIC <div style="float:left;width:33.33%;box-sizing:border-box;padding-right:2px;" onclick="c1Tab(0)"><div class="c1pb-tab" id="c1pb-tab-0" style="text-align:center;padding:14px 8px;font-size:15pt;font-weight:700;cursor:pointer;border-radius:10px 10px 0 0;border:1px solid #00A972;border-bottom:none;background:#00A972;color:#fff;">Intelligent Access<span style="display:block;font-size:14pt;font-weight:400;margin-top:2px;text-transform:uppercase;letter-spacing:1px;">Pillar 1</span></div></div>
# MAGIC <div style="float:left;width:33.34%;box-sizing:border-box;padding:0 1px;" onclick="c1Tab(1)"><div class="c1pb-tab" id="c1pb-tab-1" style="text-align:center;padding:14px 8px;font-size:15pt;font-weight:700;cursor:pointer;border-radius:10px 10px 0 0;border:1px solid #DCE0E2;border-bottom:none;background:#e8ebed;color:#666;">Intelligent Execution<span style="display:block;font-size:14pt;font-weight:400;margin-top:2px;text-transform:uppercase;letter-spacing:1px;">Pillar 2</span></div></div>
# MAGIC <div style="float:left;width:33.33%;box-sizing:border-box;padding-left:2px;" onclick="c1Tab(2)"><div class="c1pb-tab" id="c1pb-tab-2" style="text-align:center;padding:14px 8px;font-size:15pt;font-weight:700;cursor:pointer;border-radius:10px 10px 0 0;border:1px solid #DCE0E2;border-bottom:none;background:#e8ebed;color:#666;">Intelligent Maintenance<span style="display:block;font-size:14pt;font-weight:400;margin-top:2px;text-transform:uppercase;letter-spacing:1px;">Pillar 3</span></div></div>
# MAGIC </div>
# MAGIC <div style="clear:both;"></div>
# MAGIC
# MAGIC <!-- PANEL 0: Intelligent Access -->
# MAGIC <div class="c1pb-panel" id="c1pb-panel-0" style="display:block;border-color:#00A972;">
# MAGIC   <div class="c1pb-metric c1pb-km-green">
# MAGIC     <div class="c1pb-metric-value" style="font-size:20pt;">3 Layers of AI-Powered Access</div>
# MAGIC     <div class="c1pb-metric-label" style="font-size:14pt;">From natural language to embedded SQL intelligence</div>
# MAGIC   </div>
# MAGIC   <table style="width:100% !important;border-spacing:10px;table-layout:fixed !important;">
# MAGIC   <tr>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-green-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-green" style="font-size:14pt;vertical-align:middle;">Business Users</span> <span style="font-size:15pt;font-weight:700;color:#00734d;vertical-align:middle;">Genie Spaces</span>
# MAGIC       <p style="font-size:14pt;">Ask data questions in plain English. Genie translates natural language to verified SQL, executes the query, and returns visual results. No SQL knowledge needed.</p>
# MAGIC     </td>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-green-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-green" style="font-size:14pt;vertical-align:middle;">Practitioners</span> <span style="font-size:15pt;font-weight:700;color:#00734d;vertical-align:middle;">Genie Code</span>
# MAGIC       <p style="font-size:14pt;">An AI coding assistant embedded in the SQL editor. Generates, explains, debugs, and optimizes SQL. Schema-aware and context-sensitive to your warehouse.</p>
# MAGIC     </td>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-green-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-green" style="font-size:14pt;vertical-align:middle;">13+ Functions</span> <span style="font-size:15pt;font-weight:700;color:#00734d;vertical-align:middle;">AI Functions</span>
# MAGIC       <p style="font-size:14pt;">Embed LLM calls directly in SQL: <code>ai_query</code>, <code>ai_classify</code>, <code>ai_extract</code>, <code>ai_gen</code>, <code>ai_parse_document</code>, and more. Bring AI to the data.</p>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC   <div style="width:100%;overflow:hidden;border-radius:10px;border:1px solid #DCE0E2;">
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#f2f0ed;border-right:2px dashed #DCE0E2;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #ccc;color:#999;">Before (Traditional)</h4>
# MAGIC       <ul style="list-style:none;padding:0;margin:0;">
# MAGIC       <li class="c1pb-before-item" style="font-size:14pt;">Business users submit ticket to analyst</li>
# MAGIC       <li class="c1pb-before-item" style="font-size:14pt;">Days-long wait for query results</li>
# MAGIC       <li class="c1pb-before-item" style="font-size:14pt;">AI/ML requires separate Python pipeline</li>
# MAGIC       <li class="c1pb-before-item" style="font-size:14pt;">Data extraction for LLM processing</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#fff;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #00A972;color:#00A972;">After (Intelligent Access)</h4>
# MAGIC       <ul style="list-style:none;padding:0;margin:0;">
# MAGIC       <li class="c1pb-after-item-green" style="font-size:14pt;">Business users ask questions directly</li>
# MAGIC       <li class="c1pb-after-item-green" style="font-size:14pt;">Instant self-service answers</li>
# MAGIC       <li class="c1pb-after-item-green" style="font-size:14pt;">AI functions run inside SQL queries</li>
# MAGIC       <li class="c1pb-after-item-green" style="font-size:14pt;">LLM processing at the data layer</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="clear:both;"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- PANEL 1: Intelligent Execution -->
# MAGIC <div class="c1pb-panel" id="c1pb-panel-1" style="display:none;">
# MAGIC   <div class="c1pb-metric c1pb-km-teal">
# MAGIC     <div class="c1pb-metric-value" style="font-size:20pt;">4 Optimization Layers</div>
# MAGIC     <div class="c1pb-metric-label" style="font-size:14pt;">From compute engine to workload management -- every layer learns and adapts</div>
# MAGIC   </div>
# MAGIC   <table style="width:100% !important;border-spacing:10px;table-layout:fixed !important;">
# MAGIC   <tr>
# MAGIC     <td style="width:25%;vertical-align:top;" class="c1pb-card c1pb-teal-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-teal" style="font-size:14pt;vertical-align:middle;">18x Faster</span> <span style="font-size:14pt;font-weight:700;color:#1B5162;vertical-align:middle;">Photon Engine</span>
# MAGIC       <p style="font-size:14pt;">Native C++ vectorized execution engine. Processes data in CPU cache-friendly columnar batches. Delivers 18x speedup on star schema queries.</p>
# MAGIC     </td>
# MAGIC     <td style="width:25%;vertical-align:top;" class="c1pb-card c1pb-teal-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-teal" style="font-size:14pt;vertical-align:middle;">Zero Indexes</span> <span style="font-size:14pt;font-weight:700;color:#1B5162;vertical-align:middle;">Predictive I/O</span>
# MAGIC       <p style="font-size:14pt;">Deep learning replaces traditional indexes. The system learns data access patterns and predicts which files to read, skipping unnecessary I/O. Requires Photon.</p>
# MAGIC     </td>
# MAGIC     <td style="width:25%;vertical-align:top;" class="c1pb-card c1pb-teal-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-teal" style="font-size:14pt;vertical-align:middle;">Serverless</span> <span style="font-size:14pt;font-weight:700;color:#1B5162;vertical-align:middle;">IWM</span>
# MAGIC       <p style="font-size:14pt;">AI-driven query routing and proactive scaling. ML predicts resource needs and provisions from the warm pool in seconds. Serverless SQL Warehouses only.</p>
# MAGIC     </td>
# MAGIC     <td style="width:25%;vertical-align:top;" class="c1pb-card c1pb-teal-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-teal" style="font-size:14pt;vertical-align:middle;">25% Boost</span> <span style="font-size:14pt;font-weight:700;color:#1B5162;vertical-align:middle;">PQE</span>
# MAGIC       <p style="font-size:14pt;">Re-plans queries in real time using actual intermediate results. Adapts mid-execution when initial optimizer estimates were wrong. 25% performance improvement.</p>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC   <div style="width:100%;overflow:hidden;border-radius:10px;border:1px solid #DCE0E2;">
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#f2f0ed;border-right:2px dashed #DCE0E2;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #ccc;color:#999;">Before (Traditional)</h4>
# MAGIC       <ul style="list-style:none;padding:0;margin:0;">
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">JVM-based engine with serialization overhead</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Full table scans or manually maintained indexes</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Static cluster sizing (over/under provisioned)</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Fixed query plans even when estimates are wrong</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#fff;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #1B5162;color:#1B5162;">After (Intelligent Execution)</h4>
# MAGIC       <ul class="c1pb-comp-list">
# MAGIC         <li class="c1pb-after-item-teal" style="font-size:14pt;">Native C++ vectorized processing (Photon)</li>
# MAGIC         <li class="c1pb-after-item-teal" style="font-size:14pt;">ML-powered file skipping (Predictive I/O)</li>
# MAGIC         <li class="c1pb-after-item-teal" style="font-size:14pt;">AI-driven auto-scaling and routing (IWM)</li>
# MAGIC         <li class="c1pb-after-item-teal" style="font-size:14pt;">Real-time adaptive replanning (PQE)</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="clear:both;"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- PANEL 2: Intelligent Maintenance -->
# MAGIC <div class="c1pb-panel" id="c1pb-panel-2" style="display:none;">
# MAGIC   <div class="c1pb-metric c1pb-km-amber">
# MAGIC     <div class="c1pb-metric-value" style="font-size:20pt;">Zero-Touch Table Management</div>
# MAGIC     <div class="c1pb-metric-label" style="font-size:14pt;">The warehouse maintains itself -- optimization, clustering, and statistics on autopilot</div>
# MAGIC   </div>
# MAGIC   <table style="width:100% !important;border-spacing:10px;table-layout:fixed !important;">
# MAGIC   <tr>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-amber-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-amber" style="font-size:14pt;vertical-align:middle;">Default ON</span> <span style="font-size:14pt;font-weight:700;color:#8a6300;vertical-align:middle;">Predictive Optimization</span>
# MAGIC       <p style="font-size:14pt;">Automatically runs OPTIMIZE, VACUUM, and ANALYZE based on usage patterns. Enabled by default. Eliminates the most common DBA maintenance tasks entirely.</p>
# MAGIC     </td>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-amber-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-amber" style="font-size:14pt;vertical-align:middle;">AUTO Mode</span> <span style="font-size:14pt;font-weight:700;color:#8a6300;vertical-align:middle;">Auto Liquid Clustering</span>
# MAGIC       <p style="font-size:14pt;">The system analyzes query patterns and selects the best clustering keys automatically. Replaces static partitioning and manual Z-ORDER with a self-tuning layout.</p>
# MAGIC     </td>
# MAGIC     <td style="width:33%;vertical-align:top;" class="c1pb-card c1pb-amber-card">
# MAGIC       <span class="c1pb-badge c1pb-badge-amber" style="font-size:14pt;vertical-align:middle;">Always Fresh</span> <span style="font-size:14pt;font-weight:700;color:#8a6300;vertical-align:middle;">Automatic Statistics</span>
# MAGIC       <p style="font-size:14pt;">Continuously collects table and column statistics without manual ANALYZE TABLE commands. The optimizer always has fresh data to generate the best query plans.</p>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   </table>
# MAGIC   <div style="width:100%;overflow:hidden;border-radius:10px;border:1px solid #DCE0E2;">
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#f2f0ed;border-right:2px dashed #DCE0E2;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #ccc;color:#999;">Before (Traditional)</h4>
# MAGIC       <ul style="list-style:none;padding:0;margin:0;">
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Manual OPTIMIZE scheduling via cron jobs</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Forgotten VACUUM leads to storage bloat</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Hand-picked partition columns at table creation</li>
# MAGIC         <li class="c1pb-before-item" style="font-size:14pt;">Stale statistics cause bad query plans</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="float:left;width:50%;box-sizing:border-box;padding:14px 20px;background:#fff;min-height:200px;">
# MAGIC       <h4 style="font-size:14pt;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #C48E00;color:#C48E00;">After (Intelligent Maintenance)</h4>
# MAGIC       <ul style="list-style:none;padding:0;margin:0;">
# MAGIC         <li class="c1pb-after-item-amber" style="font-size:14pt;">Automatic compaction based on file patterns</li>
# MAGIC         <li class="c1pb-after-item-amber" style="font-size:14pt;">Proactive cleanup of obsolete data versions</li>
# MAGIC         <li class="c1pb-after-item-amber" style="font-size:14pt;">Self-tuning clustering adapts to real queries</li>
# MAGIC         <li class="c1pb-after-item-amber" style="font-size:14pt;">Statistics always current, plans always optimal</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div style="clear:both;"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function c1Tab(n) {
# MAGIC   var panels = document.querySelectorAll('.c1pb-panel');
# MAGIC   var labels = document.querySelectorAll('.c1pb-tab');
# MAGIC   var colors = ['#00A972', '#1B5162', '#C48E00'];
# MAGIC   panels.forEach(function(p) { p.style.display = 'none'; });
# MAGIC   labels.forEach(function(l) { l.style.background = '#e8ebed'; l.style.color = '#666'; l.style.borderColor = '#DCE0E2'; });
# MAGIC   document.getElementById('c1pb-panel-' + n).style.display = 'block';
# MAGIC   var activeTab = document.getElementById('c1pb-tab-' + n);
# MAGIC   activeTab.style.background = colors[n];
# MAGIC   activeTab.style.color = '#fff';
# MAGIC   activeTab.style.borderColor = colors[n];
# MAGIC }
# MAGIC </script>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Databricks AI: The Intelligence Foundation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Compound AI system:</strong> combines AI models, retrieval, ranking, and personalization systems. Draws signals from Unity Catalog metadata, query logs, data lineage, column statistics, and business terminology. It is not a standalone product you interact with directly; it is the brain behind every other feature listed here.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Auto-generates documentation:</strong> produces table and column comments for Unity Catalog objects. Already generating the "vast majority" of new documentation for customers. Powers intelligent search, semantic autocomplete, and data-aware suggestions across the workspace.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>What signals it uses:</strong> table and column names with descriptions, primary and foreign key relationships, query logs and usage patterns, data lineage tracked by Unity Catalog, column statistics and data distributions, and business-specific terminology, acronyms, and metrics.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Databricks AI is to Databricks SQL what the institutional knowledge of a 20-year veteran analyst is to a company. It knows what the columns mean, which tables join, what terminology your business uses, and which queries run most often.</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 1: Intelligent Access</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Genie Spaces:</strong> natural-language-to-SQL for business users. Stateful conversations with follow-up questions. Row-level security enforced through Unity Catalog. Companion Genie Spaces created automatically with new dashboards. Genie Spaces API and Managed MCP Server (Beta) available for embedding in apps and AI agent frameworks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Genie Code:</strong> AI coding assistant for data practitioners. Generates and runs code in notebooks, SQL editors, and pipeline editors. Agent Mode reached GA in May 2026, adapting behavior to context (EDA in notebooks, ETL in pipeline editors, debugging in error states). Distinct from Genie Spaces, which targets business users.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI Functions (13+):</strong> <code>ai_query</code>, <code>ai_classify</code>, <code>ai_extract</code>, <code>ai_gen</code>, <code>ai_parse_document</code>, <code>ai_similarity</code>, <code>ai_forecast</code>, <code>ai_fix_grammar</code>, <code>ai_mask</code>, <code>ai_translate</code>, <code>ai_summarize</code>, <code>ai_analyze_sentiment</code>, and <code>vector_search</code>. Call any AI model from a SQL query with automatic parallelization, retries, and scaling. Run in production batch pipelines (DBSQL, notebooks, Workflows, DLT). Up to 3x faster performance as of DAIS 2025.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Competitive edge:</strong> no other cloud data warehouse offers this breadth of natural language access combined with embedded AI functions in SQL.</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 2: Intelligent Execution</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Photon Engine:</strong> native C++ vectorized engine with SIMD instructions. Processes data in columnar batches, eliminating JVM garbage collection overhead. AQE integration dynamically changes join strategies, coalesces partitions, and handles skew. Runtime filters deliver up to 18x improvement on star schema workloads. Photon Vectorized Shuffle (DAIS 2025) keeps data columnar during shuffle for 1.5x higher throughput.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Predictive I/O:</strong> deep learning determines optimal access patterns, predicts matching row locations, and skips unnecessary data and columns. Eliminates the need for traditional B-tree or secondary indexes. Accelerated updates use deletion vectors instead of full file rewrites. Requires Photon. Available on Serverless and Pro SQL Warehouses. Zero configuration required.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Intelligent Workload Management (IWM):</strong> AI-driven query routing, preemptive scaling, and demand prediction for serverless warehouses. ML models predict resource requirements and provision clusters from the serverless warm pool in seconds. Continuously adapts to workload patterns. Serverless SQL warehouses only. Maximum queue: 1,000 queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Predictive Query Execution (PQE):</strong> real-time query re-optimization that monitors running tasks, collects metrics (spill size, CPU usage, task progress), and replans query stages mid-flight. The key evolution beyond AQE: PQE does not wait for a stage to complete before replanning. Combined with Photon Vectorized Shuffle for a 25% additional performance boost. No configuration required.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>How they work together:</strong> the query optimizer picks the plan, Photon executes it, Predictive I/O decides what data to read, PQE adjusts mid-flight if conditions change, and IWM decides which compute to use. All automatic, all simultaneous.</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 3: Intelligent Maintenance</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Predictive Optimization:</strong> autonomous OPTIMIZE, VACUUM, and ANALYZE operations on Unity Catalog managed tables. Uses data-driven triggers (not fixed schedules) with workload modeling and cost-benefit analysis. Enabled by default for all new UC tables, workspaces, and accounts. Scale: exabytes of unreferenced data vacuumed (saving tens of millions in storage), hundreds of petabytes compacted, millions of tables under management. Settings cascade from account to catalog to schema with overrides at each tier.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Automatic Liquid Clustering:</strong> AUTO mode examines query workloads, identifies optimal clustering columns, and adapts when patterns change. Only modifies keys when the predicted cost savings outweigh the clustering cost. Replaces manual partitioning and Z-ORDER entirely. Clustering keys can be changed without rewriting existing data. Customer results: up to 10x faster queries on large tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Predictive Optimization is like a self-maintaining building. The HVAC adjusts to occupancy, the lights follow the sun, and the elevators learn traffic patterns. You do not hire someone to flip switches.</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What This Means Competitively</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Snowflake:</strong> has automatic clustering and Cortex AI functions, but no natural language access at this depth (Cortex Analyst is more limited), no Predictive I/O equivalent (no ML-driven data skipping), and no real-time query replanning (no PQE equivalent). Automatic clustering requires manual key selection; Databricks AUTO mode selects keys for you.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Redshift:</strong> has ML-based automatic tuning and a sort key advisor, but no natural language access, no embedded AI functions in SQL, and no real-time query replanning. Concurrency scaling is rule-based, not AI-driven.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>BigQuery:</strong> has BigQuery ML and emerging Gemini integration, but no unified intelligence engine like Databricks AI, no Predictive I/O, and no automatic clustering key selection.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Only Databricks</strong> has the full intelligence stack: a compound AI engine (Databricks AI) powering natural language access, intelligent execution, and self-maintenance across every layer. Snowflake's approach is managed simplicity; Databricks' approach is active intelligence where ML models learn and adapt to your specific workloads.</li>
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
# MAGIC
# MAGIC D ----------
# MAGIC ## E. AI-Infused Engine
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #555; margin-top: -4px;">Section D introduced the three pillars of intelligence. This section goes deeper into the engine: how Intelligent Workload Management and Predictive I/O work under the hood.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Intelligent Workload Management
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Intelligent Workload Management (IWM)</strong> is a set of AI-powered features exclusive to serverless SQL warehouses. IWM uses ML models to predict resource requirements, check available capacity, and provision clusters from the serverless warm pool in seconds rather than minutes. The result: queries execute immediately when capacity exists, and additional clusters are provisioned proactively when the system predicts demand will increase.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-iwm-comparison ── -->
# MAGIC <style>
# MAGIC .d1-iwm-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .d1-iwm-row {
# MAGIC   width: 100%; overflow: hidden;
# MAGIC }
# MAGIC .d1-iwm-pane {
# MAGIC   float: left;
# MAGIC   width: 48%;
# MAGIC   margin: 0 1%;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .d1-iwm-pane:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .d1-iwm-header {
# MAGIC   padding: 14px 20px;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .d1-iwm-header-old { background: linear-gradient(135deg, #98102A, #b21e3a); }
# MAGIC .d1-iwm-header-new { background: linear-gradient(135deg, #00A972, #00c285); }
# MAGIC .d1-iwm-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 18px 20px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .d1-iwm-items {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   margin: 0;
# MAGIC   padding-left: 20px;
# MAGIC }
# MAGIC .d1-iwm-metric {
# MAGIC   text-align: center;
# MAGIC   margin-top: 16px;
# MAGIC   padding: 14px;
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC }
# MAGIC .d1-iwm-metric-num {
# MAGIC   font-size: 22pt;
# MAGIC   font-weight: 800;
# MAGIC   color: #1B5162;
# MAGIC }
# MAGIC .d1-iwm-metric-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="d1-iwm-wrapper">
# MAGIC   <div class="d1-iwm-row">
# MAGIC     <div class="d1-iwm-pane">
# MAGIC       <div class="d1-iwm-header d1-iwm-header-old">Classic/Pro: Reactive Scaling</div>
# MAGIC       <div class="d1-iwm-body">
# MAGIC         <ul class="d1-iwm-items">
# MAGIC           <li>Manual scaling: you configure the number of clusters</li>
# MAGIC           <li>Reactive: adds clusters after load is detected</li>
# MAGIC           <li>Fixed autoscaling rules</li>
# MAGIC           <li>All queries wait during scale-up</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d1-iwm-pane">
# MAGIC       <div class="d1-iwm-header d1-iwm-header-new">Serverless: Intelligent Workload Management</div>
# MAGIC       <div class="d1-iwm-body">
# MAGIC         <ul class="d1-iwm-items">
# MAGIC           <li>AI-driven: ML predicts resource needs before queries queue</li>
# MAGIC           <li>Proactive: provisions from warm pool in seconds</li>
# MAGIC           <li>Continuously adapts to workload patterns</li>
# MAGIC           <li>Routes queries to available capacity immediately</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d1-iwm-metric">
# MAGIC     <div class="d1-iwm-metric-label">Available exclusively on Serverless SQL Warehouses. Maximum queue: 1,000 queries.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How IWM Works</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Query arrival:</strong> IWM forecasts resource needs and examines current capacity. If resources are available, the query executes immediately. If not, it enters the queue.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Continuous monitoring:</strong> the system monitors queue status and provisions additional clusters when wait times increase. During lower demand periods, resources scale down to optimize costs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> classic autoscaling is like a restaurant that waits until tables are full and customers are standing in line before calling in extra staff. IWM is like a restaurant that checks the reservation book and weather forecast, then schedules the right number of staff before the dinner rush arrives.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Proactive vs Reactive: Why It Matters</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>No more cold starts:</strong> classic/pro warehouses add clusters reactively after load is detected. IWM provisions from the serverless warm pool in seconds, before queues build up.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>No more idle waste:</strong> IWM detects demand drops and releases resources proactively, reducing idle compute costs.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When IWM Makes the Largest Difference</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Variable workloads:</strong> BI dashboards with morning peaks, ad-hoc exploration throughout the day, and batch ETL at night. IWM adapts to each phase.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/casey/lakehouse" style="color: #2574B5; font-size: 14pt;">Casey's General Stores</a> experienced resource contention on Azure Synapse where ingestion and analytics competed for the same compute. After migrating to Databricks SQL, their platform consistently meets internal SLAs for data freshness and response time. &#x25C6;</li>
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
# MAGIC ### E2. Automatic Data Layout and Predictive I/O
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Two sets of capabilities remove manual tuning from Databricks SQL. <strong>Predictive I/O</strong> uses deep learning to optimize how individual queries read and write data, eliminating the need for traditional indexes. <strong>Predictive Optimization</strong> and <strong>Automatic Liquid Clustering</strong> handle background maintenance: automatic OPTIMIZE, VACUUM, ANALYZE, and intelligent clustering key selection based on query patterns.</p>
# MAGIC
# MAGIC <!-- ── Visual: d2-two-pane-optimization ── -->
# MAGIC <style>
# MAGIC .d2-opt-wrapper {
# MAGIC   width: 100%;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .d2-opt-row {
# MAGIC   width: 100%;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .d2-opt-pane {
# MAGIC   float: left;
# MAGIC   width: 48%;
# MAGIC   margin: 0 1%;
# MAGIC   border-radius: 12px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .d2-opt-pane:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .d2-opt-header {
# MAGIC   padding: 14px 20px;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .d2-opt-header-layout { background: #1B5162; }
# MAGIC .d2-opt-header-pio { background: #00A972; }
# MAGIC .d2-opt-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 18px 20px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .d2-opt-subtitle {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #1B5162;
# MAGIC   margin: 12px 0 6px 0;
# MAGIC }
# MAGIC .d2-opt-subtitle-pio {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #00A972;
# MAGIC   margin: 12px 0 6px 0;
# MAGIC }
# MAGIC .d2-opt-list {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   margin: 4px 0 0 0;
# MAGIC   padding-left: 20px;
# MAGIC }
# MAGIC .d2-opt-stat {
# MAGIC   display: flex;
# MAGIC   gap: 16px;
# MAGIC   margin-top: 16px;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .d2-opt-stat-box {
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 14px 24px;
# MAGIC   text-align: center;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .d2-opt-stat-num {
# MAGIC   font-size: 22pt;
# MAGIC   font-weight: 800;
# MAGIC   color: #1B5162;
# MAGIC }
# MAGIC .d2-opt-stat-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="d2-opt-wrapper">
# MAGIC   <div class="d2-opt-row">
# MAGIC     <div class="d2-opt-pane">
# MAGIC       <div class="d2-opt-header d2-opt-header-pio">Predictive I/O</div>
# MAGIC       <div class="d2-opt-body">
# MAGIC         <div class="d2-opt-subtitle-pio">Accelerated Reads</div>
# MAGIC         <ul class="d2-opt-list">
# MAGIC           <li>Deep learning determines optimal access patterns</li>
# MAGIC           <li>Eliminates unnecessary column and row decoding</li>
# MAGIC           <li>Predicts matching rows, skipping non-relevant data</li>
# MAGIC         </ul>
# MAGIC         <div class="d2-opt-subtitle-pio">Accelerated Updates</div>
# MAGIC         <ul class="d2-opt-list">
# MAGIC           <li>Deletion vectors instead of full file rewrites</li>
# MAGIC           <li>UPDATE, DELETE, MERGE operate on vectors, not files</li>
# MAGIC           <li>Requires Photon. Available on Serverless and Pro SQL Warehouses</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="d2-opt-pane">
# MAGIC       <div class="d2-opt-header d2-opt-header-layout">Predictive Optimization + Auto Liquid Clustering</div>
# MAGIC       <div class="d2-opt-body">
# MAGIC         <div class="d2-opt-subtitle">Predictive Optimization</div>
# MAGIC         <ul class="d2-opt-list">
# MAGIC           <li><strong>OPTIMIZE:</strong> incremental clustering and file size optimization</li>
# MAGIC           <li><strong>VACUUM:</strong> storage cleanup of stale files</li>
# MAGIC           <li><strong>ANALYZE:</strong> statistics refresh for query planning</li>
# MAGIC           <li>Enabled at account/catalog/schema level with inheritance</li>
# MAGIC         </ul>
# MAGIC         <div class="d2-opt-subtitle">Automatic Liquid Clustering</div>
# MAGIC         <ul class="d2-opt-list">
# MAGIC           <li>Selects optimal clustering keys from query patterns (DBR 15.4+)</li>
# MAGIC           <li>Adapts when workload patterns change</li>
# MAGIC           <li>Does not run Z-ORDER</li>
# MAGIC         </ul>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Predictive I/O: Reads and Updates</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Accelerated reads:</strong> deep learning models analyze table metadata and query patterns to predict which Parquet file row groups contain matching data. The engine skips row groups with no matching records and avoids unnecessary column or row decoding, reading only what is needed from cloud storage.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Predictive I/O accelerated reads work like a librarian who has memorized where every book is. Instead of scanning every shelf, the librarian walks directly to the right location. Traditional scanning checks every shelf in the library.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Replaces indexes:</strong> traditional databases rely on B-tree or hash indexes for fast lookups. These indexes consume storage, require maintenance, and slow down writes. Predictive I/O provides comparable lookup performance without those trade-offs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Accelerated updates:</strong> deletion vectors mark removed rows without rewriting entire files. UPDATE, DELETE, and MERGE operations write small vector files instead of rewriting large Parquet files.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> deletion vectors are like crossing out entries in a ledger with a pencil mark instead of rewriting the entire page. The page stays intact, with annotations about which lines are no longer valid.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Requirements:</strong> Predictive I/O requires Photon and is available on Serverless and Pro SQL Warehouses. Zero configuration required.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Predictive Optimization + Automatic Liquid Clustering</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Predictive Optimization</strong> automates three maintenance operations on Unity Catalog managed tables: OPTIMIZE (incremental clustering and file compaction), VACUUM (storage cleanup of stale files), and ANALYZE (statistics refresh). It runs in the background using serverless compute, triggered by data-driven signals rather than fixed schedules.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Automatic Liquid Clustering</strong> (DBR 15.4+) examines query workloads, identifies optimal clustering columns, and adapts when patterns change. It selects and adjusts clustering keys automatically. It does not run Z-ORDER.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Configuration:</strong> Predictive Optimization is enabled at the account, catalog, or schema level with inheritance. Settings cascade downward with overrides at each tier.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Predictive Optimization is like a self-maintaining building. The HVAC adjusts to occupancy, the lights follow the sun, and the elevators learn traffic patterns. You do not hire someone to flip switches.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Common Confusion</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Students often mix these up because of similar names. Predictive I/O optimizes query execution at read/write time (smarter scans, deletion vectors). Predictive Optimization automates table maintenance in the background (OPTIMIZE, VACUUM, ANALYZE). They are complementary but separate.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>When each runs:</strong> Predictive I/O is active during every query (query time). Predictive Optimization runs in the background between queries (maintenance time).</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/anyclip" style="color: #2574B5; font-size: 14pt;">AnyClip</a> migrated from Amazon Redshift and saw query performance improve by 98%, with some tables containing several terabytes of data. The combination of Photon and Predictive I/O drove these results. &#x25C6;</li>
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
# MAGIC ## F. Performance and Benchmarks

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. TPC-DS Results and Competitive Positioning
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks SQL set the official <strong>100TB TPC-DS world record</strong>, outperforming the previous record by 2.2x with superior price/performance (formally audited by the TPC council). Independent research from the Barcelona Supercomputing Center found Databricks was 2.7x faster and 12x better in price/performance compared to Snowflake. The platform is now <strong>77% faster</strong> than when it launched in 2022, with continuous improvements tracked by the Databricks Performance Index.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-benchmark-bars ── -->
# MAGIC <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px 0;">
# MAGIC <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:12px;">Databricks SQL Benchmark Results</div>
# MAGIC <table style="width:100%;border-collapse:collapse;border-spacing:0;table-layout:fixed;">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th style="padding:14px 16px;font-size:15pt;font-weight:600;text-align:left;border:1px solid #1B3139;background:#1B3139;color:#fff;">Benchmark</th>
# MAGIC       <th style="padding:14px 16px;font-size:15pt;font-weight:600;text-align:center;border:1px solid #1B3139;background:#1B3139;color:#fff;">Comparison</th>
# MAGIC       <th style="padding:14px 16px;font-size:15pt;font-weight:600;text-align:center;border:1px solid #1B3139;background:#1B3139;color:#fff;">Performance</th>
# MAGIC       <th style="padding:14px 16px;font-size:15pt;font-weight:600;text-align:center;border:1px solid #1B3139;background:#1B3139;color:#fff;">Price/Performance</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:left;">TPC-DS 100TB (official)</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:center;">vs Previous Record</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">2.2x faster</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">Superior</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:left;">Barcelona Supercomputing Center</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:center;">vs Snowflake</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">2.7x faster</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">12x better</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:left;">TPC-DI ETL (~1TB)</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:center;">vs Snowflake Gen1</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">4.3x faster</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">4.1x better TCO</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:left;">TPC-DI ETL (~1TB)</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;color:#333;border:1px solid #DCE0E2;text-align:center;">vs Snowflake Gen2</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">2.8x faster</td>
# MAGIC       <td style="padding:14px 16px;font-size:15pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">3.6x better TCO</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <p style="font-size:14pt;color:#5A6F77;text-align:center;margin:10px 0;font-style:italic;">TPC-DS and TPC-DI are standard industry benchmarks. Results depend on scale, configuration, and workload type.</p>
# MAGIC <table style="width:100%;border-collapse:collapse;border-spacing:0;table-layout:fixed;margin-top:8px;">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th colspan="3" style="padding:12px 16px;font-size:15pt;font-weight:600;text-align:center;background:#1B3139;color:#fff;border:1px solid #1B3139;">Continuous Improvement</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="width:33.33%;padding:18px 16px;text-align:center;border:1px solid #DCE0E2;background:#EEEDE9;">
# MAGIC         <div style="font-size:26pt;font-weight:800;color:#00A972;">77%</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;margin-top:4px;">Faster than 2022 launch</div>
# MAGIC       </td>
# MAGIC       <td style="width:33.33%;padding:18px 16px;text-align:center;border:1px solid #DCE0E2;background:#EEEDE9;">
# MAGIC         <div style="font-size:26pt;font-weight:800;color:#1B5162;">14%</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;margin-top:4px;">BI workload improvement (5 months)</div>
# MAGIC       </td>
# MAGIC       <td style="width:33.33%;padding:18px 16px;text-align:center;border:1px solid #DCE0E2;background:#EEEDE9;">
# MAGIC         <div style="font-size:26pt;font-weight:800;color:#1B5162;">9%</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;margin-top:4px;">ETL workload improvement (5 months)</div>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Understanding TPC-DS</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> TPC-DS is like a standardized test for data warehouses. It does not measure everything, but it provides a consistent way to compare performance under controlled conditions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>What it measures:</strong> 99 queries covering a range of analytics patterns including joins, aggregations, subqueries, and window functions across multiple fact and dimension tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Scale matters:</strong> the 100TB world record tests performance at enterprise scale. Independent benchmarks at smaller scales (like the Fivetran study) show narrower gaps between vendors. The specific workload and data volume determine which platform performs best.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ETL Benchmark Methodology</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>TPC-DI (Data Integration):</strong> measures ETL pipeline performance at a scale factor of 10,000 (approximately 1TB raw data). Both Databricks SQL Serverless and Snowflake were tested with out-of-the-box configurations using dbt Core for orchestration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Price/performance advantage:</strong> fuel efficiency is like price/performance in cars. A car might be fast, but if it costs 10x the fuel per mile, the total cost of the road trip is what matters. Databricks optimizes for both speed and cost.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Continuous Improvement</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Databricks Performance Index:</strong> a statistical measure derived from billions of production queries that tracks DBSQL improvements over time. This is a Databricks-specific methodology, not an industry-standard metric.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/rbi" style="color: #2574B5; font-size: 14pt;">Raiffeisen Bank International</a> migrated from Oracle and DB2 to Databricks SQL. Queries that took 30 days on legacy infrastructure now complete in about 12 minutes. &#x25C6;</li>
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
# MAGIC ### F2. Feature Matrix: Classic vs Pro vs Serverless
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The three SQL warehouse types follow a <strong>Good, Better, Best</strong> model. Serverless includes all features; Classic and Pro lack progressively more capabilities. Serverless is the default and recommended choice for most workloads.</p>
# MAGIC
# MAGIC <!-- ── Visual: f2-feature-matrix ── -->
# MAGIC <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px 0;">
# MAGIC <table style="width:100%;border-collapse:collapse;border-spacing:0;table-layout:fixed;">
# MAGIC   <colgroup>
# MAGIC     <col style="width:34%;" />
# MAGIC     <col style="width:22%;" />
# MAGIC     <col style="width:22%;" />
# MAGIC     <col style="width:22%;" />
# MAGIC   </colgroup>
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th style="padding:14px 16px;font-size:14pt;font-weight:700;text-align:left;border:1px solid #DCE0E2;background:#1B3139;color:#fff;">Feature</th>
# MAGIC       <th style="padding:14px 16px;font-size:14pt;font-weight:700;text-align:center;border:1px solid #DCE0E2;background:#90A5B1;color:#fff;">Classic</th>
# MAGIC       <th style="padding:14px 16px;font-size:14pt;font-weight:700;text-align:center;border:1px solid #DCE0E2;background:#618794;color:#fff;">Pro</th>
# MAGIC       <th style="padding:14px 16px;font-size:14pt;font-weight:700;text-align:center;border:1px solid #DCE0E2;background:#1B5162;color:#fff;">Serverless</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td colspan="4" style="padding:12px 16px;font-size:14pt;font-weight:700;color:#1B5162;background:#EEEDE9;border:1px solid #DCE0E2;text-align:left;">Performance</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Photon Engine</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Predictive I/O</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Intelligent Workload Management</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Query Result Caching</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td colspan="4" style="padding:12px 16px;font-size:14pt;font-weight:700;color:#1B5162;background:#EEEDE9;border:1px solid #DCE0E2;text-align:left;">Infrastructure</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Startup Time</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:center;">~4 minutes</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:center;">~4 minutes</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">2-6 seconds</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Compute Location</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:center;">Customer account</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:center;">Customer account</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:center;">Databricks account</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Serverless Compute</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td colspan="4" style="padding:12px 16px;font-size:14pt;font-weight:700;color:#1B5162;background:#EEEDE9;border:1px solid #DCE0E2;text-align:left;">Capabilities</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Exploratory SQL + BI</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Management and Governance</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Query Federation / Materialized Views</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F9F7F4;">
# MAGIC       <td style="padding:12px 16px;font-size:14pt;color:#333;border:1px solid #DCE0E2;text-align:left;font-weight:500;">Python UDFs</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#98102A;font-weight:700;">&#x2717;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC       <td style="padding:12px 16px;font-size:14pt;border:1px solid #DCE0E2;text-align:center;color:#00A972;font-weight:700;">&#x2713;</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <table style="width:100%;border-collapse:collapse;border-spacing:0;table-layout:fixed;margin-top:16px;">
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="width:33.33%;padding:14px;text-align:center;background:#EEEDE9;border:1px solid #DCE0E2;border-radius:8px;">
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#0b2026;">Classic</div>
# MAGIC         <div style="font-size:16pt;font-weight:800;color:#1B5162;margin:4px 0;">~$0.22/DBU</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;">+ cloud infra costs</div>
# MAGIC       </td>
# MAGIC       <td style="width:33.33%;padding:14px;text-align:center;background:#EEEDE9;border:1px solid #DCE0E2;border-radius:8px;">
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#0b2026;">Pro</div>
# MAGIC         <div style="font-size:16pt;font-weight:800;color:#1B5162;margin:4px 0;">~$0.55/DBU</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;">+ cloud infra costs</div>
# MAGIC       </td>
# MAGIC       <td style="width:33.33%;padding:14px;text-align:center;background:#EEEDE9;border:1px solid #DCE0E2;border-radius:8px;">
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#0b2026;">Serverless</div>
# MAGIC         <div style="font-size:16pt;font-weight:800;color:#1B5162;margin:4px 0;">~$0.70/DBU</div>
# MAGIC         <div style="font-size:14pt;color:#5A6F77;">Infra included, scales to zero</div>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pricing: Higher DBU Rate Does Not Mean Higher Total Cost</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Common assumption:</strong> students often assume serverless is more expensive because the per-DBU rate is higher ($0.70 vs $0.22 for Classic). In practice, serverless often costs less because it includes infrastructure, starts instantly (no idle warm-up), and scales to zero between queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Classic hidden costs:</strong> the $0.22/DBU rate does not include the EC2/VM costs, which the customer pays separately. Adding those costs typically brings the effective rate closer to serverless.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Idle compute waste:</strong> Classic and Pro warehouses with a 45-minute auto-stop default accumulate idle compute charges between queries. Serverless can auto-stop in as little as 5 minutes.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Feature Availability: What You Give Up</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Classic lacks:</strong> Predictive I/O, Query Federation, Materialized Views, Python UDFs, IWM, Query Result Caching. It provides Photon and basic SQL capabilities.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Pro adds:</strong> Predictive I/O, Query Federation, Materialized Views, and Python UDFs. It does not include IWM or serverless compute.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Serverless adds:</strong> IWM, instant startup, Query Result Caching, and fully managed infrastructure. This is the recommended starting point.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Sizing and Concurrency</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Cluster sizing:</strong> ranges from X-Small through 5X-Large. Each cluster handles approximately 10 concurrent queries. Set the maximum cluster count based on expected peak concurrent queries divided by 10.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>5X-Large warehouse (Public Preview April 2026):</strong> a new 5X-Large size provides 512 workers for serverless and pro warehouses, supporting the largest analytical workloads.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Queue capacity:</strong> all warehouse types support up to 1,000 queries in the queue. Monitor the Peak Queued Queries metric to determine if you need to increase the cluster count.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/att-azure-databricks" style="color: #2574B5; font-size: 14pt;">AT&amp;T</a> consolidated siloed data warehouses onto Azure Databricks, reducing more than 80 schemas and achieving a five-year ROI of 300%. &#x25C6;</li>
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
# MAGIC ## G. SQL Programming and Stored Procedures
# MAGIC
# MAGIC Sections A through F covered what Databricks SQL is and how its engine performs. This section shifts to what you can build on the platform. SQL scripting and stored procedures bring procedural programming capabilities to Databricks SQL, enabling warehouse migrations from Oracle, SQL Server, and Teradata while providing a standards-based programming model for new development.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. SQL Scripting Capabilities
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks SQL scripting follows the <strong>SQL/PSM (Persistent Stored Modules)</strong> standard, the same ANSI/ISO specification that underpins procedural SQL in PostgreSQL, MySQL, and DB2. This is not T-SQL (SQL Server) or PL/SQL (Oracle). It is the portable, open standard. Scripts run on SQL warehouses, serverless compute, and Databricks Runtime 16.3+ clusters.</p>
# MAGIC
# MAGIC <!-- ── Visual: g1-v-scripting-capabilities ── -->
# MAGIC <style>
# MAGIC .g1-tab-wrap {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC   width: 100%;
# MAGIC }
# MAGIC .g1-tab-bar {
# MAGIC   display: flex;
# MAGIC   gap: 0;
# MAGIC   border-bottom: 3px solid #1B5162;
# MAGIC }
# MAGIC .g1-tab-btn {
# MAGIC   padding: 12px 20px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #555;
# MAGIC   background: #E8E4DF;
# MAGIC   border: none;
# MAGIC   border-radius: 8px 8px 0 0;
# MAGIC   cursor: pointer;
# MAGIC   transition: background 0.2s, color 0.2s;
# MAGIC   margin-right: 2px;
# MAGIC }
# MAGIC .g1-tab-btn:hover {
# MAGIC   background: #D0CBC4;
# MAGIC }
# MAGIC .g1-tab-btn.g1-tab-active {
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .g1-tab-panel {
# MAGIC   display: none;
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #D0CBC4;
# MAGIC   border-top: none;
# MAGIC   border-radius: 0 0 8px 8px;
# MAGIC   padding: 24px 28px;
# MAGIC }
# MAGIC .g1-tab-panel.g1-tab-active {
# MAGIC   display: block;
# MAGIC }
# MAGIC .g1-tab-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC   margin-bottom: 16px;
# MAGIC }
# MAGIC .g1-tab-code {
# MAGIC   background: #272822;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 6px;
# MAGIC   font-family: 'Courier New', monospace;
# MAGIC   font-size: 14pt;
# MAGIC   line-height: 1.6;
# MAGIC   color: #f8f8f2;
# MAGIC }
# MAGIC .g1-tab-migration {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   gap: 0;
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   margin-top: 20px;
# MAGIC }
# MAGIC .g1-tab-mig-source {
# MAGIC   flex: 1;
# MAGIC   padding: 14px 18px;
# MAGIC   background: #98102A;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC }
# MAGIC .g1-tab-mig-arrow {
# MAGIC   padding: 14px 16px;
# MAGIC   background: #1B3139;
# MAGIC   color: #00A972;
# MAGIC   font-size: 18pt;
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .g1-tab-mig-target {
# MAGIC   flex: 1;
# MAGIC   padding: 14px 18px;
# MAGIC   background: #00A972;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC }
# MAGIC </style>
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click each tab to see SQL Scripting capabilities</p>
# MAGIC <div class="g1-tab-wrap">
# MAGIC   <div class="g1-tab-bar">
# MAGIC     <button class="g1-tab-btn g1-tab-active" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p1')">Compound</button>
# MAGIC     <button class="g1-tab-btn" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p2')">Variables</button>
# MAGIC     <button class="g1-tab-btn" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p3')">Control Flow</button>
# MAGIC     <button class="g1-tab-btn" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p4')">Errors</button>
# MAGIC     <button class="g1-tab-btn" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p5')">Transactions</button>
# MAGIC     <button class="g1-tab-btn" onclick="((b,p)=>{document.querySelectorAll('.g1-tab-btn').forEach(t=>t.classList.remove('g1-tab-active'));document.querySelectorAll('.g1-tab-panel').forEach(t=>t.classList.remove('g1-tab-active'));b.classList.add('g1-tab-active');document.getElementById(p).classList.add('g1-tab-active')})(this,'g1p6')">Recursive</button>
# MAGIC   </div>
# MAGIC   <div id="g1p1" class="g1-tab-panel g1-tab-active">
# MAGIC     <div class="g1-tab-desc">BEGIN...END blocks group multiple SQL operations. Variables and handlers scoped to the block.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">BEGIN</span><br/>  <span style="color:#66d9ef;font-style:italic">DECLARE</span> x <span style="color:#66d9ef;font-style:italic">INT</span>;<br/>  <span style="color:#66d9ef;font-style:italic">SET</span> x = 42;<br/><span style="color:#66d9ef;font-style:italic">END</span>;</div>
# MAGIC   </div>
# MAGIC   <div id="g1p2" class="g1-tab-panel">
# MAGIC     <div class="g1-tab-desc">Session variables persist across cells. Local variables scoped to enclosing block. Both strongly typed.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">DECLARE</span> avg_price <span style="color:#66d9ef;font-style:italic">DOUBLE</span>;<br/><span style="color:#66d9ef;font-style:italic">SET</span> avg_price = (<span style="color:#66d9ef;font-style:italic">SELECT</span> <span style="color:#a6e22e">AVG</span>(price) <span style="color:#66d9ef;font-style:italic">FROM</span> orders);</div>
# MAGIC   </div>
# MAGIC   <div id="g1p3" class="g1-tab-panel">
# MAGIC     <div class="g1-tab-desc">IF/ELSEIF/ELSE for branching. LOOP, WHILE, REPEAT, FOR for iteration. CASE for inline value selection.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">IF</span> total &gt; 10000 <span style="color:#66d9ef;font-style:italic">THEN</span><br/>  <span style="color:#66d9ef;font-style:italic">SET</span> tier = <span style="color:#e6db74">'platinum'</span>;<br/><span style="color:#66d9ef;font-style:italic">END IF</span>;</div>
# MAGIC   </div>
# MAGIC   <div id="g1p4" class="g1-tab-panel">
# MAGIC     <div class="g1-tab-desc">DECLARE HANDLER for exception routing. SIGNAL to raise custom errors. SQLSTATE codes for structured diagnostics.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">DECLARE</span> EXIT HANDLER<br/>  FOR SQLSTATE <span style="color:#e6db74">'42000'</span><br/><span style="color:#66d9ef;font-style:italic">BEGIN</span> ... <span style="color:#66d9ef;font-style:italic">END</span>;</div>
# MAGIC   </div>
# MAGIC   <div id="g1p5" class="g1-tab-panel">
# MAGIC     <div class="g1-tab-desc">ATOMIC blocks provide all-or-nothing execution. All DML succeeds together or rolls back. No manual COMMIT needed.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">BEGIN ATOMIC</span><br/>  <span style="color:#66d9ef;font-style:italic">INSERT INTO</span> audit ...;<br/>  <span style="color:#66d9ef;font-style:italic">UPDATE</span> orders ...;<br/><span style="color:#66d9ef;font-style:italic">END</span>;</div>
# MAGIC   </div>
# MAGIC   <div id="g1p6" class="g1-tab-panel">
# MAGIC     <div class="g1-tab-desc">WITH RECURSIVE traverses hierarchies, graphs, and self-referencing structures natively in SQL. Available since Runtime 17.0.</div>
# MAGIC     <div class="g1-tab-code"><span style="color:#66d9ef;font-style:italic">WITH RECURSIVE</span> org(lvl,id) <span style="color:#66d9ef;font-style:italic">AS</span> (<br/>  <span style="color:#75715e">-- base + step</span><br/>)</div>
# MAGIC   </div>
# MAGIC   <div class="g1-tab-migration">
# MAGIC     <div class="g1-tab-mig-source">T-SQL / PL/SQL / Teradata BTEQ</div>
# MAGIC     <div class="g1-tab-mig-arrow">&#x2192;</div>
# MAGIC     <div class="g1-tab-mig-target">SQL/PSM on Databricks (ANSI Standard)</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">SQL/PSM: The Open Standard</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Not T-SQL, not PL/SQL:</strong> SQL/PSM (Persistent Stored Modules) is the ANSI/ISO standard for procedural SQL. Databricks chose this standard rather than a proprietary dialect. Skills transfer to PostgreSQL, MySQL, and DB2.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Available since Runtime 16.3:</strong> SQL scripting works on SQL warehouses, serverless compute, and Databricks Runtime 16.3+ clusters. Every compound statement must be the only statement in its notebook cell.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Why this matters for SAs:</strong> when a customer asks "can we run our stored procedures on Databricks?", the answer is yes, with a standards-based approach. The migration path from T-SQL or PL/SQL requires syntax adjustments, not a rewrite.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Migration Story</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>SQL Server (T-SQL):</strong> replace @variables with DECLARE, convert stored procedures to SQL/PSM syntax, adjust error handling from TRY/CATCH to DECLARE HANDLER.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Oracle (PL/SQL):</strong> convert packages to individual procedures, replace %ROWTYPE with explicit declarations, adjust exception blocks to SQLSTATE handlers.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Teradata (BTEQ):</strong> replace BTEQ scripts with BEGIN...END compound statements, convert .IF/.THEN to SQL IF, map ACTIVITY_COUNT to ROW_COUNT.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/blog/introducing-sql-stored-procedures-databricks" style="color: #2574B5; font-size: 14pt;">ClicTechnologies</a> used SQL Stored Procedures for a customer-segmentation pipeline, noting that SQL familiarity enabled rapid implementation and production deployment. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When to Use Scripting vs Declarative Pipelines</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Use SQL scripting when:</strong> you need conditional branching, multi-step transactions, error recovery, or iterative processing that cannot be expressed as a single SELECT or DLT pipeline.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Use declarative pipelines (DLT) when:</strong> the workflow is a straightforward data transformation chain from bronze to silver to gold. DLT handles dependency management, retry logic, and data quality checks automatically.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Combine both:</strong> stored procedures can be orchestrated by Lakeflow Jobs alongside DLT pipelines, Python notebooks, and dbt models. They are complementary tools, not alternatives.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Key Syntax Reference</strong>
# MAGIC <pre style="background:#1B3139; color:#e0e0e0; border-radius:6px; padding:12px 14px; font-family:monospace; font-size:14pt; line-height:1.6; margin:8px 0 0 0; white-space:pre; overflow-x:auto"><span style="color:#7ecbf5; font-weight:600">BEGIN</span>
# MAGIC   <span style="color:#7ecbf5; font-weight:600">DECLARE</span> threshold <span style="color:#7ecbf5; font-weight:600">INT DEFAULT</span> 1000;
# MAGIC   <span style="color:#7ecbf5; font-weight:600">DECLARE</span> row_count <span style="color:#7ecbf5; font-weight:600">INT</span>;
# MAGIC
# MAGIC   <span style="color:#7ecbf5; font-weight:600">SET</span> row_count = (<span style="color:#7ecbf5; font-weight:600">SELECT COUNT</span>(*) <span style="color:#7ecbf5; font-weight:600">FROM</span> staging);
# MAGIC
# MAGIC   <span style="color:#7ecbf5; font-weight:600">IF</span> row_count > threshold <span style="color:#7ecbf5; font-weight:600">THEN</span>
# MAGIC     <span style="color:#7ecbf5; font-weight:600">INSERT INTO</span> production <span style="color:#7ecbf5; font-weight:600">SELECT</span> * <span style="color:#7ecbf5; font-weight:600">FROM</span> staging;
# MAGIC   <span style="color:#7ecbf5; font-weight:600">ELSE</span>
# MAGIC     <span style="color:#7ecbf5; font-weight:600">SIGNAL</span> SQLSTATE <span style="color:#e6db74">'45000'</span>
# MAGIC       <span style="color:#7ecbf5; font-weight:600">SET</span> MESSAGE_TEXT = <span style="color:#e6db74">'Insufficient rows'</span>;
# MAGIC   <span style="color:#7ecbf5; font-weight:600">END IF</span>;
# MAGIC <span style="color:#7ecbf5; font-weight:600">END</span>;</pre>
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
# MAGIC ### G2. Stored Procedures on Databricks
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Stored procedures turn SQL scripts into <strong>reusable, governed Unity Catalog objects</strong>. Available since Databricks Runtime 17.0, they are registered as <code>catalog.schema.procedure_name</code> and support input (IN), output (OUT), and bidirectional (INOUT) parameters. All procedures run under SQL SECURITY INVOKER, meaning they execute with the caller's identity and permissions.</p>
# MAGIC
# MAGIC <!-- ── Visual: g2-v-procedure-lifecycle ── -->
# MAGIC <style>
# MAGIC .g2-v-wrap {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC   max-width: 100%;
# MAGIC }
# MAGIC .g2-v-lifecycle {
# MAGIC   display: flex;
# MAGIC   align-items: stretch;
# MAGIC   gap: 0;
# MAGIC   border-radius: 10px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 10px rgba(0,0,0,0.10);
# MAGIC   margin-bottom: 20px;
# MAGIC }
# MAGIC .g2-v-step {
# MAGIC   flex: 1;
# MAGIC   text-align: center;
# MAGIC   padding: 18px 12px;
# MAGIC   background: #F9F7F4;
# MAGIC   border-right: 2px solid #EEEDE9;
# MAGIC   transition: background 0.25s, transform 0.25s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .g2-v-step:last-child { border-right: none; }
# MAGIC .g2-v-step:hover {
# MAGIC   background: #e8f4f8;
# MAGIC   transform: translateY(-2px);
# MAGIC }
# MAGIC .g2-v-step-icon {
# MAGIC   font-size: 24pt;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .g2-v-step-label {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B5162;
# MAGIC }
# MAGIC .g2-v-step-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #555;
# MAGIC   margin-top: 6px;
# MAGIC   line-height: 1.4;
# MAGIC }
# MAGIC .g2-v-code-box {
# MAGIC   background: #272822;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 20px 24px;
# MAGIC   font-family: 'Courier New', monospace;
# MAGIC   font-size: 14pt;
# MAGIC   line-height: 1.8;
# MAGIC   color: #f8f8f2;
# MAGIC   overflow-x: auto;
# MAGIC   margin-bottom: 16px;
# MAGIC }
# MAGIC .g2-v-params {
# MAGIC   display: flex;
# MAGIC   gap: 14px;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .g2-v-param {
# MAGIC   flex: 1;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 14px 16px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   color: #fff;
# MAGIC   font-weight: 600;
# MAGIC }
# MAGIC .g2-v-param-in { background: #1B5162; }
# MAGIC .g2-v-param-out { background: #1B3139; }
# MAGIC .g2-v-param-inout { background: #00A972; }
# MAGIC .g2-v-param-sub {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 400;
# MAGIC   margin-top: 4px;
# MAGIC   opacity: 0.85;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="g2-v-wrap">
# MAGIC   <div class="g2-v-lifecycle">
# MAGIC     <div class="g2-v-step">
# MAGIC       <div class="g2-v-step-icon">+</div>
# MAGIC       <div class="g2-v-step-label">CREATE</div>
# MAGIC       <div class="g2-v-step-desc">Define and register in Unity Catalog</div>
# MAGIC     </div>
# MAGIC     <div class="g2-v-step">
# MAGIC       <div class="g2-v-step-icon">&#x25B6;</div>
# MAGIC       <div class="g2-v-step-label">CALL</div>
# MAGIC       <div class="g2-v-step-desc">Execute with positional or named args</div>
# MAGIC     </div>
# MAGIC     <div class="g2-v-step">
# MAGIC       <div class="g2-v-step-icon">?</div>
# MAGIC       <div class="g2-v-step-label">DESCRIBE</div>
# MAGIC       <div class="g2-v-step-desc">Inspect parameters and body</div>
# MAGIC     </div>
# MAGIC     <div class="g2-v-step">
# MAGIC       <div class="g2-v-step-icon">&#x2261;</div>
# MAGIC       <div class="g2-v-step-label">SHOW</div>
# MAGIC       <div class="g2-v-step-desc">List procedures in a schema</div>
# MAGIC     </div>
# MAGIC     <div class="g2-v-step">
# MAGIC       <div class="g2-v-step-icon">&#x2717;</div>
# MAGIC       <div class="g2-v-step-label">DROP</div>
# MAGIC       <div class="g2-v-step-desc">Remove from catalog</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="g2-v-code-box"><span style="color:#66d9ef;font-style:italic">CREATE OR REPLACE PROCEDURE</span> <span style="color:#a6e22e">catalog.schema.refresh_daily_summary</span>(
# MAGIC   <span style="color:#66d9ef;font-style:italic">IN</span> target_date <span style="color:#66d9ef;font-style:italic">DATE</span>,
# MAGIC   <span style="color:#66d9ef;font-style:italic">OUT</span> rows_inserted <span style="color:#66d9ef;font-style:italic">INT</span>
# MAGIC )
# MAGIC <span style="color:#66d9ef;font-style:italic">LANGUAGE SQL</span>
# MAGIC <span style="color:#66d9ef;font-style:italic">SQL SECURITY INVOKER</span>
# MAGIC <span style="color:#66d9ef;font-style:italic">AS BEGIN</span>
# MAGIC   <span style="color:#66d9ef;font-style:italic">DELETE FROM</span> daily_summary <span style="color:#66d9ef;font-style:italic">WHERE</span> report_date = target_date;
# MAGIC   <span style="color:#66d9ef;font-style:italic">INSERT INTO</span> daily_summary
# MAGIC     <span style="color:#66d9ef;font-style:italic">SELECT</span> target_date, region, <span style="color:#a6e22e">SUM</span>(amount)
# MAGIC     <span style="color:#66d9ef;font-style:italic">FROM</span> orders <span style="color:#66d9ef;font-style:italic">WHERE</span> order_date = target_date
# MAGIC     <span style="color:#66d9ef;font-style:italic">GROUP BY</span> region;
# MAGIC   <span style="color:#66d9ef;font-style:italic">SET</span> rows_inserted = (<span style="color:#66d9ef;font-style:italic">SELECT COUNT</span>(*) <span style="color:#66d9ef;font-style:italic">FROM</span> daily_summary <span style="color:#66d9ef;font-style:italic">WHERE</span> report_date = target_date);
# MAGIC <span style="color:#66d9ef;font-style:italic">END</span>;</div>
# MAGIC   <div class="g2-v-params">
# MAGIC     <div class="g2-v-param g2-v-param-in">IN<div class="g2-v-param-sub">Read-only input (default)</div></div>
# MAGIC     <div class="g2-v-param g2-v-param-out">OUT<div class="g2-v-param-sub">Returns computed value</div></div>
# MAGIC     <div class="g2-v-param g2-v-param-inout">INOUT<div class="g2-v-param-sub">Accepts input, returns modified</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Stored Procedures vs User-Defined Functions</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Procedures:</strong> execute DDL, DML, and DCL statements, return result sets or OUT parameters, and perform side effects like INSERT or DELETE. Called with CALL, not from within SELECT.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>UDFs:</strong> accept inputs, compute a result, and return a single scalar value inline within a SELECT expression. No side effects allowed.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Decision rule:</strong> if the logic modifies data or branches across multiple tables, use a procedure. If it computes a value per row, use a UDF.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Governance and Security</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog registration:</strong> procedures live at catalog.schema.procedure_name, following the same governance model as tables and views. GRANT EXECUTE controls who can run them.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>INVOKER security only:</strong> there is no DEFINER mode. The procedure runs with the caller's permissions, so no privilege escalation is possible. This is a deliberate security design choice.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Nesting limit:</strong> procedure calls can be nested up to 64 levels deep. Each level creates a new scope for local variables and handlers.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Orchestration with Lakeflow Jobs</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Scheduled execution:</strong> Lakeflow Jobs can schedule stored procedure calls alongside DLT pipelines, Python notebooks, and dbt models in a single workflow DAG.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Parameter passing:</strong> job tasks pass parameters to procedures using named arguments. Output parameters can be captured and routed to downstream tasks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Monitoring:</strong> procedure execution is logged in Query History, providing duration, rows affected, and the SQL warehouse used.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Partner Conversation Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/blog/introducing-sql-stored-procedures-databricks" style="color: #2574B5; font-size: 14pt;">Databricks blog: Introducing SQL Stored Procedures</a>: covers the launch announcement, customer quotes, and positioning against legacy warehouse migration. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Common objection:</strong> "We have thousands of stored procedures. This migration is impossible." The response: SQL/PSM is closer to T-SQL and PL/SQL than most teams expect. Many procedures translate with syntax-level changes, not logic rewrites. Databricks also provides migration tooling through partners.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Positioning:</strong> stored procedures are not a replacement for DLT. They are the answer for procedural logic that cannot be expressed declaratively: conditional workflows, multi-step transactions, and iterative processing.</li>
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
# MAGIC ## H. Semantic Models with Metric Views
# MAGIC
# MAGIC Stored procedures address how you build logic on the platform. Metric views address how you deliver governed analytics to consumers. Unity Catalog metric views provide a semantic layer that defines business metrics once and exposes them consistently across dashboards, Genie, SQL queries, and BI tools.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### H1. What Is a Metric View and How to Build One
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Metric views are Unity Catalog objects that define reusable business metrics in YAML. They create a governed semantic layer between raw tables and consumers: every dashboard, Genie Space, and BI tool that queries a metric view gets the same calculation, the same filters, and the same grain. No more conflicting definitions of "revenue" or "active users" across teams.</p>
# MAGIC
# MAGIC <!-- TABBED INTERFACE: H1 -->
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click each tab to explore what Lakehouse Federation offers</p>
# MAGIC <div style="width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <!-- Tab buttons -->
# MAGIC   <div style="display: flex; gap: 0; border-bottom: 3px solid #1B5162;">
# MAGIC     <div id="h1-tab-btn-1" onclick="document.getElementById('h1-tab-1').style.display='block';document.getElementById('h1-tab-2').style.display='none';document.getElementById('h1-tab-3').style.display='none';document.getElementById('h1-tab-4').style.display='none';document.getElementById('h1-tab-btn-1').style.background='#1B5162';document.getElementById('h1-tab-btn-1').style.color='#fff';document.getElementById('h1-tab-btn-2').style.background='#e8f4f8';document.getElementById('h1-tab-btn-2').style.color='#1B5162';document.getElementById('h1-tab-btn-3').style.background='#e8f4f8';document.getElementById('h1-tab-btn-3').style.color='#1B5162';document.getElementById('h1-tab-btn-4').style.background='#e8f4f8';document.getElementById('h1-tab-btn-4').style.color='#1B5162';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #1B5162; color: #fff; border: none;">What Is It</div>
# MAGIC     <div id="h1-tab-btn-2" onclick="document.getElementById('h1-tab-1').style.display='none';document.getElementById('h1-tab-2').style.display='block';document.getElementById('h1-tab-3').style.display='none';document.getElementById('h1-tab-4').style.display='none';document.getElementById('h1-tab-btn-1').style.background='#e8f4f8';document.getElementById('h1-tab-btn-1').style.color='#1B5162';document.getElementById('h1-tab-btn-2').style.background='#1B5162';document.getElementById('h1-tab-btn-2').style.color='#fff';document.getElementById('h1-tab-btn-3').style.background='#e8f4f8';document.getElementById('h1-tab-btn-3').style.color='#1B5162';document.getElementById('h1-tab-btn-4').style.background='#e8f4f8';document.getElementById('h1-tab-btn-4').style.color='#1B5162';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #e8f4f8; color: #1B5162; border: none;">Components</div>
# MAGIC     <div id="h1-tab-btn-3" onclick="document.getElementById('h1-tab-1').style.display='none';document.getElementById('h1-tab-2').style.display='none';document.getElementById('h1-tab-3').style.display='block';document.getElementById('h1-tab-4').style.display='none';document.getElementById('h1-tab-btn-1').style.background='#e8f4f8';document.getElementById('h1-tab-btn-1').style.color='#1B5162';document.getElementById('h1-tab-btn-2').style.background='#e8f4f8';document.getElementById('h1-tab-btn-2').style.color='#1B5162';document.getElementById('h1-tab-btn-3').style.background='#1B5162';document.getElementById('h1-tab-btn-3').style.color='#fff';document.getElementById('h1-tab-btn-4').style.background='#e8f4f8';document.getElementById('h1-tab-btn-4').style.color='#1B5162';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #e8f4f8; color: #1B5162; border: none;">How to Build</div>
# MAGIC     <div id="h1-tab-btn-4" onclick="document.getElementById('h1-tab-1').style.display='none';document.getElementById('h1-tab-2').style.display='none';document.getElementById('h1-tab-3').style.display='none';document.getElementById('h1-tab-4').style.display='block';document.getElementById('h1-tab-btn-1').style.background='#e8f4f8';document.getElementById('h1-tab-btn-1').style.color='#1B5162';document.getElementById('h1-tab-btn-2').style.background='#e8f4f8';document.getElementById('h1-tab-btn-2').style.color='#1B5162';document.getElementById('h1-tab-btn-3').style.background='#e8f4f8';document.getElementById('h1-tab-btn-3').style.color='#1B5162';document.getElementById('h1-tab-btn-4').style.background='#1B5162';document.getElementById('h1-tab-btn-4').style.color='#fff';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #e8f4f8; color: #1B5162; border: none;">Advanced Features</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 1: What Is It -->
# MAGIC   <div id="h1-tab-1" style="display: block; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <ul style="margin: 0; padding-left: 22px; font-size: 14pt; color: #333; line-height: 1.8;">
# MAGIC       <li>A metric view is a <strong>Unity Catalog object</strong> defined in YAML that specifies reusable business metrics and the ways to slice them.</li>
# MAGIC       <li>It contains <strong>measures</strong> (aggregation expressions like SUM, COUNT, AVG) and <strong>dimensions</strong> (columns available for GROUP BY).</li>
# MAGIC       <li>It lives in the three-level UC namespace: <code style="background: #e8f4f8; padding: 2px 8px; border-radius: 4px; font-size: 13pt;">catalog.schema.metric_view</code>.</li>
# MAGIC       <li>It is governed like any UC object: permissions, lineage tracking, tags, certification badges, and audit logging all apply.</li>
# MAGIC       <li>Think of it as a <strong>restaurant menu</strong>: dishes (measures) are listed by name, categories (dimensions) organize them, and the kitchen (data pipelines) stays hidden from customers.</li>
# MAGIC       <li>A metric view is not a SQL view. A SQL view stores a query and returns rows. A metric view stores business semantics and enforces which aggregations and groupings are valid.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 2: Components -->
# MAGIC   <div id="h1-tab-2" style="display: none; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 20px;">
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 8px;">Source</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">The Delta table or view the metric is calculated from. Specified as <code style="font-size: 13pt;">catalog.schema.table</code>. Can also reference another metric view for composability.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 8px;">Measures</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Named aggregation expressions: SUM, COUNT, AVG, MIN, MAX, and more. These are the KPIs your business consumers see. Must use aggregate functions.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 8px;">Dimensions</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Columns available for GROUP BY: date, region, product category, and so on. Cannot use aggregate functions. These are the slicing axes.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 8px;">Filters</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Default WHERE conditions applied automatically at query time. Use them for business focus (only current fiscal year) and cost control (limit rows scanned).</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- YAML skeleton with labels -->
# MAGIC     <div style="background: #272822; border-radius: 8px; padding: 20px 24px; font-family: 'Courier New', monospace; font-size: 14pt; line-height: 1.9; color: #f8f8f2; overflow-x: auto;">
# MAGIC       <span style="color: #66d9ef;">version</span>: <span style="color: #a6e22e;">1.1</span> <span style="color: #75715e;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# spec version</span><br/>
# MAGIC       <span style="color: #66d9ef;">source</span>: <span style="color: #a6e22e;">catalog.schema.table</span> <span style="color: #75715e;">&nbsp;# fact table</span><br/>
# MAGIC       <span style="color: #66d9ef;">filter</span>: <span style="color: #e6db74;">YEAR(source.date) >= 2024</span><br/>
# MAGIC       <span style="color: #66d9ef;">dimensions</span>:<br/>
# MAGIC       &nbsp;&nbsp;- <span style="color: #66d9ef;">name</span>: <span style="color: #a6e22e;">order_date</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">expr</span>: <span style="color: #a6e22e;">source.order_date</span><br/>
# MAGIC       <span style="color: #66d9ef;">measures</span>:<br/>
# MAGIC       &nbsp;&nbsp;- <span style="color: #66d9ef;">name</span>: <span style="color: #a6e22e;">total_revenue</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">expr</span>: <span style="color: #a6e22e;">SUM(source.amount)</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 3: How to Build -->
# MAGIC   <div id="h1-tab-3" style="display: none; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <p style="font-size: 14pt; color: #333; line-height: 1.6; margin: 0 0 16px 0;">Create a metric view using the SQL DDL statement. The YAML definition goes between <code style="font-size: 13pt;">$$</code> delimiters.</p>
# MAGIC     <div style="background: #272822; border-radius: 8px; padding: 20px 24px; font-family: 'Courier New', monospace; font-size: 14pt; line-height: 1.9; color: #f8f8f2; overflow-x: auto;">
# MAGIC       <span style="color: #66d9ef; font-style: italic;">CREATE OR REPLACE VIEW</span> <span style="color: #a6e22e;">catalog.schema.orders_metrics</span> (<br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #f8f8f2;">order_month, order_status,</span><br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #f8f8f2;">total_revenue</span><br/>
# MAGIC       )<br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">WITH METRICS</span><br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">LANGUAGE YAML</span><br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">AS</span> <span style="color: #e6db74;">$$</span><br/>
# MAGIC       <span style="color: #66d9ef;">version</span>: <span style="color: #a6e22e;">1.1</span><br/>
# MAGIC       <span style="color: #66d9ef;">source</span>: <span style="color: #a6e22e;">samples.tpch.orders</span><br/>
# MAGIC       <span style="color: #66d9ef;">filter</span>: <span style="color: #e6db74;">o_orderdate > '1990-01-01'</span><br/>
# MAGIC       <span style="color: #66d9ef;">dimensions</span>:<br/>
# MAGIC       &nbsp;&nbsp;- <span style="color: #66d9ef;">name</span>: <span style="color: #a6e22e;">order_month</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">expr</span>: <span style="color: #a6e22e;">DATE_TRUNC('MONTH', o_orderdate)</span><br/>
# MAGIC       &nbsp;&nbsp;- <span style="color: #66d9ef;">name</span>: <span style="color: #a6e22e;">order_status</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">expr</span>: |<br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #e6db74;">CASE WHEN o_orderstatus = 'O' THEN 'Open'</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #e6db74;">WHEN o_orderstatus = 'P' THEN 'Processing'</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #e6db74;">WHEN o_orderstatus = 'F' THEN 'Fulfilled'</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #e6db74;">END</span><br/>
# MAGIC       <span style="color: #66d9ef;">measures</span>:<br/>
# MAGIC       &nbsp;&nbsp;- <span style="color: #66d9ef;">name</span>: <span style="color: #a6e22e;">total_revenue</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">expr</span>: <span style="color: #a6e22e;">SUM(o_totalprice)</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">display_name</span>: <span style="color: #e6db74;">Total Revenue</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">format</span>:<br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">type</span>: <span style="color: #a6e22e;">currency</span><br/>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #66d9ef;">currency_code</span>: <span style="color: #a6e22e;">USD</span><br/>
# MAGIC       <span style="color: #e6db74;">$$</span>
# MAGIC     </div>
# MAGIC     <div style="display: flex; gap: 16px; margin-top: 16px;">
# MAGIC       <div style="flex: 1; background: #e8f4f8; border-left: 4px solid #1B5162; border-radius: 4px; padding: 12px 16px;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 4px;">Also available</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">ALTER VIEW, DROP VIEW, and DESCRIBE work on metric views just like other UC objects.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; background: #e8f4f8; border-left: 4px solid #1B5162; border-radius: 4px; padding: 12px 16px;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 4px;">Alternative methods</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Catalog Explorer UI (point-and-click) and Genie Code (describe in natural language, Genie generates YAML).</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 4: Advanced Features -->
# MAGIC   <div id="h1-tab-4" style="display: none; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <div style="display: flex; flex-wrap: wrap; gap: 16px;">
# MAGIC       <div style="flex: 1; min-width: 220px; border: 2px solid #618794; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">Composability</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Metric views can reference other metric views as their source. One team owns base definitions; another team builds specialized views on top. Measures reference earlier measures using MEASURE() in the YAML. Changes to a base measure automatically propagate to all composed measures.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 220px; border: 2px solid #618794; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">Window Measures</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Running totals, moving averages, and period-over-period calculations. Window measures use a dedicated <code style="font-size: 13pt;">window</code> block in the YAML that specifies the ordering dimension and the frame.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 220px; border: 2px solid #618794; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">LOD Expressions</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Fixed level-of-detail calculations that compute at a grain different from the query grain. Useful for "revenue as a percentage of regional total" style metrics.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 220px; border: 2px solid #618794; border-radius: 8px; padding: 16px; background: #fff;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px;">Semantic Enrichment</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">YAML supports display_name, description, synonyms, sample_questions, and format specifications. These metadata fields flow automatically into Genie and Catalog Explorer for improved discoverability.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
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
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Semantic Layer Concept</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           When the same revenue formula is duplicated across thirty queries, each copy drifts independently. Finance calculates revenue one way, sales another, and marketing a third. A semantic layer prevents this by placing a governed, business-friendly contract between raw data and the people who consume it. Metric views are that contract on Databricks. They abstract data-engineering concerns from consumers, define business logic once and reuse it many times, and hide changes to underlying data schemas so that renaming a column in the source table does not break downstream dashboards.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">YAML Deep Dive</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           YAML stands for "Yet Another Markup Language." It uses indentation (spaces, not tabs) to show structure, similar to Python. Lists use hyphens. Multiline SQL expressions use the pipe character (|). The "source" keyword in expressions refers back to the fact table defined at the top of the file. Joins are always LEFT JOINs and use a name/source/on structure. Nested joins support snowflake schemas. The format field supports type (currency, number, percent), decimal_places, and currency_code. Avoid null values in source data; LEFT JOINs and unmatched CASE statements introduce nulls that can silently affect aggregations. Always provide an ELSE clause in CASE expressions.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Composability Patterns</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           Within a single metric view, dimensions can reference earlier-defined dimensions (define order_month once, then use it in fiscal_quarter). Measures can reference earlier measures via MEASURE() in the YAML expression (for example, MEASURE(total_revenue) - MEASURE(total_tax)). Across metric views, one view can use another as its source, inheriting both dimensions and measures. If the definition of a base measure changes, all composed measures that reference it automatically inherit the change. When a metric view appears in a joins block instead of as the source, only its dimensions are accessible.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why This Matters for Partner SAs</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 0 0;">
# MAGIC           Customers frequently report "200 dashboards and no one trusts the numbers." Metric views centralize the calculation so every dashboard reads from the same governed definition. The specification is being open-sourced in Apache Spark, which means it is not Databricks-proprietary and the spec will work across engines. This is a strong positioning point in competitive situations: Databricks builds the semantic layer natively into Unity Catalog with first-class Genie integration, while competitors require third-party tools like dbt or Looker to achieve similar governance.
# MAGIC         </p>
# MAGIC
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
# MAGIC ### H2. Querying, Governing, and Consuming Metric Views
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Once a metric view is defined, any authorized user can query it with SQL using the MEASURE() function. Unity Catalog enforces the same permissions model as tables: GRANT SELECT to control who can access the metric. Consumers interact with metric views through four surfaces: the SQL Editor, AI/BI Genie, AI/BI Dashboards, and SQL Alerts.</p>
# MAGIC
# MAGIC <!-- TABBED INTERFACE: H2 -->
# MAGIC <div style="width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <!-- Tab buttons -->
# MAGIC   <div style="display: flex; gap: 0; border-bottom: 3px solid #1B5162;">
# MAGIC     <div id="h2-tab-btn-1" onclick="document.getElementById('h2-tab-1').style.display='block';document.getElementById('h2-tab-2').style.display='none';document.getElementById('h2-tab-3').style.display='none';document.getElementById('h2-tab-btn-1').style.background='#1B5162';document.getElementById('h2-tab-btn-1').style.color='#fff';document.getElementById('h2-tab-btn-2').style.background='#e8f4f8';document.getElementById('h2-tab-btn-2').style.color='#1B5162';document.getElementById('h2-tab-btn-3').style.background='#e8f4f8';document.getElementById('h2-tab-btn-3').style.color='#1B5162';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #1B5162; color: #fff; border: none;">Query Syntax</div>
# MAGIC     <div id="h2-tab-btn-2" onclick="document.getElementById('h2-tab-1').style.display='none';document.getElementById('h2-tab-2').style.display='block';document.getElementById('h2-tab-3').style.display='none';document.getElementById('h2-tab-btn-1').style.background='#e8f4f8';document.getElementById('h2-tab-btn-1').style.color='#1B5162';document.getElementById('h2-tab-btn-2').style.background='#1B5162';document.getElementById('h2-tab-btn-2').style.color='#fff';document.getElementById('h2-tab-btn-3').style.background='#e8f4f8';document.getElementById('h2-tab-btn-3').style.color='#1B5162';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #e8f4f8; color: #1B5162; border: none;">Security Model</div>
# MAGIC     <div id="h2-tab-btn-3" onclick="document.getElementById('h2-tab-1').style.display='none';document.getElementById('h2-tab-2').style.display='none';document.getElementById('h2-tab-3').style.display='block';document.getElementById('h2-tab-btn-1').style.background='#e8f4f8';document.getElementById('h2-tab-btn-1').style.color='#1B5162';document.getElementById('h2-tab-btn-2').style.background='#e8f4f8';document.getElementById('h2-tab-btn-2').style.color='#1B5162';document.getElementById('h2-tab-btn-3').style.background='#1B5162';document.getElementById('h2-tab-btn-3').style.color='#fff';" style="padding: 12px 24px; font-size: 14pt; font-weight: 700; cursor: pointer; border-radius: 8px 8px 0 0; background: #e8f4f8; color: #1B5162; border: none;">Consumer Surfaces</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 1: Query Syntax -->
# MAGIC   <div id="h2-tab-1" style="display: block; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <p style="font-size: 14pt; color: #333; line-height: 1.6; margin: 0 0 16px 0;">Query metric views using standard SELECT syntax. MEASURE() is required for every measure column. SELECT * is not supported. Query-time JOINs are not allowed.</p>
# MAGIC     <div style="background: #272822; border-radius: 8px; padding: 20px 24px; font-family: 'Courier New', monospace; font-size: 14pt; line-height: 1.9; color: #f8f8f2; overflow-x: auto;">
# MAGIC       <span style="color: #66d9ef; font-style: italic;">SELECT</span><br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #f8f8f2;">order_date,</span><br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #f8f8f2;">region,</span><br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #a6e22e;">MEASURE</span><span style="color: #f8f8f2;">(total_revenue),</span><br/>
# MAGIC       &nbsp;&nbsp;<span style="color: #a6e22e;">MEASURE</span><span style="color: #f8f8f2;">(order_count)</span><br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">FROM</span> <span style="color: #e6db74;">catalog.schema.sales_metrics</span><br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">GROUP BY</span> <span style="color: #f8f8f2;">order_date, region</span><br/>
# MAGIC       <span style="color: #66d9ef; font-style: italic;">ORDER BY</span> <span style="color: #f8f8f2;">order_date;</span>
# MAGIC     </div>
# MAGIC     <div style="display: flex; gap: 16px; margin-top: 16px;">
# MAGIC       <div style="flex: 1; background: #e8f4f8; border-left: 4px solid #1B5162; border-radius: 4px; padding: 12px 16px;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 4px;">MEASURE() required</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Wrap every measure column. Dimensions are selected without any wrapper. Without MEASURE(), the engine treats the column as a dimension and the query fails.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; background: #e8f4f8; border-left: 4px solid #1B5162; border-radius: 4px; padding: 12px 16px;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 4px;">GROUP BY sets the grain</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">The aggregation logic is fixed in the definition. The grain is flexible at query time. The same metric view can return daily, monthly, or per-region totals.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="display: flex; gap: 16px; margin-top: 12px;">
# MAGIC       <div style="flex: 1; border: 2px solid #c62828; border-radius: 8px; padding: 14px 16px; background: #fff8f8;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #c62828; margin-bottom: 6px;">SELECT * Not Permitted</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Metric views expose named business metrics, not raw columns. You must reference each field explicitly by name.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; border: 2px solid #c62828; border-radius: 8px; padding: 14px 16px; background: #fff8f8;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #c62828; margin-bottom: 6px;">No Query-Time JOINs</div>
# MAGIC         <div style="font-size: 14pt; color: #333; line-height: 1.6;">Joins must be declared in the metric view definition. Ad-hoc joins would bypass the governed aggregation path.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 2: Security Model -->
# MAGIC   <div id="h2-tab-2" style="display: none; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 12px;">Consumer Privileges</div>
# MAGIC     <div style="display: flex; align-items: center; gap: 0; margin-bottom: 20px;">
# MAGIC       <div style="flex: 1; border-radius: 8px; padding: 14px 16px; background: #fff; border: 2px solid #1B5162; text-align: center;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 6px;">Catalog</div>
# MAGIC         <span style="display: inline-block; background: #1B5162; color: #fff; border-radius: 4px; padding: 2px 10px; font-size: 14pt; font-family: monospace;">USE CATALOG</span>
# MAGIC       </div>
# MAGIC       <div style="padding: 0 10px; font-size: 20pt; color: #618794;">&#x2192;</div>
# MAGIC       <div style="flex: 1; border-radius: 8px; padding: 14px 16px; background: #fff; border: 2px solid #1B5162; text-align: center;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 6px;">Schema</div>
# MAGIC         <span style="display: inline-block; background: #1B5162; color: #fff; border-radius: 4px; padding: 2px 10px; font-size: 14pt; font-family: monospace;">USE SCHEMA</span>
# MAGIC       </div>
# MAGIC       <div style="padding: 0 10px; font-size: 20pt; color: #618794;">&#x2192;</div>
# MAGIC       <div style="flex: 1; border-radius: 8px; padding: 14px 16px; background: #fff; border: 2px solid #1B5162; text-align: center;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 6px;">Metric View</div>
# MAGIC         <span style="display: inline-block; background: #1B5162; color: #fff; border-radius: 4px; padding: 2px 10px; font-size: 14pt; font-family: monospace;">SELECT</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <ul style="margin: 0; padding-left: 22px; font-size: 14pt; color: #333; line-height: 1.8;">
# MAGIC       <li><strong>Owner-based execution:</strong> when a consumer queries the metric view, Databricks checks SELECT on the metric view using the consumer's identity, then uses the <strong>owner's identity</strong> to resolve the underlying tables. Consumers do not need direct access to source tables.</li>
# MAGIC       <li><strong>Row-level security and column masking</strong> applied at the source table level are enforced automatically. The metric view inherits those controls; no additional configuration is needed on the metric view itself.</li>
# MAGIC       <li><strong>Unity Catalog lineage</strong> tracks metric view dependencies end to end, from source tables through to dashboards and Genie spaces.</li>
# MAGIC       <li><strong>Recommended practice:</strong> grant privileges to Unity Catalog groups rather than individual accounts. Group membership changes propagate automatically without re-running GRANT statements.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- TAB 3: Consumer Surfaces -->
# MAGIC   <div id="h2-tab-3" style="display: none; background: #F9F7F4; border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 24px 28px;">
# MAGIC     <div style="display: flex; flex-wrap: wrap; gap: 16px;">
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; overflow: hidden;">
# MAGIC         <div style="background: #1B5162; color: #fff; padding: 12px 16px; font-size: 14pt; font-weight: 700;">SQL Editor</div>
# MAGIC         <div style="padding: 14px 16px; font-size: 14pt; color: #333; line-height: 1.6; background: #fff;">Write MEASURE() queries directly. Compose SELECT statements, set the grain with GROUP BY, and create visualizations from the results pane. Save and share queries with teams.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; overflow: hidden;">
# MAGIC         <div style="background: #1B5162; color: #fff; padding: 12px 16px; font-size: 14pt; font-weight: 700;">AI/BI Genie</div>
# MAGIC         <div style="padding: 14px 16px; font-size: 14pt; color: #333; line-height: 1.6; background: #fff;">Ask questions in natural language. Genie reads display_name, synonyms, and descriptions from the YAML and generates MEASURE() queries automatically. Create managed Genie spaces for published, auditable self-service analytics.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; overflow: hidden;">
# MAGIC         <div style="background: #1B5162; color: #fff; padding: 12px 16px; font-size: 14pt; font-weight: 700;">AI/BI Dashboards</div>
# MAGIC         <div style="padding: 14px 16px; font-size: 14pt; color: #333; line-height: 1.6; background: #fff;">Use metric views as data sources for Lakeview dashboards. Display names auto-populate chart titles and axis labels. Publish and refresh on a schedule. Every chart reads from the same governed logic.</div>
# MAGIC       </div>
# MAGIC       <div style="flex: 1; min-width: 200px; border: 2px solid #1B5162; border-radius: 8px; overflow: hidden;">
# MAGIC         <div style="background: #1B5162; color: #fff; padding: 12px 16px; font-size: 14pt; font-weight: 700;">SQL Alerts</div>
# MAGIC         <div style="padding: 14px 16px; font-size: 14pt; color: #333; line-height: 1.6; background: #fff;">Monitor KPIs with scheduled MEASURE() queries. Configure threshold conditions and notification destinations (email, Slack, webhook). Alerts use agreed-upon metric definitions for trustworthy monitoring.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
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
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">MEASURE() Syntax Details</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           MEASURE() tells the query engine "this column is a calculation to be aggregated, not a raw value to be selected." The aggregation logic defined in the YAML executes at whatever grain the GROUP BY clause specifies. If a dimension or measure name contains spaces, wrap it in backticks: <code style="background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-size: 13pt;">MEASURE(`total revenue`)</code>. Best practice: use underscore names in the YAML definition and set a readable display_name for UI surfaces. Display names and synonyms cannot be used in SQL queries; they are for human and AI consumers only. A CTE workaround exists for combining metric view output with another table, but it routes around governance controls and should be used only when a definition change is not an option.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Owner-Based Security Model</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           The metric view owner must hold SELECT on every underlying table referenced in the definition (fact table and all joined lookup tables). When a consumer queries the metric view, Databricks checks the consumer's SELECT privilege on the metric view object, then resolves the source tables using the owner's credentials. This two-layer model means consumers never touch the underlying tables directly. Removing a consumer from a group immediately revokes access without touching table-level grants. Row-level security, column masking, and ABAC applied at source tables are evaluated at query time based on the querying user's identity, so the metric view respects those policies automatically. You cannot apply RLS or column masks directly to a metric view; they must be configured on the underlying tables.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Genie Integration</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 12px 0;">
# MAGIC           When you add a metric view to a Genie space, Genie reads the semantic metadata from the YAML definition: display_name, description, synonyms, and sample_questions. A user asks "what was total revenue last quarter by region?" and Genie generates the correct SELECT with MEASURE() calls and the appropriate GROUP BY. Ad-hoc Genie (via the Ask Genie button in Catalog Explorer) is useful during development to validate that field names and synonyms are clear enough. Managed Genie spaces are Unity Catalog objects with governed permissions, audit trails, curated instructions, and publishable access for consumer teams.
# MAGIC         </p>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Competitive Positioning</strong>
# MAGIC         <p style="font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 0 0;">
# MAGIC           Snowflake has no equivalent governed semantic layer built into its platform. Customers using Snowflake must rely on dbt metrics, Looker, or other third-party tools to achieve similar centralized metric definitions. Databricks builds metric views natively into Unity Catalog with first-class Genie integration and full governance: permissions, lineage, certification badges, and audit logging. The metric view specification is being open-sourced in Apache Spark, making it a cross-engine standard rather than a proprietary feature. For Partner SAs, this is a strong differentiator: "define your metrics once in Unity Catalog, query them from SQL, Genie, dashboards, and alerts, and govern them like any other UC object."
# MAGIC         </p>
# MAGIC
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
# MAGIC ### Conclusion and Key Takeaways
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks SQL addresses the challenges that have plagued data warehousing for decades. By building the warehouse directly on the lakehouse, it eliminates data movement and silos. Databricks AI makes the warehouse intelligent: natural language access broadens who can use it, predictive optimization removes manual tuning, and the Photon engine with Predictive I/O delivers performance at scale. Serverless SQL warehouses with Intelligent Workload Management provide instant startup, proactive scaling, and optimized cost. SQL scripting and stored procedures provide the procedural capabilities needed for warehouse migration and complex workflows. Metric views add a governed semantic layer that ensures every consumer, from dashboards to Genie, gets the same answer for the same question.</p>
# MAGIC
# MAGIC <ul style="font-size: 14pt; line-height: 1.8; color: #333; margin: 16px 0 16px 24px;">
# MAGIC   <li><strong>SQL scripting follows the SQL/PSM standard:</strong> BEGIN...END blocks, variables, control flow, error handling, and ATOMIC transactions bring full procedural capabilities to Databricks SQL. This is the open standard, not a proprietary dialect.</li>
# MAGIC   <li><strong>Stored procedures enable warehouse migration:</strong> teams migrating from SQL Server, Oracle, or Teradata can bring their procedural logic to Databricks. Procedures are Unity Catalog objects with standard GRANT/REVOKE access control and INVOKER security.</li>
# MAGIC   <li><strong>Metric views provide a governed semantic layer:</strong> YAML-based definitions centralize business metrics (measures and dimensions) so every dashboard, Genie space, and SQL query reads from the same source of truth.</li>
# MAGIC   <li><strong>Genie reads metric view metadata automatically:</strong> display names, synonyms, and descriptions defined once in the YAML flow into the conversational interface without additional configuration.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Data warehousing on Databricks (<a href="https://docs.databricks.com/aws/en/sql">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql">GCP</a>): Overview of Databricks SQL capabilities and getting started</li>
# MAGIC         <li>SQL warehouse types (<a href="https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-types">Azure</a> | <a href="https://docs.databricks.com/gcp/en/compute/sql-warehouse/warehouse-types">GCP</a>): Feature comparison for Classic, Pro, and Serverless warehouses</li>
# MAGIC         <li>What is Photon? (<a href="https://docs.databricks.com/aws/en/compute/photon">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/compute/photon">Azure</a> | <a href="https://docs.databricks.com/gcp/en/compute/photon">GCP</a>): Architecture, supported operations, and performance benefits</li>
# MAGIC         <li>What is Predictive I/O? (<a href="https://docs.databricks.com/aws/en/optimizations/predictive-io">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-io">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/predictive-io">GCP</a>): Accelerated reads and accelerated updates</li>
# MAGIC         <li>Predictive optimization (<a href="https://docs.databricks.com/aws/en/optimizations/predictive-optimization">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/predictive-optimization">GCP</a>): Automatic OPTIMIZE, VACUUM, and ANALYZE for managed tables</li>
# MAGIC         <li>SQL scripting and stored procedures (<a href="https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-scripting">Azure</a> | <a href="https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-scripting">GCP</a>): SQL/PSM scripting, compound statements, control flow, and stored procedure creation</li>
# MAGIC         <li>Unity Catalog metric views (<a href="https://docs.databricks.com/aws/en/business-semantics/metric-views">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/business-semantics/metric-views">Azure</a> | <a href="https://docs.databricks.com/gcp/en/business-semantics/metric-views">GCP</a>): Governed semantic layer for consistent business metrics across dashboards, Genie, and BI tools</li>
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
