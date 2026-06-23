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
# MAGIC # 9 Lecture - Competitive Positioning
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The data and AI market includes dozens of platforms, each addressing part of the data lifecycle. Cloud data warehouses, open-source processing frameworks, managed ML services, and GenAI APIs all compete for customer attention and budget. As a Partner Solution Architect, your role is not to memorize feature comparison grids but to understand the architectural and strategic differences that determine which platform delivers the most value for a given customer's needs.
# MAGIC
# MAGIC This lecture provides a framework for competitive conversations that focuses on customer business outcomes rather than feature-by-feature comparisons. It covers the two most common competitive scenarios (Snowflake and AWS built-in services), explains the architectural advantages of the Databricks Lakehouse, and equips you with cost data and customer migration stories to support your positioning.
# MAGIC
# MAGIC This lecture covers 5 sections:
# MAGIC
# MAGIC - **A. Value-Driven Selling** -- Leading with business outcomes, not features
# MAGIC - **B. The Lakehouse Advantage** -- Lakehouse vs traditional warehouse vs data lake positioning
# MAGIC - **C. Databricks vs Snowflake** -- Architecture comparison, TCO, GenAI, streaming, and governance
# MAGIC - **D. Databricks vs AWS built-in** -- EMR, Redshift, SageMaker, and Bedrock comparison
# MAGIC - **E. Platform Differentiators** -- Photon, Unity Catalog, Mosaic AI, multi-cloud, and open source
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Articulate the value-driven selling approach for competitive positioning, focusing on business outcomes rather than feature-by-feature comparisons
# MAGIC - Compare and contrast the Lakehouse architecture with traditional data warehouses and data lakes, including when each approach fits
# MAGIC - Differentiate Databricks from Snowflake across architecture, cost, streaming, ML/AI, and governance dimensions
# MAGIC - Differentiate Databricks from the AWS data ecosystem (EMR, Redshift, SageMaker, Bedrock), including when AWS-built-in services may be preferred
# MAGIC - Use cost comparison data and customer migration stories to quantify the TCO advantages of Databricks for common data workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Value-Driven Selling

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Leading with Business Outcomes
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Competitors will try to bring the conversation to a feature-by-feature comparison, sometimes called a "speeds and feeds" proof of concept. While Databricks performs well in those comparisons, they are not where you win the deal. The most effective competitive approach is to <strong>shift the conversation toward the business outcomes the customer is trying to achieve</strong>. Ask what problems they are solving, not which product has the longer feature list.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Databricks Value Framework organizes this approach into three layers. Each layer builds on the one below it, and the value increases as you move up:</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-value-accordion ── -->
# MAGIC <style>
# MAGIC .a1v-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a1v-label { font-size: 14pt; font-weight: 700; color: #1B5162; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 10px; }
# MAGIC .a1v-acc { border-radius: 12px; overflow: hidden; margin-bottom: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.10); }
# MAGIC .a1v-acc summary { list-style: none; cursor: pointer; user-select: none; }
# MAGIC .a1v-acc summary::-webkit-details-marker { display: none; }
# MAGIC .a1v-hdr { padding: 18px 22px; display: flex; align-items: center; gap: 14px; }
# MAGIC .a1v-arrow { font-size: 16px; color: rgba(255,255,255,0.7); transition: transform 0.2s; display: inline-block; }
# MAGIC .a1v-acc[open] .a1v-arrow { transform: rotate(90deg); }
# MAGIC .a1v-num { font-size: 14pt; font-weight: 800; color: rgba(255,255,255,0.4); flex-shrink: 0; }
# MAGIC .a1v-title { font-size: 16pt; font-weight: 700; color: #fff; flex: 1; }
# MAGIC .a1v-pill { font-size: 14pt; font-weight: 600; padding: 3px 12px; border-radius: 14px; background: rgba(255,255,255,0.15); color: rgba(255,255,255,0.8); }
# MAGIC .a1v-body { background: #fff; padding: 18px 22px; }
# MAGIC .a1v-desc { font-size: 14pt; color: #333; line-height: 1.6; margin-bottom: 14px; }
# MAGIC .a1v-section-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; margin: 16px 0 8px 0; padding-bottom: 4px; border-bottom: 2px solid; }
# MAGIC .a1v-tp-grid { display: flex; gap: 10px; margin-bottom: 6px; }
# MAGIC .a1v-tp { flex: 1; font-size: 14pt; color: #333; font-style: italic; line-height: 1.5; padding: 12px 14px; background: #F9F7F4; border-radius: 8px; border-left: 4px solid; }
# MAGIC .a1v-stories { display: flex; gap: 12px; }
# MAGIC .a1v-scard { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s; cursor: default; display: flex; flex-direction: column; }
# MAGIC .a1v-scard:hover { transform: translateY(-2px); box-shadow: 0 5px 12px rgba(27,49,57,0.13); }
# MAGIC .a1v-shdr { padding: 10px 14px; display: flex; align-items: center; gap: 8px; }
# MAGIC .a1v-sname { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .a1v-sname a { color: #fff; text-decoration: underline; font-size: 14pt; }
# MAGIC .a1v-sind { font-size: 14pt; color: rgba(255,255,255,0.7); margin-left: auto; }
# MAGIC .a1v-sbody { background: #fff; padding: 10px 14px; flex: 1; font-size: 14pt; color: #444; line-height: 1.5; }
# MAGIC .a1v-smetric { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .a1v-grad { display: flex; align-items: center; gap: 10px; margin-top: 4px; }
# MAGIC .a1v-gradline { flex: 1; height: 3px; border-radius: 2px; background: linear-gradient(90deg, #1B3139, #618794); }
# MAGIC .a1v-gradlabel { font-size: 14pt; font-weight: 600; color: #618794; white-space: nowrap; }
# MAGIC </style>
# MAGIC <div class="a1v-wrap">
# MAGIC <div class="a1v-label">&#x25B2; Start here: highest customer value</div>
# MAGIC <!-- Tier 1: Business-Impacting Use Cases -->
# MAGIC <details class="a1v-acc" open>
# MAGIC   <summary><div class="a1v-hdr" style="background:#1B3139;"><span class="a1v-arrow">&#x25B6;</span><span class="a1v-num">1</span><div class="a1v-title">Business-Impacting Use Cases</div><span class="a1v-pill">Start here</span></div></summary>
# MAGIC   <div class="a1v-body">
# MAGIC     <div class="a1v-desc">Lead with the business outcomes the customer is trying to achieve: revenue growth, cost savings, customer satisfaction, risk reduction. Map those outcomes to data and AI use cases the platform enables. This is where you win the deal.</div>
# MAGIC     <div class="a1v-section-label" style="color:#1B3139;border-color:#1B3139;">Talking Points</div>
# MAGIC     <div class="a1v-tp-grid">
# MAGIC       <div class="a1v-tp" style="border-color:#1B3139;">"What business outcomes are you trying to achieve with data and AI? Revenue? Cost reduction? Customer experience?"</div>
# MAGIC       <div class="a1v-tp" style="border-color:#1B3139;">"Walk me through your top three use cases. What would it mean for the business if those were in production next quarter?"</div>
# MAGIC       <div class="a1v-tp" style="border-color:#1B3139;">"Which use case would have the biggest revenue or cost impact? That is the one we should scope first."</div>
# MAGIC     </div>
# MAGIC     <div class="a1v-section-label" style="color:#1B3139;border-color:#1B3139;">Customer Proof Points</div>
# MAGIC     <div class="a1v-stories">
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B3139;"><div class="a1v-sname"><a href="https://www.databricks.com/blog/accelerating-innovation-jetblue-using-databricks">JetBlue</a></div><div class="a1v-sind">Aviation</div></div><div class="a1v-sbody"><div class="a1v-smetric">10+ ML products, high ROI in 2 years</div>Dynamic pricing, recommendation engines, supply chain optimization, customer sentiment NLP</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B3139;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/pandora">Pandora</a></div><div class="a1v-sind">Retail</div></div><div class="a1v-sbody"><div class="a1v-smetric">50% click-to-open lift, 80% engagement growth</div>65M personalized product recommendations/year. Marketers run queries without data team support.</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B3139;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/trackunit">Trackunit</a></div><div class="a1v-sind">Construction IoT</div></div><div class="a1v-sbody"><div class="a1v-smetric">6M machines, 3B data points daily</div>Shifted from reporting to predictive maintenance, driving tangible ROI for manufacturers.</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <!-- Tier 2: Productivity Gains -->
# MAGIC <details class="a1v-acc">
# MAGIC   <summary><div class="a1v-hdr" style="background:#1B5162;"><span class="a1v-arrow">&#x25B6;</span><span class="a1v-num">2</span><div class="a1v-title">Productivity Gains</div><span class="a1v-pill">Middle layer</span></div></summary>
# MAGIC   <div class="a1v-body">
# MAGIC     <div class="a1v-desc">A unified platform eliminates the productivity tax of managing multiple tools and handoffs between data engineering, data science, and analytics teams. Show how consolidation accelerates the full lifecycle from raw data to production models.</div>
# MAGIC     <div class="a1v-section-label" style="color:#1B5162;border-color:#1B5162;">Talking Points</div>
# MAGIC     <div class="a1v-tp-grid">
# MAGIC       <div class="a1v-tp" style="border-color:#1B5162;">"How many tools does your data team manage today? How much time goes to maintenance vs building solutions?"</div>
# MAGIC       <div class="a1v-tp" style="border-color:#1B5162;">"What does the handoff look like between engineers and data scientists? How many copies of the data exist?"</div>
# MAGIC       <div class="a1v-tp" style="border-color:#1B5162;">"If all your personas worked on one governed platform, how much faster could you ship new use cases?"</div>
# MAGIC     </div>
# MAGIC     <div class="a1v-section-label" style="color:#1B5162;border-color:#1B5162;">Customer Proof Points</div>
# MAGIC     <div class="a1v-stories">
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B5162;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/freshworks">Freshworks</a></div><div class="a1v-sind">SaaS</div></div><div class="a1v-sbody"><div class="a1v-smetric">60% productivity gain, 75% lower maintenance</div>Migrated 500+ TB from Cloudera CDH (chose Databricks over AWS/Azure native tools). ML models trained 4x faster.</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B5162;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/yipitdata">YipitData</a></div><div class="a1v-sind">Financial Data</div></div><div class="a1v-sbody"><div class="a1v-smetric">4-5x scale increase, lower spend</div>Migrated from ~50 Redshift clusters. 40+ analysts evolved into hybrid engineer/analysts.</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#1B5162;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/iterable">Iterable</a></div><div class="a1v-sind">MarTech</div></div><div class="a1v-sbody"><div class="a1v-smetric">5,000+ pipelines, 2,000+ ML projects</div>Migrated from EMR. "We spent resources maintaining infrastructure rather than developing solutions."</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <!-- Tier 3: Infrastructure Optimization -->
# MAGIC <details class="a1v-acc">
# MAGIC   <summary><div class="a1v-hdr" style="background:#618794;"><span class="a1v-arrow">&#x25B6;</span><span class="a1v-num">3</span><div class="a1v-title">Infrastructure Optimization</div><span class="a1v-pill">Foundation</span></div></summary>
# MAGIC   <div class="a1v-body">
# MAGIC     <div class="a1v-desc">Performance, scalability, and lower infrastructure costs through autoscaling, Photon engine, and serverless compute. Discuss this last, not first. Infrastructure advantages support the business outcomes and productivity gains above; they do not replace them.</div>
# MAGIC     <div class="a1v-section-label" style="color:#618794;border-color:#618794;">Talking Points</div>
# MAGIC     <div class="a1v-tp-grid">
# MAGIC       <div class="a1v-tp" style="border-color:#618794;">"Once the business value and productivity story resonate, infrastructure advantages become supporting evidence, not the lead."</div>
# MAGIC       <div class="a1v-tp" style="border-color:#618794;">"What does your current infrastructure spend look like? Where are the biggest cost drivers: storage, compute, data movement, or licensing?"</div>
# MAGIC       <div class="a1v-tp" style="border-color:#618794;">"If you could run the same workloads at a fraction of the cost, where would you reinvest those savings?"</div>
# MAGIC     </div>
# MAGIC     <div class="a1v-section-label" style="color:#618794;border-color:#618794;">Customer Proof Points</div>
# MAGIC     <div class="a1v-stories">
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#618794;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/getyourguide/dbsql">GetYourGuide</a></div><div class="a1v-sind">Travel</div></div><div class="a1v-sbody"><div class="a1v-smetric">20% cost reduction, 35% faster queries</div>Migrated 750 Snowflake tables. BI reports landing 1.5 hours earlier. 2 engineers, 4.5 months.</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#618794;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/casey/lakehouse">Casey's</a></div><div class="a1v-sind">Retail</div></div><div class="a1v-sbody"><div class="a1v-smetric">Migration in half the expected time</div>Migrated from Azure Synapse. 100% of savings reinvested to support growing data volumes.</div></div>
# MAGIC       <div class="a1v-scard"><div class="a1v-shdr" style="background:#618794;"><div class="a1v-sname"><a href="https://www.databricks.com/customers/bayada">BAYADA</a></div><div class="a1v-sind">Healthcare</div></div><div class="a1v-sbody"><div class="a1v-smetric">35% faster processing, 20% lower costs</div>Consolidated Snowflake + SQL Server. 40% reduction in reconciliation efforts.</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <div class="a1v-grad"><span class="a1v-gradlabel">&#x25BC; Discuss last</span><div class="a1v-gradline"></div><span class="a1v-gradlabel">Foundation layer</span></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Feature Comparisons Are a Trap</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Feature parity is temporary:</strong> every vendor ships new capabilities every quarter. A feature gap you highlight today may be addressed next release, and the customer remembers that you positioned on something that changed.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">POC traps:</strong> competitors may propose a narrowly scoped benchmark (for example, a SQL-only TPC-DS run) that avoids areas where Databricks excels, such as ML, streaming, and governance. If you accept that framing, you compete on their terms.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">The better question:</strong> instead of "which platform runs this query faster?" ask "what business outcome are you trying to achieve, and what does your full data and AI lifecycle look like?"</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How to Use the Value Framework</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Start at the top:</strong> ask about the customer's business goals (revenue, cost reduction, risk, customer experience). Map those to data and AI use cases the platform enables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Middle layer:</strong> show how a unified platform eliminates the productivity tax of managing multiple tools and handoffs between teams.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Foundation layer:</strong> only after establishing business value should you discuss infrastructure advantages like Photon performance, autoscaling, and serverless pricing.</li>
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
# MAGIC ## B. The Lakehouse Advantage
# MAGIC
# MAGIC Section A established that competitive conversations should lead with business outcomes. The Lakehouse architecture is the structural foundation behind those outcomes. Understanding how it differs from traditional data warehouses and data lakes is essential for positioning against Snowflake, Redshift, and other warehouse-centric platforms.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Warehouse vs Lake vs Lakehouse
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Data warehouses and data lakes each solved part of the data management problem, but each introduced tradeoffs that limited what organizations could do. The <strong>Lakehouse</strong> combines the low-cost, scalable storage of data lakes with the performance, reliability, and ACID transactions of data warehouses, stored in open formats on the customer's own cloud storage.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-three-arch-compare ── -->
# MAGIC <style>
# MAGIC .b1a-wrap { display: flex; gap: 16px; margin: 24px 0; align-items: stretch; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b1a-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1a-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.15); }
# MAGIC .b1a-accent { height: 7px; flex-shrink: 0; }
# MAGIC .b1a-bar { padding: 16px 18px; text-align: center; }
# MAGIC .b1a-title { font-size: 16pt; font-weight: 700; color: #fff; line-height: 1.3; }
# MAGIC .b1a-body { background: #fff; padding: 16px 18px; flex: 1; }
# MAGIC .b1a-body ul { margin: 0; padding: 0 0 0 20px; }
# MAGIC .b1a-body li { font-size: 14pt; color: #333; line-height: 1.6; margin-bottom: 6px; }
# MAGIC .b1a-tag { display: inline-block; font-size: 11pt; font-weight: 600; padding: 3px 10px; border-radius: 12px; margin-top: 10px; }
# MAGIC .b1a-tag-limit { background: #FABFBA; color: #98102A; }
# MAGIC .b1a-tag-ok { background: #c8f7e1; color: #006644; }
# MAGIC .b1a-focus { border: 3px solid #1B5162; box-shadow: 0 0 0 3px rgba(27,81,98,0.15); }
# MAGIC </style>
# MAGIC <div class="b1a-wrap">
# MAGIC   <div class="b1a-card"><div class="b1a-accent" style="background:#618794;"></div><div class="b1a-bar" style="background:#618794;"><div class="b1a-title">Data Warehouse</div></div><div class="b1a-body"><ul><li><strong>Structured data</strong> and SQL/BI workloads</li><li>Proprietary storage formats</li><li>Strong governance and ACID transactions</li><li>Struggles with semi-structured/unstructured data, ML, and cost-effective scaling</li></ul><span class="b1a-tag b1a-tag-limit">Limited ML / Streaming</span></div></div>
# MAGIC   <div class="b1a-card"><div class="b1a-accent" style="background:#90A5B1;"></div><div class="b1a-bar" style="background:#90A5B1;"><div class="b1a-title">Data Lake</div></div><div class="b1a-body"><ul><li><strong>Any data type</strong> stored cheaply</li><li>Open formats, supports DS/ML</li><li>Lacks governance and data quality controls</li><li>Becomes a "data swamp" without active management</li></ul><span class="b1a-tag b1a-tag-limit">No Governance / Quality</span></div></div>
# MAGIC   <div class="b1a-card b1a-focus"><div class="b1a-accent" style="background:#1B5162;"></div><div class="b1a-bar" style="background:#1B5162;"><div class="b1a-title">Lakehouse</div></div><div class="b1a-body"><ul><li><strong>All data types</strong> on open formats (Delta Lake, Iceberg)</li><li>ACID transactions, schema enforcement, data quality</li><li>Unified batch, streaming, ML, and BI on one platform</li><li>Data stored on customer's cloud storage, not in vendor environment</li></ul><span class="b1a-tag b1a-tag-ok">Unified Platform</span></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Warehouses Hit a Wall</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Proprietary formats create lock-in:</strong> traditional warehouses store data in proprietary columnar formats. Moving data out (for ML, for example) requires expensive extraction and transformation.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>SQL-only limitation:</strong> warehouses were designed for SQL analysts. Data scientists who need Python, R, or Scala must use separate tools with separate copies of the data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cost scaling:</strong> as data volumes grow, warehouse costs increase because storage and compute are often coupled or priced at a premium versus raw cloud storage.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Lakes Became Swamps</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>No governance:</strong> data lakes store raw files without schema enforcement or access control, leading to data quality problems and compliance risks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Poor query performance:</strong> without optimization (indexing, statistics, caching), query performance on raw lakes is orders of magnitude slower than a warehouse.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> a data warehouse is a library with a strict card catalog that only accepts books. A data lake is a giant warehouse that can store anything but has no catalog. The lakehouse is a modern library with a universal catalog that handles books, videos, audio, and digital media, all organized and searchable.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How the Lakehouse Converges Both</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Delta Lake</strong> provides ACID transactions, schema enforcement, and time travel on top of cloud object storage in open Parquet-based format.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Apache Iceberg</strong> support via UniForm means data can be read by any engine that supports Iceberg, Delta, or Parquet, eliminating format lock-in.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog</strong> provides unified governance across all data and AI assets with fine-grained access control, lineage, and auditing.</li>
# MAGIC           <li style="font-size: 14pt;">The result: one copy of the data, one governance layer, accessible to all personas (SQL analysts, data engineers, data scientists, ML engineers) without duplication.</li>
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
# MAGIC ### B2. Six Requirements of an Open Data Architecture
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">A modern data platform must satisfy six requirements to avoid the limitations of both warehouses and lakes. These requirements provide a structured framework you can use with customers to evaluate any platform, including Databricks and its competitors.</p>
# MAGIC
# MAGIC <!-- ── Visual: b2-six-requirements ── -->
# MAGIC <style>
# MAGIC .b2r-wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b2r-card { border: 2px solid #DCE0E2; border-radius: 10px; padding: 16px 18px; background: #fff; display: flex; align-items: flex-start; gap: 14px; transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s; cursor: default; }
# MAGIC .b2r-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); border-color: #1B5162; }
# MAGIC .b2r-num { font-size: 18pt; font-weight: 800; color: #fff; background: #1B5162; width: 36px; height: 36px; border-radius: 50%; display: flex; align-items: center; justify-content: center; flex-shrink: 0; box-shadow: 0 2px 6px rgba(27,81,98,0.3); }
# MAGIC .b2r-text { font-size: 14pt; color: #333; line-height: 1.5; }
# MAGIC .b2r-text strong { color: #1B3139; }
# MAGIC .b2r-db { font-size: 12pt; color: #618794; margin-top: 4px; }
# MAGIC </style>
# MAGIC <div class="b2r-wrap">
# MAGIC   <div class="b2r-card"><div class="b2r-num">1</div><div><div class="b2r-text"><strong>Handles all of your data</strong></div><div class="b2r-db">Databricks: Delta Engine + Spark process structured, semi-structured, and unstructured data</div></div></div>
# MAGIC   <div class="b2r-card"><div class="b2r-num">2</div><div><div class="b2r-text"><strong>Reliable, cost-effective data pipelines</strong></div><div class="b2r-db">Databricks: Auto Loader + Lakeflow Connect + Structured Streaming for batch and streaming</div></div></div>
# MAGIC   <div class="b2r-card"><div class="b2r-num">3</div><div><div class="b2r-text"><strong>Open format, accessible across tools</strong></div><div class="b2r-db">Databricks: Delta Lake and Apache Iceberg (open source, no vendor lock-in)</div></div></div>
# MAGIC   <div class="b2r-card"><div class="b2r-num">4</div><div><div class="b2r-text"><strong>Curated data for analytics</strong></div><div class="b2r-db">Databricks: Databricks SQL + Unity Catalog governance</div></div></div>
# MAGIC   <div class="b2r-card"><div class="b2r-num">5</div><div><div class="b2r-text"><strong>Data science on all data</strong></div><div class="b2r-db">Databricks: built-in Python, R, Scala + MLflow + Mosaic AI</div></div></div>
# MAGIC   <div class="b2r-card"><div class="b2r-num">6</div><div><div class="b2r-text"><strong>Build once, access many times</strong></div><div class="b2r-db">Databricks: All personas work from the same governed platform</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How to Use This Framework</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Customer discovery tool:</strong> walk through these six requirements with the customer and ask how their current platform handles each one. Gaps in coverage reveal migration opportunities.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Snowflake gaps:</strong> Snowflake covers requirements 1 (partially, structured and semi-structured only), 4 (SQL analytics), and parts of 2. It falls short on unstructured data, open formats (proprietary internal storage), built-in data science (requirement 5), and unified multi-persona access (requirement 6).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AWS gaps:</strong> AWS covers individual requirements through separate services (Redshift for #4, SageMaker for #5, S3 + Glue for #2), but no single service satisfies all six. Integration and governance across services is the customer's responsibility.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Competitive Advantage</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Databricks is the only platform that addresses all six requirements natively in a single governed environment. This is not a feature comparison; it is an architectural argument.</li>
# MAGIC           <li style="font-size: 14pt;">As noted in the deck: "With Databricks, you don't have to re-architect for every new use case. All data personas are supported natively, which is not something that Snowflake can say."</li>
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
# MAGIC ## C. Databricks vs Snowflake
# MAGIC
# MAGIC Snowflake is the most frequent competitor in customer evaluations. This section covers the architectural differences, cost data, streaming and ML/AI gaps, and GenAI positioning. The goal is not to disparage Snowflake but to provide factual, defensible contrasts that help customers make informed decisions.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Architecture: Where Each Platform Stands
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Snowflake positions as the <strong>"AI Data Cloud"</strong>: a SaaS platform available on AWS, Azure, and GCP. Native tables use proprietary micro-partitioned storage in Snowflake's managed environment, though Iceberg Tables (GA, v3) now offer an open-format option. Databricks positions as the <strong>"Data Intelligence Platform"</strong>: an open architecture where data lives in Delta Lake or Apache Iceberg on the customer's own cloud storage, governed by Unity Catalog.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-arch-comparison ── -->
# MAGIC <style>
# MAGIC .c1t-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c1t-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .c1t-table thead th { background: #1B3139; color: #fff; padding: 14px 16px; font-size: 14pt; text-align: center; border: none; }
# MAGIC .c1t-table tbody tr { transition: background 0.2s, transform 0.15s; }
# MAGIC .c1t-table tbody tr:hover { background: #E8F0ED !important; transform: scale(1.005); }
# MAGIC .c1t-table tbody td { padding: 14px 16px; font-size: 14pt; border-bottom: 1px solid #EEEDE9; vertical-align: top; }
# MAGIC .c1t-table tbody tr:last-child td { border-bottom: none; }
# MAGIC .c1t-dim { font-weight: 700; color: #1B3139; background: #f8f8f8; }
# MAGIC .c1t-src { font-size: 14pt; color: #618794; margin-top: 4px; }
# MAGIC .c1t-src a { color: #2574B5; font-size: 14pt; }
# MAGIC </style>
# MAGIC <div class="c1t-wrap">
# MAGIC <table class="c1t-table">
# MAGIC   <thead><tr><th style="width:22%;">Dimension</th><th style="width:39%;">Snowflake (AI Data Cloud)</th><th style="width:39%;">Databricks (Data Intelligence Platform)</th></tr></thead>
# MAGIC   <tbody>
# MAGIC     <tr><td class="c1t-dim">Positioning</td><td>AI Data Cloud (SaaS). Expanding into agentic AI with Cortex Agents and Project SnowWork.</td><td>Data Intelligence Platform (open architecture). Lakehouse is the underlying storage/compute model.</td></tr>
# MAGIC     <tr><td class="c1t-dim">Storage Format</td><td>Native tables: proprietary micro-partitions in Snowflake-managed storage. Iceberg Tables: fully GA (v3, May 2026), can use external or Snowflake-managed storage.</td><td>Open formats: Delta Lake and managed Iceberg tables (GA on AWS and Azure; Public Preview on GCP, DBR 16.4 LTS+). UniForm enables external Iceberg and Hudi clients to read Delta tables without data duplication (read-only for external clients; Iceberg v3 support as of May 2026). Iceberg REST Catalog for external engine access (Public Preview).</td></tr>
# MAGIC     <tr><td class="c1t-dim">Data Residency</td><td>Native tables: Snowflake-managed environment. Iceberg Tables: can use customer's external storage.</td><td>Customer's cloud storage (S3, ADLS, GCS). Databricks provides compute and governance; customer retains data ownership.</td></tr>
# MAGIC     <tr><td class="c1t-dim">Data Types</td><td>Structured, semi-structured (native). Unstructured via Cortex AI functions (document parsing, image analysis) and staged files with Directory Tables.</td><td>Structured, semi-structured, unstructured (UC Volumes for files, <code style="font-size:14pt;">ai_parse_document</code> for PDFs/images). Audio/video processing available via hosted Gemini models through Model Serving.</td></tr>
# MAGIC     <tr><td class="c1t-dim">Languages</td><td>SQL (primary). Python, Java, Scala via Snowpark and native UDFs/stored procedures. JavaScript for UDFs/stored procedures. GA notebooks (Python + SQL, February 2026).</td><td>SQL, Python (primary). Scala, R in notebooks. Java via JAR tasks in Lakeflow Jobs.</td></tr>
# MAGIC     <tr><td class="c1t-dim">Data Sharing</td><td>Listings and Snowflake Marketplace (800+ providers, 3,400+ listings). Cross-cloud auto-fulfillment. Secure Data Sharing for same-region. Snowflake Open Catalog (managed Apache Polaris, open-source Iceberg REST catalog). Horizon Catalog enables external engine read/write to Iceberg tables (GA, May 2026).</td><td>Delta Sharing (open protocol, no platform requirement). Directory-based access mode (GA, April 2026). External sharing to Iceberg clients (GA). Foreign Iceberg table sharing (Private Preview).</td></tr>
# MAGIC     <tr><td class="c1t-dim">Streaming</td><td>Snowpipe Streaming: row-level ingestion, sub-10s latency, GA on all clouds (2025). Classic Snowpipe: file-based micro-batch.</td><td>Structured Streaming: true streaming with sub-second latency (Real-Time Mode). Auto Loader for incremental file ingestion.</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <div style="font-size:14pt;color:#618794;margin-top:10px;">Sources: <a href="https://docs.snowflake.com/en/user-guide/tables-iceberg" style="color:#2574B5;font-size:14pt;">Snowflake Iceberg Tables</a> | <a href="https://docs.databricks.com/aws/en/iceberg/" style="color:#2574B5;font-size:14pt;">Databricks Iceberg</a> | <a href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview" style="color:#2574B5;font-size:14pt;">Snowpipe Streaming</a> | <a href="https://docs.databricks.com/aws/en/structured-streaming/" style="color:#2574B5;font-size:14pt;">Structured Streaming</a> | <a href="https://docs.snowflake.com/en/guides-overview-sharing" style="color:#2574B5;font-size:14pt;">Snowflake Sharing</a> | <a href="https://docs.databricks.com/aws/en/delta-sharing/" style="color:#2574B5;font-size:14pt;">Delta Sharing</a></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The "Black Box" Concern</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Data sovereignty:</strong> Snowflake manages data in its own cloud environment. For regulated industries (financial services, healthcare, public sector), this "black box" model raises compliance concerns about where data physically resides and who has access.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Databricks approach:</strong> data stays on the customer's cloud storage account. Databricks provides the compute and management layer, but the customer retains ownership and physical control of their data.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Snowflake's Lakehouse Positioning</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Snowflake has rebranded from "Cloud Data Warehouse" to "The AI Data Cloud" and adopted lakehouse terminology. However, native tables still use proprietary micro-partitioned storage.</li>
# MAGIC           <li style="font-size: 14pt;">Snowflake's Iceberg Tables reached v3 GA (May 2026) with deletion vectors and external engine support, but some Snowflake-native features (such as extended time travel beyond 7 days and certain DML optimizations) may not yet be available for Iceberg tables. The rebranding validates the lakehouse vision Databricks pioneered.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Common student question:</strong> "Isn't Snowflake also a lakehouse now?" Answer: Snowflake has adopted the terminology and expanded Python/ML capabilities (GA notebooks, February 2026; Snowpark ML with GPU support), but its core data management architecture remains warehouse-centric with SQL as the primary query interface. The data residency model and proprietary internal format for native tables have not changed.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Acknowledge Snowflake's Strengths</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">For SQL/BI and data warehousing workloads, Snowflake provides a simple, fully managed experience with an intuitive SQL interface. Snowflake has also expanded into ML (Snowpark ML with GPU support), AI (Cortex AI functions, Cortex Agents), and development (GA notebooks, Snowflake Postgres). These capabilities are maturing rapidly. Acknowledging this builds credibility.</li>
# MAGIC           <li style="font-size: 14pt;">The pivot question: "Where do you need the most flexibility -- model development, streaming pipelines, or multi-cloud portability?" This shifts the conversation to areas where the architectural differences matter.</li>
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
# MAGIC ### C2. Cost and TCO Comparison
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Cost data is one of the strongest tools in competitive positioning. Internal Databricks analysis shows that customers spend <strong>$0.10 to $0.35 for every $1.00</strong> Snowflake customers spend across common workloads. The savings vary by workload type, with the largest differences in data movement (ingress/egress) and the smallest in data science notebooks.</p>
# MAGIC
# MAGIC <!-- ── Visual: c2-cost-comparison ── -->
# MAGIC <style>
# MAGIC .c2c-wrap { max-width: 760px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c2c-title { text-align: center; font-size: 15pt; color: #1B5162; font-weight: 700; margin-bottom: 18px; }
# MAGIC .c2c-row { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
# MAGIC .c2c-label { font-size: 14pt; color: #333; width: 140px; text-align: right; font-weight: 600; flex-shrink: 0; }
# MAGIC .c2c-bar-bg { flex: 1; background: #EEEDE9; border-radius: 8px; height: 38px; position: relative; overflow: hidden; }
# MAGIC .c2c-bar-fill { height: 100%; border-radius: 8px; display: flex; align-items: center; justify-content: flex-end; padding-right: 12px; animation: c2cGrow 1s ease-out forwards; transform-origin: left; }
# MAGIC @keyframes c2cGrow { from { transform: scaleX(0); } to { transform: scaleX(1); } }
# MAGIC .c2c-bar-val { font-size: 13pt; font-weight: 700; color: #fff; }
# MAGIC .c2c-note { font-size: 12pt; color: #618794; text-align: center; margin-top: 10px; font-style: italic; }
# MAGIC </style>
# MAGIC <div class="c2c-wrap">
# MAGIC   <div class="c2c-title">Databricks Cost per $1.00 of Snowflake Spend</div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">Ingress/Egress</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:10%;background:#00A972;"><div class="c2c-bar-val">$0.10</div></div></div></div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">SQL/BI</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:22%;background:#00A972;"><div class="c2c-bar-val">$0.22</div></div></div></div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">ETL</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:28%;background:#1B5162;"><div class="c2c-bar-val">$0.28</div></div></div></div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">Streaming</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:29%;background:#1B5162;"><div class="c2c-bar-val">$0.29</div></div></div></div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">ML</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:31%;background:#618794;"><div class="c2c-bar-val">$0.31</div></div></div></div>
# MAGIC   <div class="c2c-row"><div class="c2c-label">DS Notebooks</div><div class="c2c-bar-bg"><div class="c2c-bar-fill" style="width:35%;background:#618794;"><div class="c2c-bar-val">$0.35</div></div></div></div>
# MAGIC   <div class="c2c-note">Source: Internal Databricks competitive analysis. Actual savings vary by workload profile and configuration.<br/>* Ingress/Egress: applies to heavy cross-region/cross-cloud egress scenarios. Snowflake does not charge ingress; egress applies only cross-region/cross-cloud at $20-$155/TB. Snowflake Egress Cost Optimizer (April 2025) reduces costs up to 96%. Databricks also charges platform-level transfer fees for serverless products.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why the Cost Difference Exists</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Data movement costs:</strong> Snowflake does not charge for data ingestion, but egress fees apply when data is transferred out of Snowflake across regions or clouds (e.g., via COPY INTO unload or database replication). With Databricks, data remains on the customer's cloud storage, avoiding the managed-warehouse storage model. Cross-region data access, serverless networking fees, and cloud-provider egress still apply. The key advantage is that customers retain full control over storage placement and can co-locate compute with data to minimize transfer costs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Compute model:</strong> Snowflake uses credit-based pricing with virtual warehouses; multi-cluster autoscaling and Adaptive Compute (June 2025, automatic sizing) help manage costs. Databricks uses DBU-based pricing plus separate cloud infrastructure costs, with autoscaling clusters, serverless options, and spot/preemptible instance support for significant savings on sustained workloads.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Third-party analysis:</strong> a 2021 TPC-DS-derived benchmark by the Barcelona Supercomputing Center found Databricks to be 2.7x faster and 7-12x better in price-performance (depending on pricing model). Note: Snowflake disputed the methodology, and both platforms have released newer benchmarks since (see caveats below).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Migration Case Studies</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/getyourguide/dbsql" style="color: #2574B5; font-size: 14pt;">GetYourGuide</a> (travel) migrated 750 Snowflake tables to Databricks SQL: 20% reduction in BI serving costs, 35% reduction in average query time for their marketing model, and BI reports landing 1.5 hours earlier. Completed in 4.5 months with 2 engineers and partner support. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/bayada" style="color: #2574B5; font-size: 14pt;">BAYADA</a> (healthcare) consolidated Snowflake + SQL Server into Databricks: expected 35% faster data processing, 40% reduction in reconciliation efforts, and 20% lower operational costs. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/company/partners/consulting-and-si/partner-solutions/lovelytics-snowflake-migration" style="color: #2574B5; font-size: 14pt;">Lovelytics</a> (partner solution) standardized Snowflake-to-Databricks migration solution, citing platform-level advantages of 2.7x faster performance and up to 12x cost efficiency (Barcelona Supercomputing Center benchmark). &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Important Caveats</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Third-party analysis notes: for pure SQL workloads with bursty usage patterns, Snowflake's per-second billing (with a 60-second minimum per resume) and auto-suspend can be competitive. The largest Databricks TCO advantages appear in sustained processing, ML, and streaming workloads.</li>
# MAGIC           <li style="font-size: 14pt;">ETL workloads alone can account for 50% or more of total platform spend (Onehouse, industry analyses). Neither vendor's published rate card captures what a customer will pay once you factor in storage, egress, support contracts, and engineering overhead.</li>
# MAGIC           <li style="font-size: 14pt;">Databricks' cost advantage depends on optimization expertise. Total cost includes DBUs plus separate cloud-provider charges for VMs, storage, and networking, which can equal the DBU bill (Flexera, 2026). Snowflake's all-inclusive credit model is simpler to forecast. Poorly optimized Databricks deployments can exceed Snowflake costs for equivalent workloads.</li>
# MAGIC           <li style="font-size: 14pt;">Snowflake has published counter-benchmarks, including a May 2026 TPCx-AI result claiming up to 2.5x faster distributed ML training vs Databricks. Databricks' own 2025 TPC-DI benchmark showed 2.8x faster ETL. As with all vendor-run benchmarks, treat these as directional rather than definitive.</li>
# MAGIC           <li style="font-size: 14pt;">Snowflake pricing has evolved in 2025-2026: Adaptive Compute (June 2025) for automatic warehouse sizing, capacity commitments (25-45% per-credit discount), and Egress Cost Optimizer (up to 96% reduction in cross-region replication costs). SAs should be current on these to maintain credibility in customer conversations.</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html" style="color:#2574B5;font-size:12pt;">BSC Benchmark (Databricks)</a> | <a href="https://docs.snowflake.com/en/user-guide/cost-understanding-compute" style="color:#2574B5;font-size:12pt;">Snowflake Compute Costs</a> | <a href="https://docs.snowflake.com/en/user-guide/cost-understanding-data-transfer" style="color:#2574B5;font-size:12pt;">Snowflake Data Transfer Costs</a> | <a href="https://www.databricks.com/customers/getyourguide/dbsql" style="color:#2574B5;font-size:12pt;">GetYourGuide</a> | <a href="https://www.databricks.com/customers/bayada" style="color:#2574B5;font-size:12pt;">BAYADA</a></div>
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
# MAGIC ### C3. Streaming and Data Engineering
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Streaming architectures differ significantly between the two platforms. Snowflake's streaming capabilities have expanded substantially since 2024 -- <strong>Snowpipe Streaming</strong> (sub-10s, GA September 2025) and <strong>Openflow</strong> (23+ native connectors) address ingestion, while <strong>Dynamic Tables</strong> handle incremental transformation. Databricks treats streaming and batch as equals via <strong>Structured Streaming</strong>, with a unified programming model that supports true low-latency stream processing down to 5ms end-to-end (<strong>Real-Time Mode</strong>, GA March 2026).</p>
# MAGIC
# MAGIC <!-- ── Visual: c3-streaming-comparison ── -->
# MAGIC <style>
# MAGIC .c3t-wrap { margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c3t-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .c3t-table thead th { background: #1B3139; color: #fff; padding: 14px 16px; font-size: 14pt; text-align: center; border: none; }
# MAGIC .c3t-table tbody tr { transition: background 0.2s, transform 0.15s; }
# MAGIC .c3t-table tbody tr:hover { background: #E8F0ED !important; transform: scale(1.005); }
# MAGIC .c3t-table tbody td { padding: 12px 16px; font-size: 14pt; border-bottom: 1px solid #EEEDE9; vertical-align: top; }
# MAGIC .c3t-table tbody tr:last-child td { border-bottom: none; }
# MAGIC .c3t-cap { font-weight: 700; color: #1B3139; background: #f8f8f8; }
# MAGIC </style>
# MAGIC <div class="c3t-wrap">
# MAGIC <table class="c3t-table">
# MAGIC   <thead><tr><th style="width:25%;">Capability</th><th style="width:37.5%;">Snowflake</th><th style="width:37.5%;">Databricks</th></tr></thead>
# MAGIC   <tbody>
# MAGIC     <tr><td class="c3t-cap">Ingestion</td><td>Snowpipe Streaming: row-level, sub-10s latency, in-flight transforms (filtering, casts, expressions via PIPE objects). Classic Snowpipe: file-based micro-batch, minute-level latency, limited transforms (column reordering, casts, string functions -- no filtering, joins, or aggregations).</td><td>Auto Loader + Structured Streaming: continuous ingestion with transformations, as low as 5ms end-to-end latency with Real-Time Mode (GA March 2026)</td></tr>
# MAGIC     <tr><td class="c3t-cap">Pipelines</td><td>Dynamic Tables: primarily SQL-based (UDFs supported), 1-minute minimum target lag, data quality monitoring via data metric functions (DMFs). Lineage via Horizon governance (column-level via GET_LINEAGE).</td><td>Lakeflow Spark Declarative Pipelines: SQL + Python, schema evolution, data quality expectations, Unity Catalog lineage</td></tr>
# MAGIC     <tr><td class="c3t-cap">Sources</td><td>Kafka, Amazon Kinesis, Azure Event Hubs, REST API, custom sources via SDKs (Java, Python, Node.js). Snowflake Openflow (GA): 23+ native managed connectors including CDC for Oracle, PostgreSQL, MySQL, SQL Server, and Salesforce.</td><td>Kafka, Kinesis, Event Hubs, IoT Hub, Delta tables (change feeds), cloud storage (Auto Loader), custom sources via foreachBatch</td></tr>
# MAGIC     <tr><td class="c3t-cap">Latency</td><td>Classic Snowpipe: minutes. Snowpipe Streaming: sub-10 seconds.</td><td>As low as 5ms end-to-end (Real-Time Mode, GA March 2026). Workload-dependent; sub-second for most streaming patterns.</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <div style="font-size:14pt;color:#618794;margin-top:10px;">Sources: <a href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro" style="color:#2574B5;font-size:14pt;">Snowpipe</a> | <a href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview" style="color:#2574B5;font-size:14pt;">Snowpipe Streaming</a> | <a href="https://docs.snowflake.com/en/user-guide/dynamic-tables-about" style="color:#2574B5;font-size:14pt;">Dynamic Tables</a> | <a href="https://docs.snowflake.com/en/user-guide/data-quality-expectations" style="color:#2574B5;font-size:14pt;">Snowflake Data Quality</a> | <a href="https://docs.snowflake.com/en/user-guide/data-load-transform" style="color:#2574B5;font-size:14pt;">Snowflake Load Transforms</a> | <a href="https://docs.databricks.com/aws/en/structured-streaming/" style="color:#2574B5;font-size:14pt;">Structured Streaming</a></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Clarifying "Snowflake Streaming"</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Snowpipe is a file-based ingestion service, not a streaming engine. It supports limited transformations during ingestion (column reordering, casts, string functions via SELECT in COPY INTO) but cannot filter, join, or aggregate. It does not support upserts, focuses on a single table at a time, and auto-ingest mode requires external cloud event services (SNS, Event Grid, Pub/Sub) to trigger loads; alternatively, loads can be triggered via REST API.</li>
# MAGIC           <li style="font-size: 14pt;">Note: Snowpipe Streaming (distinct from classic Snowpipe) supports row-level ingestion with in-flight filtering, casts, and expressions. The transform limitations above apply to classic Snowpipe only.</li>
# MAGIC           <li style="font-size: 14pt;">Dynamic Tables support data quality monitoring via data metric functions (DMFs) and Cortex Data Quality. Minimum target lag is 1 minute. Python/Java/Scala UDFs are supported but force full refresh unless marked IMMUTABLE. Custom Incremental (Public Preview, May 2026) and Adaptive refresh modes offer workarounds for some patterns. Lag accumulation remains possible in multi-hop pipelines.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Classic Snowpipe is postal delivery (minutes). Snowpipe Streaming is express courier (sub-10 seconds). Databricks Structured Streaming is a direct phone line (sub-second). Real-Time Mode is instant messaging (as low as 5ms).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Developer Framework Comparison</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Snowpark is Snowflake's developer framework for Python/Java/Scala. Snowpark Connect for Spark (GA) enables running existing Spark workloads on Snowflake without code rewrites, and pandas on Snowflake provides a pandas-compatible API. Some Python library restrictions apply in Snowflake's sandboxed runtime. Compute is billed at Snowflake warehouse rates.</li>
# MAGIC           <li style="font-size: 14pt;">Databricks natively supports Apache Spark, Python, Scala, SQL, and R. Existing Spark code runs unchanged with Photon acceleration. The key differentiator is Databricks' broader ecosystem support: GPU clusters, distributed training, and direct access to data on the customer's cloud storage without staging.</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro" style="color:#2574B5;font-size:12pt;">Snowpipe</a> | <a href="https://docs.snowflake.com/en/user-guide/data-load-transform" style="color:#2574B5;font-size:12pt;">Snowpipe Transforms</a> | <a href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto" style="color:#2574B5;font-size:12pt;">Snowpipe Auto-Ingest</a> | <a href="https://docs.snowflake.com/en/user-guide/dynamic-tables-about" style="color:#2574B5;font-size:12pt;">Dynamic Tables</a> | <a href="https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview" style="color:#2574B5;font-size:12pt;">Snowpark Connect</a> | <a href="https://docs.snowflake.com/en/developer-guide/snowpark/python/pandas-on-snowflake" style="color:#2574B5;font-size:12pt;">pandas on Snowflake</a></div>
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
# MAGIC ### C4. ML/AI and GenAI Positioning
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The GenAI landscape remains a key differentiator, though the gap has narrowed as Snowflake and AWS have expanded their AI capabilities through 2025-2026. Databricks supports <strong>all four major AI architectural patterns</strong>: prompt engineering, RAG, fine-tuning, and pretraining. Snowflake Cortex provides SQL-accessible LLM functions, managed RAG (Cortex Search + Agents), and parameter-efficient fine-tuning of select open-source models, but does not support pretraining custom foundation models from scratch. Databricks offers broader model flexibility, GPU-intensive training, and deeper customization across the full AI lifecycle.</p>
# MAGIC
# MAGIC <!-- ── Visual: c4-genai-matrix ── -->
# MAGIC <style>
# MAGIC .c4m-wrap { margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c4m-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .c4m-table thead th { background: #1B3139; color: #fff; padding: 14px 16px; font-size: 14pt; text-align: center; border: none; }
# MAGIC .c4m-table tbody tr { transition: background 0.2s, transform 0.15s; }
# MAGIC .c4m-table tbody tr:hover { background: #E8F0ED !important; transform: scale(1.005); }
# MAGIC .c4m-table tbody td { padding: 12px 16px; font-size: 14pt; border-bottom: 1px solid #EEEDE9; vertical-align: middle; text-align: center; }
# MAGIC .c4m-table tbody tr:last-child td { border-bottom: none; }
# MAGIC .c4m-pat { font-weight: 700; color: #1B3139; text-align: left !important; background: #f8f8f8; }
# MAGIC .c4m-yes { color: #00A972; font-weight: 700; }
# MAGIC .c4m-ltd { color: #FFAB00; font-weight: 700; }
# MAGIC .c4m-no { color: #98102A; font-weight: 700; }
# MAGIC </style>
# MAGIC <div class="c4m-wrap">
# MAGIC <table class="c4m-table">
# MAGIC   <thead><tr><th style="width:30%;">GenAI Pattern</th><th style="width:23%;">Databricks<br/>(Mosaic AI)</th><th style="width:23%;">Snowflake<br/>(Cortex)</th><th style="width:24%;">AWS<br/>(Bedrock + SageMaker)</th></tr></thead>
# MAGIC   <tbody>
# MAGIC     <tr><td class="c4m-pat">Prompt Engineering</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td></tr>
# MAGIC     <tr><td class="c4m-pat">RAG</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td></tr>
# MAGIC     <tr><td class="c4m-pat">Fine-Tuning</td><td class="c4m-yes">&#x2713;</td><td class="c4m-ltd">Limited</td><td class="c4m-ltd">Limited</td></tr>
# MAGIC     <tr><td class="c4m-pat">Pretraining</td><td class="c4m-yes">&#x2713;</td><td class="c4m-no">&#x2717;</td><td class="c4m-yes">&#x2713;</td></tr>
# MAGIC     <tr><td class="c4m-pat">Agentic AI</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td><td class="c4m-yes">&#x2713;</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <div style="font-size:14pt;color:#618794;margin-top:10px;">Sources: <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-finetuning" style="color:#2574B5;font-size:14pt;">Cortex Fine-tuning</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview" style="color:#2574B5;font-size:14pt;">Cortex Search (RAG)</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents" style="color:#2574B5;font-size:14pt;">Cortex Agents</a> | <a href="https://docs.databricks.com/aws/en/machine-learning/" style="color:#2574B5;font-size:14pt;">Mosaic AI</a> | <a href="https://docs.databricks.com/aws/en/generative-ai/external-models/" style="color:#2574B5;font-size:14pt;">Bedrock Integration</a></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Persona-Based Positioning</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>SQL analysts:</strong> Snowflake Cortex provides 15+ AI functions via SQL, with AI_COMPLETE offering general-purpose LLM access to models from OpenAI, Anthropic, Meta, Mistral, and DeepSeek -- all running within Snowflake's security perimeter. Functions cover classification, extraction, sentiment analysis, translation, summarization, embedding generation, document parsing, and PII redaction. For SQL analysts, Cortex offers fast time-to-value with minimal engineering.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Developers and data scientists:</strong> for pretraining domain-specific foundation models from scratch, Snowflake has no offering. For fine-tuning, RAG, and traditional ML model building, Snowflake now provides managed services (Cortex Fine-tuning, Cortex Search/Agents, Snowpark ML). Databricks Mosaic AI offers greater flexibility: broader model support, GPU cluster access for distributed training, custom training loops, and a more mature MLOps lifecycle with experiment tracking, model evaluation, and multi-cloud serving. Snowflake also supports bring-your-own-model (BYOM) via the Model Registry, allowing externally trained or fine-tuned models to be deployed for real-time inference within Snowflake's governed environment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Key question for customers:</strong> "How much control do you need over the model layer itself? Snowflake provides managed AI services with increasing customization (agents, fine-tuning, BYOM). Databricks provides that plus direct GPU cluster access, custom training loops, and full pretraining for organizations that need to own the model development lifecycle end-to-end."</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">AI Governance Comparison</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Databricks:</strong> Unity Catalog tracks prompts, embeddings, vector indexes, RAG knowledge bases, and model lineage in the same governance layer that manages data -- one policy engine and one audit trail across the full ML lifecycle (experiment tracking, model registry, feature store, serving).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Snowflake:</strong> Horizon Catalog provides RBAC/ABAC, data classification, lineage, and metadata for AI agents. Cortex Guard filters harmful LLM responses at runtime. Row access policies are enforced on Cortex Agents.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Key differentiator:</strong> Unity Catalog's advantage is tighter integration across the full ML lifecycle within one governance boundary. Snowflake's advantage is governance applied directly to its managed AI services within the Snowflake perimeter. Both are operationally significant for regulated industries where auditors need to trace how data flows through AI systems.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Agentic AI Comparison</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Snowflake Cortex Agents (GA Nov 2025) provide multi-step reasoning, tool orchestration via MCP, and governed data access for agentic workflows. They orchestrate across Cortex Search (unstructured) and Cortex Analyst (structured) with sandboxed code execution and row access policy enforcement.</li>
# MAGIC           <li style="font-size: 14pt;">Databricks Mosaic AI Agent Framework offers broader model flexibility, custom tool chains, deeper MLOps integration, and the ability to deploy agents as production endpoints with tracing and evaluation via MLflow.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Databricks + Bedrock: Complementary, Not Competitive</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">In October 2024, Databricks and AWS announced a strategic collaboration for custom models built with Mosaic AI on AWS Trainium. Databricks Model Serving can proxy to Bedrock models as external endpoints, applying Unity Catalog governance and MLflow tracing to Bedrock API calls.</li>
# MAGIC           <li style="font-size: 14pt;">Position Bedrock as a complement: customers can use Bedrock for API-based access to pre-trained models (with supervised, reinforcement, and distillation fine-tuning on select models like Titan, Nova, Llama, and Qwen; broader model support and custom training loops available via SageMaker) and Databricks for the full AI development lifecycle (data preparation, fine-tuning, evaluation, deployment, governance).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Interoperability note:</strong> Many enterprise customers run both Databricks and Snowflake. For organizations using Databricks for ML/AI and Snowflake for BI/SQL, Apache Iceberg provides a bidirectional interoperability layer (both platforms support Iceberg v3 GA as of May 2026), enabling shared data access without proprietary lock-in.</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql" style="color:#2574B5;font-size:12pt;">Cortex AI Functions</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-finetuning" style="color:#2574B5;font-size:12pt;">Cortex Fine-tuning</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents" style="color:#2574B5;font-size:12pt;">Cortex Agents</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview" style="color:#2574B5;font-size:12pt;">Cortex Search</a> | <a href="https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/overview" style="color:#2574B5;font-size:12pt;">Snowflake Model Registry</a> | <a href="https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/bring-your-own-model-types" style="color:#2574B5;font-size:12pt;">Snowflake BYOM</a> | <a href="https://www.snowflake.com/en/product/features/horizon/" style="color:#2574B5;font-size:12pt;">Horizon Catalog</a> | <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-ai-guardrails" style="color:#2574B5;font-size:12pt;">Cortex Guard</a> | <a href="https://docs.databricks.com/aws/en/generative-ai/external-models/" style="color:#2574B5;font-size:12pt;">Databricks + Bedrock</a> | <a href="https://docs.databricks.com/aws/en/mlflow3/genai/tracing/integrations/bedrock" style="color:#2574B5;font-size:12pt;">MLflow Bedrock Tracing</a> | <a href="https://aws.amazon.com/bedrock/agents/" style="color:#2574B5;font-size:12pt;">Bedrock Agents</a> | <a href="https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-hyperpod.html" style="color:#2574B5;font-size:12pt;">SageMaker HyperPod</a></div>
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
# MAGIC ## D. Databricks vs AWS built-in
# MAGIC
# MAGIC AWS is the second most common competitive scenario. The challenge is different from Snowflake: rather than competing against a single platform, you are competing against a collection of AWS services (EMR, Redshift, SageMaker, Bedrock, Glue, Athena, Kinesis) that customers must assemble and integrate themselves.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. The AWS Ecosystem Challenge
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">AWS provides a broad ecosystem of data and AI services with Amazon S3 at the center. Each service addresses a specific workload: EMR for Spark processing, Redshift for SQL warehousing, SageMaker for ML, Bedrock for GenAI, Glue for ETL, Athena for ad-hoc queries, and Kinesis for streaming. Historically, <strong>no single AWS service covered the full data and AI lifecycle</strong>, and integrating them required significant orchestration, governance stitching, and DevOps effort. AWS has responded with <strong>SageMaker Unified Studio (GA March 2025)</strong>, which provides a single IDE spanning EMR, Glue, Athena, Redshift, Bedrock, and SageMaker AI with governance via SageMaker Catalog. This reduces the integration burden, though customers still manage separate underlying services, billing, and IAM configurations. The Databricks advantage shifts from "single platform vs many services" to depth of Unity Catalog governance, Photon-optimized performance, and a longer track record as an integrated lakehouse.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-aws-hub ── -->
# MAGIC <style>
# MAGIC .d1h-wrap { max-width: 700px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .d1h-center { background: #1B3139; color: #fff; border-radius: 50%; width: 130px; height: 130px; display: flex; align-items: center; justify-content: center; font-size: 16pt; font-weight: 700; text-align: center; margin: 0 auto 12px auto; line-height: 1.3; box-shadow: 0 4px 16px rgba(27,49,57,0.22); }
# MAGIC .d1h-conn { display: flex; justify-content: center; height: 14px; margin-bottom: 4px; }
# MAGIC .d1h-conn::before { content: ''; width: 3px; height: 100%; background: #94b3be; border-radius: 2px; display: block; }
# MAGIC .d1h-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }
# MAGIC .d1h-svc { border: 2px solid #DCE0E2; border-radius: 10px; padding: 14px 10px; text-align: center; background: #fff; transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s; cursor: default; }
# MAGIC .d1h-svc:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.12); border-color: #618794; }
# MAGIC .d1h-name { font-size: 14pt; font-weight: 600; color: #1B5162; }
# MAGIC .d1h-role { font-size: 12pt; color: #618794; margin-top: 2px; }
# MAGIC .d1h-note { text-align: center; font-size: 14pt; color: #98102A; font-weight: 600; margin-top: 18px; }
# MAGIC </style>
# MAGIC <div class="d1h-wrap">
# MAGIC   <div class="d1h-center">Amazon<br/>S3</div>
# MAGIC   <div class="d1h-conn"></div>
# MAGIC   <div class="d1h-grid">
# MAGIC     <div class="d1h-svc"><div class="d1h-name">EMR</div><div class="d1h-role">Spark Processing</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">Redshift</div><div class="d1h-role">SQL Warehouse</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">SageMaker</div><div class="d1h-role">ML Studio</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">Bedrock</div><div class="d1h-role">GenAI / LLMs</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">Glue</div><div class="d1h-role">ETL</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">Kinesis</div><div class="d1h-role">Streaming</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">Athena</div><div class="d1h-role">Ad-Hoc Queries</div></div>
# MAGIC     <div class="d1h-svc" style="border-color:#E5A100;border-width:3px;"><div class="d1h-name">Lake Formation</div><div class="d1h-role">Cross-cutting Governance Layer</div></div>
# MAGIC     <div class="d1h-svc"><div class="d1h-name">QuickSight</div><div class="d1h-role">BI</div></div>
# MAGIC   </div>
# MAGIC   <div class="d1h-note">Each service requires separate management, governance, and integration</div>
# MAGIC   <div style="margin-top:12px;padding:12px 16px;background:#FFF8E1;border:2px solid #E5A100;border-radius:8px;font-size:12pt;color:#1B3139;"><strong style="color:#E5A100;">Note:</strong> AWS responded with SageMaker Unified Studio (GA March 2025), which provides a single IDE across these services. The underlying services, billing, and IAM remain separate, but the user-facing integration gap is narrowing.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Integration Tax</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Orchestration overhead:</strong> a common AWS data pipeline for analytics and ML might flow S3 to Glue to Redshift to SageMaker, with each handoff requiring configuration, IAM permissions, and monitoring. Even simpler patterns (e.g., Athena queries on S3) still require separate governance and IAM setup per service. Databricks handles the data platform layer -- compute, governance, orchestration, and ML lifecycle -- in a single governed environment. Initial AWS infrastructure setup (IAM roles, VPC, S3 policies) is still required, but ongoing data operations are unified under Unity Catalog.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Governance stitching:</strong> AWS distributes governance across multiple services: Glue Data Catalog for metadata, Lake Formation for fine-grained access control (column, row, and cell-level security with LF-tag-based ABAC), and SageMaker Catalog for lineage and data discovery. Unity Catalog consolidates all of this -- metadata, access control, lineage, and AI asset governance -- into a single system with a single permission model, which is simpler to administer and audit.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> AWS has historically been a toolkit -- powerful individual services that customers assemble into custom architectures. This offers maximum flexibility but increases integration and governance overhead. Databricks takes the opposite approach: a pre-integrated platform where compute, governance, and ML lifecycle are unified by design. AWS is narrowing this gap with SageMaker Unified Studio (GA March 2025), but Databricks' integration is deeper and more mature. When selling against AWS, emphasize Unity Catalog's single permission model and the operational simplicity of one platform.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>EMR Serverless (GA June 2022):</strong> eliminates cluster management for Spark workloads with automatic scaling and per-second billing. The Databricks advantage over EMR Serverless is not cluster management but rather the integrated governance, ML lifecycle, and SQL analytics that EMR Serverless alone does not provide.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Zero-ETL integrations:</strong> AWS Zero-ETL (Aurora/DynamoDB to Redshift, GA October 2024) reduces pipeline overhead for specific source-to-warehouse patterns. However, Zero-ETL only covers the ingestion step into Redshift; it does not unify governance, ML, or streaming workloads.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Migration Stories</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/freshworks" style="color: #2574B5; font-size: 14pt;">Freshworks</a> (SaaS) migrated 500+ TB from a self-managed Hadoop (Cloudera CDH) platform to Databricks, choosing Databricks over AWS and Azure native tools after evaluation: 75% reduction in platform maintenance costs, 60% increase in data team productivity, 4-5x faster data processing. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/iterable" style="color: #2574B5; font-size: 14pt;">Iterable</a> (MarTech) migrated from EMR: "It was really hard to leverage our data and rapidly build models with EMR, as we spent significant resources maintaining infrastructure and debugging Spark issues rather than developing solutions to help our customers win." -- Ankur Mathur, Senior Engineering Manager for AI and Experimentation, Iterable. Now runs 5,000+ pipelines and 2,000+ MLflow projects on Databricks. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When AWS Native May Be Harder to Displace</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The customer runs primarily Redshift Serverless workloads with no ML/AI requirements.</li>
# MAGIC           <li style="font-size: 14pt;">The customer has a simple S3 + Athena + QuickSight stack with minimal governance complexity.</li>
# MAGIC           <li style="font-size: 14pt;">The customer is deeply embedded in Bedrock/SageMaker for GenAI with no analytics needs on Databricks.</li>
# MAGIC           <li style="font-size: 14pt;">The customer uses SageMaker Unified Studio successfully for combined data engineering + ML.</li>
# MAGIC           <li style="font-size: 14pt;">In these scenarios, lead with a specific workload expansion (e.g., streaming, ML governance, multi-cloud) rather than full platform displacement.</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://aws.amazon.com/sagemaker/unified-studio/" style="color:#2574B5;font-size:12pt;">SageMaker Unified Studio</a> | <a href="https://aws.amazon.com/sagemaker/catalog/" style="color:#2574B5;font-size:12pt;">SageMaker Catalog</a> | <a href="https://aws.amazon.com/lake-formation/features/" style="color:#2574B5;font-size:12pt;">Lake Formation</a> | <a href="https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html" style="color:#2574B5;font-size:12pt;">EMR Serverless</a> | <a href="https://aws.amazon.com/rds/aurora/zero-etl/" style="color:#2574B5;font-size:12pt;">Zero-ETL</a> | <a href="https://www.databricks.com/customers/freshworks" style="color:#2574B5;font-size:12pt;">Freshworks</a> | <a href="https://www.databricks.com/customers/iterable" style="color:#2574B5;font-size:12pt;">Iterable</a></div>
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
# MAGIC ### D2. EMR, Redshift, SageMaker, and Bedrock
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Each AWS service competes with a specific Databricks capability. Historically, AWS provided point solutions that required assembly, while Databricks offered a unified platform. AWS has responded with SageMaker Unified Studio (GA March 2025), which brings data engineering, analytics, ML, and GenAI into a single environment. However, the underlying services remain architecturally separate, and Databricks' platform was designed as unified from the ground up. The comparison below reflects the current state as of June 2026.</p>
# MAGIC
# MAGIC <!-- ── Visual: d2-aws-service-cards ── -->
# MAGIC <style>
# MAGIC .d2f-wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; perspective: 1200px; }
# MAGIC .d2f-card { height: 280px; perspective: 1000px; cursor: pointer; }
# MAGIC .d2f-inner { position: relative; width: 100%; height: 100%; transition: transform 0.6s cubic-bezier(0.4,0,0.2,1); transform-style: preserve-3d; }
# MAGIC .d2f-card.flipped .d2f-inner { transform: rotateY(180deg); }
# MAGIC .d2f-front, .d2f-back { position: absolute; top: 0; left: 0; width: 100%; height: 100%; backface-visibility: hidden; -webkit-backface-visibility: hidden; border-radius: 12px; display: flex; flex-direction: column; box-sizing: border-box; overflow: hidden; }
# MAGIC .d2f-front { background: #fff; border: 2px solid #1B5162; box-shadow: 0 2px 8px rgba(27,49,57,0.08); }
# MAGIC .d2f-front-hdr { background: #1B5162; padding: 16px 18px; }
# MAGIC .d2f-front-title { font-size: 16pt; font-weight: 700; color: #fff; }
# MAGIC .d2f-front-sub { font-size: 12pt; color: #DCE0E2; margin-top: 2px; }
# MAGIC .d2f-front-body { padding: 14px 18px; flex: 1; }
# MAGIC .d2f-front-body ul { margin: 0; padding: 0 0 0 18px; font-size: 13pt; color: #333; line-height: 1.6; }
# MAGIC .d2f-front-hint { font-size: 10pt; color: #618794; text-align: center; padding: 6px; opacity: 0.6; }
# MAGIC .d2f-back { transform: rotateY(180deg); background: #1B5162; color: #fff; padding: 20px 18px; justify-content: center; }
# MAGIC .d2f-card.flipped .d2f-back { box-shadow: 0 0 0 3px rgba(255,111,66,0.4); }
# MAGIC .d2f-back-title { font-size: 14pt; font-weight: 700; margin-bottom: 6px; }
# MAGIC .d2f-back-win { font-size: 13pt; color: #c8f7e1; line-height: 1.5; margin-bottom: 8px; }
# MAGIC .d2f-back-lose { font-size: 13pt; color: #DCE0E2; line-height: 1.5; }
# MAGIC </style>
# MAGIC <div class="d2f-wrap">
# MAGIC   <div class="d2f-card" onclick="this.classList.toggle('flipped')"><div class="d2f-inner"><div class="d2f-front"><div class="d2f-front-hdr"><div class="d2f-front-title">Amazon EMR</div><div class="d2f-front-sub">Spark / Hadoop Processing</div></div><div class="d2f-front-body"><ul><li>Open-source Spark, Hive, Flink, Trino</li><li>Three models: EC2 (self-managed), EKS (Kubernetes), Serverless (no cluster mgmt, GA June 2022)</li><li>Native Delta Lake/Iceberg/Hudi support</li><li>AWS-only; no multi-cloud</li></ul></div><div class="d2f-front-hint">Click to flip &#x279C;</div></div><div class="d2f-back"><div class="d2f-back-title">Databricks vs EMR</div><div class="d2f-back-win">&#x2713; Databricks wins: Unity Catalog governance across all workloads, integrated SQL analytics + ML lifecycle, Photon engine, multi-cloud, founded by Spark creators with continued major OSS contributions</div><div class="d2f-back-lose">&#x25CB; EMR wins: lower per-instance cost (EMR service fees significantly below DBU charges), Spot Instances (up to 90% discount), EMR Serverless (no cluster mgmt), native Delta Lake/Iceberg/Hudi, broader framework support (Flink, Trino, Hive), deep AWS integration</div></div></div></div>
# MAGIC   <div class="d2f-card" onclick="this.classList.toggle('flipped')"><div class="d2f-inner"><div class="d2f-front"><div class="d2f-front-hdr"><div class="d2f-front-title">Amazon Redshift</div><div class="d2f-front-sub">SQL Data Warehouse</div></div><div class="d2f-front-body"><ul><li>Cloud-first warehouse for OLAP/BI</li><li>Provisioned (RA3) and Serverless (GA July 2022), both with separated storage/compute</li><li>AWS-only. Queries S3 via Spectrum; Iceberg read/write (GA Nov 2025)</li><li>Includes Redshift ML (SQL-native CREATE MODEL); SageMaker for advanced ML</li></ul></div><div class="d2f-front-hint">Click to flip &#x279C;</div></div><div class="d2f-back"><div class="d2f-back-title">Databricks vs Redshift</div><div class="d2f-back-win">&#x2713; Databricks wins: unified ML + SQL + streaming on one platform, multi-cloud portability, Unity Catalog governance across all workloads (not just SQL), open formats by default, Photon-optimized performance for both ETL and SQL</div><div class="d2f-back-lose">&#x25CB; Redshift wins: Serverless (no cluster mgmt, auto-scaling), AQUA acceleration for scan-heavy queries, Concurrency Scaling for high-concurrency, Redshift ML (SQL-native), Zero-ETL from Aurora/DynamoDB (GA Oct 2024), Iceberg read/write (GA Nov 2025), Data Sharing (cross-account), deep AWS integration</div></div></div></div>
# MAGIC   <div class="d2f-card" onclick="this.classList.toggle('flipped')"><div class="d2f-inner"><div class="d2f-front"><div class="d2f-front-hdr"><div class="d2f-front-title">Amazon SageMaker</div><div class="d2f-front-sub">ML Studio</div></div><div class="d2f-front-body"><ul><li>End-to-end ML build, train, deploy</li><li>Unified Studio (GA March 2025): unifies data eng, analytics, ML, and GenAI in one environment</li><li>HyperPod for distributed training with auto-recovery</li><li>AWS-only; no multi-cloud</li></ul></div><div class="d2f-front-hint">Click to flip &#x279C;</div></div><div class="d2f-back"><div class="d2f-back-title">Databricks vs SageMaker</div><div class="d2f-back-win">&#x2713; Databricks wins: unified data + ML with deeper integration (data and ML governed by Unity Catalog from day one), collaborative notebooks with native Spark, multi-cloud portability, open formats (Delta Lake, Iceberg)</div><div class="d2f-back-lose">&#x25CB; SageMaker wins: Unified Studio (GA March 2025) for combined data eng + ML + GenAI, HyperPod for large-scale distributed training with auto-fault-recovery, built-in model monitoring and bias detection (Clarify), A/B testing, managed MLOps pipelines, deep AWS IAM/VPC integration</div></div></div></div>
# MAGIC   <div class="d2f-card" onclick="this.classList.toggle('flipped')"><div class="d2f-inner"><div class="d2f-front"><div class="d2f-front-hdr"><div class="d2f-front-title">Amazon Bedrock</div><div class="d2f-front-sub">GenAI / Foundation Models</div></div><div class="d2f-front-body"><ul><li>Managed GenAI: model API access, fine-tuning (supervised, reinforcement, distillation)</li><li>Knowledge Bases (managed RAG), Agents + AgentCore (GA Oct 2025) for multi-agent orchestration</li><li>Guardrails for safety filtering</li><li><strong>Complementary for data eng</strong>; <strong>competitive for GenAI</strong></li></ul></div><div class="d2f-front-hint">Click to flip &#x279C;</div></div><div class="d2f-back"><div class="d2f-back-title">Databricks vs Bedrock</div><div class="d2f-back-win">&#x2713; Databricks wins: data-centric AI (data prep + training + serving unified), full pretraining on GPU clusters, Unity Catalog AI governance across the full ML lifecycle, multi-cloud portability</div><div class="d2f-back-lose">&#x25CB; Bedrock wins: production agentic AI (Agents + AgentCore, multi-agent collaboration, GA 2025), managed RAG (Knowledge Bases), Guardrails (99% hallucination detection), fine-tuning (supervised + reinforcement + distillation), minimal infrastructure setup</div></div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">EMR: The Most Direct Competitor</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Spark expertise:</strong> Databricks was founded by the creators of Apache Spark and remains the largest single contributor to the project, continuing to donate major features including Declarative Pipelines (2025). EMR runs open-source Spark (with native Delta Lake, Iceberg, and Hudi support since EMR 6.9.0) with some AWS optimizations.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Management overhead:</strong> EMR on EC2 requires manual cluster provisioning, node type selection, and Spark tuning. EMR Serverless (GA June 2022) eliminates cluster management with automatic scaling and per-second billing. Databricks provides automated cluster management, Spark-level autoscaling (per stage, not just per instance), and serverless compute. The Databricks advantage over EMR Serverless specifically is the integrated governance (Unity Catalog), SQL analytics, and ML lifecycle.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cost nuance:</strong> EMR has lower per-instance costs, and Spot Instances offer up to 90% discount for fault-tolerant workloads. However, the total cost includes DevOps hours, debugging time, and the need to integrate additional services for governance, ML, and orchestration.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When AWS Built-in Services Win</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>EMR:</strong> EMR Serverless wins for zero-cluster-management Spark jobs with Spot pricing. Teams needing Flink, Trino, or Hive alongside Spark. Organizations deeply invested in CloudFormation/EKS-based infrastructure or running native Delta Lake/Iceberg/Hudi without Databricks overhead.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Redshift:</strong> Redshift Serverless wins for SQL-only analytics with auto-scaling, AQUA acceleration for scan-heavy queries, Concurrency Scaling for high-concurrency workloads, and Zero-ETL integrations from Aurora/DynamoDB (GA October 2024). Iceberg read/write (GA November 2025) reduces the open-format argument.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>SageMaker:</strong> wins when teams need Unified Studio (GA March 2025) for combined data engineering + ML + GenAI in one AWS-native environment, or HyperPod for large-scale distributed training with auto-fault-recovery. Built-in model monitoring (Clarify), A/B testing, and managed MLOps pipelines.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Bedrock:</strong> wins for teams needing production agentic AI with Guardrails and minimal infrastructure, managed RAG (Knowledge Bases) with no data engineering needs, or fine-tuning (supervised + reinforcement + distillation) within the AWS perimeter.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Migration Proof Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/yipitdata" style="color: #2574B5; font-size: 14pt;">YipitData</a> (financial data) consolidated approximately 50 Redshift clusters into a single Databricks Lakehouse. Despite a 4-5x increase in analysis and reporting scale, operational spending decreased. 40+ analysts evolved into hybrid data engineer/analysts. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/thrivent/migration" style="color: #2574B5; font-size: 14pt;">Thrivent</a> (financial services) completed an enterprise-wide migration from Cloudera (Hadoop) to Databricks on AWS using 3 internal engineers and RCG Global Services (consulting partner, with Databricks architecture guidance) over 11 months. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://aws.amazon.com/emr/" style="color:#2574B5;font-size:12pt;">Amazon EMR</a> | <a href="https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html" style="color:#2574B5;font-size:12pt;">EMR Serverless</a> | <a href="https://aws.amazon.com/redshift/" style="color:#2574B5;font-size:12pt;">Amazon Redshift</a> | <a href="https://aws.amazon.com/redshift/redshift-serverless/" style="color:#2574B5;font-size:12pt;">Redshift Serverless</a> | <a href="https://aws.amazon.com/sagemaker/unified-studio/" style="color:#2574B5;font-size:12pt;">SageMaker Unified Studio</a> | <a href="https://aws.amazon.com/sagemaker/ai/hyperpod/" style="color:#2574B5;font-size:12pt;">SageMaker HyperPod</a> | <a href="https://aws.amazon.com/bedrock/" style="color:#2574B5;font-size:12pt;">Amazon Bedrock</a> | <a href="https://aws.amazon.com/bedrock/agents/" style="color:#2574B5;font-size:12pt;">Bedrock Agents</a> | <a href="https://aws.amazon.com/bedrock/agentcore/" style="color:#2574B5;font-size:12pt;">Bedrock AgentCore</a> | <a href="https://aws.amazon.com/rds/aurora/zero-etl/" style="color:#2574B5;font-size:12pt;">Zero-ETL</a> | <a href="https://www.databricks.com/customers/yipitdata" style="color:#2574B5;font-size:12pt;">YipitData</a> | <a href="https://www.databricks.com/customers/thrivent/migration" style="color:#2574B5;font-size:12pt;">Thrivent</a></div>
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
# MAGIC ## E. Platform Differentiators
# MAGIC
# MAGIC Sections C and D covered specific competitor comparisons. This section consolidates the Databricks platform differentiators that apply across all competitive scenarios: Photon, Unity Catalog, Mosaic AI, multi-cloud, and open source.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Three Pillars of Differentiation
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Databricks competitive advantage rests on three pillars. Each pillar connects a platform capability to a business outcome, reinforcing the value-driven selling approach from Section A.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-three-pillars ── -->
# MAGIC <style>
# MAGIC .e1p-wrap { display: flex; gap: 16px; margin: 24px 0; align-items: stretch; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .e1p-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .e1p-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.15); }
# MAGIC .e1p-top { padding: 20px 20px 16px; text-align: center; }
# MAGIC .e1p-cap { font-size: 10pt; color: #90A5B1; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
# MAGIC .e1p-title { font-size: 17pt; font-weight: 700; color: #fff; margin-top: 6px; }
# MAGIC .e1p-arrow { font-size: 14pt; color: #fff; margin-top: 4px; }
# MAGIC .e1p-outcome { font-size: 14pt; font-weight: 600; color: #FFAB00; margin-top: 4px; }
# MAGIC .e1p-body { background: #F9F7F4; padding: 16px 18px; flex: 1; }
# MAGIC .e1p-body ul { margin: 0; padding: 0 0 0 18px; }
# MAGIC .e1p-body li { font-size: 14pt; color: #333; line-height: 1.6; margin-bottom: 6px; }
# MAGIC </style>
# MAGIC <div class="e1p-wrap">
# MAGIC   <div class="e1p-card"><div class="e1p-top" style="background:#1B3139;"><div class="e1p-cap">Capability</div><div class="e1p-title">Expertise</div><div class="e1p-arrow">&#x25BC;</div><div class="e1p-outcome">Lower TCO</div></div><div class="e1p-body"><ul><li>Creators of Apache Spark (largest single contributor; donated Declarative Pipelines, 2025)</li><li>Photon: up to 12x speedup vs. open-source Spark on TPC-DS (typical: 3-8x; 2x DBU multiplier, so savings are workload-dependent)</li><li>Predictive Optimization: 2x storage cost reduction, up to 20x query improvement (Dec 2024 customer data)</li><li>Creators of MLflow, Delta Lake, Mosaic AI</li></ul></div></div>
# MAGIC   <div class="e1p-card"><div class="e1p-top" style="background:#1B5162;"><div class="e1p-cap">Capability</div><div class="e1p-title">Simplicity</div><div class="e1p-arrow">&#x25BC;</div><div class="e1p-outcome">Faster Time-to-Value</div></div><div class="e1p-body"><ul><li>Serverless compute eliminates cluster management (auto-provisioning, autoscaling, automatic runtime upgrades)</li><li>Unified UI for all personas (engineers, scientists, analysts)</li><li>Self-service via SQL, notebooks, AI/BI Dashboards, Genie</li><li>Open APIs and infrastructure-as-code (Terraform)</li></ul></div></div>
# MAGIC   <div class="e1p-card"><div class="e1p-top" style="background:#618794;"><div class="e1p-cap">Capability</div><div class="e1p-title">Performance</div><div class="e1p-arrow">&#x25BC;</div><div class="e1p-outcome">Higher Efficiency</div></div><div class="e1p-body"><ul><li>Delta Lake: ACID transactions, schema enforcement, time travel</li><li>Photon engine: vectorized C++ execution, adaptive query planning</li><li>DBSQL Serverless: 5x performance gain across real-world customer workloads since 2022 (Jun 2025)</li><li>H2 2024: BI 14% faster, ETL 9% faster, exploration 13% faster. 2025: Predictive Query Execution + Photon Vectorized Shuffle added up to 25% more</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Photon Engine</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Photon is a vectorized C++ execution engine that runs existing Spark code (SQL, Python, R, Scala, Java) without rewrites. It is ANSI-compliant and uses adaptive query planning.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> upgrading from a four-cylinder engine to a V12 without changing the car body. Your existing Spark code runs unchanged but much faster.</li>
# MAGIC           <li style="font-size: 14pt;">Predictive Optimization (enabled by default for new accounts since November 2024, with gradual rollout to existing accounts) has compacted hundreds of petabytes and vacuumed exabytes of unreferenced data, saving tens of millions of dollars in storage costs (Feb 2026 blog).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unity Catalog: Beyond Metadata</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Unity Catalog is not just a catalog. It provides access control, lineage, auditing, data classification, quality monitoring, and AI asset governance (prompts, embeddings, vector indexes, model lineage).</li>
# MAGIC           <li style="font-size: 14pt;">Supports Iceberg REST Catalog API, enabling external engines to read and write Unity Catalog-managed tables.</li>
# MAGIC           <li style="font-size: 14pt;">Catalog federation (Foreign Iceberg GA, May 2026) governs Iceberg tables in AWS Glue, Hive Metastore, Snowflake Horizon, Google Cloud Lakehouse, and more, without copying data.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Open Standards and Multi-Cloud</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Open source:</strong> Delta Lake, Apache Iceberg (via UniForm), Apache Spark, MLflow are all open source. Customers can access their data with any compatible engine.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Multi-cloud:</strong> Databricks runs on AWS, Azure, and GCP with cross-cloud governance via Unity Catalog. This is a structural differentiator against AWS-built-in services (single cloud) and Snowflake (SaaS model; Iceberg Tables now support customer-managed external storage, but native tables remain in Snowflake-managed storage).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lakebase:</strong> a Postgres-compatible (16/17) transactional database (GA on AWS, Feb 2026; Azure in preview). Autoscaling with scale-to-zero is the default deployment model. Integrates transactional workloads with the lakehouse, extending the platform beyond analytics into operational use cases.</li>
# MAGIC         </ul>
# MAGIC         <div style="font-size:12pt;color:#618794;margin-top:12px;border-top:1px solid #EEEDE9;padding-top:8px;">Sources: <a href="https://www.databricks.com/product/photon" style="color:#2574B5;font-size:12pt;">Photon</a> | <a href="https://www.databricks.com/blog/predictive-optimization-scale-year-innovation-and-whats-next" style="color:#2574B5;font-size:12pt;">Predictive Optimization (Feb 2026)</a> | <a href="https://www.databricks.com/blog/predictive-optimization-automatically-delivers-faster-queries-and-lower-tco" style="color:#2574B5;font-size:12pt;">Pred. Opt. Customer Data (Dec 2024)</a> | <a href="https://www.databricks.com/blog/databricks-sql-accelerates-customer-workloads-5x-just-three-years" style="color:#2574B5;font-size:12pt;">DBSQL 5x (Jun 2025)</a> | <a href="https://www.databricks.com/blog/whats-new-databricks-sql-february-2025" style="color:#2574B5;font-size:12pt;">DBSQL Feb 2025</a> | <a href="https://www.databricks.com/blog/unity-catalog-and-next-era-apache-icebergtm" style="color:#2574B5;font-size:12pt;">Unity Catalog Iceberg (May 2026)</a> | <a href="https://www.databricks.com/blog/databricks-lakebase-generally-available" style="color:#2574B5;font-size:12pt;">Lakebase GA</a></div>
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
# MAGIC ### E2. Putting It All Together
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture provided a framework for competitive conversations. The table below summarizes where Databricks has a clear advantage and where competitors have legitimate strengths. Use this as a reference when preparing for customer engagements.</p>
# MAGIC
# MAGIC <table style="width: 100% !important; max-width: 100% !important; border-collapse: collapse; table-layout: fixed; margin: 24px 0; border: 1px solid #ddd;">
# MAGIC   <thead>
# MAGIC     <tr style="background-color: #1B3139;">
# MAGIC       <th style="padding: 14px; text-align: center; border: 1px solid #ddd; color: #fff; font-size: 14pt; width: 25%;">Scenario</th>
# MAGIC       <th style="padding: 14px; text-align: center; border: 1px solid #ddd; color: #00A972; font-size: 14pt; width: 37.5%;">Databricks Advantage</th>
# MAGIC       <th style="padding: 14px; text-align: center; border: 1px solid #ddd; color: #DCE0E2; font-size: 14pt; width: 37.5%;">Competitor Strength</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd; background: #f5f5f5; font-weight: 600;">vs Snowflake</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Open formats, data residency, ML/AI, streaming, GenAI (pretraining and full fine-tuning on custom models; deeper GPU/distributed training), 30-65% cost savings on compute-intensive workloads</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Mature managed SQL, Cortex AI (Agents GA, fine-tuning, RAG), GPU-enabled notebooks, Iceberg support with managed and external storage, strong data sharing ecosystem</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd; background: #f5f5f5; font-weight: 600;">vs EMR</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Unity Catalog governance across all workloads, integrated SQL analytics + ML lifecycle, Photon engine, multi-cloud, largest single Spark contributor</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Lower per-instance cost, EMR Serverless (no cluster mgmt), Spot Instances (up to 90% discount), native Delta Lake/Iceberg/Hudi, broader framework support (Trino, Flink, Hive)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd; background: #f5f5f5; font-weight: 600;">vs Redshift</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Open formats (no lock-in), multi-cloud portability, unified ML + SQL + streaming in one platform, Unity Catalog governance across all workloads</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Serverless (auto-scaling), AQUA acceleration, Concurrency Scaling, Redshift ML, Zero-ETL from Aurora/DynamoDB (GA Oct 2024), Iceberg read/write (GA Nov 2025), deep AWS integration</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd; background: #f5f5f5; font-weight: 600;">vs SageMaker</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Unified data + ML with deeper governance integration (Unity Catalog from day one), collaborative notebooks with native Spark, multi-cloud, open formats</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Unified Studio (GA March 2025) for combined data eng + ML + GenAI, HyperPod for distributed training, model monitoring (Clarify), managed MLOps pipelines, deep AWS integration</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd; background: #f5f5f5; font-weight: 600;">vs Bedrock</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">Full model lifecycle (pre-train, fine-tune, serve, govern), open-source model flexibility, unified with data platform</td>
# MAGIC       <td style="padding: 12px; font-size: 14pt; border: 1px solid #ddd;">40+ foundation models via API, Agents and Knowledge Bases (managed RAG), fine-tuning (supervised + reinforcement + distillation), Guardrails, pay-per-token simplicity</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Key Takeaway</strong>
# MAGIC   <div style="color:#333; font-size: 14pt;">Lead with the customer's business outcomes. Use the Value Framework to move the conversation beyond features. Support your positioning with architecture (lakehouse vs warehouse), cost data (30-65% savings on compute-intensive workloads, with customer proof points), and customer migration stories. Always acknowledge competitor strengths; it builds credibility and demonstrates that you understand the customer's full landscape.</div>
# MAGIC </div>
# MAGIC
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
# MAGIC ## Conclusion

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Summary
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture provided a framework for competitive conversations that starts with business outcomes, not feature lists. The <strong>Lakehouse architecture</strong> combines the best of data warehouses and data lakes in open formats on customer-owned storage. Against <strong>Snowflake</strong>, the key differentiators are open formats, data residency, streaming, pretraining and full fine-tuning, and cost (30-65% savings on compute-intensive workloads). Against <strong>AWS built-in services</strong>, the advantage is a platform designed as unified from the ground up -- while SageMaker Unified Studio (GA March 2025) has begun consolidating the AWS experience, Databricks' integration across data engineering, ML, and governance via Unity Catalog is deeper and more mature. The <strong>three pillars of differentiation</strong> (expertise, simplicity, performance) connect platform capabilities to business outcomes: lower TCO, faster time-to-value, and higher efficiency.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">In every competitive scenario, lead with the customer's business goals, acknowledge competitor strengths, and use architecture, cost data, and migration stories to support your positioning.</p>
# MAGIC
# MAGIC ## Additional Context
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 1.4em;">&#128279;</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Databricks vs. Snowflake (<a href="https://www.databricks.com/databricks-vs-snowflake">Official Page</a>): Competitive comparison covering TCO, GenAI, and platform capabilities</li>
# MAGIC         <li>What is a data lakehouse? (<a href="https://docs.databricks.com/aws/en/lakehouse">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse">GCP</a>): Official lakehouse architecture documentation</li>
# MAGIC         <li>Photon Engine (<a href="https://www.databricks.com/product/photon">Product Page</a>): Performance and TCO details for the vectorized execution engine</li>
# MAGIC         <li>Unity Catalog (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog">GCP</a>): Unified governance for data and AI assets</li>
# MAGIC         <li>EMR to Databricks Migration Guide (<a href="https://www.databricks.com/resources/guide/emr-databricks-migration-guide">Guide</a>): Official migration guide for EMR customers</li>
# MAGIC         <li>Migrating from Redshift to Databricks (<a href="https://www.databricks.com/blog/migrating-redshift-databricks-field-guide-data-teams">Field Guide</a>): Practical guidance for Redshift migration</li>
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
