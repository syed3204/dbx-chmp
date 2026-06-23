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
# MAGIC # 7 Lecture: AI/BI
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Raw data does not influence decisions. Tables of numbers sitting in a data warehouse are informative to engineers, but they do not persuade executives, align cross-functional teams, or surface anomalies for front-line managers. Dashboards bridge that gap: they transform data into a shared understanding of KPIs, goals, and trends. But traditional BI tools introduce their own problems. Per-seat licensing limits who can access insights. Data extraction pipelines create latency and governance gaps. And business users remain dependent on analysts for every new question.
# MAGIC
# MAGIC Databricks AI/BI addresses these problems with two GA products: AI/BI Dashboards and Genie Spaces. Dashboards provide AI-powered, low-code visualizations with cross-filtering, parameters, and caching, all governed by Unity Catalog. Genie Spaces add a natural language interface so business users can ask follow-up questions grounded in their enterprise data. For teams that need more than dashboards or chat, Databricks Apps provides a governed serverless platform for building custom applications with familiar frameworks.
# MAGIC
# MAGIC **Recent platform change:** The workspace navigation was reorganized in May 2026: a new Analytics section groups Dashboards, Genie Spaces, and Alerts, while the SQL section was renamed to Lakehouse.
# MAGIC
# MAGIC This lecture covers 6 sections:
# MAGIC
# MAGIC - **A. AI/BI Dashboards** -- How dashboards combine data, queries, visualizations, and AI to deliver interactive insights at scale
# MAGIC - **B. Dashboard Sharing and Governance** -- Publishing, embedded credentials, account-level users, caching, and external embedding
# MAGIC - **C. AI/BI Genie** -- Natural language analytics: how Genie interprets your business data and answers questions
# MAGIC - **D. Genie Spaces Setup** -- Data preparation, curated instructions, benchmarks, monitoring, and troubleshooting
# MAGIC - **E. Genie Agent Mode and APIs** -- Deep reasoning for complex questions, Conversation APIs, and business user access channels
# MAGIC - **F. Databricks Apps** -- Governed custom applications using familiar frameworks on serverless compute
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain how AI/BI Dashboards combine data, queries, visualizations, and AI to deliver interactive business insights at scale
# MAGIC - Differentiate between browser-side filters, parameters, and query-based parameters, and describe when to use each for interactivity and performance
# MAGIC - Describe how Genie uses natural language, Unity Catalog metadata, curated instructions, and user feedback to answer business questions grounded in enterprise data
# MAGIC - Outline the Genie setup process, including focused topics, clean data, curated instructions, benchmarks, and monitoring, and identify good versus bad use cases
# MAGIC - Explain the role of Databricks Apps in the AI/BI ecosystem, including when to use Apps versus Dashboards versus Genie

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. AI/BI Dashboards

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Why Dashboards Matter
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">You cannot influence decisions by pointing at raw data. A dashboard is a collection of visual elements: tables, graphs, and indicators on a coherent topic, designed to communicate patterns that drive action. Different audiences need different perspectives: leadership needs top-level KPIs with clarity and simplicity, while analysts need diagnostics with interactivity and filtering.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks AI/BI is a business intelligence solution that uses compound AI to enhance data analysis with self-service insights, governance, and performance. It includes two GA products, <strong>AI/BI Dashboards</strong> (formerly AI/BI dashboards) and <strong>Genie Spaces</strong>, both governed and secured with Unity Catalog. Over 3,000 customers and 30,000 users rely on AI/BI Dashboards every week.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-data-to-insights ── -->
# MAGIC <style>
# MAGIC .a1f-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a1f-flow { display: flex; align-items: stretch; gap: 0; }
# MAGIC .a1f-card { flex: 1; border-radius: 12px; padding: 28px 20px; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 10px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a1f-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.15); }
# MAGIC .a1f-s1 { background: #F9F7F4; border: 2px solid #DCE0E2; }
# MAGIC .a1f-s2 { background: #1B3139; border: 2px solid #1B3139; }
# MAGIC .a1f-s3 { background: #F9F7F4; border: 2px solid #00A972; }
# MAGIC .a1f-icon { font-size: 32px; line-height: 1; }
# MAGIC .a1f-label { font-size: 14pt; font-weight: 700; line-height: 1.3; }
# MAGIC .a1f-desc { font-size: 14pt; line-height: 1.5; }
# MAGIC .a1f-pill { display: inline-block; font-size: 14pt; font-weight: 700; letter-spacing: 0.6px; text-transform: uppercase; padding: 2px 10px; border-radius: 20px; margin-top: 4px; }
# MAGIC .a1f-arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 0 8px; min-width: 60px; }
# MAGIC .a1f-arrow .a1f-line { width: 40px; height: 3px; position: relative; overflow: hidden; border-radius: 2px; background: #c8e6c9; }
# MAGIC .a1f-arrow .a1f-line::before { content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%; background: linear-gradient(90deg, transparent, #00A972, transparent); animation: a1fFlow 1.5s linear infinite; }
# MAGIC @keyframes a1fFlow { 0% { left: -100%; } 100% { left: 100%; } }
# MAGIC .a1f-arrow .a1f-head { color: #00A972; font-size: 16px; margin-top: 2px; }
# MAGIC .a1f-arrow .a1f-text { font-size: 14pt; color: #618794; margin-top: 3px; }
# MAGIC </style>
# MAGIC <div class="a1f-wrap">
# MAGIC   <div class="a1f-flow">
# MAGIC     <div class="a1f-card a1f-s1">
# MAGIC       <div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div>
# MAGIC       <div class="a1f-label" style="color:#0b2026;">Raw Data</div>
# MAGIC       <div class="a1f-desc" style="color:#5A6F77;">Delta tables in Unity Catalog</div>
# MAGIC       <span class="a1f-pill" style="background:#e8f0fe;color:#2b6cb0;">Source</span>
# MAGIC     </div>
# MAGIC     <div class="a1f-arrow"><div class="a1f-line"></div><span class="a1f-head">&#x25B6;</span><span class="a1f-text">Queries</span></div>
# MAGIC     <div class="a1f-card a1f-s2">
# MAGIC       <div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div>
# MAGIC       <div class="a1f-label" style="color:#fff;">AI/BI Dashboards</div>
# MAGIC       <div class="a1f-desc" style="color:#DCE0E2;">Queries, visualizations, AI assistance</div>
# MAGIC       <span class="a1f-pill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">Transform</span>
# MAGIC     </div>
# MAGIC     <div class="a1f-arrow"><div class="a1f-line"></div><span class="a1f-head">&#x25B6;</span><span class="a1f-text">Insights</span></div>
# MAGIC     <div class="a1f-card a1f-s3">
# MAGIC       <div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div>
# MAGIC       <div class="a1f-label" style="color:#0b2026;">Business Insights</div>
# MAGIC       <div class="a1f-desc" style="color:#5A6F77;">Shared understanding of KPIs and trends</div>
# MAGIC       <span class="a1f-pill" style="background:#e8f5e9;color:#006644;">Outcome</span>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Dashboards Bridge the Gap</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Persuasive communication:</strong> dashboards translate raw data into visual patterns. A line chart showing a revenue decline tells a different story than a table of monthly figures. The visual encoding makes anomalies, trends, and comparisons immediately apparent.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Role-specific perspectives:</strong> leadership dashboards emphasize top-level KPIs with clarity and simplicity. Analyst dashboards provide diagnostics with interactivity and filtering. Designing for the audience determines whether the dashboard gets used or ignored.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Shared understanding:</strong> when the sales team, finance team, and executive staff view the same dashboard, they operate from a common set of facts. This reduces the "my spreadsheet says something different" problem that plagues organizations with fragmented BI.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why BI Directly on Databricks</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>No per-seat pricing:</strong> unlike traditional BI tools that charge per viewer, Databricks AI/BI Dashboards have no per-seat licensing. This removes friction for sharing dashboards broadly across an organization.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>No data extraction:</strong> many BI tools require extracting data into a separate engine. AI/BI Dashboards query where the data lives, eliminating pipelines, reducing latency, and keeping governance consistent through Unity Catalog.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Performance at scale:</strong> traditional BI tools can struggle with petabyte-scale datasets. Dashboards use SQL Warehouses and the Photon engine, querying billions of rows directly.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/wineshipping/ai-bi-dashboards" style="color: #2574B5; font-size: 14pt;">Wineshipping</a> centralized fragmented data systems and migrated from Power BI to Databricks AI/BI Dashboards, enabling self-service analytics across finance, sales, operations, and customer service teams. &#x25C6;</li>
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
# MAGIC ### A2. Dashboard Components and AI Assistance
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Every AI/BI Dashboard bundles four layers into one governed space. Click each tab below to explore the data sources, visualizations, query mechanics, and AI capabilities that make up the dashboard experience.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-four-components ── -->
# MAGIC <style>
# MAGIC .a2t-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a2t-wrap input[type="radio"] { display: none; }
# MAGIC .a2t-tabs { display: flex; gap: 4px; margin-bottom: 0; }
# MAGIC .a2t-tab { flex: 1; text-align: center; padding: 14px 8px; font-size: 14pt; font-weight: 700; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-bottom: none; border-radius: 8px 8px 0 0; background: #fff; transition: all 0.2s; user-select: none; position: relative; }
# MAGIC .a2t-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 6px 6px 0 0; }
# MAGIC .a2t-tab[for="a2r1"]::before { background: #1B5162; }
# MAGIC .a2t-tab[for="a2r2"]::before { background: #00A972; }
# MAGIC .a2t-tab[for="a2r3"]::before { background: #2272B4; }
# MAGIC .a2t-tab[for="a2r4"]::before { background: #618794; }
# MAGIC #a2r1:checked ~ .a2t-tabs .a2t-tab[for="a2r1"] { border-color: #1B5162; background: #E8F0ED; }
# MAGIC #a2r2:checked ~ .a2t-tabs .a2t-tab[for="a2r2"] { border-color: #00A972; background: #E8F5E9; }
# MAGIC #a2r3:checked ~ .a2t-tabs .a2t-tab[for="a2r3"] { border-color: #2272B4; background: #E8F0F8; }
# MAGIC #a2r4:checked ~ .a2t-tabs .a2t-tab[for="a2r4"] { border-color: #618794; background: #F0F2F4; }
# MAGIC .a2t-panels { border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; background: #fff; }
# MAGIC .a2t-panel { display: none; padding: 24px 28px; }
# MAGIC #a2r1:checked ~ .a2t-panels .a2t-p1 { display: block; }
# MAGIC #a2r2:checked ~ .a2t-panels .a2t-p2 { display: block; }
# MAGIC #a2r3:checked ~ .a2t-panels .a2t-p3 { display: block; }
# MAGIC #a2r4:checked ~ .a2t-panels .a2t-p4 { display: block; }
# MAGIC /* Shared inner styles */
# MAGIC .a2t-cat { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; margin: 0 0 8px 0; padding-bottom: 4px; border-bottom: 2px solid; }
# MAGIC .a2t-grid { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 18px; }
# MAGIC .a2t-chip { background: #F9F7F4; border: 1px solid #E8E3DC; border-radius: 8px; padding: 8px 14px; font-size: 14pt; color: #333; display: flex; align-items: center; gap: 8px; transition: transform 0.15s, box-shadow 0.15s, border-color 0.15s; cursor: default; }
# MAGIC .a2t-chip:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(27,49,57,0.10); border-color: #1B5162; }
# MAGIC .a2t-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
# MAGIC .a2t-chip strong { color: #1B3139; }
# MAGIC .a2t-highlight { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC .a2t-highlight strong { color: #fff; }
# MAGIC .a2t-highlight:hover { border-color: #0B2026; }
# MAGIC .a2t-note { font-size: 14pt; color: #618794; margin-top: 4px; line-height: 1.5; font-style: italic; }
# MAGIC /* Flow row for Queries tab */
# MAGIC .a2t-flow { display: flex; align-items: center; gap: 0; margin: 14px 0; }
# MAGIC .a2t-fstep { flex: 1; background: #F9F7F4; border: 2px solid #DCE0E2; border-radius: 10px; padding: 14px 12px; text-align: center; transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
# MAGIC .a2t-fstep:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(27,49,57,0.10); }
# MAGIC .a2t-fstep-dark { background: #1B3139; border-color: #1B3139; color: #fff; }
# MAGIC .a2t-fstep-accent { background: #F9F7F4; border-color: #00A972; }
# MAGIC .a2t-fname { font-size: 14pt; font-weight: 700; }
# MAGIC .a2t-fsub { font-size: 14pt; margin-top: 3px; }
# MAGIC .a2t-farr { display: flex; align-items: center; justify-content: center; padding: 0 4px; min-width: 36px; }
# MAGIC .a2t-farr::after { content: ''; width: 0; height: 0; border-top: 7px solid transparent; border-bottom: 7px solid transparent; border-left: 10px solid #1B5162; }
# MAGIC /* AI cards */
# MAGIC .a2t-ai-row { display: flex; gap: 12px; margin-bottom: 12px; }
# MAGIC .a2t-ai-card { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s, box-shadow 0.15s; cursor: default; display: flex; flex-direction: column; }
# MAGIC .a2t-ai-card:hover { transform: translateY(-2px); box-shadow: 0 5px 14px rgba(27,49,57,0.13); }
# MAGIC .a2t-ai-bar { height: 5px; flex-shrink: 0; }
# MAGIC .a2t-ai-body { padding: 14px 16px; background: #fff; flex: 1; }
# MAGIC .a2t-ai-name { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .a2t-ai-desc { font-size: 14pt; color: #5A6F77; line-height: 1.45; }
# MAGIC .a2t-ai-pill { display: inline-block; font-size: 14pt; font-weight: 700; letter-spacing: 0.6px; text-transform: uppercase; padding: 2px 8px; border-radius: 12px; margin-top: 6px; }
# MAGIC </style>
# MAGIC <div class="a2t-wrap">
# MAGIC <input type="radio" name="a2grp" id="a2r1" checked>
# MAGIC <input type="radio" name="a2grp" id="a2r2">
# MAGIC <input type="radio" name="a2grp" id="a2r3">
# MAGIC <input type="radio" name="a2grp" id="a2r4">
# MAGIC <div class="a2t-tabs">
# MAGIC   <label class="a2t-tab" for="a2r1">Data Sources</label>
# MAGIC   <label class="a2t-tab" for="a2r2">Visualizations</label>
# MAGIC   <label class="a2t-tab" for="a2r3">Queries + Refresh</label>
# MAGIC   <label class="a2t-tab" for="a2r4">AI Assistance</label>
# MAGIC </div>
# MAGIC <div class="a2t-panels">
# MAGIC   <!-- ── Tab 1: Data Sources ── -->
# MAGIC   <div class="a2t-panel a2t-p1">
# MAGIC     <div class="a2t-cat" style="color: #1B5162; border-color: #1B5162;">Unity Catalog Objects</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Tables</strong></div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Views</strong></div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Materialized Views</strong></div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Streaming Tables</strong></div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #00A972; border-color: #00A972;">Semantic Layer (Metric Views)</div>
# MAGIC     <div style="display:flex;gap:14px;margin-bottom:18px;">
# MAGIC       <!-- UC Metric Views card -->
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(27,49,57,0.08);border:2px solid #1B5162;">
# MAGIC         <div style="background:#1B5162;padding:10px 14px;display:flex;align-items:center;gap:8px;">
# MAGIC           <div style="width:8px;height:8px;border-radius:50%;background:#00A972;flex-shrink:0;"></div>
# MAGIC           <strong style="color:#fff;font-size:14pt;">UC Metric Views</strong>
# MAGIC           <span style="margin-left:auto;font-size:14pt;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;background:rgba(255,255,255,0.15);color:#DCE0E2;padding:2px 8px;border-radius:10px;">Global</span>
# MAGIC         </div>
# MAGIC         <div style="background:#fff;padding:12px 14px;">
# MAGIC           <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;">
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#e8f5e9;color:#006644;padding:3px 10px;border-radius:12px;">Dimensions</span>
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#e8f0fe;color:#2b6cb0;padding:3px 10px;border-radius:12px;">Measures</span>
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#fef3cd;color:#856404;padding:3px 10px;border-radius:12px;">Joins</span>
# MAGIC           </div>
# MAGIC           <div style="font-size:14pt;color:#333;line-height:1.5;">
# MAGIC             <div style="margin-bottom:4px;"><strong style="color:#1B3139;">Scope:</strong> shared across dashboards, Genie, BI tools</div>
# MAGIC             <div style="margin-bottom:4px;"><strong style="color:#1B3139;">Governed:</strong> registered in Unity Catalog</div>
# MAGIC             <div><strong style="color:#1B3139;">Example:</strong> <code style="font-size:14pt;background:#F9F7F4;padding:1px 5px;border-radius:3px;">revenue = SUM(order_total) WHERE status='completed'</code></div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Local Metric Views card -->
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(27,49,57,0.08);border:2px solid #DCE0E2;">
# MAGIC         <div style="background:#618794;padding:10px 14px;display:flex;align-items:center;gap:8px;">
# MAGIC           <div style="width:8px;height:8px;border-radius:50%;background:#00A972;flex-shrink:0;"></div>
# MAGIC           <strong style="color:#fff;font-size:14pt;">Local Metric Views</strong>
# MAGIC           <span style="margin-left:auto;font-size:14pt;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;background:rgba(255,255,255,0.15);color:#DCE0E2;padding:2px 8px;border-radius:10px;">Dashboard</span>
# MAGIC         </div>
# MAGIC         <div style="background:#fff;padding:12px 14px;">
# MAGIC           <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;">
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#e8f5e9;color:#006644;padding:3px 10px;border-radius:12px;">Dimensions</span>
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#e8f0fe;color:#2b6cb0;padding:3px 10px;border-radius:12px;">Measures</span>
# MAGIC             <span style="font-size:14pt;font-weight:600;background:#fef3cd;color:#856404;padding:3px 10px;border-radius:12px;">Joins</span>
# MAGIC           </div>
# MAGIC           <div style="font-size:14pt;color:#333;line-height:1.5;">
# MAGIC             <div style="margin-bottom:4px;"><strong style="color:#1B3139;">Scope:</strong> single dashboard only</div>
# MAGIC             <div style="margin-bottom:4px;"><strong style="color:#1B3139;">Created:</strong> visual editor, no UC write access needed</div>
# MAGIC             <div style="margin-bottom:4px;"><strong style="color:#1B3139;">Use for:</strong> prototyping, iteration, validation</div>
# MAGIC             <div><strong style="color:#1B3139;">Promote:</strong> export to UC when ready for broader use</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #618794; border-color: #618794;">Other Sources</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div><strong>SQL Datasets</strong> &mdash; arbitrary SQL with CTEs, window functions, subqueries</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div><strong>File Upload</strong> &mdash; CSV/Excel uploaded from the editor, registered as Delta tables in UC</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-note">All data sources are governed by Unity Catalog: permissions, lineage, metadata, and audit logging.</div>
# MAGIC   </div>
# MAGIC   <!-- ── Tab 2: Visualizations ── -->
# MAGIC   <div class="a2t-panel a2t-p2">
# MAGIC     <div class="a2t-cat" style="color: #00A972; border-color: #00A972;">Charts (13 types)</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Bar</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Line</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Area</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Scatter</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Pie</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Combo</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Bubble</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Funnel</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Waterfall</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Sankey</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Heatmap</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Histogram</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#00A972;"></div>Box</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #2272B4; border-color: #2272B4;">Maps (2 types)</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#2272B4;"></div>Choropleth Map</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#2272B4;"></div>Point Map</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #1B5162; border-color: #1B5162;">Data Displays (3 types)</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div>Table</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div>Pivot Table</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div>Counter <span style="font-size:14pt;color:#618794;">(single KPI + sparkline)</span></div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #618794; border-color: #618794;">Canvas Widgets</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Text / Markdown</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Images</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Filter Controls</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-note">Cross-filtering is enabled by default. Clicking any data point filters related visualizations on the same page.</div>
# MAGIC   </div>
# MAGIC   <!-- ── Tab 3: Queries + Refresh ── -->
# MAGIC   <div class="a2t-panel a2t-p3">
# MAGIC     <div class="a2t-flow">
# MAGIC       <div class="a2t-fstep"><div class="a2t-fname" style="color:#0b2026;">Author writes SQL</div><div class="a2t-fsub" style="color:#618794;">Databricks SQL in the editor</div></div>
# MAGIC       <div class="a2t-farr"></div>
# MAGIC       <div class="a2t-fstep" style="border-color:#1B5162;"><div class="a2t-fname" style="color:#0b2026;">Stored in dashboard</div><div class="a2t-fsub" style="color:#618794;">Self-contained, portable</div></div>
# MAGIC       <div class="a2t-farr"></div>
# MAGIC       <div class="a2t-fstep"><div class="a2t-fname" style="color:#0b2026;">SQL Warehouse</div><div class="a2t-fsub" style="color:#618794;">Photon engine, auto-scaling</div></div>
# MAGIC       <div class="a2t-farr"></div>
# MAGIC       <div class="a2t-fstep a2t-fstep-accent"><div class="a2t-fname" style="color:#006644;">Cached 24h</div><div class="a2t-fsub" style="color:#618794;">Near-instant viewer loads</div></div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #2272B4; border-color: #2272B4;">Scheduled Refresh (Dashboard Schedule)</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#2272B4;"></div><strong>Refresh-only</strong> -- runs all queries on a cadence, populates the shared cache, no notifications</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#2272B4;"></div><strong>With notifications</strong> -- same refresh, plus dashboard snapshots sent via email, Slack, or Teams</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #1B5162; border-color: #1B5162;">Lakeflow Jobs Dashboard Task</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Job-triggered refresh</strong> -- a Lakeflow Jobs task type that refreshes a published dashboard as part of a workflow</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Any job trigger</strong> -- can run on a schedule, file arrival, table update, or as a downstream task after a pipeline completes</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#1B5162;"></div><strong>Filters + notifications</strong> -- pre-apply filter values during refresh; notify subscribers on completion</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #618794; border-color: #618794;">Cache Re-execution</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div><strong>Parameter change</strong> -- query re-runs only if the new parameter value has not been cached in the last 24 hours</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div><strong>Filter on large dataset</strong> -- re-executes when filters apply to datasets over 100K rows (unless already cached)</div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #618794; border-color: #618794;">SQL Capabilities</div>
# MAGIC     <div class="a2t-grid">
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>CTEs</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Window Functions</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Subqueries</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Calculated Measures</div>
# MAGIC       <div class="a2t-chip"><div class="a2t-dot" style="background:#618794;"></div>Custom Dimensions</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- ── Tab 4: AI Assistance ── -->
# MAGIC   <div class="a2t-panel a2t-p4">
# MAGIC     <div class="a2t-cat" style="color: #1B5162; border-color: #1B5162;">Authoring</div>
# MAGIC     <div class="a2t-ai-row">
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#1B5162;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">Genie Code</div><div class="a2t-ai-desc">Create datasets, visualizations, layouts, and filters using natural language prompts. Automates multi-step dashboard workflows in Agent mode.</div><span class="a2t-ai-pill" style="background:#e8f5e9;color:#006644;">GA</span><br/><a href="https://docs.databricks.com/aws/en/dashboards/manage/dashboard-agent" style="font-size:14pt;color:#2574B5;">Docs: Genie Code for dashboard authoring</a></div></div>
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#2272B4;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">AI Chart Suggestion</div><div class="a2t-ai-desc">Recommends the most appropriate visualization type based on your data structure and query results.</div><span class="a2t-ai-pill" style="background:#e8f5e9;color:#006644;">GA</span><br/><a href="https://docs.databricks.com/aws/en/dashboards/manage/visualizations/" style="font-size:14pt;color:#2574B5;">Docs: Dashboard visualizations</a></div></div>
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#618794;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">BI Workbook Import</div><div class="a2t-ai-desc">Import Tableau (.twb/.twbx) or Power BI (.pbit) files into Genie Code Agent mode with <code style="font-size:14pt;">/importBI</code>. Generates dashboards connected to metric views that mirror the source business logic.</div><span class="a2t-ai-pill" style="background:#fef3cd;color:#856404;">Beta</span><br/><a href="https://docs.databricks.com/aws/en/dashboards/manage/import-bi" style="font-size:14pt;color:#2574B5;">Docs: Import BI files using Genie Code</a></div></div>
# MAGIC     </div>
# MAGIC     <div class="a2t-cat" style="color: #00A972; border-color: #00A972;">Viewer Experience</div>
# MAGIC     <div class="a2t-ai-row">
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#00A972;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">Ask Genie</div><div class="a2t-ai-desc">Enabled by default on published dashboards (can be disabled via the Enable Genie toggle). Viewers ask natural language follow-up questions. Databricks auto-generates a Genie Space from the dashboard, or authors can link an existing one.</div><span class="a2t-ai-pill" style="background:#e8f5e9;color:#006644;">GA</span><br/><a href="https://docs.databricks.com/aws/en/dashboards/genie-spaces" style="font-size:14pt;color:#2574B5;">Docs: Genie Spaces with dashboards</a></div></div>
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#00A972;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">AI Forecast</div><div class="a2t-ai-desc">Add predictive forecast overlays to line charts. Requires a temporal date x-axis and a single numeric y-axis. Click the + icon next to Forecast, then Clone with AI Forecast.</div><span class="a2t-ai-pill" style="background:#e8f5e9;color:#006644;">GA</span><br/><a href="https://docs.databricks.com/aws/en/dashboards/manage/visualizations/" style="font-size:14pt;color:#2574B5;">Docs: Dashboard visualizations</a></div></div>
# MAGIC       <div class="a2t-ai-card"><div class="a2t-ai-bar" style="background:#1B5162;"></div><div class="a2t-ai-body"><div class="a2t-ai-name">Explain This Change</div><div class="a2t-ai-desc">Right-click a data point on bar, line, area, pie, heatmap, or pivot charts. Genie enters Agent mode to analyze the change and identify top drivers.</div><span class="a2t-ai-pill" style="background:#e8f5e9;color:#006644;">GA</span><br/><a href="https://docs.databricks.com/aws/en/ai-bi/release-notes/2026" style="font-size:14pt;color:#2574B5;">Docs: AI/BI release notes 2026</a></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Data Sources Tab: What Feeds a Dashboard</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>UC objects:</strong> tables, views, materialized views, and streaming tables are all valid data sources. Any object registered in Unity Catalog and queryable via SQL can feed a dashboard dataset.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>UC Metric Views (global semantic layer):</strong> governed definitions of dimensions, measures, and joins shared across dashboards, Genie Spaces, and BI tools. When a metric view defines "revenue" as <code style="font-size: 14pt;">SUM(order_total) WHERE status = 'completed'</code>, that definition is consistent everywhere. Covered in depth in Module 6.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Local Metric Views:</strong> define dimensions, measures, and joins visually inside a single dashboard without UC write access. Useful for prototyping. Promote to UC when ready (Data tab, kebab menu, Export to Metric View). You can also extend an existing UC metric view by adding dashboard-specific measures on top of it.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>File upload and SQL datasets:</strong> upload CSV/Excel directly from the editor (registers as a Delta table in UC), or write arbitrary SQL with CTEs, window functions, and subqueries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> UC metric views are published recipes in a company cookbook: every kitchen follows the same instructions. Local metric views are a chef's personal draft, tested in one kitchen before submitting to the cookbook.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Visualizations Tab: 19 Types Across 5 Categories</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Charts (13):</strong> bar, line, area, scatter, pie, combo, bubble, funnel, waterfall, sankey, heatmap, histogram, box. Combo charts support dual y-axes. Bar charts can display top/bottom N categories.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Maps (2):</strong> choropleth (colors geographic regions by metric) and point map (symbols at lat/lon coordinates, cross-filtering enabled).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data displays (3):</strong> table (conditional formatting, banded rows), pivot table (hierarchies, sticky headers, up to 15,000 rows), and counter (single KPI with optional sparkline).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Canvas widgets:</strong> text/markdown (rich text, images from Volumes or public URLs), images, and filter controls.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Queries + Refresh Tab: How Data Stays Current</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Self-contained queries:</strong> SQL queries are stored as part of the dashboard definition. This makes dashboards portable and version-controllable.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Scheduled refresh:</strong> two types: refresh-only (populates the shared cache with no notifications) and with notifications (same refresh plus dashboard snapshots sent via email, Slack, or Teams).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lakeflow Jobs dashboard task:</strong> a job task type that refreshes a published dashboard as part of a workflow. Can be triggered on a schedule, file arrival, table update, or as a downstream task after a pipeline completes. Pre-apply filter values and notify subscribers on completion.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cache re-execution:</strong> queries re-run when a parameter value changes (unless already cached within 24 hours) or when filters apply to datasets over 100K rows (unless the same filter combination was cached).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">AI Assistance Tab: Six Capabilities</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Genie Code (GA):</strong> create datasets, visualizations, layouts, and filters using natural language. Automates multi-step dashboard workflows in Agent mode.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI Chart Suggestion (GA):</strong> recommends the most appropriate visualization type based on your data structure and query results.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>BI Workbook Import (Beta):</strong> import Tableau (.twb/.twbx) or Power BI (.pbit) files into Genie Code Agent mode with <code style="font-size: 14pt;">/importBI</code>. Generates dashboards connected to metric views that mirror the source business logic.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Ask Genie (GA):</strong> enabled by default on published dashboards (can be disabled). Databricks auto-generates a Genie Space from the dashboard, or authors can link an existing one. Viewers ask natural language follow-up questions without leaving the dashboard.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI Forecast (GA):</strong> add predictive forecast overlays to line charts. Requires a temporal date x-axis and a single numeric y-axis.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Explain This Change (GA):</strong> right-click a data point on bar, line, area, pie, heatmap, or pivot charts. Genie enters Agent mode to analyze the change and identify top drivers.</li>
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
# MAGIC ### A3. Dashboard Interactivity
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Dashboards provide six ways for viewers to explore data without leaving the dashboard.</p>
# MAGIC
# MAGIC <!-- ── Visual: a3-interactivity-grid ── -->
# MAGIC <style>
# MAGIC .a3g-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a3g-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
# MAGIC .a3g-card { border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s, box-shadow 0.15s; cursor: default; display: flex; flex-direction: column; }
# MAGIC .a3g-card:hover { transform: translateY(-2px); box-shadow: 0 5px 14px rgba(27,49,57,0.13); }
# MAGIC .a3g-top { padding: 14px 16px; }
# MAGIC .a3g-name { font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .a3g-one { font-size: 14pt; color: rgba(255,255,255,0.8); margin-top: 3px; }
# MAGIC .a3g-bot { background: #fff; padding: 12px 16px; flex: 1; font-size: 14pt; color: #5A6F77; line-height: 1.45; }
# MAGIC </style>
# MAGIC <div class="a3g-wrap">
# MAGIC <div class="a3g-grid">
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#00A972;"><div class="a3g-name">Cross-Filtering</div><div class="a3g-one">Click a chart element, related charts update</div></div><div class="a3g-bot">Enabled by default. No setup. Works on bar, pie, scatter, heatmap, histogram, box, point map.</div></div>
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#2272B4;"><div class="a3g-name">Filters</div><div class="a3g-one">Dropdowns, date pickers, sliders, text entry</div></div><div class="a3g-bot">Three scopes: global (all pages), page-level, or widget-level (static, author-set). Bind to multiple datasets.</div></div>
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#1B5162;"><div class="a3g-name">Parameters</div><div class="a3g-one">Substitute values into SQL at runtime</div></div><div class="a3g-bot">Use <code style="font-size:14pt;">:param</code> syntax. One parameterized dashboard replaces many duplicated ones.</div></div>
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#1B5162;"><div class="a3g-name">Query-Based Params</div><div class="a3g-one">Dropdown values from a live query</div></div><div class="a3g-bot">Value list auto-updates as data changes. No hardcoded options to maintain.</div></div>
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#618794;"><div class="a3g-name">Drill-Through</div><div class="a3g-one">Click to navigate to a detail page</div></div><div class="a3g-bot">Filters auto-populate from the selection context. Summary &#x279C; detail flow across pages.</div></div>
# MAGIC   <div class="a3g-card"><div class="a3g-top" style="background:#90A5B1;"><div class="a3g-name">Active Filter Bar</div><div class="a3g-one">Shows all active filters at the top</div></div><div class="a3g-bot">Viewers always know what is filtering their data. Click to modify or remove any filter.</div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Information</strong>
# MAGIC   <div style="color:#333; font-size: 14pt;">Filters are like sunglasses that change what you see without changing the data (for small datasets). Parameters are like a different lens prescription that changes what data the query retrieves. Drill-through is like clicking a chapter heading in a table of contents: it takes you to the detail page with the right context already applied.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Filters: The Size Threshold</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Small datasets (&le;100K rows, ~100MB):</strong> filters run entirely in the browser after the initial query loads data. No warehouse interaction, no entry in query history, near-instant results.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Large datasets (&gt;100K rows):</strong> filters are handled server-side. The system wraps the original query in a SQL <code>WITH</code> clause, re-executes it on the warehouse, and the resulting query appears in query history. Results are cached 24 hours, so identical filter combinations within that window do not re-execute.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Filter scoping:</strong> global filters apply across all pages. Page-level filters apply to one page. Widget-level filters are static values set by the author that viewers cannot change, useful for showing different slices of the same dataset in different widgets on the same page.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Multi-dataset binding:</strong> a single filter widget can be bound to multiple datasets. A "Region" filter can simultaneously filter the sales chart, returns chart, and customer count table.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Parameters: Always Execute</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>SQL substitution:</strong> parameters use <code>:parameter_name</code> syntax. At runtime, the viewer's selected value replaces the placeholder in the SQL, changing what data the query retrieves. This always appears in query history, regardless of dataset size.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>24-hour cache:</strong> if the same parameter value was used within the last 24 hours, the cached result is returned without re-executing. Common default values improve cache hit rates across viewers.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Reusability:</strong> a parameterized dashboard can serve multiple audiences. One dashboard for "Sales by Region" with a <code>:region</code> parameter replaces a dozen duplicated dashboards.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Drill-Through</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Click-to-navigate:</strong> viewers click a chart element (bar segment, pie slice, table row, map point) and navigate to a target page with filters auto-populated from the selection context. Supported on area, bar, box, combo, heatmap, histogram, line, pie, pivot, scatter, point map, and table charts.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>No explicit target filter required:</strong> drill-through now filters any visualization on the target page that shares the same dataset as the source selection. This was added in 2025.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Active Filter Bar</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">All active filters display in a bar near the top of the dashboard, including global filters, page-level filters, and cross-filter selections. This gives viewers a clear view of what filters are currently shaping their data.</li>
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
# MAGIC ## B. Dashboard Sharing and Governance

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Publishing, Permissions, and Embedded Credentials
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Dashboards move through two states: <strong>Draft</strong> (iterative development, editable) and <strong>Published</strong> (locked snapshot for consumers). Publishing creates a read-only snapshot that remains static until republished, even if the draft continues being edited. At publish time, the author selects a <strong>data permission model</strong> that determines how viewers access the underlying data.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-permission-models ── -->
# MAGIC <style>
# MAGIC .b1p-wrap { display: flex; gap: 18px; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; align-items: stretch; }
# MAGIC .b1p-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .b1p-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .b1p-accent { height: 7px; flex-shrink: 0; }
# MAGIC .b1p-accent.teal { background: #1B5162; }
# MAGIC .b1p-accent.grey { background: #618794; }
# MAGIC .b1p-body { padding: 20px 20px 18px; flex: 1; background: #fff; }
# MAGIC .b1p-pill { display: inline-block; font-size: 14pt; font-weight: 700; letter-spacing: 0.7px; text-transform: uppercase; padding: 3px 10px; border-radius: 20px; margin-bottom: 10px; }
# MAGIC .b1p-pill-default { background: #e8f5e9; color: #006644; }
# MAGIC .b1p-pill-alt { background: #fef3cd; color: #856404; }
# MAGIC .b1p-title { font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 10px; }
# MAGIC .b1p-desc { font-size: 14pt; color: #333; line-height: 1.6; }
# MAGIC .b1p-tag { display: inline-block; font-size: 14pt; font-weight: 600; padding: 4px 12px; border-radius: 6px; margin-top: 12px; }
# MAGIC </style>
# MAGIC <div class="b1p-wrap">
# MAGIC   <div class="b1p-card"><div class="b1p-accent teal"></div><div class="b1p-body"><span class="b1p-pill b1p-pill-default">Default</span><div class="b1p-title">Share Data Permissions</div><div class="b1p-desc">Viewers run queries using the <strong>publisher's</strong> credentials. Viewers do not need direct access to compute or data resources. Enables <strong>shared cache</strong> across all viewers. UC row filters and column masks still apply per user.</div><div class="b1p-tag" style="background:#e8f5e9;color:#006644;">Shared Cache</div></div></div>
# MAGIC   <div class="b1p-card"><div class="b1p-accent grey"></div><div class="b1p-body"><span class="b1p-pill b1p-pill-alt">Internal Users</span><div class="b1p-title">Individual Data Permissions</div><div class="b1p-desc">Viewers run queries using their <strong>own</strong> credentials. Data access is determined by each viewer's UC permissions. Compute is still provided via the publisher's credentials. Results in <strong>per-user cache</strong>.</div><div class="b1p-tag" style="background:#fef3cd;color:#856404;">Per-User Cache</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">User Types and Access</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Workspace users:</strong> have SQL entitlement and can access both draft and published dashboards. They can use the SQL Editor, Notebooks, Jobs, and Compute. With CAN EDIT permission, they can modify drafts.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Account-level users:</strong> provisioned at the Databricks account level (via SCIM APIs or identity providers like Okta or Azure AD) without workspace access. They get view-only access using the publisher's embedded credentials. They support email, passcode, or SSO authentication. They cannot run notebooks, jobs, or access compute directly.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Three permission levels:</strong> CAN RUN (run and filter), CAN EDIT (modify dashboard and datasets), CAN MANAGE (full control including sharing settings).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Sharing Options</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Direct share:</strong> share with individual users by email or username.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Group share:</strong> share with a group (for example, "Sales-Viewers") managed through your identity provider.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>All account users:</strong> share with everyone in the Databricks account.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">External Embedding</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>For partners and customers:</strong> dashboards can be embedded into external web portals using service principals and scoped access tokens, without provisioning Databricks accounts. The authentication flow uses OAuth token exchange with user-scoped filtering via <code>__aibi_external_value</code> in SQL queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Service principal publishing:</strong> for programmatic publishing, authenticate as a service principal and call the dashboard publish API with <code>embed_credentials</code> set to <code>true</code>.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Limitation:</strong> Ask Genie is unavailable in embedded dashboards. Use the Genie Conversation API instead for natural language querying in external applications.</li>
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
# MAGIC ## C. AI/BI Genie

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. What Is Genie?
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">A Genie Space is a domain-specific natural-language chat interface in Databricks where users ask questions of their data and get back SQL queries, results tables, and visualizations. Genie is not a general-purpose AI chatbot. It answers based on your business data, semantics, and logic, drawing from Unity Catalog metadata, curated instructions, dashboards, and user feedback. Unlike fixed dashboard views, Genie responds to user queries with adaptable visualizations and suggestions, seeks clarification when needed, and improves over time through human feedback.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Genie employs a <strong>compound AI system</strong> that combines multiple interacting components. It uses Chain-of-Thought reasoning to break questions into steps: identifying relevant data, planning SQL generation, and combining components. When a response is generated from an exact parameterized example query or SQL function text, Genie designates it as <strong>"Trusted,"</strong> conveying enhanced accuracy assurance.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-genie-inputs ── -->
# MAGIC <style>
# MAGIC .c1g-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c1g-center { background: #1B3139; color: #fff; padding: 20px; border-radius: 14px; text-align: center; font-size: 16pt; font-weight: 700; max-width: 40%; margin: 0 auto 8px auto; box-shadow: 0 4px 16px rgba(27,49,57,0.22); }
# MAGIC .c1g-center-sub { font-size: 14pt; color: #90A5B1; font-weight: 400; margin-top: 4px; }
# MAGIC .c1g-conn { display: flex; justify-content: center; height: 30px; }
# MAGIC .c1g-conn::before { content: ''; display: block; width: 3px; height: 100%; background: #94b3be; border-radius: 2px; }
# MAGIC .c1g-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
# MAGIC .c1g-card { background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .c1g-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .c1g-accent { height: 6px; flex-shrink: 0; }
# MAGIC .c1g-body { padding: 16px 18px; text-align: center; }
# MAGIC .c1g-icon { font-size: 26px; margin-bottom: 6px; }
# MAGIC .c1g-label { font-size: 14pt; font-weight: 700; color: #0b2026; margin-bottom: 4px; }
# MAGIC .c1g-desc { font-size: 14pt; color: #5A6F77; line-height: 1.5; }
# MAGIC </style>
# MAGIC <div class="c1g-wrap">
# MAGIC   <div class="c1g-center">Genie Space<div class="c1g-center-sub">Compound AI system for natural-language analytics</div></div>
# MAGIC   <div class="c1g-conn"></div>
# MAGIC   <div style="text-align: center; color: #618794; font-size: 14pt; margin-bottom: 14px; font-weight: 600;">Four inputs power every Genie response</div>
# MAGIC   <div class="c1g-grid">
# MAGIC     <div class="c1g-card"><div class="c1g-accent" style="background:#1B5162;"></div><div class="c1g-body"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="c1g-label">Data (Tables and Views)</div><div class="c1g-desc">Tables, views, materialized views, and metric views selected for the space. Genie generates SQL against these objects.</div></div></div>
# MAGIC     <div class="c1g-card"><div class="c1g-accent" style="background:#2272B4;"></div><div class="c1g-body"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="c1g-label">Unity Catalog Metadata</div><div class="c1g-desc">Table names, column descriptions, PK/FK relationships, permissions, and column-level example values</div></div></div>
# MAGIC     <div class="c1g-card"><div class="c1g-accent" style="background:#00A972;"></div><div class="c1g-body"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="c1g-label">Curated Instructions</div><div class="c1g-desc">General instructions, domain terms, KPI definitions, example SQL queries, and SQL functions</div></div></div>
# MAGIC     <div class="c1g-card"><div class="c1g-accent" style="background:#E5A100;"></div><div class="c1g-body"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="c1g-label">You (the User)</div><div class="c1g-desc">Chat history provides in-session context; thumbs up/down feedback improves accuracy over time</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What Genie Can and Cannot Do</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Effective questions:</strong> sales figures by time period or segment, customer revenue rankings, regional performance metrics, campaign performance analysis. Genie excels at data-driven inquiries that translate to SQL.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Ineffective questions:</strong> "why" questions (root cause analysis), strategic recommendations, interpretive insights, and predictions. Genie generates SQL queries against structured data; it does not perform statistical modeling or causal inference.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Conversation context:</strong> context from earlier messages in a thread informs later messages in the same thread. This context does not carry over to other chats. Each conversation is independent.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Response Generation and Security</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Compound AI architecture:</strong> when processing a question, Genie leverages UC metadata, column descriptions, knowledge store context, example SQL, SQL functions, general instructions, and chat history. Generated queries are read-only and run on the configured SQL Warehouse.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data security:</strong> access governance relies on Unity Catalog permissions. Queries run using compute credentials embedded by the space author. Row filters and column masks are enforced per user automatically. Users who lack access to the underlying data receive empty responses.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Proof Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/hp/ai-bi-genie" style="color: #2574B5; font-size: 14pt;">HP</a> deployed Genie across finance, product, and go-to-market teams, enabling enterprise-wide self-service analytics in natural language. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/finthrive/ai-bi-genie" style="color: #2574B5; font-size: 14pt;">FinThrive</a> reduced data query time from days to minutes for sales representatives, eliminating dependence on analyst backlogs. &#x25C6;</li>
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
# MAGIC ## D. Genie Spaces Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Setup Process: Topic, Data, and Instructions
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Setting up a Genie Space follows a three-step process: select a <strong>focused topic with clean data</strong>, <strong>test and adjust</strong> through simulated usage, and <strong>validate accuracy</strong> with benchmarks and stakeholder review. Genie is not a "set and forget" solution; it requires ongoing engagement and refinement. Think of it as onboarding a new data analyst: provide concise documentation, explain your terminology, and give examples of how you want questions answered.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-setup-workflow ── -->
# MAGIC <style>
# MAGIC .d1s-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .d1s-flow { display: flex; align-items: flex-start; gap: 0; }
# MAGIC .d1s-step { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10); display: flex; flex-direction: column; transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
# MAGIC .d1s-step:hover { transform: translateY(-2px); box-shadow: 0 5px 14px rgba(27,49,57,0.13); }
# MAGIC .d1s-shdr { padding: 16px 16px 12px; display: flex; align-items: center; gap: 12px; }
# MAGIC .d1s-num { width: 36px; height: 36px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 16pt; font-weight: 800; color: #fff; flex-shrink: 0; }
# MAGIC .d1s-sname { font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .d1s-sbody { background: #fff; padding: 14px 18px; flex: 1; }
# MAGIC .d1s-sbody ul { margin: 0; padding: 0 0 0 18px; }
# MAGIC .d1s-sbody li { font-size: 14pt; color: #333; line-height: 1.6; margin-bottom: 4px; }
# MAGIC .d1s-arr { display: flex; align-items: center; justify-content: center; padding: 0 6px; min-width: 40px; flex-shrink: 0; padding-top: 22px; }
# MAGIC .d1s-arr-line { width: 28px; height: 3px; position: relative; overflow: hidden; border-radius: 2px; background: #c8e6c9; }
# MAGIC .d1s-arr-line::before { content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%; background: linear-gradient(90deg, transparent, #1B5162, transparent); animation: d1sFlow 1.5s linear infinite; }
# MAGIC @keyframes d1sFlow { 0% { left: -100%; } 100% { left: 100%; } }
# MAGIC .d1s-arr-head { color: #1B5162; font-size: 14px; margin-left: 2px; }
# MAGIC .d1s-loop { display: flex; align-items: center; justify-content: center; margin-top: 14px; gap: 8px; }
# MAGIC .d1s-loop-line { flex: 1; height: 2px; background: #E5A100; border-radius: 2px; position: relative; overflow: hidden; }
# MAGIC .d1s-loop-line::before { content: ''; position: absolute; top: 0; left: 100%; width: 100%; height: 100%; background: linear-gradient(270deg, transparent, #E5A100, transparent); animation: d1sLoop 2s linear infinite; }
# MAGIC @keyframes d1sLoop { 0% { left: 100%; } 100% { left: -100%; } }
# MAGIC .d1s-loop-label { font-size: 14pt; font-weight: 700; color: #E5A100; text-transform: uppercase; letter-spacing: 0.6px; white-space: nowrap; padding: 0 8px; }
# MAGIC </style>
# MAGIC <div class="d1s-wrap">
# MAGIC <div class="d1s-flow">
# MAGIC   <div class="d1s-step"><div class="d1s-shdr" style="background:#1B5162;"><div class="d1s-num" style="background:#0B2026;">1</div><div class="d1s-sname">Focus + Data</div></div><div class="d1s-sbody"><ul><li>Clear business purpose</li><li>5 or fewer well-scoped UC tables</li><li>Column descriptions populated</li><li>PK/FK relationships defined</li><li>Remove unnecessary columns</li></ul></div></div>
# MAGIC   <div class="d1s-arr"><div class="d1s-arr-line"></div><span class="d1s-arr-head">&#x25B6;</span></div>
# MAGIC   <div class="d1s-step"><div class="d1s-shdr" style="background:#2272B4;"><div class="d1s-num" style="background:#1B5162;">2</div><div class="d1s-sname">Test + Adjust</div></div><div class="d1s-sbody"><ul><li>Simulate real user questions</li><li>Review generated SQL</li><li>Refine instructions and terms</li><li>Add example SQL queries</li><li>Narrow or expand scope</li></ul></div></div>
# MAGIC   <div class="d1s-arr"><div class="d1s-arr-line"></div><span class="d1s-arr-head">&#x25B6;</span></div>
# MAGIC   <div class="d1s-step"><div class="d1s-shdr" style="background:#00A972;"><div class="d1s-num" style="background:#006644;">3</div><div class="d1s-sname">Validate</div></div><div class="d1s-sbody"><ul><li>Benchmark questions + verified SQL</li><li>Automated accuracy tests</li><li>Stakeholder real-world testing</li><li>Track feedback over time</li><li>Monitor question themes</li></ul></div></div>
# MAGIC </div>
# MAGIC <div class="d1s-loop"><div class="d1s-loop-line"></div><div class="d1s-loop-label">&#x21BB; Iterate: refine instructions, re-run benchmarks, repeat</div><div class="d1s-loop-line"></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Good vs. Bad Topics for Genie</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Good topics:</strong> exploratory data analysis, tracking revenue streams, monitoring profit margins, measuring engagement and churn. Well-structured tables with clear relationships and consistent schema across Sales, Finance, HR, and Marketing domains.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Bad topics:</strong> root cause analysis ("Why did X drop?"), explanatory modeling, volatile or frequently changing datasets, poorly normalized data requiring complex joins, and topics where the decision-making layer is abstracted from data owners.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Clean Data Preparation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Enhance metadata:</strong> add column-level comments describing business meaning, example values, and user-friendly synonyms (for example, "stage" vs. "status"). This is the single highest-use action for improving Genie accuracy.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Define relationships:</strong> set primary and foreign key relationships in Unity Catalog. Alternatively, pre-join reference tables into materialized views to simplify Genie's SQL generation.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Remove noise:</strong> remove unnecessary columns. Start with a focused set of UC tables aligned to the business domain (for example, Salesforce CRM, Workday HRIS).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Instruction Hierarchy</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>SQL expressions (most effective):</strong> define recurring business concepts as reusable metric definitions (revenue, active_customers, churn_rate).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Example SQL queries (second):</strong> provide complete SQL examples for common multi-part questions. These teach Genie patterns for complex queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Text instructions (last resort):</strong> use for clarification triggers ("When users ask about sales performance without specifying a time range, ask a clarification question"). Keep text instructions concise; vague or lengthy instructions introduce noise.</li>
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
# MAGIC ### D2. Benchmarks, Monitoring, and Troubleshooting
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Benchmarks are "unit tests" for Genie Spaces. Each benchmark pairs a business question with a verified SQL statement. Running benchmarks produces side-by-side comparisons of Genie's generated SQL versus the approved query, assessed as Good or Needs Review. Over time, benchmark history tracks accuracy percentages and identifies patterns in failures.</p>
# MAGIC
# MAGIC <!-- ── Visual: d2-troubleshooting ── -->
# MAGIC <style>
# MAGIC .d2t-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; flex-direction: column; gap: 10px; }
# MAGIC .d2t-row { display: flex; gap: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.08); transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .d2t-row:hover { transform: translateY(-2px); box-shadow: 0 5px 14px rgba(27,49,57,0.12); }
# MAGIC .d2t-issue { flex: 1; background: #FFF0EE; padding: 16px 18px; border-left: 5px solid #98102A; }
# MAGIC .d2t-issue-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; color: #98102A; margin-bottom: 4px; }
# MAGIC .d2t-issue-text { font-size: 14pt; font-weight: 600; color: #1B3139; }
# MAGIC .d2t-fix { flex: 1.3; background: #E8F5E9; padding: 16px 18px; border-right: 5px solid #00A972; }
# MAGIC .d2t-fix-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; color: #00A972; margin-bottom: 4px; }
# MAGIC .d2t-fix-text { font-size: 14pt; color: #333; line-height: 1.5; }
# MAGIC </style>
# MAGIC <div class="d2t-wrap">
# MAGIC   <div class="d2t-row"><div class="d2t-issue"><div class="d2t-issue-label">Issue</div><div class="d2t-issue-text">Genie joins tables incorrectly</div></div><div class="d2t-fix"><div class="d2t-fix-label">Solution</div><div class="d2t-fix-text">Pre-join into materialized views, define PK/FK in UC, map join relationships in knowledge store, or add example SQL showing the correct join</div></div></div>
# MAGIC   <div class="d2t-row"><div class="d2t-issue"><div class="d2t-issue-label">Issue</div><div class="d2t-issue-text">Wrong column values in WHERE clause</div></div><div class="d2t-fix"><div class="d2t-fix-label">Solution</div><div class="d2t-fix-text">Enable example values and value dictionaries on relevant columns (e.g., "CA" instead of "California")</div></div></div>
# MAGIC   <div class="d2t-row"><div class="d2t-issue"><div class="d2t-issue-label">Issue</div><div class="d2t-issue-text">Genie ignores general instructions</div></div><div class="d2t-fix"><div class="d2t-fix-label">Solution</div><div class="d2t-fix-text">Add example SQL queries instead of text; remove unnecessary or vague instructions that introduce noise</div></div></div>
# MAGIC   <div class="d2t-row"><div class="d2t-issue"><div class="d2t-issue-label">Issue</div><div class="d2t-issue-text">Hitting the token limit</div></div><div class="d2t-fix"><div class="d2t-fix-label">Solution</div><div class="d2t-fix-text">Remove unused columns, streamline column descriptions, prune lengthy example SQL, simplify instructions</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Benchmark Recommendations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Multiple phrasings:</strong> create several variations of the same business question to test whether Genie handles different wording consistently.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Run after changes:</strong> re-run benchmarks after changing instructions, column comments, or example SQL. This catches regressions early.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Include edge cases:</strong> ambiguous questions, abbreviations, and questions that combine multiple concepts help identify gaps in the knowledge store.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Monitoring and Feedback Loops</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Monitoring tab:</strong> review individual questions and responses, view user feedback (thumbs up/down), and identify responses flagged for review. Filter by time, rating, user, or status.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Ask for review:</strong> users can flag a response for editor review. Space editors and authors review the feedback, can edit the SQL, or mark it as correct. The business user is then notified of the resolution.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Five Approaches to Improve Genie Over Time</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>1. Benchmarks with ground truth SQL:</strong> track accuracy over time. This is the foundation and the easiest to implement.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>2. Example SQL for common questions:</strong> helps Genie understand how to answer the most-asked queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>3. Expanded instructions:</strong> guide Genie on how to respond. Requires both business and SQL knowledge.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>4. Revalidate data model:</strong> more complex; requires backend data fluency. Consider when a majority of queries fail.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>5. Add SQL functions/UDFs:</strong> advanced approach that boosts consistency for similar queries across use cases.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/webmotors/ai-bi-genie" style="color: #2574B5; font-size: 14pt;">Webmotors</a> trained 214 users across 18 sessions on AI/BI Genie, recovering 200 hours of analyst time per month by evolving from reactive data support to scalable self-service. &#x25C6;</li>
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
# MAGIC ## E. Genie Agent Mode and APIs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Genie Agent Mode (Deep Reasoning)
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Standard Genie excels at straightforward data lookups. But complex, exploratory questions like "Why was there a spike in encounters in March 2025?" require multi-step investigation. <strong>Genie Agent Mode</strong> (GA, May 2026) extends Genie with multi-step reasoning and hypothesis testing. It creates a structured research plan, runs multiple SQL queries, incorporates each result, adjusts its approach, and delivers a comprehensive report with citations, visualizations, and supporting tables.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-agent-mode-tabs ── -->
# MAGIC <style>
# MAGIC .e1a-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .e1a-wrap input[type="radio"] { display: none; }
# MAGIC .e1a-tabs { display: flex; gap: 4px; margin-bottom: 0; }
# MAGIC .e1a-tab { flex: 1; text-align: center; padding: 14px 8px; font-size: 14pt; font-weight: 700; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-bottom: none; border-radius: 8px 8px 0 0; background: #fff; transition: all 0.2s; user-select: none; position: relative; }
# MAGIC .e1a-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 6px 6px 0 0; }
# MAGIC .e1a-tab[for="e1r1"]::before { background: #1B5162; }
# MAGIC .e1a-tab[for="e1r2"]::before { background: #00A972; }
# MAGIC #e1r1:checked ~ .e1a-tabs .e1a-tab[for="e1r1"] { border-color: #1B5162; background: #E8F0ED; }
# MAGIC #e1r2:checked ~ .e1a-tabs .e1a-tab[for="e1r2"] { border-color: #00A972; background: #E8F5E9; }
# MAGIC .e1a-panels { border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; background: #fff; }
# MAGIC .e1a-panel { display: none; padding: 24px 28px; }
# MAGIC #e1r1:checked ~ .e1a-panels .e1a-p1 { display: block; }
# MAGIC #e1r2:checked ~ .e1a-panels .e1a-p2 { display: block; }
# MAGIC /* Comparison boxes */
# MAGIC .e1a-cmp { display: flex; gap: 14px; margin-bottom: 20px; }
# MAGIC .e1a-cmp-box { flex: 1; border-radius: 10px; padding: 18px 20px; }
# MAGIC .e1a-cmp-std { background: #F9F7F4; border: 2px solid #DCE0E2; }
# MAGIC .e1a-cmp-agent { background: #1B5162; color: #fff; }
# MAGIC .e1a-cmp-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 6px; }
# MAGIC .e1a-cmp-title { font-size: 15pt; font-weight: 700; margin-bottom: 8px; }
# MAGIC .e1a-cmp-desc { font-size: 14pt; line-height: 1.5; }
# MAGIC .e1a-cmp-ex { font-size: 14pt; margin-top: 10px; padding: 10px 14px; border-radius: 8px; font-style: italic; }
# MAGIC /* Process flow */
# MAGIC .e1a-flow { display: flex; align-items: flex-start; gap: 0; margin-bottom: 14px; }
# MAGIC .e1a-fstep { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s; cursor: default; }
# MAGIC .e1a-fstep:hover { transform: translateY(-2px); box-shadow: 0 5px 12px rgba(27,49,57,0.13); }
# MAGIC .e1a-fhdr { padding: 12px 14px; display: flex; align-items: center; gap: 10px; }
# MAGIC .e1a-fnum { width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14pt; font-weight: 800; color: #fff; flex-shrink: 0; }
# MAGIC .e1a-fname { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .e1a-fbody { background: #fff; padding: 10px 14px; font-size: 14pt; color: #444; line-height: 1.5; }
# MAGIC .e1a-farr { display: flex; align-items: center; justify-content: center; padding: 0 4px; min-width: 32px; flex-shrink: 0; padding-top: 14px; }
# MAGIC .e1a-farr::after { content: ''; width: 0; height: 0; border-top: 7px solid transparent; border-bottom: 7px solid transparent; border-left: 10px solid #1B5162; }
# MAGIC .e1a-farr-loop::after { border-left-color: #00A972; }
# MAGIC .e1a-loop { text-align: center; }
# MAGIC .e1a-loop-pill { display: inline-flex; align-items: center; gap: 8px; padding: 6px 16px; background: #FFF8E1; border-radius: 20px; font-size: 14pt; font-weight: 600; color: #E5A100; }
# MAGIC /* Capability cards */
# MAGIC .e1a-caps { display: flex; gap: 12px; margin-top: 18px; }
# MAGIC .e1a-cap { flex: 1; background: #F9F7F4; border-radius: 10px; padding: 14px 16px; border-left: 4px solid; }
# MAGIC .e1a-cap-name { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .e1a-cap-desc { font-size: 14pt; color: #5A6F77; line-height: 1.45; }
# MAGIC /* Output cards */
# MAGIC .e1a-out-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
# MAGIC .e1a-out-card { border-radius: 10px; padding: 18px 16px; text-align: center; border: 2px solid #DCE0E2; background: #fff; transition: transform 0.15s, border-color 0.15s; cursor: default; }
# MAGIC .e1a-out-card:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(27,49,57,0.10); border-color: #00A972; }
# MAGIC .e1a-out-dot { width: 10px; height: 10px; border-radius: 50%; margin: 0 auto 10px auto; }
# MAGIC .e1a-out-name { font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 6px; }
# MAGIC .e1a-out-desc { font-size: 14pt; color: #618794; line-height: 1.45; }
# MAGIC </style>
# MAGIC <div class="e1a-wrap">
# MAGIC <input type="radio" name="e1grp" id="e1r1" checked>
# MAGIC <input type="radio" name="e1grp" id="e1r2">
# MAGIC <div class="e1a-tabs">
# MAGIC   <label class="e1a-tab" for="e1r1">How It Works</label>
# MAGIC   <label class="e1a-tab" for="e1r2">Report Output</label>
# MAGIC </div>
# MAGIC <div class="e1a-panels">
# MAGIC   <!-- Tab 1: How It Works -->
# MAGIC   <div class="e1a-panel e1a-p1">
# MAGIC     <div class="e1a-cmp">
# MAGIC       <div class="e1a-cmp-box e1a-cmp-std"><div class="e1a-cmp-label" style="color:#618794;">Standard Genie</div><div class="e1a-cmp-title" style="color:#1B3139;">One Question, One Query, One Answer</div><div class="e1a-cmp-desc" style="color:#5A6F77;">User asks a question. Genie generates one SQL query. Returns one result set.</div><div class="e1a-cmp-ex" style="background:#EEEDE9;color:#5A6F77;">"What was revenue this quarter?" &#x279C; One SQL, one table</div></div>
# MAGIC       <div class="e1a-cmp-box e1a-cmp-agent"><div class="e1a-cmp-label" style="color:#90A5B1;">Agent Mode</div><div class="e1a-cmp-title">Complex Question, Multi-Step Research</div><div class="e1a-cmp-desc" style="color:#DCE0E2;">User asks an exploratory question. Genie builds a research plan, runs multiple SQL queries in parallel, tests hypotheses, iterates, and delivers a report.</div><div class="e1a-cmp-ex" style="background:rgba(0,0,0,0.2);color:#DCE0E2;">"Why did revenue spike in June?" &#x279C; Research plan, multiple queries, report</div></div>
# MAGIC     </div>
# MAGIC     <div style="font-size:14pt;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#1B5162;margin-bottom:10px;">How Agent Mode Processes a Question</div>
# MAGIC     <div class="e1a-flow">
# MAGIC       <div class="e1a-fstep"><div class="e1a-fhdr" style="background:#1B5162;"><div class="e1a-fnum" style="background:#0B2026;">1</div><div class="e1a-fname">Question</div></div><div class="e1a-fbody">User asks an exploratory "why" or "what factors" question. May ask clarifying follow-ups.</div></div>
# MAGIC       <div class="e1a-farr"></div>
# MAGIC       <div class="e1a-fstep"><div class="e1a-fhdr" style="background:#2272B4;"><div class="e1a-fnum" style="background:#1B5162;">2</div><div class="e1a-fname">Plan</div></div><div class="e1a-fbody">Develops hypotheses and a structured approach. Determines which data to query.</div></div>
# MAGIC       <div class="e1a-farr"></div>
# MAGIC       <div class="e1a-fstep"><div class="e1a-fhdr" style="background:#618794;"><div class="e1a-fnum" style="background:#1B5162;">3</div><div class="e1a-fname">Query</div></div><div class="e1a-fbody">Executes multiple SQL queries in parallel. Gathers evidence from different angles.</div></div>
# MAGIC       <div class="e1a-farr e1a-farr-loop"></div>
# MAGIC       <div class="e1a-fstep"><div class="e1a-fhdr" style="background:#E5A100;"><div class="e1a-fnum" style="background:#856404;">4</div><div class="e1a-fname">Iterate</div></div><div class="e1a-fbody">Evaluates results. Refines hypotheses. Runs more queries if needed.</div></div>
# MAGIC       <div class="e1a-farr"></div>
# MAGIC       <div class="e1a-fstep"><div class="e1a-fhdr" style="background:#00A972;"><div class="e1a-fnum" style="background:#006644;">5</div><div class="e1a-fname">Report</div></div><div class="e1a-fbody">Comprehensive report with findings, charts, and citations.</div></div>
# MAGIC     </div>
# MAGIC     <div class="e1a-loop"><span class="e1a-loop-pill">&#x21BB; Steps 3-4 repeat until confident. Click "Answer Now" to stop early.</span></div>
# MAGIC     <div class="e1a-caps">
# MAGIC       <div class="e1a-cap" style="border-color:#1B5162;"><div class="e1a-cap-name">Thinking Traces</div><div class="e1a-cap-desc">Streams reasoning steps inline so users follow the analysis. Cites definitions applied.</div></div>
# MAGIC       <div class="e1a-cap" style="border-color:#2272B4;"><div class="e1a-cap-name">Parallel Execution</div><div class="e1a-cap-desc">Runs multiple SQL queries simultaneously. Prompt caching reduces multi-turn latency.</div></div>
# MAGIC       <div class="e1a-cap" style="border-color:#00A972;"><div class="e1a-cap-name">Same Context</div><div class="e1a-cap-desc">Uses the same UC tables, instructions, and knowledge store as standard Genie.</div></div>
# MAGIC       <div class="e1a-cap" style="border-color:#E5A100;"><div class="e1a-cap-name">UI Only</div><div class="e1a-cap-desc">Available in Databricks UI. Not available via API. No extra cost beyond SQL Warehouse compute.</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 2: Report Output -->
# MAGIC   <div class="e1a-panel e1a-p2">
# MAGIC     <div style="font-size:14pt;color:#333;line-height:1.6;margin-bottom:18px;">When Agent Mode finishes its research (or the user clicks "Answer Now"), it delivers a structured report containing four components:</div>
# MAGIC     <div class="e1a-out-grid">
# MAGIC       <div class="e1a-out-card"><div class="e1a-out-dot" style="background:#1B5162;"></div><div class="e1a-out-name">Findings</div><div class="e1a-out-desc">Specific conclusions with supporting evidence from each research step</div></div>
# MAGIC       <div class="e1a-out-card"><div class="e1a-out-dot" style="background:#2272B4;"></div><div class="e1a-out-name">Visualizations</div><div class="e1a-out-desc">Charts and tables generated during analysis, respecting workspace themes</div></div>
# MAGIC       <div class="e1a-out-card"><div class="e1a-out-dot" style="background:#00A972;"></div><div class="e1a-out-name">Citations</div><div class="e1a-out-desc">References to each SQL query and research step so reviewers can verify</div></div>
# MAGIC       <div class="e1a-out-card"><div class="e1a-out-dot" style="background:#618794;"></div><div class="e1a-out-name">PDF Export</div><div class="e1a-out-desc">Download the full report with all findings and visuals for offline sharing</div></div>
# MAGIC     </div>
# MAGIC     <div style="font-size:14pt;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#1B5162;margin-bottom:10px;">Example: Agent Mode in Action</div>
# MAGIC     <div style="background:#1B3139;border-radius:12px;padding:20px 22px;color:#fff;">
# MAGIC       <div style="font-size:15pt;font-style:italic;color:#E5A100;margin-bottom:14px;padding:12px 16px;background:rgba(229,161,0,0.1);border-radius:8px;border-left:4px solid #E5A100;">"Why did encounters spike in March 2025?"</div>
# MAGIC       <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:10px;"><div style="width:26px;height:26px;border-radius:50%;background:#1B5162;display:flex;align-items:center;justify-content:center;font-size:14pt;font-weight:700;flex-shrink:0;">1</div><div><div style="font-size:14pt;font-weight:600;">Builds research plan</div><div style="font-size:14pt;color:#90A5B1;">Hypotheses: seasonal pattern, new provider onboarding, billing code change, regional expansion</div></div></div>
# MAGIC       <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:10px;"><div style="width:26px;height:26px;border-radius:50%;background:#2272B4;display:flex;align-items:center;justify-content:center;font-size:14pt;font-weight:700;flex-shrink:0;">2</div><div><div style="font-size:14pt;font-weight:600;">Runs initial queries</div><div style="font-size:14pt;color:#90A5B1;">Monthly encounter counts, new provider registrations by date, encounter volume by region</div></div></div>
# MAGIC       <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:10px;"><div style="width:26px;height:26px;border-radius:50%;background:#E5A100;display:flex;align-items:center;justify-content:center;font-size:14pt;font-weight:700;flex-shrink:0;">3</div><div><div style="font-size:14pt;font-weight:600;">Evaluates: new providers account for 40% of spike</div><div style="font-size:14pt;color:#90A5B1;">Refines: drills into which regions added providers, which specialties</div></div></div>
# MAGIC       <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:10px;"><div style="width:26px;height:26px;border-radius:50%;background:#E5A100;display:flex;align-items:center;justify-content:center;font-size:14pt;font-weight:700;flex-shrink:0;">4</div><div><div style="font-size:14pt;font-weight:600;">Runs follow-up queries</div><div style="font-size:14pt;color:#90A5B1;">Encounters by region + specialty for new providers, year-over-year comparison</div></div></div>
# MAGIC       <div style="display:flex;align-items:flex-start;gap:12px;"><div style="width:26px;height:26px;border-radius:50%;background:#00A972;display:flex;align-items:center;justify-content:center;font-size:14pt;font-weight:700;flex-shrink:0;">5</div><div><div style="font-size:14pt;font-weight:600;">Delivers report</div><div style="font-size:14pt;color:#DCE0E2;">Finding: 3 new cardiology providers in the Southeast drove 62% of the March spike. Charts, tables, and citations to each query included.</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How It Works Tab: The Five-Step Process</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Standard Genie vs Agent Mode:</strong> standard Genie handles direct data lookups ("What was revenue this quarter?") with one SQL query and one result. Agent Mode handles exploratory questions ("Why did revenue spike in June?") by building a research plan, running multiple queries in parallel, testing hypotheses, iterating, and delivering a report.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Steps 1-2 (Question and Plan):</strong> the user asks an exploratory question. Agent Mode may ask clarifying follow-ups, then develops a structured approach with hypotheses and determines which data to query and in what order.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Steps 3-4 (Query and Iterate):</strong> Agent Mode executes multiple SQL queries in parallel, gathers evidence from different angles, evaluates results, refines hypotheses, and runs additional queries if needed. These steps repeat until the system is confident in its answer.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Step 5 (Report):</strong> delivers a comprehensive report with specific findings, visualizations, and citations to each research step. Users can click "Answer Now" at any point during steps 3-4 to stop reasoning and receive findings based on evidence gathered so far.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Four Capabilities That Power Agent Mode</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Thinking traces:</strong> reasoning steps stream inline so you can follow the analysis as it happens. Traces cite the definitions applied to answer questions, giving you visibility into how Agent Mode reached its conclusions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Parallel execution:</strong> multiple SQL queries run simultaneously for faster results. Prompt caching reduces latency in multi-turn conversations.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Same context:</strong> Agent Mode uses the same UC tables, instructions, example SQL, and knowledge store as standard Genie. No separate setup or configuration is required.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>UI only:</strong> Agent Mode is available through the Databricks UI. It is not available via the Genie API. No additional cost beyond standard SQL Warehouse compute.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Report Output Tab: What You Get</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Findings:</strong> specific conclusions with supporting evidence from each research step. Not just raw data, but synthesized insights with context.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Visualizations:</strong> charts and tables generated during the analysis, respecting workspace-level themes for consistent branding.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Citations:</strong> references to each SQL query and research step so reviewers can verify how Agent Mode arrived at its conclusions. Users can request reviews on Agent Mode conversations for quality assurance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>PDF export:</strong> download the full report with findings, visualizations, and citations for offline sharing with stakeholders.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">GA Status and Recent Additions</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>GA (May 2026):</strong> Genie Spaces Agent Mode is available by default for compliance security profile workspaces (HIPAA, PCI-DSS, FedRAMP). Expanded to Americas, Europe, Australia, New Zealand, and Japan.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Increased token limits:</strong> maximum token limits for Agent Mode conversations increased, improving performance across long multi-turn interactions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>API access to traces:</strong> reasoning traces are accessible through the <code style="font-size: 14pt;">GenieQueryAttachments</code> field in API responses for programmatic integration.</li>
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
# MAGIC ### E2. Business User Access to Genie Spaces
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Genie Spaces are designed for business users who need data answers without SQL skills or workspace access. Five channels deliver Genie to users where they already work: directly in Databricks, embedded in external tools, integrated into collaboration platforms, built into custom applications, and surfaced through account-level discovery.</p>
# MAGIC
# MAGIC <!-- ── Visual: e2-access-tabs ── -->
# MAGIC <style>
# MAGIC .e2a-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .e2a-wrap input[type="radio"] { display: none; }
# MAGIC .e2a-tabs { display: flex; gap: 4px; margin-bottom: 0; }
# MAGIC .e2a-tab { flex: 1; text-align: center; padding: 14px 8px; font-size: 14pt; font-weight: 700; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-bottom: none; border-radius: 8px 8px 0 0; background: #fff; transition: all 0.2s; user-select: none; position: relative; }
# MAGIC .e2a-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 6px 6px 0 0; }
# MAGIC .e2a-tab[for="e2r1"]::before { background: #1B5162; }
# MAGIC .e2a-tab[for="e2r2"]::before { background: #2272B4; }
# MAGIC #e2r1:checked ~ .e2a-tabs .e2a-tab[for="e2r1"] { border-color: #1B5162; background: #E8F0ED; }
# MAGIC #e2r2:checked ~ .e2a-tabs .e2a-tab[for="e2r2"] { border-color: #2272B4; background: #E8F0F8; }
# MAGIC .e2a-panels { border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; background: #fff; }
# MAGIC .e2a-panel { display: none; padding: 24px 28px; }
# MAGIC #e2r1:checked ~ .e2a-panels .e2a-p1 { display: block; }
# MAGIC #e2r2:checked ~ .e2a-panels .e2a-p2 { display: block; }
# MAGIC /* Access channel cards */
# MAGIC .e2a-grid { display: flex; gap: 12px; margin-bottom: 14px; }
# MAGIC .e2a-card { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s; cursor: default; display: flex; flex-direction: column; }
# MAGIC .e2a-card:hover { transform: translateY(-2px); box-shadow: 0 5px 12px rgba(27,49,57,0.13); }
# MAGIC .e2a-chdr { padding: 12px 14px; display: flex; align-items: center; gap: 8px; }
# MAGIC .e2a-cdot { width: 8px; height: 8px; border-radius: 50%; background: #fff; flex-shrink: 0; }
# MAGIC .e2a-cname { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .e2a-cpill { margin-left: auto; font-size: 14pt; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase; padding: 2px 8px; border-radius: 10px; }
# MAGIC .e2a-cbody { background: #fff; padding: 12px 14px; flex: 1; }
# MAGIC .e2a-cbody ul { margin: 0; padding: 0 0 0 16px; }
# MAGIC .e2a-cbody li { font-size: 14pt; color: #444; line-height: 1.55; margin-bottom: 3px; }
# MAGIC /* API endpoint rows */
# MAGIC .e2a-api-cat { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; margin: 14px 0 8px 0; padding-bottom: 4px; border-bottom: 2px solid; }
# MAGIC .e2a-ep { display: flex; align-items: center; gap: 10px; padding: 8px 0; border-bottom: 1px solid #EEEDE9; }
# MAGIC .e2a-ep:last-child { border-bottom: none; }
# MAGIC .e2a-method { font-size: 14pt; font-weight: 700; font-family: monospace; padding: 2px 8px; border-radius: 4px; flex-shrink: 0; min-width: 44px; text-align: center; }
# MAGIC .e2a-path { font-size: 14pt; font-family: monospace; color: #618794; flex: 1; }
# MAGIC .e2a-epdesc { font-size: 14pt; color: #333; flex: 1; }
# MAGIC </style>
# MAGIC <div class="e2a-wrap">
# MAGIC <input type="radio" name="e2grp" id="e2r1" checked>
# MAGIC <input type="radio" name="e2grp" id="e2r2">
# MAGIC <div class="e2a-tabs">
# MAGIC   <label class="e2a-tab" for="e2r1">Access Channels</label>
# MAGIC   <label class="e2a-tab" for="e2r2">Genie Spaces API</label>
# MAGIC </div>
# MAGIC <div class="e2a-panels">
# MAGIC   <!-- Tab 1: Access Channels -->
# MAGIC   <div class="e2a-panel e2a-p1">
# MAGIC     <div class="e2a-grid">
# MAGIC       <div class="e2a-card"><div class="e2a-chdr" style="background:#1B5162;"><div class="e2a-cdot"></div><div class="e2a-cname">Workspace UI</div><span class="e2a-cpill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">GA</span></div><div class="e2a-cbody"><ul><li>Direct access in Databricks workspace</li><li>Ask Genie button on published dashboards</li><li>Agent Mode for deep analysis</li></ul></div></div>
# MAGIC       <div class="e2a-card"><div class="e2a-chdr" style="background:#2272B4;"><div class="e2a-cdot"></div><div class="e2a-cname">Account-Level Genie</div><span class="e2a-cpill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">GA</span></div><div class="e2a-cbody"><ul><li>Unified view across all workspaces</li><li>Discover dashboards, Genie Spaces, and Apps from one entry point</li><li>Consumer entitlement, no workspace access needed</li></ul></div></div>
# MAGIC     </div>
# MAGIC     <div class="e2a-grid">
# MAGIC       <div class="e2a-card"><div class="e2a-chdr" style="background:#00A972;"><div class="e2a-cdot"></div><div class="e2a-cname">Embedded Genie</div><span class="e2a-cpill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">Beta</span></div><div class="e2a-cbody"><ul><li>Embed a Genie Space as an iframe in external websites or apps</li><li>Users authenticate via Databricks SSO</li><li>Admin configures trusted domains</li></ul></div></div>
# MAGIC       <div class="e2a-card"><div class="e2a-chdr" style="background:#618794;"><div class="e2a-cdot"></div><div class="e2a-cname">Custom Apps via API</div><span class="e2a-cpill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">GA</span></div><div class="e2a-cbody"><ul><li>Build chatbots and agents with the Genie Conversation API</li><li>Stateful multi-turn conversations</li><li>Integrate into Databricks Apps, Slack bots, or any application</li></ul></div></div>
# MAGIC     </div>
# MAGIC     <div class="e2a-grid">
# MAGIC       <div class="e2a-card"><div class="e2a-chdr" style="background:#1B3139;"><div class="e2a-cdot"></div><div class="e2a-cname">Slack / Teams</div><span class="e2a-cpill" style="background:rgba(255,255,255,0.15);color:#DCE0E2;">GA</span></div><div class="e2a-cbody"><ul><li>Dashboard snapshot subscriptions to Slack channels and Teams</li><li>Build Genie-powered bots using Conversation API + agent framework</li><li>Query data in natural language where teams collaborate</li></ul></div></div>
# MAGIC     </div>
# MAGIC     <div style="font-size:14pt;color:#618794;margin-top:10px;font-style:italic;">All channels are governed by Unity Catalog. Users only access data they have permission to view.</div>
# MAGIC   </div>
# MAGIC   <!-- Tab 2: Genie Spaces API -->
# MAGIC   <div class="e2a-panel e2a-p2">
# MAGIC     <div class="e2a-api-cat" style="color:#1B5162;border-color:#1B5162;">Conversation Endpoints</div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f5e9;color:#006644;">POST</span><div class="e2a-epdesc">Start a new conversation in a Genie Space</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f5e9;color:#006644;">POST</span><div class="e2a-epdesc">Send a follow-up message in an existing conversation</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f0fe;color:#2b6cb0;">GET</span><div class="e2a-epdesc">Retrieve message status and generated SQL</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f0fe;color:#2b6cb0;">GET</span><div class="e2a-epdesc">Fetch query results for a message attachment</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f0fe;color:#2b6cb0;">GET</span><div class="e2a-epdesc">List all conversations in a space</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#FABFBA;color:#98102A;">DELETE</span><div class="e2a-epdesc">Remove a conversation</div></div>
# MAGIC     <div class="e2a-api-cat" style="color:#618794;border-color:#618794;">Space Management Endpoints</div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f5e9;color:#006644;">POST</span><div class="e2a-epdesc">Create a new Genie Space with tables, instructions, and example SQL</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#e8f0fe;color:#2b6cb0;">GET</span><div class="e2a-epdesc">List all spaces in a workspace or retrieve a specific space</div></div>
# MAGIC     <div class="e2a-ep"><span class="e2a-method" style="background:#fef3cd;color:#856404;">POST</span><div class="e2a-epdesc">Update space settings, instructions, or table configuration</div></div>
# MAGIC     <div style="display:flex;gap:14px;margin-top:16px;">
# MAGIC       <div style="flex:1;background:#F9F7F4;border-radius:8px;padding:12px 14px;border-left:4px solid #1B5162;"><div style="font-size:14pt;font-weight:700;color:#1B3139;margin-bottom:4px;">Rate Limit</div><div style="font-size:14pt;color:#444;">5 questions/min per workspace (POST only). GET polling requests are excluded.</div></div>
# MAGIC       <div style="flex:1;background:#F9F7F4;border-radius:8px;padding:12px 14px;border-left:4px solid #E5A100;"><div style="font-size:14pt;font-weight:700;color:#1B3139;margin-bottom:4px;">Limitation</div><div style="font-size:14pt;color:#444;">API returns structured tabular data only. No chart rendering. Use Databricks Apps for visual interfaces.</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Account-Level Genie</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Unified cross-workspace discovery:</strong> account-level Genie gives business users a single entry point to discover and interact with AI/BI Dashboards, Genie Spaces, and Databricks Apps from every workspace they have access to. Trending assets, recently accessed content, and favorites surface automatically.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Consumer entitlement:</strong> users with Consumer access on at least one workspace can use account-level Genie. They do not need workspace-level access or SQL entitlements. Administrators enable it via the account console's Feature enablement tab.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data residency note:</strong> metadata may process in US-based services, while actual data remains in the workspace's region. Organizations with strict residency requirements can disable the feature.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Embedded Genie Spaces</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Iframe embedding:</strong> space authors generate embed code through the Share dialog. The iframe is pasted into external apps or portals. Users authenticate via Databricks SSO before interacting.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Admin controls:</strong> workspace admins configure trusted domains that are allowed to host the embedded iframe. End users must have explicit access to both the Genie Space and its underlying data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Rate limit:</strong> 20 questions per minute per workspace across all embedded Genie Spaces.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Conversation API Integration Patterns</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Stateful conversations:</strong> the API supports multi-turn conversations. Each follow-up message inherits context from previous messages in the thread, so users can refine and explore naturally.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>CI/CD for Genie Spaces:</strong> the Space Management API enables programmatic creation, configuration, and deployment of Genie Spaces across workspaces, supporting infrastructure-as-code workflows.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Conversation sharing (Beta):</strong> individual conversations can be shared with other users, groups, or service principals. Sharing levels: Private, Reviewable by space managers (default), or All account users.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Proof Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/the-aa/ai-bi-genie-api" style="color: #2574B5; font-size: 14pt;">The AA</a> integrated Genie into Microsoft Teams through the Conversation API, enabling trading, marketing, and product teams to query data in plain English and achieve 70% faster insights. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/italgas/ai-bi" style="color: #2574B5; font-size: 14pt;">Italgas</a> embedded Genie into their meter management system (DEVA), a commercial product sold to other gas distributors, enabling operators to query system performance using natural language. &#x25C6;</li>
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
# MAGIC ## F. Databricks Apps

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. Databricks Apps: Architecture and Security Model
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">A Databricks App is a containerized web application that runs on the Databricks serverless platform. It uses familiar frameworks (Streamlit, Dash, Flask, Gradio for Python; React, Angular, Svelte, Express for Node.js) and connects to platform services through a governed resource model. Each app receives an auto-provisioned service principal with a dual authorization model: the app's identity handles backend resource access, while individual users authenticate via SSO and access data according to their own UC permissions. Over 20,000 apps have been built by over 2,500 customers.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-apps-arch-and-security ── -->
# MAGIC <style>
# MAGIC .f1a-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .f1a-cols { display: flex; gap: 16px; align-items: stretch; }
# MAGIC .f1a-arch { flex: 3; display: flex; flex-direction: column; gap: 6px; }
# MAGIC .f1a-sec { flex: 2; display: flex; flex-direction: column; gap: 10px; }
# MAGIC .f1a-layer { border-radius: 10px; overflow: hidden; transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
# MAGIC .f1a-layer:hover { transform: translateX(4px); box-shadow: 0 4px 12px rgba(27,49,57,0.12); }
# MAGIC .f1a-lhdr { padding: 12px 16px; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .f1a-lbody { background: #F9F7F4; padding: 10px 16px; display: flex; gap: 8px; flex-wrap: wrap; }
# MAGIC .f1a-chip { background: #fff; border: 1px solid #EEEDE9; border-radius: 8px; padding: 6px 12px; font-size: 14pt; color: #333; font-weight: 600; transition: background 0.15s, border-color 0.15s; cursor: default; }
# MAGIC .f1a-chip:hover { background: #E8F0ED; border-color: #1B5162; }
# MAGIC .f1a-conn { display: flex; justify-content: center; height: 12px; }
# MAGIC .f1a-conn::before { content: ''; width: 3px; height: 100%; background: #94b3be; border-radius: 2px; display: block; }
# MAGIC .f1a-scard { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s; cursor: default; }
# MAGIC .f1a-scard:hover { transform: translateY(-2px); box-shadow: 0 5px 12px rgba(27,49,57,0.13); }
# MAGIC .f1a-shdr { padding: 12px 14px; display: flex; align-items: center; gap: 8px; }
# MAGIC .f1a-sdot { width: 8px; height: 8px; border-radius: 50%; background: #fff; flex-shrink: 0; }
# MAGIC .f1a-sname { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .f1a-sbody { background: #fff; padding: 12px 14px; }
# MAGIC .f1a-sbody ul { margin: 0; padding: 0 0 0 16px; }
# MAGIC .f1a-sbody li { font-size: 14pt; color: #444; line-height: 1.55; margin-bottom: 3px; }
# MAGIC </style>
# MAGIC <div class="f1a-wrap">
# MAGIC <div class="f1a-cols">
# MAGIC   <!-- Architecture (left) -->
# MAGIC   <div class="f1a-arch">
# MAGIC     <div style="font-size:14pt;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#1B5162;margin-bottom:4px;">Three-Layer Architecture</div>
# MAGIC     <div class="f1a-layer"><div class="f1a-lhdr" style="background:#1B5162;">Control Plane</div><div class="f1a-lbody"><div class="f1a-chip">Web App UI</div><div class="f1a-chip">Compute Manager</div><div class="f1a-chip">Unity Catalog</div><div class="f1a-chip">OAuth / SSO</div></div></div>
# MAGIC     <div class="f1a-conn"></div>
# MAGIC     <div class="f1a-layer"><div class="f1a-lhdr" style="background:#2272B4;">App Resources (Serverless)</div><div class="f1a-lbody"><div class="f1a-chip">SQL Warehouse</div><div class="f1a-chip">Model Serving</div><div class="f1a-chip">Vector Search</div><div class="f1a-chip">Genie Spaces</div><div class="f1a-chip">Lakebase</div><div class="f1a-chip">Lakeflow Jobs</div><div class="f1a-chip">Volumes</div><div class="f1a-chip">Secrets</div></div></div>
# MAGIC     <div class="f1a-conn"></div>
# MAGIC     <div class="f1a-layer"><div class="f1a-lhdr" style="background:#618794;">Data Plane (Customer Cloud)</div><div class="f1a-lbody"><div class="f1a-chip">Cloud Storage</div><div class="f1a-chip">UC Tables</div><div class="f1a-chip">External APIs</div></div></div>
# MAGIC   </div>
# MAGIC   <!-- Security model (right) -->
# MAGIC   <div class="f1a-sec">
# MAGIC     <div style="font-size:14pt;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#00A972;margin-bottom:4px;">Dual Authorization Model</div>
# MAGIC     <div class="f1a-scard"><div class="f1a-shdr" style="background:#1B5162;"><div class="f1a-sdot"></div><div class="f1a-sname">App Authorization</div></div><div class="f1a-sbody"><ul><li>Auto-provisioned service principal per app</li><li>Developer grants SELECT/MODIFY on UC tables</li><li>App accesses resources as its own identity</li></ul></div></div>
# MAGIC     <div class="f1a-scard"><div class="f1a-shdr" style="background:#2272B4;"><div class="f1a-sdot"></div><div class="f1a-sname">User Authorization</div></div><div class="f1a-sbody"><ul><li>Individual users authenticate via SSO</li><li>UC permissions enforced per user</li><li>Row filters and column masks apply</li></ul></div></div>
# MAGIC     <div class="f1a-scard"><div class="f1a-shdr" style="background:#618794;"><div class="f1a-sdot"></div><div class="f1a-sname">Key Constraints</div></div><div class="f1a-sbody"><ul><li>Apps cannot create new resources</li><li>No in-memory state after restarts</li><li>Persist data via UC tables, Volumes, or Lakebase</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Three-Layer Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Control Plane:</strong> the Web App UI, Compute Manager, Unity Catalog, and OAuth/SSO authentication layer. This is the management surface where you create, configure, and deploy apps.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>App Resources (serverless):</strong> apps connect to existing platform services: SQL Warehouses, Model Serving, Vector Search, Genie Spaces, Lakebase, Lakeflow Jobs, Volumes, and Secrets. Apps cannot create new resources. Workspace administrators review and approve resource access during deployment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data Plane:</strong> the customer's cloud account (AWS, Azure, GCP) containing cloud storage, UC tables, and external APIs. Data stays in your environment. The app moves to the data, not the data to the app.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Databricks Apps is like Heroku for the lakehouse: you write your Streamlit/Dash/Flask app, push it, and Databricks handles the infrastructure, security, and scaling.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Dual Authorization Model</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>App authorization (service principal):</strong> Databricks automatically provisions a service principal for each app. Developers grant the service principal SELECT/MODIFY privileges on UC tables. The app accesses backend resources as its own identity, not as any individual user.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>User authorization (SSO):</strong> individual app users authenticate via OAuth 2.0 / SSO. Their UC permissions are enforced per user, including row filters and column masks. This dual model provides both consistent backend access and per-user data governance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Isolation:</strong> each app receives dedicated compute resources, network segmentation, encryption at rest and in transit, and least-privilege access. Compliance security profile is supported across all regions.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Key Constraints to Teach</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Cannot create resources:</strong> apps connect to existing SQL Warehouses, model serving endpoints, Genie Spaces, and Lakebase instances. If a resource does not exist, you create it separately, then add it to the app's configuration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>No in-memory state:</strong> apps do not preserve in-memory state after restarts. Developers must persist data explicitly using UC tables, workspace files, Unity Catalog Volumes, or Lakebase instances.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Horizontal scaling (Beta May 2026):</strong> apps can now run across multiple instances behind a single URL with session affinity and zero-downtime deployments.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/coop" style="color: #2574B5; font-size: 14pt;">Coop</a> built "AskCap," a Genie-powered decision tool as a branded internal application that turns business questions into data-driven decisions. &#x25C6;</li>
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
# MAGIC ### F2. Configuration, Deployment, and Resources
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Apps declare dependencies using two files: <code style="font-size:14pt;">app.yaml</code> (startup command, environment variables, required resources) and <code style="font-size:14pt;">requirements.txt</code> or <code style="font-size:14pt;">package.json</code> (package dependencies). This separation of declaration from configuration allows the same app code to work across development and production environments with different resource instances.</p>
# MAGIC
# MAGIC <!-- ── Visual: f2-config-deploy ── -->
# MAGIC <style>
# MAGIC .f2d-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .f2d-cols { display: flex; gap: 14px; align-items: stretch; margin-bottom: 18px; }
# MAGIC .f2d-card { flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 6px rgba(27,49,57,0.08); transition: transform 0.15s; cursor: default; display: flex; flex-direction: column; }
# MAGIC .f2d-card:hover { transform: translateY(-2px); box-shadow: 0 5px 12px rgba(27,49,57,0.13); }
# MAGIC .f2d-chdr { padding: 12px 14px; display: flex; align-items: center; gap: 8px; }
# MAGIC .f2d-cdot { width: 8px; height: 8px; border-radius: 50%; background: #fff; flex-shrink: 0; }
# MAGIC .f2d-cname { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .f2d-cbody { background: #fff; padding: 12px 14px; flex: 1; }
# MAGIC .f2d-cbody ul { margin: 0; padding: 0 0 0 16px; }
# MAGIC .f2d-cbody li { font-size: 14pt; color: #444; line-height: 1.55; margin-bottom: 3px; }
# MAGIC .f2d-code { background: #1B3139; border-radius: 10px; padding: 16px 18px; font-family: monospace; font-size: 14pt; color: #DCE0E2; line-height: 1.6; margin-bottom: 18px; }
# MAGIC .f2d-code-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; color: #618794; margin-bottom: 6px; }
# MAGIC .f2d-code-key { color: #E5A100; }
# MAGIC .f2d-code-val { color: #00A972; }
# MAGIC </style>
# MAGIC <div class="f2d-wrap">
# MAGIC   <div class="f2d-code-label">app.yaml Example</div>
# MAGIC   <div class="f2d-code"><span class="f2d-code-key">command:</span> <span class="f2d-code-val">['streamlit', 'run', 'app.py']</span><br/><span class="f2d-code-key">resources:</span><br/>  - <span class="f2d-code-key">name:</span> <span class="f2d-code-val">my-sql-warehouse</span><br/>    <span class="f2d-code-key">sql_warehouse:</span> <span class="f2d-code-val">{warehouse_id}</span><br/>  - <span class="f2d-code-key">name:</span> <span class="f2d-code-val">my-lakebase</span><br/>    <span class="f2d-code-key">database:</span> <span class="f2d-code-val">{database_name}</span></div>
# MAGIC   <div class="f2d-cols">
# MAGIC     <div class="f2d-card"><div class="f2d-chdr" style="background:#1B5162;"><div class="f2d-cdot"></div><div class="f2d-cname">Deployment</div></div><div class="f2d-cbody"><ul><li>Serverless compute, no cluster management</li><li>Billed per hour based on provisioned capacity</li><li>Git and CI/CD integration, deploys in seconds</li><li>Horizontal scaling (Beta): multi-instance with session affinity</li></ul></div></div>
# MAGIC     <div class="f2d-card"><div class="f2d-chdr" style="background:#2272B4;"><div class="f2d-cdot"></div><div class="f2d-cname">Available Resources</div></div><div class="f2d-cbody"><ul><li>SQL Warehouses for data queries</li><li>Model Serving endpoints for ML inference</li><li>Vector Search for RAG applications</li><li>Genie Spaces for NL data exploration</li><li>Lakebase for managed Postgres backend</li><li>Volumes and Secrets</li></ul></div></div>
# MAGIC     <div class="f2d-card"><div class="f2d-chdr" style="background:#618794;"><div class="f2d-cdot"></div><div class="f2d-cname">Sharing and Access</div></div><div class="f2d-cbody"><ul><li>Unique URL per app, tied to workspace</li><li>Users must be in the same Databricks account</li><li>CAN_USE or CAN_MANAGE permissions</li><li>Compliance security profile supported</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Lakebase as an App Resource</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Managed Postgres backend:</strong> add a Lakebase project as an app resource. Databricks creates a Postgres role for the app's service principal and injects connection details as environment variables. The app connects without managing credentials or connection strings.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lakebase Autoscaling:</strong> new Lakebase instances are created as Autoscaling projects (since March 2026) with autoscaling compute, scale-to-zero, branching, and instant restore.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Use cases:</strong> agent state persistence (checkpointers), user session data, application configuration, write-back from interactive forms.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Common Student Question</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>"Why not deploy a Streamlit app on AWS/GCP/Azure directly?"</strong> Databricks Apps provides built-in UC governance, automatic service principal management, OAuth/SSO, zero data copy (app runs where the data lives), and no separate infrastructure to manage. You trade some flexibility for integrated security and governance.</li>
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
# MAGIC
# MAGIC ### F3. When to Use Dashboards vs. Genie vs. Apps
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The AI/BI ecosystem provides a spectrum of tools. Each step adds control and customization but increases implementation effort. The decision depends on the audience, the complexity of the interaction, and how much custom logic is needed.</p>
# MAGIC
# MAGIC <!-- ── Visual: f3-decision-grid ── -->
# MAGIC <style>
# MAGIC .f3d-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .f3d-table { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC .f3d-table thead th { background: #1B3139; color: #fff; padding: 14px 16px; font-size: 14pt; text-align: left; border: none; }
# MAGIC .f3d-table tbody tr { transition: background 0.2s; }
# MAGIC .f3d-table tbody tr:hover { background: #E8F0ED; }
# MAGIC .f3d-table tbody td { padding: 14px 16px; font-size: 14pt; border-bottom: 1px solid #EEEDE9; vertical-align: top; }
# MAGIC .f3d-table tbody tr:last-child td { border-bottom: none; }
# MAGIC .f3d-tool { font-weight: 700; color: #fff; padding: 4px 12px; border-radius: 6px; display: inline-block; font-size: 14pt; }
# MAGIC </style>
# MAGIC <div class="f3d-wrap">
# MAGIC <table class="f3d-table">
# MAGIC   <thead><tr><th style="width:20%;">Tool</th><th style="width:25%;">Use When</th><th style="width:25%;">Audience</th><th style="width:30%;">Example</th></tr></thead>
# MAGIC   <tbody>
# MAGIC     <tr><td><span class="f3d-tool" style="background:#1B5162;">Dashboards</span></td><td>Standard KPI views with filters, parameters, and cross-filtering</td><td>Executives, managers, analysts (broad)</td><td>Regional sales dashboard with date and region filters</td></tr>
# MAGIC     <tr><td><span class="f3d-tool" style="background:#2272B4;">Genie</span></td><td>Ad-hoc NL follow-up questions on well-structured data</td><td>Business users who cannot write SQL</td><td>"What was revenue for EMEA last quarter?"</td></tr>
# MAGIC     <tr><td><span class="f3d-tool" style="background:#618794;">App Frameworks</span></td><td>Custom viz, write-back, multi-step workflows, ML-powered interfaces</td><td>Domain teams needing interactivity beyond dashboards</td><td>Streamlit app for demand forecasting with input sliders</td></tr>
# MAGIC     <tr><td><span class="f3d-tool" style="background:#1B3139;">Custom Apps</span></td><td>Full-stack control combining multiple Databricks services</td><td>Engineering teams building production tools</td><td>RAG chatbot (React + Model Serving + Vector Search + Lakebase)</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:14px;margin-top:18px;">
# MAGIC   <div style="flex:1;background:#F9F7F4;border-radius:10px;padding:14px 16px;border-left:4px solid #1B5162;"><div style="font-size:14pt;font-weight:700;color:#1B3139;margin-bottom:4px;">Built by</div><div style="font-size:14pt;color:#444;">Data scientists, data engineers, software engineers. Any user with notebook access and Python skills can create and publish.</div></div>
# MAGIC   <div style="flex:1;background:#F9F7F4;border-radius:10px;padding:14px 16px;border-left:4px solid #00A972;"><div style="font-size:14pt;font-weight:700;color:#1B3139;margin-bottom:4px;">Used by</div><div style="font-size:14pt;color:#444;">Non-technical teams in sales, marketing, finance, operations, HR. Feedback drives model retraining and app improvements.</div></div>
# MAGIC   <div style="flex:1;background:#F9F7F4;border-radius:10px;padding:14px 16px;border-left:4px solid #E5A100;"><div style="font-size:14pt;font-weight:700;color:#1B3139;margin-bottom:4px;">Customer Example</div><div style="font-size:14pt;color:#444;">&#x25C6; <a href="https://www.databricks.com/customers/coop" style="color: #2574B5;">Coop</a> built "AskCap," a Genie-powered decision tool as a branded internal application. &#x25C6;</div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Summary
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks AI/BI provides a spectrum of tools for delivering data insights to every persona in an organization. <strong>AI/BI Dashboards</strong> combine data, queries, visualizations, and AI assistance into governed, interactive displays with browser-side filtering, parameterized queries, and a 24-hour shared cache. <strong>Genie Spaces</strong> add a natural language interface grounded in your enterprise data through Unity Catalog metadata, curated instructions, and user feedback. <strong>Genie Agent Mode</strong> extends this to complex, exploratory questions with multi-step reasoning. And <strong>Databricks Apps</strong> provides a governed serverless platform for custom applications when dashboards and chat are not enough.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Across the entire spectrum, Unity Catalog provides the governance foundation: access control, lineage, auditing, and semantic metadata that powers both human and AI understanding of the data.</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>AI/BI Overview (<a href="https://docs.databricks.com/aws/en/ai-bi/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-bi/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-bi/">GCP</a>): product overview, concepts, and release notes for AI/BI Dashboards and Genie</li>
# MAGIC         <li>Share a Dashboard (<a href="https://docs.databricks.com/aws/en/dashboards/share/share">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/dashboards/share/share">Azure</a> | <a href="https://docs.databricks.com/gcp/en/dashboards/share/share">GCP</a>): publishing, permission models, embedded credentials, and account-level user sharing</li>
# MAGIC         <li>What Is a Genie Space (<a href="https://docs.databricks.com/aws/en/genie/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/genie/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/genie/">GCP</a>): Genie architecture, response generation, trusted assets, and data security</li>
# MAGIC         <li>Curate an Effective Genie Space (<a href="https://docs.databricks.com/aws/en/genie/best-practices">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/genie/best-practices">Azure</a> | <a href="https://docs.databricks.com/gcp/en/genie/best-practices">GCP</a>): instruction hierarchy, testing, benchmarks, and monitoring</li>
# MAGIC         <li>Genie Spaces API (<a href="https://docs.databricks.com/aws/en/genie/conversation-api">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/genie/conversation-api">Azure</a> | <a href="https://docs.databricks.com/gcp/en/genie/conversation-api">GCP</a>): conversation endpoints, management APIs, and integration patterns</li>
# MAGIC         <li>Databricks Apps (<a href="https://docs.databricks.com/aws/en/dev-tools/databricks-apps/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/dev-tools/databricks-apps/">GCP</a>): overview, key concepts, frameworks, and deployment</li>
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
