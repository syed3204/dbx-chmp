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
# MAGIC # 1 Lecture - Databricks Data Intelligence Platform
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Most organizations rely on eight or more disconnected systems to manage their data and AI workloads. Each tool comes from a different vendor, stores data in a different format, and requires specialized skills to operate. When a company tries to add generative AI into this fragmented landscape, the complexity compounds: data governance becomes harder to enforce, privacy risks multiply, and the small pool of technical experts who can stitch it all together becomes the bottleneck. The result is slower time to value, higher costs, and insights that never reach the people who need them.
# MAGIC
# MAGIC Databricks built the Data Lakehouse to eliminate that fragmentation, and then layered Generative AI on top to create the Data Intelligence Platform. This lecture traces that evolution: the problems that motivated the architecture, how the lakehouse unifies storage and compute on open formats, the open-source projects that form the foundation, and the three pillars that make the platform accessible to every persona in an organization.
# MAGIC
# MAGIC This lecture covers 4 sections that build on each other:
# MAGIC
# MAGIC - **A. The Problem Space**: Why organizations struggle to integrate data and AI, and the three core problems Databricks was created to solve
# MAGIC - **B. The Data Lakehouse**: The architecture that unifies data lakes and data warehouses on an open, governed foundation
# MAGIC - **C. An Open Foundation**: The open-source projects and partner ecosystem that make the platform flexible and interoperable
# MAGIC - **D. The Data Intelligence Platform**: How combining the Lakehouse with Generative AI democratizes data and AI for every user
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Describe the three core problems organizations face integrating data and AI (silos, privacy/control, skilled staff dependency)
# MAGIC - Explain the Data Lakehouse architecture and how it unifies data lakes and data warehouses on an open foundation
# MAGIC - Identify the key open-source projects Databricks has created and how they form the platform's foundation
# MAGIC - Articulate what the Data Intelligence Platform is and how combining the Lakehouse with Generative AI democratizes data and AI
# MAGIC - Describe the three pillars of the DIP: Accessible (natural language), Simple (AI-driven operations), and Private (governance and security)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The Problem Space

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Integration Challenge
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Most organizations need <strong>eight or more systems</strong> to handle their data and AI workloads: data warehouses for structured analytics, data lakes for raw storage, BI platforms for visualization, ETL tools for data movement, streaming engines for real-time processing, governance tools for compliance, data science platforms for ML, and now generative AI services. Each comes from a <strong>different vendor</strong> with its own format, security model, and skill requirements.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-component-grid ── -->
# MAGIC <style>
# MAGIC .a1-v-wrap { max-width: 920px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a1-v-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 14px; }
# MAGIC .a1-v-card { background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a1-v-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .a1-v-accent { height: 6px; flex-shrink: 0; }
# MAGIC .a1-v-body { padding: 16px 14px 14px; flex: 1; display: flex; flex-direction: column; align-items: center; text-align: center; gap: 6px; }
# MAGIC .a1-v-icon { font-size: 26px; line-height: 1; }
# MAGIC .a1-v-label { font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .a1-v-pill { display: inline-block; font-size: 8pt; font-weight: 700; letter-spacing: 0.8px; text-transform: uppercase; padding: 2px 9px; border-radius: 20px; background: #e8f0fe; color: #2b6cb0; margin-top: 4px; }
# MAGIC .a1-v-center { grid-column: 1 / -1; background: #1B3139; border-radius: 14px; padding: 22px 24px; text-align: center; box-shadow: 0 4px 16px rgba(27,49,57,0.22); position: relative; overflow: hidden; }
# MAGIC .a1-v-center::before { content: ''; position: absolute; top: -40px; right: -40px; width: 120px; height: 120px; background: rgba(255,255,255,0.04); border-radius: 50%; }
# MAGIC .a1-v-center-t { font-size: 16pt; font-weight: 700; color: #fff; margin-bottom: 4px; }
# MAGIC .a1-v-center-s { font-size: 14pt; color: #a8c9d6; }
# MAGIC @keyframes a1-v-pulse { 0%,100% { opacity: 0.6; } 50% { opacity: 1; } }
# MAGIC .a1-v-pulse-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #FF3621; margin-right: 8px; animation: a1-v-pulse 2s ease-in-out infinite; vertical-align: middle; }
# MAGIC </style>
# MAGIC <div class="a1-v-wrap">
# MAGIC   <div class="a1-v-grid">
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#4299E0;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#4299E0;margin:0 auto 6px;"></div><div class="a1-v-label">Data Lake</div><div class="a1-v-pill">Storage</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#1B5162;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#1B5162;margin:0 auto 6px;"></div><div class="a1-v-label">Data Warehouse</div><div class="a1-v-pill">Analytics</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#00A972;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#00A972;margin:0 auto 6px;"></div><div class="a1-v-label">BI Platform</div><div class="a1-v-pill">Visualization</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#FFAB00;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#FFAB00;margin:0 auto 6px;"></div><div class="a1-v-label">ETL &amp; Orchestration</div><div class="a1-v-pill">Pipelines</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#618794;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#618794;margin:0 auto 6px;"></div><div class="a1-v-label">Streaming</div><div class="a1-v-pill">Real-time</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#2272B4;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#2272B4;margin:0 auto 6px;"></div><div class="a1-v-label">Governance</div><div class="a1-v-pill">Compliance</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#98102A;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#98102A;margin:0 auto 6px;"></div><div class="a1-v-label">Machine Learning</div><div class="a1-v-pill">Models</div></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#9C27B0;"></div><div class="a1-v-body"><div class="a1-v-dot" style="width:12px;height:12px;border-radius:50%;background:#9C27B0;margin:0 auto 6px;"></div><div class="a1-v-label">Generative AI</div><div class="a1-v-pill">GenAI</div></div></div>
# MAGIC   </div>
# MAGIC   <div class="a1-v-center">
# MAGIC     <div class="a1-v-center-t"><span class="a1-v-pulse-dot"></span>8+ Disconnected Systems</div>
# MAGIC     <div class="a1-v-center-s">Different vendors, formats, security models, and skill requirements</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Fragmented Data Landscape</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Eight capability areas:</strong> Data Warehouses, Data Lakes, Business Intelligence, ETL and Orchestration, Streaming, Governance, Data Science and ML, and Generative AI. Each domain evolved independently, with specialized vendors building tools that solve one piece of the puzzle.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Vendor sprawl creates friction:</strong> purchasing and stitching together tools from different vendors leads to integration challenges, data silos, and inefficiencies. Think of it like building a house where the plumber, electrician, and carpenter each speak a different language: every handoff introduces risk and delay.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data duplication compounds the problem:</strong> when each tool has its own copy of the data, those copies drift out of sync. Business processes that rely on different copies produce conflicting results, eroding trust in the data itself.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Generative AI Makes It Worse</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>New opportunities, new complexity:</strong> Generative AI has opened new possibilities for every organization, but it also introduces new data flows, new privacy concerns, and new infrastructure requirements on top of the existing stack.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI needs all the data:</strong> effective AI models require access to data across the entire organization, not just the data trapped in one silo. A recommendation engine needs customer data, product data, transaction data, and behavioral data, often from different systems.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Governance at the seams:</strong> when data moves between systems (lake to warehouse to ML platform to AI service), governance becomes difficult to enforce consistently. Each handoff is a potential compliance gap.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Cost of Fragmentation</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Slower time to value:</strong> integrating multiple tools adds weeks or months to every analytics and AI project. Teams spend more time on infrastructure plumbing than on delivering business insight.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Higher total cost:</strong> each vendor charges separately for compute, storage, licensing, and support. A unified platform reduces that to a single contract and a single billing model.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/casey/lakehouse" style="color: #2574B5; font-size: 14pt;">Casey's General Stores</a> experienced these challenges firsthand: their legacy Azure Synapse environment could not keep pace with performance needs, so they migrated their SQL analytics directly onto their existing Databricks medallion architecture, eliminating redundant data copies and completing the migration in less than half the expected time. &#x25C6;</li>
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
# MAGIC ### A2. Three Core Problems
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The fragmented landscape creates three problems that drove the founding of Databricks. Organizations <strong>silo data, AI, and governance</strong> across systems with different security models. <strong>Data privacy and control are challenged by AI</strong> as models risk exposing sensitive information. And organizations remain <strong>dependent on highly technical staff</strong> to operate these systems, creating bottlenecks that slow innovation.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-three-pane-cards ── -->
# MAGIC <style>
# MAGIC .a2-v-wrap { max-width: 920px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a2-v-row { display: flex; gap: 16px; }
# MAGIC .a2-v-card { flex: 1; background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; position: relative; }
# MAGIC .a2-v-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .a2-v-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 7px; }
# MAGIC .a2-v-c1::before { background: #E24B4A; }
# MAGIC .a2-v-c2::before { background: #FFAB00; }
# MAGIC .a2-v-c3::before { background: #618794; }
# MAGIC .a2-v-head { padding: 24px 20px 10px; display: flex; align-items: center; gap: 10px; }
# MAGIC .a2-v-num { width: 36px; height: 36px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 16pt; font-weight: 800; color: #fff; flex-shrink: 0; }
# MAGIC .a2-v-n1 { background: #E24B4A; }
# MAGIC .a2-v-n2 { background: #FFAB00; }
# MAGIC .a2-v-n3 { background: #618794; }
# MAGIC .a2-v-title { font-size: 15pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .a2-v-body { padding: 0 20px 20px; flex: 1; }
# MAGIC .a2-v-desc { font-size: 14pt; color: #4a5568; line-height: 1.6; }
# MAGIC .a2-v-foot { padding: 10px 20px; background: #f8f6f3; border-top: 1px solid #EEEDE9; }
# MAGIC .a2-v-impact { font-size: 10pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; }
# MAGIC .a2-v-i1 { color: #E24B4A; }
# MAGIC .a2-v-i2 { color: #e65100; }
# MAGIC .a2-v-i3 { color: #618794; }
# MAGIC .a2-v-conn { display: flex; justify-content: center; align-items: center; padding: 8px 0; }
# MAGIC .a2-v-arrow-line { height: 2px; flex: 1; background: repeating-linear-gradient(90deg, #94b3be 0px, #94b3be 6px, transparent 6px, transparent 12px); }
# MAGIC .a2-v-arrow-tip { color: #1B5162; font-size: 14pt; padding: 0 4px; }
# MAGIC .a2-v-result { background: #1B3139; border-radius: 12px; padding: 16px 24px; text-align: center; margin-top: 4px; }
# MAGIC .a2-v-result-t { font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .a2-v-result-s { font-size: 14pt; color: #a8c9d6; margin-top: 2px; }
# MAGIC </style>
# MAGIC <div class="a2-v-wrap">
# MAGIC   <div class="a2-v-row">
# MAGIC     <div class="a2-v-card a2-v-c1"><div class="a2-v-head"><div class="a2-v-num a2-v-n1">1</div><div class="a2-v-title">Data and AI Are Siloed</div></div><div class="a2-v-body"><div class="a2-v-desc">Fragmented across data lakes, warehouses, ML systems, and BI platforms with different security models</div></div><div class="a2-v-foot"><div class="a2-v-impact a2-v-i1">Impact: Duplicated data, conflicting results</div></div></div>
# MAGIC     <div class="a2-v-card a2-v-c2"><div class="a2-v-head"><div class="a2-v-num a2-v-n2">2</div><div class="a2-v-title">Privacy Challenged by AI</div></div><div class="a2-v-body"><div class="a2-v-desc">Generative AI introduces new data privacy risks under GDPR, CCPA, and model exposure concerns</div></div><div class="a2-v-foot"><div class="a2-v-impact a2-v-i2">Impact: Compliance gaps, IP exposure</div></div></div>
# MAGIC     <div class="a2-v-card a2-v-c3"><div class="a2-v-head"><div class="a2-v-num a2-v-n3">3</div><div class="a2-v-title">Dependent on Technical Staff</div></div><div class="a2-v-body"><div class="a2-v-desc">Shortage of skilled professionals creates bottlenecks that slow innovation across the organization</div></div><div class="a2-v-foot"><div class="a2-v-impact a2-v-i3">Impact: Weeks of delay per request</div></div></div>
# MAGIC   </div>
# MAGIC   <div class="a2-v-conn"><div class="a2-v-arrow-line"></div><div class="a2-v-arrow-tip">&#x25BC;</div><div class="a2-v-arrow-line"></div></div>
# MAGIC   <div class="a2-v-result"><div class="a2-v-result-t">Why Databricks Was Founded</div><div class="a2-v-result-s">Unify data and AI on one open, governed platform</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Problem 1: Data, AI, and Governance Are Siloed</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Fragmented data silos</strong> span data lakes, data warehouses, ML systems, and BI platforms. Each silo requires a different security and governance approach, making consistent policy enforcement nearly impossible.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Redundant effort:</strong> teams in different business units often build the same pipeline or create the same report independently because they cannot discover or trust what already exists in another silo.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/trackunit" style="color: #2574B5; font-size: 14pt;">Trackunit</a> had Finance, Engineering, and RevOps each operating separate "lakehouses" for their 6 million connected machines. Unifying on a single Databricks Lakehouse eliminated these silos and enabled their transition from backward-looking reporting to predictive AI. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Problem 2: Data Privacy and Control Challenged by AI</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Generative AI introduces new privacy risks:</strong> models trained on or accessing organizational data may expose sensitive information. Regulations like GDPR and CCPA require organizations to prove they control how data flows into and out of AI systems.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data control is not optional:</strong> organizations need to build custom models on their own private data while retaining full ownership of both the data and the intellectual property the models produce. Sending data to third-party AI services creates compliance and security exposure.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>The governance gap:</strong> traditional governance tools were designed for structured data in warehouses. They were not built to track how unstructured data, embeddings, and model weights flow through generative AI pipelines.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Problem 3: Dependence on Highly Technical Staff</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Shortage of skilled professionals:</strong> there are not enough data engineers, ML engineers, and platform administrators to manage the complexity these systems create. Building and maintaining data pipelines, creating AI-driven data products, and tuning infrastructure all require deep technical expertise.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Innovation bottleneck:</strong> when only a few people in the organization can access and operate the data tools, every request flows through the same small team. Business stakeholders wait days or weeks for answers that should take minutes.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What This Means for You as a Partner SA</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>These are discovery questions:</strong> when you meet with a customer, ask which of these three problems they feel most acutely. The answer tells you where to focus the architecture conversation and which Databricks capabilities to highlight first.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>The unified platform is the remedy:</strong> the rest of this lecture shows how Databricks, through the Lakehouse architecture and the Data Intelligence Platform, addresses all three problems with a single, open, AI-powered platform.</li>
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
# MAGIC ## B. The Data Lakehouse
# MAGIC
# MAGIC Section A established the three core problems created by fragmented data systems. The Data Lakehouse is the architectural response: a single platform that combines the best of data lakes and data warehouses on an open, governed foundation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Architecture and Core Features
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Databricks platform organizes into three tiers. At the foundation, open formats (Delta Lake, Iceberg, Postgres) and unified governance through Unity Catalog ensure data is accessible, secure, and portable. The middle tier provides the engines: Lakehouse for data warehousing, Lakebase for serverless Postgres, and Lakeflow for ingestion, ETL, and streaming. An AI layer powered by Genie adds enterprise context across all workloads. At the top, applications like Agent Bricks, AI/BI, and Custom Apps deliver value directly to business users.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-arch-platform-diagram ── -->
# MAGIC <style>
# MAGIC .b1-arch-wrap { max-width: 920px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b1-arch-lbl { font-size: 11pt; font-weight: 600; color: #5A6F77; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; padding-left: 4px; }
# MAGIC .b1-arch-app-row { display: flex; gap: 10px; margin-bottom: 14px; }
# MAGIC .b1-arch-app-card { flex: 1; background: #F4F3F0; border: 1.5px solid #DCE0E2; border-radius: 8px; padding: 16px 12px; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 4px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1-arch-app-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(27,49,57,0.14); }
# MAGIC .b1-arch-app-title { font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .b1-arch-app-sub { font-size: 14pt; color: #5A6F77; line-height: 1.3; }
# MAGIC .b1-arch-ai-bar { display: flex; align-items: center; border: 2px solid #1B5162; border-radius: 8px; padding: 16px 20px; margin-bottom: 14px; background: rgba(27,81,98,0.04); transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1-arch-ai-bar:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(27,81,98,0.16); }
# MAGIC .b1-arch-ai-main { flex: 1; font-size: 14pt; font-weight: 700; color: #1B5162; }
# MAGIC .b1-arch-ai-right { font-size: 14pt; font-weight: 600; color: #1B5162; padding-left: 16px; border-left: 2px solid #1B5162; margin-left: 16px; }
# MAGIC .b1-arch-engine-wrap { border: 2px solid #E8453C; border-radius: 10px; padding: 14px; margin-bottom: 0; }
# MAGIC .b1-arch-engine-row { display: flex; gap: 10px; margin-bottom: 10px; }
# MAGIC .b1-arch-engine-card { flex: 1; background: #fff; border: 1.5px solid #E8453C; border-radius: 8px; padding: 14px 12px; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 4px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1-arch-engine-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(232,69,60,0.14); }
# MAGIC .b1-arch-engine-title { font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .b1-arch-engine-sub { font-size: 14pt; color: #5A6F77; line-height: 1.3; }
# MAGIC .b1-arch-gov-bar { display: flex; align-items: center; border: 1.5px solid #E8453C; border-radius: 8px; padding: 14px 20px; margin-bottom: 10px; background: #fff; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1-arch-gov-bar:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(232,69,60,0.12); }
# MAGIC .b1-arch-gov-main { flex: 1; font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .b1-arch-gov-right { font-size: 14pt; font-weight: 600; color: #1B3139; padding-left: 16px; border-left: 2px solid #E8453C; margin-left: 16px; }
# MAGIC .b1-arch-fmt-bar { display: flex; align-items: center; border: 1.5px solid #E8453C; border-radius: 8px; padding: 14px 20px; background: #fff; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b1-arch-fmt-bar:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(232,69,60,0.12); }
# MAGIC .b1-arch-fmt-main { flex: 1; font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .b1-arch-fmt-right { display: flex; gap: 12px; align-items: center; padding-left: 16px; border-left: 2px solid #E8453C; margin-left: 16px; }
# MAGIC .b1-arch-fmt-tag { font-size: 14pt; font-weight: 600; color: #5A6F77; background: #F4F3F0; border-radius: 4px; padding: 4px 10px; }
# MAGIC .b1-arch-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; vertical-align: middle; }
# MAGIC </style>
# MAGIC <div class="b1-arch-wrap">
# MAGIC   <div class="b1-arch-lbl">Applications</div>
# MAGIC   <div class="b1-arch-app-row">
# MAGIC     <div class="b1-arch-app-card"><div class="b1-arch-app-title">Agent Bricks</div><div class="b1-arch-app-sub">Production AI agents</div></div>
# MAGIC     <div class="b1-arch-app-card"><div class="b1-arch-app-title">AI/BI</div><div class="b1-arch-app-sub">Agentic business intelligence</div></div>
# MAGIC     <div class="b1-arch-app-card"><div class="b1-arch-app-title">Custom Apps</div><div class="b1-arch-app-sub">Secure data and AI apps</div></div>
# MAGIC     <div class="b1-arch-app-card"><div class="b1-arch-app-title">And more...</div><div class="b1-arch-app-sub">APIs, SDKs, partner integrations</div></div>
# MAGIC   </div>
# MAGIC   <div class="b1-arch-lbl">AI Layer</div>
# MAGIC   <div class="b1-arch-ai-bar">
# MAGIC     <div class="b1-arch-ai-main"><span class="b1-arch-dot" style="background: #1B5162;"></span>AI with Enterprise Context</div>
# MAGIC     <div class="b1-arch-ai-right">Genie</div>
# MAGIC   </div>
# MAGIC   <div class="b1-arch-lbl">Data Platform</div>
# MAGIC   <div class="b1-arch-engine-wrap">
# MAGIC     <div class="b1-arch-engine-row">
# MAGIC       <div class="b1-arch-engine-card"><div class="b1-arch-engine-title">Lakehouse</div><div class="b1-arch-engine-sub">Data warehousing</div></div>
# MAGIC       <div class="b1-arch-engine-card"><div class="b1-arch-engine-title">Lakebase</div><div class="b1-arch-engine-sub">Serverless Postgres</div></div>
# MAGIC       <div class="b1-arch-engine-card"><div class="b1-arch-engine-title">Lakeflow</div><div class="b1-arch-engine-sub">Ingest, ETL, streaming</div></div>
# MAGIC     </div>
# MAGIC     <div class="b1-arch-gov-bar">
# MAGIC       <div class="b1-arch-gov-main"><span class="b1-arch-dot" style="background: #E8453C;"></span>Unified Governance</div>
# MAGIC       <div class="b1-arch-gov-right">Unity Catalog</div>
# MAGIC     </div>
# MAGIC     <div class="b1-arch-fmt-bar">
# MAGIC       <div class="b1-arch-fmt-main"><span class="b1-arch-dot" style="background: #E8453C;"></span>Open Formats</div>
# MAGIC       <div class="b1-arch-fmt-right"><span class="b1-arch-fmt-tag">Postgres</span><span class="b1-arch-fmt-tag">Delta Lake</span><span class="b1-arch-fmt-tag">Iceberg</span></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br/>
# MAGIC <details>
# MAGIC   <summary style="cursor: pointer; list-style: none; user-select: none;">
# MAGIC     <div style="border-left: 4px solid #1B5162; background: transparent; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC       <div style="display: flex; align-items: center; gap: 12px;">
# MAGIC         <span style="font-size: 20px; display: inline-block; transition: transform 0.2s ease;">&#x25B6;</span>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Expand for More Details</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </summary>
# MAGIC   <div style="border-left: 4px solid #1B5162; background: transparent; padding: 0 20px 16px 20px; border-radius: 0 0 4px 4px; margin: -16px 0 16px 0;">
# MAGIC     <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC       <span style="font-size: 20px; visibility: hidden;">&#x25B6;</span>
# MAGIC       <div>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Three-Tier Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Foundation tier:</strong> open formats eliminate vendor lock-in. Delta Lake provides ACID transactions over cloud storage. Apache Iceberg is supported natively via REST Catalog API (GA May 2026). Lakebase adds Postgres-compatible OLTP. Unity Catalog governs all assets with fine-grained access controls, lineage, and discovery.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Engine tier:</strong> Lakehouse handles analytical workloads through Databricks SQL powered by Photon. Lakebase provides serverless Postgres for transactional workloads (via Neon acquisition, 2025). Lakeflow covers the full data engineering lifecycle: Connect for managed ingestion, Pipelines for declarative ETL, and Jobs for orchestration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI tier:</strong> the Data Intelligence Engine (Genie) adds enterprise context by indexing Unity Catalog metadata, table semantics, and organizational terminology. This powers natural language interfaces across the platform.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Applications That Deliver Value</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Agent Bricks:</strong> build, deploy, and optimize production AI agents with minimal code (GA 2025, Supervisor Agent nesting GA May 2026).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI/BI:</strong> agentic business intelligence combining dashboards with Genie for natural language analytics (Agent Mode GA May 2026).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Custom Apps:</strong> Databricks Apps provide a governed deployment surface for Streamlit, Dash, Flask, FastAPI applications with built-in OAuth and Unity Catalog integration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>And more:</strong> the "And more" category reflects the platform's extensibility through APIs, SDKs, and partner integrations.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Key Differentiators for Partner Conversations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Open formats at every layer:</strong> no proprietary storage. Customer data stays in their cloud account in Delta Lake, Iceberg, or Postgres formats.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unified governance:</strong> Unity Catalog applies consistent access controls, auditing, and lineage across analytical (Lakehouse), transactional (Lakebase), and AI workloads (Agent Bricks) from a single control plane.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI is not bolted on:</strong> the Intelligence Engine is embedded throughout the platform, not a separate product. Genie adds context to SQL queries, notebooks, dashboards, and agent development.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What Changed from the Old Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Previous model:</strong> the old "layered stack" (Open Data Lake, Delta Lake, Unity Catalog, Workloads) was accurate but missed 2025 additions: Lakebase for OLTP, Agent Bricks for production agents, and the AI context layer.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Post-DAIS 2025:</strong> the new diagram reflects Lakehouse, Lakebase, and Lakeflow as three distinct engines, with AI and governance as cross-cutting layers.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Market context:</strong> 74% of global CIOs report having a lakehouse (MIT Technology Review, 2023), but the platform now extends well beyond the lakehouse into OLTP and agentic AI.</li>
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
# MAGIC ### B2. End-to-End Data Flow
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The lakehouse architecture organizes data into the <strong>medallion pattern</strong>: raw data lands in <strong>Bronze</strong>, cleansed and conformed data moves to <strong>Silver</strong>, and business-ready aggregates are served from <strong>Gold</strong>. This layered approach ensures that every consumer, from ML models to BI dashboards to data applications, works from governed, high-quality data.</p>
# MAGIC
# MAGIC <!-- ── Visual: b2-medallion-flow ── -->
# MAGIC <style>
# MAGIC .b2-v-wrap { max-width: 960px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b2-v-main { display: flex; align-items: stretch; gap: 0; }
# MAGIC .b2-v-col { display: flex; flex-direction: column; gap: 6px; justify-content: center; }
# MAGIC .b2-v-lbl { font-size: 10pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #5A6F77; text-align: center; margin-bottom: 4px; }
# MAGIC .b2-v-item { background: #fff; border: 1.5px solid #1B5162; border-radius: 8px; padding: 10px 14px; text-align: center; font-size: 14pt; color: #1B3139; font-weight: 600; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b2-v-item:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.12); }
# MAGIC .b2-v-in { width: 160px; min-width: 140px; }
# MAGIC .b2-v-out { width: 160px; min-width: 140px; }
# MAGIC .b2-v-center { flex: 1; display: flex; flex-direction: column; align-items: center; }
# MAGIC .b2-v-mrow { display: flex; align-items: center; width: 100%; }
# MAGIC .b2-v-tier { flex: 1; border-radius: 10px; padding: 20px 16px; text-align: center; display: flex; flex-direction: column; gap: 4px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; position: relative; overflow: hidden; }
# MAGIC .b2-v-tier:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.15); }
# MAGIC .b2-v-bronze { background: linear-gradient(135deg, #CD7F32 0%, #a86b28 100%); }
# MAGIC .b2-v-silver { background: linear-gradient(135deg, #90A5B1 0%, #7a929e 100%); }
# MAGIC .b2-v-gold { background: linear-gradient(135deg, #FFCC66 0%, #f0b93a 100%); }
# MAGIC .b2-v-tname { font-size: 16pt; font-weight: 800; color: #fff; text-shadow: 0 1px 3px rgba(0,0,0,0.2); }
# MAGIC .b2-v-gold .b2-v-tname { color: #1B3139; text-shadow: none; }
# MAGIC .b2-v-tsub { font-size: 12pt; color: rgba(255,255,255,0.9); font-weight: 500; }
# MAGIC .b2-v-gold .b2-v-tsub { color: #5A6F77; }
# MAGIC @keyframes b2-v-flow { 0% { background-position: 0 0; } 100% { background-position: 24px 0; } }
# MAGIC .b2-v-arrow { display: flex; align-items: center; justify-content: center; padding: 0 6px; padding-top: 28px; }
# MAGIC .b2-v-arrow-bar { width: 28px; height: 3px; background: repeating-linear-gradient(90deg, #1B5162 0px, #1B5162 4px, transparent 4px, transparent 8px); background-size: 8px 3px; animation: b2-v-flow 0.6s linear infinite; }
# MAGIC .b2-v-arrow-tip { width: 0; height: 0; border-top: 7px solid transparent; border-bottom: 7px solid transparent; border-left: 9px solid #1B5162; }
# MAGIC .b2-v-marrow { display: flex; align-items: center; padding: 0 6px; }
# MAGIC .b2-v-platform { background: #1B3139; border-radius: 10px; padding: 14px 20px; text-align: center; color: #DCE0E2; font-size: 14pt; font-weight: 600; letter-spacing: 0.03em; margin-top: 14px; }
# MAGIC </style>
# MAGIC <div class="b2-v-wrap">
# MAGIC   <div class="b2-v-main">
# MAGIC     <div class="b2-v-col b2-v-in"><div class="b2-v-lbl">Ingestion</div><div class="b2-v-item">Auto Loader</div><div class="b2-v-item">COPY INTO</div><div class="b2-v-item">Structured Streaming</div></div>
# MAGIC     <div class="b2-v-arrow"><div class="b2-v-arrow-bar"></div><div class="b2-v-arrow-tip"></div></div>
# MAGIC     <div class="b2-v-col b2-v-center"><div class="b2-v-lbl">Medallion Architecture</div>
# MAGIC       <div class="b2-v-mrow">
# MAGIC         <div class="b2-v-tier b2-v-bronze"><div class="b2-v-tname">Bronze</div><div class="b2-v-tsub">Raw Ingestion</div></div>
# MAGIC         <div class="b2-v-marrow"><div class="b2-v-arrow-bar"></div><div class="b2-v-arrow-tip"></div></div>
# MAGIC         <div class="b2-v-tier b2-v-silver"><div class="b2-v-tname">Silver</div><div class="b2-v-tsub">Cleansed</div></div>
# MAGIC         <div class="b2-v-marrow"><div class="b2-v-arrow-bar"></div><div class="b2-v-arrow-tip"></div></div>
# MAGIC         <div class="b2-v-tier b2-v-gold"><div class="b2-v-tname">Gold</div><div class="b2-v-tsub">Business-Ready</div></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="b2-v-arrow"><div class="b2-v-arrow-bar"></div><div class="b2-v-arrow-tip"></div></div>
# MAGIC     <div class="b2-v-col b2-v-out"><div class="b2-v-lbl">Consumers</div><div class="b2-v-item">ML Models</div><div class="b2-v-item">Databricks SQL</div><div class="b2-v-item">BI + Apps</div></div>
# MAGIC   </div>
# MAGIC   <div class="b2-v-platform">Platform: Unity Catalog &nbsp;|&nbsp; Lakeflow Jobs &nbsp;|&nbsp; Multi-Cloud (AWS, Azure, GCP)</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Medallion Architecture</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Bronze (Raw Ingestion):</strong> data arrives from batch sources, streaming feeds, and CDC pipelines using ingestion tools like Auto Loader, COPY INTO, and Structured Streaming. Bronze tables are append-only and preserve the raw source data exactly as received. Think of it like refining crude oil: bronze is the raw material.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Silver (Cleansed and Conformed):</strong> data is cleansed, deduplicated, validated, and conformed to consistent schemas. Silver tables represent the "single source of truth" that multiple downstream consumers can rely on.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Gold (Business-Ready):</strong> the platform aggregates data into business-level tables, feature tables for ML, and curated datasets for BI. Gold tables are optimized for query performance and serve specific business domains.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Consumers and Workloads</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Machine Learning:</strong> MLflow manages the lifecycle from development (Python, R, Spark) through versioning and managing features and models to batch and real-time deployment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data Warehousing:</strong> Databricks SQL provides SQL analytics directly on the lakehouse, powered by the Photon engine, with connectors to Power BI, Tableau, Looker, and other BI tools.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data Apps and Consumers:</strong> downstream applications, partner systems, and data consumers all access the same governed data through APIs, Delta Sharing, and Marketplace.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Platform Layer</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Governance:</strong> Unity Catalog provides centralized access control, auditing, and lineage tracking across all data assets in the lakehouse.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Orchestration:</strong> Lakeflow Jobs (Lakeflow Jobs) coordinates ETL pipelines, ML training runs, and data quality checks. Declarative Pipelines define the medallion flow with quality expectations built in.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Multi-cloud:</strong> the architecture runs on AWS, Azure, and GCP, with the same capabilities and APIs across all three cloud providers.</li>
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
# MAGIC ## C. An Open Foundation
# MAGIC
# MAGIC The lakehouse architecture from Section B is built entirely on open-source technologies. This section examines the four core projects Databricks has created and the partner ecosystem they enable.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Open-Source Projects
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks was founded in 2013 by seven researchers from UC Berkeley's AMPLab, the original creators of <strong>Apache Spark</strong>. Since then, the company has created and open-sourced <strong>Delta Lake</strong>, <strong>MLflow</strong>, <strong>Unity Catalog</strong>, and <strong>Delta Sharing</strong>. No proprietary data formats are used in the platform: this open foundation is what enables the 200+ partner integration ecosystem.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-timeline-vertical ── -->
# MAGIC <style>
# MAGIC .c1-v-wrap { max-width: 800px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c1-v-title { font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 24px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2; }
# MAGIC .c1-v-list { position: relative; padding-left: 44px; }
# MAGIC @keyframes c1-v-line-grow { 0% { height: 0; } 100% { height: calc(100% - 12px); } }
# MAGIC .c1-v-list::before { content: ''; position: absolute; left: 14px; top: 6px; bottom: 6px; width: 3px; background: linear-gradient(180deg, #1B5162, #00A972); border-radius: 2px; animation: c1-v-line-grow 1.2s ease-out forwards; }
# MAGIC .c1-v-item { position: relative; margin-bottom: 20px; padding: 14px 18px; background: #fff; border-radius: 10px; box-shadow: 0 2px 8px rgba(27,49,57,0.08); transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .c1-v-item:hover { transform: translateX(4px); box-shadow: 0 4px 14px rgba(27,49,57,0.14); }
# MAGIC .c1-v-item:last-child { margin-bottom: 0; }
# MAGIC .c1-v-dot { position: absolute; left: -36px; top: 16px; width: 16px; height: 16px; border-radius: 50%; background: #1B5162; border: 3px solid #fff; box-shadow: 0 0 0 2px #1B5162; transition: transform 0.2s; }
# MAGIC .c1-v-item:hover .c1-v-dot { transform: scale(1.3); }
# MAGIC .c1-v-head { display: flex; align-items: center; gap: 10px; margin-bottom: 4px; }
# MAGIC .c1-v-year { font-size: 10pt; font-weight: 700; color: #fff; background: #1B5162; padding: 2px 10px; border-radius: 12px; letter-spacing: 0.06em; }
# MAGIC .c1-v-event { font-size: 15pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .c1-v-desc { font-size: 14pt; color: #5A6F77; line-height: 1.5; }
# MAGIC </style>
# MAGIC <div class="c1-v-wrap">
# MAGIC   <div class="c1-v-title">Open-Source Timeline</div>
# MAGIC   <div class="c1-v-list">
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot"></div><div class="c1-v-head"><span class="c1-v-year">2013</span><span class="c1-v-event">Apache Spark</span></div><div class="c1-v-desc">Founded at UC Berkeley AMPLab. Databricks co-founded by Spark creators.</div></div>
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot"></div><div class="c1-v-head"><span class="c1-v-year">2018</span><span class="c1-v-event">MLflow</span></div><div class="c1-v-desc">Open-source ML lifecycle platform. Now 30M+ monthly downloads.</div></div>
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot"></div><div class="c1-v-head"><span class="c1-v-year">2019</span><span class="c1-v-event">Delta Lake</span></div><div class="c1-v-desc">Open-sourced ACID storage layer for data lakes at Spark+AI Summit.</div></div>
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot"></div><div class="c1-v-head"><span class="c1-v-year">2022</span><span class="c1-v-event">Delta Lake 2.0</span></div><div class="c1-v-desc">All features donated to Linux Foundation. 190+ contributors across 70+ orgs.</div></div>
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot"></div><div class="c1-v-head"><span class="c1-v-year">2024</span><span class="c1-v-event">Unity Catalog</span></div><div class="c1-v-desc">Open-sourced under Apache 2.0. Unified governance for data and AI.</div></div>
# MAGIC     <div class="c1-v-item"><div class="c1-v-dot" style="background:#1B5162; box-shadow: 0 0 0 2px #1B5162;"></div><div class="c1-v-head"><span class="c1-v-year" style="background:#1B5162;">2025</span><span class="c1-v-event">Declarative Pipelines</span></div><div class="c1-v-desc">Donated to Apache Spark. Declarative ETL becomes part of Spark itself.</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Four Core Projects</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Apache Spark (2013):</strong> the distributed compute engine for large-scale data processing. Databricks was founded by Spark's creators and continues as a primary contributor. Spark Connect, contributed by Databricks, enables Spark on virtually any device.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Delta Lake (2019):</strong> the open-source ACID storage layer that extends Parquet with a file-based transaction log. Donated to the Linux Foundation with all features open-sourced in Delta Lake 2.0 (2022). Over 190 contributors across 70+ organizations, with nearly two-thirds from outside Databricks. Think of Delta Lake's transaction log as a flight data recorder for your data lake: every change is recorded, and you can replay history at any time.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>MLflow (2018):</strong> the largest open-source AI engineering platform with 30+ million monthly downloads. Covers experiment tracking, model registry, model serving, and (as of MLflow 3.0) generative AI tracing. MLflow is to machine learning what Git is to code: it tracks experiments, parameters, metrics, and model versions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog (2024):</strong> open-sourced under the Apache 2.0 license, providing unified governance for data and AI assets. In 2025, Databricks also donated Declarative Pipelines to the Apache Spark project.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Open Source Matters</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Flexibility:</strong> you can integrate tools you already have today or new ones you adopt in the future. Open formats check or verify your data is never locked to a single vendor.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Interoperability:</strong> the platform works with a diverse ecosystem of partners. Delta Sharing provides an open protocol for secure data sharing across organizations regardless of computing platform.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Common question: "If these projects are open source, why do customers pay for Databricks?"</strong> The managed platform adds production-grade capabilities: the Photon query engine, serverless compute, enterprise security, Unity Catalog deep integration, and 24/7 support. The open-source projects are the foundation; the managed platform adds operational readiness and performance.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Open Source AI Models</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Beyond data infrastructure:</strong> Databricks has also contributed open-source AI models including Dolly, DBRX, and Mosaic MPT, extending the open-source commitment into the generative AI layer.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Strategy:</strong> Databricks' approach resembles Android: create the open-source foundation that the ecosystem runs on, then build premium managed services on top. This grows the overall market while positioning Databricks as the best way to run these technologies in production.</li>
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
# MAGIC ### C2. Ecosystem and Market Position
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The open foundation supports a <strong>200+ partner integration ecosystem</strong> spanning data partners, integration tools, orchestration, business intelligence, governance, and data science platforms. Databricks serves <strong>20,000+ organizations</strong> worldwide, including roughly 60% of the Fortune 500, with $5.4B+ in annual revenue run-rate growing 65%+ year over year.</p>
# MAGIC
# MAGIC <!-- ── Visual: c2-kpi-metrics ── -->
# MAGIC <style>
# MAGIC .c2-v-wrap { max-width: 960px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c2-v-title { font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 6px; }
# MAGIC .c2-v-sub { font-size: 14pt; color: #5A6F77; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2; }
# MAGIC .c2-v-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
# MAGIC .c2-v-card { background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.08); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; position: relative; }
# MAGIC .c2-v-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.14); }
# MAGIC .c2-v-accent { height: 5px; flex-shrink: 0; }
# MAGIC .c2-v-body { padding: 18px 16px 14px; flex: 1; display: flex; flex-direction: column; gap: 4px; }
# MAGIC .c2-v-val { font-size: 26pt; font-weight: 800; line-height: 1.1; }
# MAGIC .c2-v-lbl { font-size: 14pt; color: #5A6F77; font-weight: 600; line-height: 1.3; }
# MAGIC .c2-v-trend { font-size: 11pt; font-weight: 700; padding: 3px 10px; border-radius: 12px; display: inline-block; margin-top: 4px; }
# MAGIC .c2-v-up { background: #e6f9f0; color: #00A972; }
# MAGIC .c2-v-neutral { background: #f0f2f4; color: #618794; }
# MAGIC .c2-v-row-label { font-size: 14pt; font-weight: 700; color: #1B5162; margin: 18px 0 8px 2px; }
# MAGIC .c2-v-mcard { background: #fff; border-radius: 12px; overflow: hidden; border: 2px solid #DCE0E2; display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s; cursor: default; position: relative; }
# MAGIC .c2-v-mcard:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.14); border-color: #1B5162; }
# MAGIC .c2-v-mbody { padding: 16px 16px 14px; flex: 1; display: flex; flex-direction: column; gap: 4px; }
# MAGIC .c2-v-mval { font-size: 16pt; font-weight: 800; color: #1B5162; line-height: 1.2; }
# MAGIC .c2-v-mlbl { font-size: 14pt; color: #5A6F77; font-weight: 500; line-height: 1.3; }
# MAGIC </style>
# MAGIC <div class="c2-v-wrap">
# MAGIC   <div class="c2-v-title">Databricks at a Glance</div>
# MAGIC   <div class="c2-v-sub">Key Numbers</div>
# MAGIC   <div class="c2-v-grid">
# MAGIC     <div class="c2-v-card"><div class="c2-v-accent" style="background:#1B5162;"></div><div class="c2-v-body"><div class="c2-v-val" style="color:#1B5162;">20,000+</div><div class="c2-v-lbl">Global Customers</div><span class="c2-v-trend c2-v-up">&#x25B2; 65%+ YoY</span></div></div>
# MAGIC     <div class="c2-v-card"><div class="c2-v-accent" style="background:#00A972;"></div><div class="c2-v-body"><div class="c2-v-val" style="color:#00A972;">$5.4B+</div><div class="c2-v-lbl">Revenue Run-Rate</div><span class="c2-v-trend c2-v-up">&#x25B2; Growing</span></div></div>
# MAGIC     <div class="c2-v-card"><div class="c2-v-accent" style="background:#2272B4;"></div><div class="c2-v-body"><div class="c2-v-val" style="color:#2272B4;">60%+</div><div class="c2-v-lbl">Fortune 500</div><span class="c2-v-trend c2-v-neutral">&#x25A0; Steady</span></div></div>
# MAGIC     <div class="c2-v-card"><div class="c2-v-accent" style="background:#FFAB00;"></div><div class="c2-v-body"><div class="c2-v-val" style="color:#1B3139;">$134B</div><div class="c2-v-lbl">Valuation</div><span class="c2-v-trend c2-v-neutral">&#x25A0; Series L</span></div></div>
# MAGIC   </div>
# MAGIC   <div class="c2-v-row-label">Platform Momentum</div>
# MAGIC   <div class="c2-v-grid">
# MAGIC     <div class="c2-v-mcard"><div class="c2-v-mbody"><div class="c2-v-mval">+500% AI/BI</div><div class="c2-v-mlbl">User growth in past year</div></div></div>
# MAGIC     <div class="c2-v-mcard"><div class="c2-v-mbody"><div class="c2-v-mval">200+ Partners</div><div class="c2-v-mlbl">Integrations and ecosystem</div></div></div>
# MAGIC     <div class="c2-v-mcard"><div class="c2-v-mbody"><div class="c2-v-mval">Free Edition</div><div class="c2-v-mlbl">Full platform, free forever</div></div></div>
# MAGIC     <div class="c2-v-mcard"><div class="c2-v-mbody"><div class="c2-v-mval">Triple Leader</div><div class="c2-v-mlbl">Gartner + Forrester + IDC</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Partner Ecosystem</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Six integration categories:</strong> Data Partners (Bloomberg, Salesforce, S&amp;P Global), Data Integration (Airbyte, Fivetran, dbt), Orchestration (Astronomer, Prefect), Business Intelligence (Tableau, Power BI, Looker, Qlik), Data Governance and Security (Immuta, Privacera, Atlan), and Data Science/ML (Anthropic, Cohere, LangChain, Pinecone).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Brownfield advantage:</strong> open standards mean the platform integrates with tools and systems already in place. You do not have to rip and replace your existing BI tools or orchestration platform to adopt Databricks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Delta Sharing:</strong> the open protocol enables secure data sharing with any organization, regardless of what computing platform they use. This extends to Marketplace (sharing datasets, models, and notebooks) and Clean Rooms (privacy-protecting environments for joint analytics).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Databricks by the Numbers</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Scale:</strong> 20,000+ organizations worldwide, 60%+ of the Fortune 500, ~14,900 employees. 800+ customers at $1M+ annual revenue, 70+ at $10M+. (<a href="https://www.databricks.com/company/newsroom/press-releases/databricks-grows-65-yoy-surpasses-5-4-billion-revenue-run-rate" style="color: #2574B5;">Q4 FY2026 press release</a>)</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Revenue:</strong> $5.4B+ run-rate (Q4 FY2026), growing 65%+ year over year. AI products alone generate $1.4B in annualized revenue. AI/BI saw +500% user growth in the past year. Non-GAAP subscription gross margins exceed 80%.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Valuation:</strong> $134B (Series L, February 2026), among the most valuable private technology companies globally. (<a href="https://www.databricks.com/company/newsroom/press-releases/databricks-surpasses-4-8b-revenue-run-rate-growing-55-year-over-year" style="color: #2574B5;">Series L press release</a>)</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Free Edition:</strong> virtually all of Databricks, free forever. Backed by a $100M investment in training and education. This removes the barrier to customer trials: partners can demo the full platform without procurement friction.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Industry Recognition</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Gartner Leader:</strong> Cloud Database Management Systems (2024) and Data Science and Machine Learning Platforms (4 consecutive years, positioned highest for Ability to run).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Forrester Wave Leader:</strong> Data Lakehouses Q2 2024, with 5/5 scores in 19 of 24 criteria across 13 evaluated vendors.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>IDC Leader:</strong> Data Platform Software 2025 and Unified AI Governance Platforms 2025-2026.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Strategic Acquisitions</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>MosaicML ($1.4B, 2023):</strong> brought generative AI model training, fine-tuning, and inference capabilities. Powers Mosaic AI and models like DBRX and MPT.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Tabular ($1B+, 2024):</strong> the Apache Iceberg table format company, founded by the creators of Iceberg. The founders of both Delta Lake and Iceberg now work at Databricks. Enabled full built-in Iceberg support via REST Catalog API.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Neon (~$1B, 2025):</strong> serverless PostgreSQL technology that became Lakebase, bringing OLTP capabilities to the platform.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Arcion ($100M, 2023):</strong> data replication technology powering Lakeflow Connect's managed ingestion connectors.</li>
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
# MAGIC ## D. The Data Intelligence Platform
# MAGIC
# MAGIC Section C showed that the lakehouse is built on open-source technologies that power a broad partner ecosystem. Now we add the final layer: Generative AI transforms the Lakehouse into the Data Intelligence Platform, making data and AI accessible to every person in the organization.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. From Lakehouse to Intelligence Platform
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The <strong>Data Intelligence Platform</strong> is the equation: <strong>Data Lakehouse + Generative AI = Data Intelligence Platform</strong>. Through the 2023 acquisition of MosaicML, Databricks added a Data Intelligence Engine that indexes the unique semantics of each organization's data, then automatically optimizes performance and manages infrastructure. The relationship is bidirectional: data is democratized through AI (natural language access, automatic optimization), and AI is democratized through your data (enterprise context, governed training data, secure model deployment).</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-bidirectional ── -->
# MAGIC <style>
# MAGIC .d1-v-wrap { max-width: 920px; margin: 28px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .d1-v-top { display: flex; align-items: stretch; justify-content: center; gap: 0; position: relative; }
# MAGIC .d1-v-card { border-radius: 14px; padding: 26px 22px; flex: 1; max-width: 340px; display: flex; flex-direction: column; gap: 10px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; position: relative; overflow: hidden; }
# MAGIC .d1-v-card:hover { transform: translateY(-4px); box-shadow: 0 8px 24px rgba(27,49,57,0.16); }
# MAGIC .d1-v-light { background: #fff; border: 2px solid #DCE0E2; box-shadow: 0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .d1-v-t-l { font-size: 18pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .d1-v-s-l { font-size: 14pt; color: #5A6F77; line-height: 1.4; font-style: italic; }
# MAGIC .d1-v-bullets { list-style: none; padding: 0; margin: 6px 0 0 0; }
# MAGIC .d1-v-bullets li { font-size: 14pt; color: #1B3139; padding: 4px 0 4px 14px; position: relative; line-height: 1.4; }
# MAGIC .d1-v-bullets li::before { content: ''; position: absolute; left: 0; top: 12px; width: 6px; height: 6px; border-radius: 50%; }
# MAGIC .d1-v-bl li::before { background: #1B5162; }
# MAGIC .d1-v-br li::before { background: #9C27B0; }
# MAGIC .d1-v-arrows { display: flex; flex-direction: column; justify-content: center; align-items: center; min-width: 200px; gap: 16px; padding: 0 8px; }
# MAGIC .d1-v-arrow-row { display: flex; align-items: center; gap: 8px; }
# MAGIC .d1-v-arrow-line { height: 3px; width: 90px; position: relative; }
# MAGIC .d1-v-arrow-ltr .d1-v-arrow-line { background: linear-gradient(90deg, #1B5162, #9C27B0); }
# MAGIC .d1-v-arrow-rtl .d1-v-arrow-line { background: linear-gradient(90deg, #9C27B0, #1B5162); }
# MAGIC .d1-v-arrow-head-r { width: 0; height: 0; border-top: 8px solid transparent; border-bottom: 8px solid transparent; border-left: 12px solid #9C27B0; flex-shrink: 0; }
# MAGIC .d1-v-arrow-head-l { width: 0; height: 0; border-top: 8px solid transparent; border-bottom: 8px solid transparent; border-right: 12px solid #1B5162; flex-shrink: 0; }
# MAGIC .d1-v-arrow-label { font-size: 14pt; font-weight: 600; line-height: 1.3; max-width: 180px; text-align: center; }
# MAGIC .d1-v-arrow-ltr .d1-v-arrow-label { color: #1B5162; }
# MAGIC .d1-v-arrow-rtl .d1-v-arrow-label { color: #9C27B0; }
# MAGIC @keyframes d1-v-pulse-r { 0% { transform: translateX(0); opacity: 1; } 50% { transform: translateX(6px); opacity: 0.7; } 100% { transform: translateX(0); opacity: 1; } }
# MAGIC @keyframes d1-v-pulse-l { 0% { transform: translateX(0); opacity: 1; } 50% { transform: translateX(-6px); opacity: 0.7; } 100% { transform: translateX(0); opacity: 1; } }
# MAGIC .d1-v-arrow-ltr .d1-v-arrow-head-r { animation: d1-v-pulse-r 2.5s ease-in-out infinite; }
# MAGIC .d1-v-arrow-rtl .d1-v-arrow-head-l { animation: d1-v-pulse-l 2.5s ease-in-out infinite 0.4s; }
# MAGIC .d1-v-result { display: flex; justify-content: center; margin-top: 20px; }
# MAGIC .d1-v-rcard { background: linear-gradient(135deg, #1B5162 0%, #0d3a4a 100%); border: 2px solid #1B5162; border-radius: 14px; padding: 22px 36px; text-align: center; position: relative; overflow: hidden; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .d1-v-rcard:hover { transform: translateY(-3px); }
# MAGIC .d1-v-rcard::before { content: ''; position: absolute; top: -40px; right: -40px; width: 120px; height: 120px; background: rgba(255,255,255,0.04); border-radius: 50%; }
# MAGIC @keyframes d1-v-glow { 0%,100% { box-shadow: 0 4px 16px rgba(27,81,98,0.25); } 50% { box-shadow: 0 4px 28px rgba(27,81,98,0.45); } }
# MAGIC .d1-v-rcard { animation: d1-v-glow 3s ease-in-out infinite; }
# MAGIC .d1-v-rt { font-size: 18pt; font-weight: 700; color: #fff; line-height: 1.3; }
# MAGIC .d1-v-rs { font-size: 14pt; color: #a8c9d6; line-height: 1.4; margin-top: 4px; }
# MAGIC .d1-v-connector { display: flex; justify-content: center; margin-top: -2px; }
# MAGIC .d1-v-vline { width: 3px; height: 20px; background: linear-gradient(180deg, #DCE0E2, #1B5162); }
# MAGIC </style>
# MAGIC <div class="d1-v-wrap">
# MAGIC   <div class="d1-v-top">
# MAGIC     <div class="d1-v-card d1-v-light">
# MAGIC       <div class="d1-v-t-l">Your Data</div>
# MAGIC       <div class="d1-v-s-l">Governed, open, enterprise-wide</div>
# MAGIC       <ul class="d1-v-bullets d1-v-bl"><li>Delta Lake</li><li>Iceberg</li><li>Postgres (Lakebase)</li><li>Unity Catalog</li></ul>
# MAGIC     </div>
# MAGIC     <div class="d1-v-arrows">
# MAGIC       <div class="d1-v-arrow-row d1-v-arrow-ltr"><div class="d1-v-arrow-line"></div><div class="d1-v-arrow-head-r"></div></div>
# MAGIC       <div class="d1-v-arrow-label" style="color:#1B5162;">AI democratized through your data</div>
# MAGIC       <div style="height:8px;"></div>
# MAGIC       <div class="d1-v-arrow-label" style="color:#9C27B0;">Data democratized through AI</div>
# MAGIC       <div class="d1-v-arrow-row d1-v-arrow-rtl"><div class="d1-v-arrow-head-l"></div><div class="d1-v-arrow-line"></div></div>
# MAGIC     </div>
# MAGIC     <div class="d1-v-card d1-v-light">
# MAGIC       <div class="d1-v-t-l">AI</div>
# MAGIC       <div class="d1-v-s-l">Contextual, agentic, embedded</div>
# MAGIC       <ul class="d1-v-bullets d1-v-br"><li>Genie</li><li>Agent Bricks</li><li>Mosaic AI</li><li>MLflow</li></ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="d1-v-connector"><div class="d1-v-vline"></div></div>
# MAGIC   <div class="d1-v-result">
# MAGIC     <div class="d1-v-rcard">
# MAGIC       <div class="d1-v-rt">Data Intelligence Platform</div>
# MAGIC       <div class="d1-v-rs">Democratize data and AI across your entire organization</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Bidirectional Equation: Data + AI = DIP</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Not a rebrand:</strong> the Lakehouse is the architectural foundation (unified storage, compute, governance). The Data Intelligence Platform adds a generative AI layer on top. The relationship between the two sides is bidirectional, like a two-way street: data fuels AI, and AI unlocks data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data democratized through AI:</strong> natural language interfaces (Genie), agentic business intelligence (AI/BI), and AI-assisted coding let every persona query, explore, and act on data without writing SQL or Python. AI removes the technical barrier between people and their data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI democratized through your data:</strong> enterprise context from Unity Catalog metadata, table semantics, and organizational terminology makes AI accurate and trustworthy. Without governed, high-quality data, AI models produce generic or unreliable results. Your data is what makes AI work for your organization.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>MosaicML acquisition (2023):</strong> brought model training, fine-tuning, and inference at scale. This technology powers Mosaic AI, including models like DBRX and MPT, and the AI features embedded throughout the platform.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Your Data: The Left Side of the Equation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Delta Lake:</strong> the open ACID storage layer that provides time travel, schema enforcement, and transaction guarantees over cloud object storage.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Iceberg:</strong> natively supported via REST Catalog API (GA May 2026), enabling interoperability with the broader lakehouse ecosystem. The founders of both Delta Lake and Iceberg now work at Databricks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Postgres (Lakebase):</strong> serverless OLTP database (via Neon acquisition, 2025) that brings transactional workloads onto the same governed platform, managed by the same Unity Catalog policies.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog:</strong> the governance layer that indexes and secures all data assets. It provides the metadata, lineage, and access controls that make the AI side of the equation trustworthy.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">AI: The Right Side of the Equation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Genie:</strong> the Data Intelligence Engine that indexes the meaning of your data (table names, column relationships, business terminology) and powers natural language interfaces across the platform. Think of Genie as a translator who speaks both "business question" and "SQL query" fluently because it has read your entire data dictionary.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Agent Bricks:</strong> build, deploy, and optimize production AI agents with minimal code (GA 2025, Supervisor Agent nesting GA May 2026). Agents act on data autonomously within governed boundaries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Mosaic AI:</strong> model training, fine-tuning, and inference capabilities. Powers custom model development on your private data with full IP ownership.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>MLflow:</strong> the open-source ML lifecycle platform (30M+ monthly downloads) that tracks experiments, manages model versions, and supports generative AI tracing (MLflow 3.0).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Result: Data Intelligence Platform</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Collocating applications with data:</strong> the platform philosophy is to bring compute to the data rather than moving data to different tools. This minimizes data movement, reduces security exposure, and improves performance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Six personas served:</strong> Data Engineers, Data Scientists, ML Engineers, Business Analysts, App Developers, and Business Partners. Each gets a tailored experience on the same platform.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>DAIS 2025 expansions:</strong> Free Edition for full-platform access, Databricks One for simplified business user experience, DBSQL Serverless with 5x performance gains, Lakebase GA for OLTP workloads, and storage-optimized Vector Search at 7x lower cost.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/block" style="color: #2574B5; font-size: 14pt;">Block (Square)</a> uses the Data Intelligence Platform to unify data from payments, banking, and commerce into a single governed foundation, enabling AI-driven fraud detection models that improve in accuracy as they ingest more transaction data: a direct example of the bidirectional Data-AI relationship. &#x25C6;</li>
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
# MAGIC ### D2. Three Pillars of Democratization
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Data Intelligence Platform democratizes data and AI through three pillars. <strong>Accessible with Natural Language:</strong> everyone can discover insights without writing code. <strong>Simple to Operate:</strong> AI removes infrastructure complexity and heavy lifting. <strong>Strong Governance and Security:</strong> organizations can train and serve custom GenAI applications while retaining full ownership of their data and intellectual property.</p>
# MAGIC
# MAGIC <!-- ── Visual: d2-three-pane-cards ── -->
# MAGIC <style>
# MAGIC .d2-v-wrap { max-width: 920px; margin: 24px auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .d2-v-row { display: flex; gap: 16px; }
# MAGIC .d2-v-card { flex: 1; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(27,49,57,0.08); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .d2-v-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.14); }
# MAGIC .d2-v-top { padding: 22px 20px 14px; position: relative; }
# MAGIC .d2-v-top::after { content: ''; position: absolute; bottom: 0; left: 20px; right: 20px; height: 1px; background: #EEEDE9; }
# MAGIC .d2-v-icon { font-size: 28px; margin-bottom: 8px; display: block; }
# MAGIC .d2-v-pill { display: inline-block; font-size: 8pt; font-weight: 700; letter-spacing: 0.8px; text-transform: uppercase; padding: 2px 10px; border-radius: 20px; margin-bottom: 8px; }
# MAGIC .d2-v-p1 { background: #e3f2fd; color: #1565c0; }
# MAGIC .d2-v-p2 { background: #e8f5e9; color: #2e7d32; }
# MAGIC .d2-v-p3 { background: #fce4ec; color: #c62828; }
# MAGIC .d2-v-title { font-size: 15pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .d2-v-body { padding: 14px 20px 20px; flex: 1; }
# MAGIC .d2-v-desc { font-size: 14pt; color: #4a5568; line-height: 1.6; }
# MAGIC .d2-v-foot { padding: 12px 20px; background: #f8f6f3; border-top: 1px solid #EEEDE9; display: flex; align-items: center; gap: 6px; }
# MAGIC .d2-v-solves { font-size: 10pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; color: #618794; }
# MAGIC .d2-v-problem { font-size: 10pt; font-weight: 700; color: #1B3139; }
# MAGIC .d2-v-bar { height: 7px; }
# MAGIC </style>
# MAGIC <div class="d2-v-wrap">
# MAGIC   <div class="d2-v-row">
# MAGIC     <div class="d2-v-card"><div class="d2-v-bar" style="background:linear-gradient(90deg,#4299E0,#1B5162);"></div><div class="d2-v-top"><span class="d2-v-pill d2-v-p1">Pillar 1</span><div class="d2-v-title">Accessible with Natural Language</div></div><div class="d2-v-body"><div class="d2-v-desc">Anyone in the organization can ask questions of their data in plain English. Genie translates natural language into SQL queries, visualizations, and summaries, removing the dependency on technical specialists.</div></div><div class="d2-v-foot"><span class="d2-v-solves">Solves:</span><span class="d2-v-problem">Staff Dependency</span></div></div>
# MAGIC     <div class="d2-v-card"><div class="d2-v-bar" style="background:linear-gradient(90deg,#00A972,#1B5162);"></div><div class="d2-v-top"><span class="d2-v-pill d2-v-p2">Pillar 2</span><div class="d2-v-title">Simple to Operate</div></div><div class="d2-v-body"><div class="d2-v-desc">One platform replaces 8+ disconnected tools. Automatic infrastructure tuning, serverless compute, and declarative pipelines eliminate the need to stitch together separate systems.</div></div><div class="d2-v-foot"><span class="d2-v-solves">Solves:</span><span class="d2-v-problem">Data Silos</span></div></div>
# MAGIC     <div class="d2-v-card"><div class="d2-v-bar" style="background:linear-gradient(90deg,#E24B4A,#1B5162);"></div><div class="d2-v-top"><span class="d2-v-pill d2-v-p3">Pillar 3</span><div class="d2-v-title">Strong Governance and Security</div></div><div class="d2-v-body"><div class="d2-v-desc">Unity Catalog enforces consistent access controls, auditing, and lineage across all data and AI assets. Organizations retain full ownership of their data and intellectual property.</div></div><div class="d2-v-foot"><span class="d2-v-solves">Solves:</span><span class="d2-v-problem">Privacy + Control</span></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 1: Accessible with Natural Language</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>AI/BI Genie:</strong> a natural language interface that lets anyone ask questions of their data in plain English. Genie generates SQL, returns text summaries, tabular data, and visualizations with explanations. No SQL or Python skills required.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Genie Code:</strong> an AI coding assistant embedded in notebooks and the SQL editor. Data engineers and analysts use it to write, explain, and debug code through conversation rather than from scratch.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Databricks One:</strong> a simplified, code-free experience for business users launched at DAIS 2025. Dashboards, Genie Spaces, and Alerts are grouped in a single interface designed for non-technical users.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/pandora" style="color: #2574B5; font-size: 14pt;">Pandora</a> democratized data access so marketers with limited SQL capability can now perform ad hoc queries without waiting for the data team. Result: a <strong>50% increase in click-to-open rates</strong> and 65 million personalized emails per year across 8 markets. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 2: Simple to Operate</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>One platform, not eight:</strong> Lakehouse (warehousing), Lakeflow (ETL and orchestration), Lakebase (OLTP), and Mosaic AI (agents and ML) all run on a single platform. This eliminates the integration overhead of stitching together separate vendors for each workload.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Serverless by default:</strong> SQL Warehouses, notebooks, pipelines, and model serving all run on serverless compute. No cluster configuration, no capacity planning, no patching. You describe what you want; the platform handles how it runs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Automatic optimization:</strong> Predictive I/O, automatic data layout, intelligent workload management, and Predictive Optimization (VACUUM, OPTIMIZE, ANALYZE) reduce manual tuning to near zero.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/viessmann" style="color: #2574B5; font-size: 14pt;">Viessmann</a> adopted the platform as their central hub for IoT data from heating systems. Data teams moved from idea to production in record time, and remote service innovation reduced mandatory onsite visits by <strong>50%</strong>. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 3: Strong Governance and Security</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog governs everything:</strong> access controls, auditing, and lineage tracking apply consistently to tables, views, models, functions, agents, and dashboards. One governance model covers SQL analytics, Python pipelines, ML experiments, and GenAI agents.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data stays in the customer's account:</strong> unlike SaaS platforms that copy data into proprietary storage, Databricks stores data in the customer's own cloud storage (S3, ADLS, GCS) in open formats. The customer retains full ownership.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Compliance ready:</strong> the platform supports GDPR, CCPA, HIPAA, FedRAMP, and other regulatory frameworks through centralized governance policies. ABAC (GA April 2026) adds attribute-based row and column security at scale.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Connecting the Pillars to the Problems</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Staff dependency &#x279C; Accessible:</strong> natural language removes the SQL/Python barrier. Business users get answers directly instead of filing tickets with the data team.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data silos &#x279C; Simple:</strong> one platform replaces the patchwork of disconnected tools. Warehousing, ETL, OLTP, and AI run side by side with shared storage and governance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Privacy &#x279C; Private:</strong> unified governance ensures data privacy and control are maintained across all workloads. Data stays in the customer's cloud account in open formats.</li>
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
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture traced the path from fragmented data systems to a unified intelligence platform. You started with the three core problems that organizations face when their data and AI tools are siloed across vendors. You then saw how the Data Lakehouse eliminates that fragmentation by combining the best of data lakes and data warehouses on open formats, governed by Unity Catalog, and flowing through the medallion architecture. The open-source foundation (Spark, Delta Lake, MLflow, Unity Catalog) enables a 100+ partner ecosystem and avoids vendor lock-in. And by adding Generative AI on top of the Lakehouse, the Data Intelligence Platform makes data and AI accessible to every persona through natural language, simple through AI-driven automation, and private through unified governance.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Key takeaways from this lecture include:</p>
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>Three core problems drive the need:</strong> data/AI/governance silos, privacy challenges from AI, and dependence on scarce technical staff all slow organizations down.</li>
# MAGIC   <li><strong>The Lakehouse unifies storage and compute:</strong> ACID transactions, open formats, and a governance layer (Unity Catalog) replace the fragmented approach of separate data lakes and warehouses.</li>
# MAGIC   <li><strong>Open source is the foundation:</strong> Delta Lake, MLflow, Apache Spark, and Unity Catalog are all open source, enabling flexibility, interoperability, and a broad partner ecosystem.</li>
# MAGIC   <li><strong>Lakehouse + GenAI = Data Intelligence Platform:</strong> the Data Intelligence Engine adds semantic understanding and automatic optimization, transforming the Lakehouse into a platform that serves every persona.</li>
# MAGIC   <li><strong>Three pillars of democratization:</strong> Accessible (natural language), Simple (AI-driven operations), and Private (governance and security) map directly to the three core problems.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Next:</strong> The Activity (Customer Stories or Analyze and Map) gives you hands-on practice applying these concepts to real customer scenarios, reinforcing how Partner Solution Architects position the Databricks platform in customer conversations.</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333; font-size: 14pt;">
# MAGIC         <li style="font-size: 14pt;">What is Databricks? (<a href="https://docs.databricks.com/aws/en/introduction/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/introduction/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/introduction/" style="color: #2574B5;">GCP</a>): Platform overview covering all use cases, personas, and capabilities</li>
# MAGIC         <li style="font-size: 14pt;">What is a data lakehouse? (<a href="https://docs.databricks.com/aws/en/lakehouse" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse" style="color: #2574B5;">GCP</a>): Architecture definition, foundational technologies, and operational layers</li>
# MAGIC         <li style="font-size: 14pt;">Lakehouse architecture scope (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/scope" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/scope" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/scope" style="color: #2574B5;">GCP</a>): Six personas, eleven functional domains, and collaboration capabilities</li>
# MAGIC         <li style="font-size: 14pt;">Well-Architected Lakehouse Framework (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/well-architected" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/well-architected" style="color: #2574B5;">GCP</a>): Seven pillars for evaluating and optimizing lakehouse implementations</li>
# MAGIC         <li style="font-size: 14pt;"><a href="https://www.databricks.com/product/data-intelligence-platform" style="color: #2574B5;">Data Intelligence Platform product page</a>: Marketing overview of the DIP value proposition and capabilities</li>
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
