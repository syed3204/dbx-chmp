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
# MAGIC # 2 Lecture - Well-Architected Lakehouse
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Module 1 introduced the Data Intelligence Platform and the lakehouse architecture that underpins it. But building a lakehouse is not the same as building a _well-architected_ lakehouse. Without deliberate architectural decisions around governance, security, performance, cost, and reliability, organizations end up with a platform that works but does not scale, costs too much, or fails to meet compliance requirements.
# MAGIC
# MAGIC The Well-Architected Lakehouse Framework provides a structured approach to evaluating and improving lakehouse implementations. Adapted from cloud provider Well-Architected Frameworks (AWS, Azure, GCP), it adds two lakehouse-specific pillars and organizes best practices into a hierarchy that solution architects can use in every customer engagement. This lecture walks through the guiding principles, the seven pillars, the implementation methodology, and the reference architecture that ties it all together.
# MAGIC
# MAGIC This lecture covers 6 sections that build on each other:
# MAGIC
# MAGIC - **A. Guiding Principles**: Six foundational principles that shape every architectural decision in a well-architected lakehouse
# MAGIC - **B. The Seven Pillars**: The framework's seven pillars, distinguishing the five cloud-standard pillars from the two lakehouse-specific additions
# MAGIC - **C. Applying the Framework**: When and how to apply the WAF across the customer lifecycle (Gain, Grow, Retain)
# MAGIC - **D. Implementation Methodology**: The five-phase continuous improvement cycle: Assess, Prioritize, Remediate, Educate, Automate
# MAGIC - **E. Pillar Best Practices**: Headline best practices for each pillar, with the Principles and Best Practices documentation structure
# MAGIC - **F. Reference Architecture**: The Databricks Data Intelligence Platform reference architecture and use case overlays
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Identify and explain the 6 guiding principles for a well-architected lakehouse and how they shape platform design decisions
# MAGIC - Describe the 7 pillars of the Well-Architected Lakehouse Framework, distinguishing between the 5 cloud-standard pillars and the 2 lakehouse-specific pillars
# MAGIC - Explain how to apply the Well-Architected Framework across the customer lifecycle (Gain, Grow, Retain) with appropriate activities at each stage
# MAGIC - Walk through the 5-phase WAF implementation methodology (Assess, Prioritize, Remediate, Educate, Automate/Monitor/Iterate) and identify the key actions in each phase
# MAGIC - Map Databricks platform capabilities to the reference architecture dimensions across common use case patterns
# MAGIC - Given a customer scenario, assess pillar maturity and draft a prescriptive remediation plan with quick wins and strategic fixes

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Guiding Principles
# MAGIC
# MAGIC Before examining the framework's pillars and best practices, we start with the six guiding principles that define the vision for a well-architected lakehouse. These principles are the "why" behind every recommendation that follows.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Six Principles for Lakehouse Design
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Well-Architected Lakehouse begins with <strong>six guiding principles</strong> that influence every data, analytics, and AI architecture decision. These are not Databricks-specific features. They are organizational commitments that shape how teams build, govern, and consume data. Think of them as zoning laws for your data platform: they set the vision for what should be built and where, before any building codes (pillars) specify how to build it safely.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-guiding-principles-grid ── -->
# MAGIC <style>
# MAGIC .a1-v-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a1-v-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
# MAGIC .a1-v-card { background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.08); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a1-v-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .a1-v-accent { height: 7px; flex-shrink: 0; }
# MAGIC .a1-v-body { padding: 18px 16px; flex: 1; display: flex; flex-direction: column; gap: 6px; }
# MAGIC .a1-v-num { display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; border-radius: 50%; font-size: 14pt; font-weight: 800; color: #fff; margin-bottom: 4px; }
# MAGIC .a1-v-title { font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .a1-v-desc { font-size: 14pt; color: #5A6F77; line-height: 1.5; }
# MAGIC .a1-v-pill { display: inline-block; font-size: 14pt; font-weight: 700; letter-spacing: 0.7px; text-transform: uppercase; padding: 2px 8px; border-radius: 12px; margin-top: 6px; }
# MAGIC </style>
# MAGIC <div class="a1-v-wrap">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2;">Six Guiding Principles</div>
# MAGIC   <div class="a1-v-grid">
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#1B5162;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#1B5162;">1</div><div class="a1-v-title">Curate Data as Products</div><div class="a1-v-desc">Treat data like a product with clear definition, schema, lifecycle, and progressive quality gates</div><span class="a1-v-pill" style="background:#e8f0fe;color:#1565c0;">Data Quality</span></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#618794;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#618794;">2</div><div class="a1-v-title">Eliminate Data Silos</div><div class="a1-v-desc">Minimize data movement and copies; use sharing tools for secure access to current versions</div><span class="a1-v-pill" style="background:#fef3cd;color:#856404;">Sharing</span></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#00A972;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#00A972;">3</div><div class="a1-v-title">Democratize Value Creation</div><div class="a1-v-desc">Lower barriers for all business units through self-service access and discoverable datasets</div><span class="a1-v-pill" style="background:#e8f5e9;color:#2e7d32;">Self-Service</span></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#2272B4;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#2272B4;">4</div><div class="a1-v-title">Adopt Org-Wide Governance</div><div class="a1-v-desc">Actively manage data quality, cataloging, access control, and audit logging at enterprise scale</div><span class="a1-v-pill" style="background:#e3f2fd;color:#1565c0;">Governance</span></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#FFAB00;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#FFAB00;">5</div><div class="a1-v-title">Open Interfaces and Formats</div><div class="a1-v-desc">Prevent vendor lock-in with open standards (Delta Lake, Iceberg, open APIs) for portability</div><span class="a1-v-pill" style="background:#fff3e0;color:#e65100;">Open Standards</span></div></div>
# MAGIC     <div class="a1-v-card"><div class="a1-v-accent" style="background:#90A5B1;"></div><div class="a1-v-body"><div class="a1-v-num" style="background:#90A5B1;">6</div><div class="a1-v-title">Build to Scale</div><div class="a1-v-desc">Decouple storage from compute for on-demand scaling; optimize for both performance and cost</div><span class="a1-v-pill" style="background:#f0f2f4;color:#455a64;">Performance</span></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Curate Data and Offer Trusted Data-as-Products</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Treat data like a product</strong> with a clear definition, schema, and lifecycle. A layered architecture (ingest, curated, final) establishes quality standards across tiers, with validation occurring at each transition point.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> think of it like a publishing house. Raw manuscripts arrive (ingest), editors refine them (curated), and polished books reach readers (final). Each stage has quality gates, and the end consumer trusts the product because every step is deliberate.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/natura/unity-catalog" style="color: #2574B5; font-size: 14pt;">Natura</a> operationalized this principle by creating 178 data products across 19 domains, sourced from 170+ systems, with 1,100 monthly active users consuming curated datasets. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Eliminate Data Silos and Minimize Data Movement</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Copies become silos.</strong> Every operational copy of a dataset risks falling out of sync. The framework distinguishes between throwaway experimental copies (acceptable) and operational silos (problematic).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Enterprise sharing over duplication:</strong> tools like Delta Sharing and Lakehouse Federation enable secure access to current dataset versions rather than distributing outdated copies across teams.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/hipages" style="color: #2574B5; font-size: 14pt;">hipages</a> consolidated three generations of data warehouses (MySQL, Redshift, Athena) into a single lakehouse, eliminating two-day data transition delays and enabling self-service analytics. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Democratize, Govern, and Scale</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Democratize value creation:</strong> lower barriers for all business units through lean data management, discoverable datasets, and self-service access. Every user should be able to produce and consume data without duplicating infrastructure.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Adopt organization-wide governance:</strong> data is a critical asset requiring active management. Three governance dimensions are essential: data quality (contracts, SLAs), data catalog (discovery, lineage, metadata), and access control (fine-grained permissions, audit logging).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Open formats and interfaces:</strong> using open standards (Delta Lake, Iceberg, open APIs) prevents vendor lock-in, increases data portability, and enables ecosystem integration. The lakehouse should work with the tools customers already have.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Build to Scale and Optimize for Performance and Cost</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Decouple storage from compute:</strong> this fundamental architectural choice enables horizontal and vertical scaling with on-demand resource provisioning, optimizing the performance-to-cost ratio as data volumes grow.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/kaltura" style="color: #2574B5; font-size: 14pt;">Kaltura</a> reduced infrastructure costs by 20% after decoupling storage from compute, eliminating bottlenecks from their always-on cluster and reducing data latency for support engineers from hours to 5 minutes. &#x25C6;</li>
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
# MAGIC ## B. The Seven Pillars
# MAGIC
# MAGIC With the guiding principles in place, we now formalize them into the seven pillars of the Well-Architected Lakehouse Framework. Five of these pillars are shared with cloud provider WAFs, and two are unique to the lakehouse.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. From Cloud WAFs to the Lakehouse
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">AWS, Azure, and GCP each publish their own Well-Architected Framework with five common pillars: Operational Excellence, Security, Reliability, Performance Efficiency, and Cost Optimization. The <strong>Well-Architected Lakehouse</strong> adopts all five and adds two lakehouse-specific pillars: <strong>Data and AI Governance</strong> and <strong>Interoperability and Usability</strong>. This is not a rebranding of cloud principles. It is a purposeful adaptation to data and AI workloads, with Unity Catalog and open table formats (Delta Lake, Iceberg) forming the foundational layer.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-seven-pillars-diagram ── -->
# MAGIC <style>
# MAGIC .b1-v-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
# MAGIC .b1-v-heading { font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2; }
# MAGIC /* Overview bar: all 7 pillars */
# MAGIC .b1-v-overview { display: flex; gap: 4px; margin-bottom: 16px; }
# MAGIC .b1-v-ov-pill { flex: 1; padding: 10px 4px; border-radius: 6px; text-align: center; font-size: 14pt; font-weight: 700; color: #fff; line-height: 1.2; transition: transform 0.2s, box-shadow 0.2s, opacity 0.2s; opacity: 0.5; }
# MAGIC .b1-v-ov-pill.b1-v-ov-active { opacity: 1; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.2); }
# MAGIC .b1-v-ov-cl { background: #618794; }
# MAGIC .b1-v-ov-lh { background: #1B5162; }
# MAGIC .b1-v-ov-sep { width: 2px; background: #DCE0E2; border-radius: 1px; flex-shrink: 0; align-self: stretch; }
# MAGIC /* Tabs */
# MAGIC .b1-v-tabs { display: flex; gap: 0; }
# MAGIC .b1-v-tab { flex: 1; padding: 14px 10px; text-align: center; font-size: 14pt; font-weight: 700; cursor: pointer; border: 2px solid #DCE0E2; border-bottom: none; border-radius: 8px 8px 0 0; user-select: none; transition: background 0.2s, color 0.2s; }
# MAGIC .b1-v-tab-cl-on { background: #618794; color: #fff; border-color: #618794; }
# MAGIC .b1-v-tab-cl-off { background: #EEEDE9; color: #618794; }
# MAGIC .b1-v-tab-lh-on { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC .b1-v-tab-lh-off { background: #EEEDE9; color: #1B5162; }
# MAGIC .b1-v-tcount { font-size: 14pt; font-weight: 400; opacity: 0.85; }
# MAGIC /* Panels */
# MAGIC .b1-v-panel { border: 2px solid #DCE0E2; border-top: none; border-radius: 0 0 8px 8px; background: #fff; display: none; }
# MAGIC .b1-v-panel.b1-v-show { display: block; }
# MAGIC .b1-v-pcl.b1-v-show { border-color: #618794; }
# MAGIC .b1-v-plh.b1-v-show { border-color: #1B5162; }
# MAGIC /* Accordion rows */
# MAGIC .b1-v-arow { border-bottom: 1px solid #EEEDE9; }
# MAGIC .b1-v-arow:last-child { border-bottom: none; }
# MAGIC .b1-v-ahead { padding: 16px 20px; display: flex; align-items: center; gap: 12px; cursor: pointer; user-select: none; transition: background 0.15s; }
# MAGIC .b1-v-arow.b1-v-aopen .b1-v-ahead { background: #F0F4F6; }
# MAGIC .b1-v-ahead:active { background: #F9F7F4; }
# MAGIC .b1-v-adot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
# MAGIC .b1-v-aname { font-size: 14pt; font-weight: 700; color: #1B3139; flex: 1; }
# MAGIC .b1-v-achev { width: 10px; height: 10px; border-right: 2px solid #90A5B1; border-bottom: 2px solid #90A5B1; transform: rotate(-45deg); transition: transform 0.2s; flex-shrink: 0; }
# MAGIC .b1-v-arow.b1-v-aopen .b1-v-achev { transform: rotate(45deg); }
# MAGIC .b1-v-abody { max-height: 0; overflow: hidden; transition: max-height 0.3s ease, padding 0.3s ease; padding: 0 20px 0 42px; }
# MAGIC .b1-v-arow.b1-v-aopen .b1-v-abody { max-height: 300px; padding: 0 20px 16px 42px; }
# MAGIC .b1-v-adesc { font-size: 14pt; color: #333; line-height: 1.6; margin-bottom: 8px; }
# MAGIC .b1-v-acap { font-size: 14pt; color: #5A6F77; line-height: 1.5; }
# MAGIC .b1-v-acap strong { color: #1B3139; font-size: 14pt; }
# MAGIC /* Foundation */
# MAGIC .b1-v-found { margin-top: 16px; }
# MAGIC .b1-v-fbar { text-align: center; padding: 11px; border-radius: 6px; font-weight: 700; color: #fff; margin-bottom: 4px; font-size: 14pt; }
# MAGIC .b1-v-fbar-uc { background: #1B5162; }
# MAGIC .b1-v-fbar-fmt { background: #90A5B1; font-size: 14pt; border-radius: 0 0 6px 6px; }
# MAGIC </style>
# MAGIC <div class="b1-v-wrap">
# MAGIC   <div class="b1-v-heading">Well-Architected Lakehouse: Seven Pillars</div>
# MAGIC   <!-- Overview bar: all 7 pillars, highlights active -->
# MAGIC   <div class="b1-v-overview" id="b1-ov">
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-cl b1-v-ov-active" data-idx="0">Operational Excellence</div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-cl b1-v-ov-active" data-idx="1">Security, Privacy, Compliance</div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-cl b1-v-ov-active" data-idx="2">Reliability</div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-cl b1-v-ov-active" data-idx="3">Performance Efficiency</div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-cl b1-v-ov-active" data-idx="4">Cost Optimization</div>
# MAGIC     <div class="b1-v-ov-sep"></div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-lh" data-idx="5">Data and AI Governance</div>
# MAGIC     <div class="b1-v-ov-pill b1-v-ov-lh" data-idx="6">Interoperability and Usability</div>
# MAGIC   </div>
# MAGIC   <!-- Tabs -->
# MAGIC   <div class="b1-v-tabs">
# MAGIC     <div class="b1-v-tab b1-v-tab-cl-on" id="b1-tcl" onclick="b1tab('cl')">Cloud-Shared <span class="b1-v-tcount">(5 pillars)</span></div>
# MAGIC     <div class="b1-v-tab b1-v-tab-lh-off" id="b1-tlh" onclick="b1tab('lh')">Lakehouse-Specific <span class="b1-v-tcount">(2 pillars)</span></div>
# MAGIC   </div>
# MAGIC   <!-- Cloud-Shared Panel -->
# MAGIC   <div class="b1-v-panel b1-v-pcl b1-v-show" id="b1-pcl">
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,0)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#618794;"></div><div class="b1-v-aname">Operational Excellence</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Operations processes that keep the lakehouse running: automation, monitoring, incident response, and change management.</div><div class="b1-v-acap"><strong>Databricks:</strong> IaC with Terraform, CI/CD pipelines, system tables for observability</div></div></div>
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,1)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#618794;"></div><div class="b1-v-aname">Security, Privacy, Compliance</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Protect applications, workloads, and data from threats while meeting regulatory requirements.</div><div class="b1-v-acap"><strong>Databricks:</strong> Fine-grained access control (row filters, column masks), PrivateLink, GDPR/HIPAA auditability</div></div></div>
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,2)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#618794;"></div><div class="b1-v-aname">Reliability</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Recover from failures and continue to function across data and compute layers.</div><div class="b1-v-acap"><strong>Databricks:</strong> ACID transactions, autoscaling, disaster recovery via Terraform rebuilds and Delta DEEP CLONE</div></div></div>
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,3)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#618794;"></div><div class="b1-v-aname">Performance Efficiency</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Adapt to changes in load with elastic, right-sized resources.</div><div class="b1-v-acap"><strong>Databricks:</strong> Serverless compute, Photon engine, liquid clustering, predictive optimization</div></div></div>
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,4)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#618794;"></div><div class="b1-v-aname">Cost Optimization</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Manage costs to maximize the value delivered by the platform.</div><div class="b1-v-acap"><strong>Databricks:</strong> T-shirt sizing via compute policies, auto-termination, tagging for cost attribution</div></div></div>
# MAGIC   </div>
# MAGIC   <!-- Lakehouse-Specific Panel -->
# MAGIC   <div class="b1-v-panel b1-v-plh" id="b1-plh">
# MAGIC     <div class="b1-v-arow b1-v-aopen" onclick="b1acc(this,5)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#1B5162;"></div><div class="b1-v-aname">Data and AI Governance</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Ensure data and AI bring value: centralized cataloging, domain-level ownership, data quality enforcement, and AI asset governance.</div><div class="b1-v-acap"><strong>Databricks:</strong> Unity Catalog for cataloging and lineage, data quality monitoring, policy-driven lifecycle management</div></div></div>
# MAGIC     <div class="b1-v-arow" onclick="b1acc(this,6)"><div class="b1-v-ahead"><div class="b1-v-adot" style="background:#1B5162;"></div><div class="b1-v-aname">Interoperability and Usability</div><div class="b1-v-achev"></div></div><div class="b1-v-abody"><div class="b1-v-adesc">Ability of the lakehouse to interact with users and other systems through open standards and self-service tooling.</div><div class="b1-v-acap"><strong>Databricks:</strong> Open APIs and formats, multi-language tooling (SQL, Python, Scala, R), Delta Sharing</div></div></div>
# MAGIC   </div>
# MAGIC   <!-- Foundation -->
# MAGIC   <div class="b1-v-found">
# MAGIC     <div class="b1-v-fbar b1-v-fbar-uc">Unity Catalog</div>
# MAGIC     <div class="b1-v-fbar b1-v-fbar-fmt">Delta Lake &nbsp;&#x2022;&nbsp; Apache Iceberg &nbsp;&#x2022;&nbsp; Cloud Object Storage</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function b1ovGroup(group){var pills=document.querySelectorAll('.b1-v-ov-pill');pills.forEach(function(p){p.classList.remove('b1-v-ov-active');});if(group==='cl'){pills.forEach(function(p){if(p.classList.contains('b1-v-ov-cl'))p.classList.add('b1-v-ov-active');});}else if(group==='lh'){pills.forEach(function(p){if(p.classList.contains('b1-v-ov-lh'))p.classList.add('b1-v-ov-active');});}}
# MAGIC function b1ovOne(idx){var pills=document.querySelectorAll('.b1-v-ov-pill');pills.forEach(function(p){p.classList.remove('b1-v-ov-active');});var t=document.querySelector('.b1-v-ov-pill[data-idx="'+idx+'"]');if(t)t.classList.add('b1-v-ov-active');}
# MAGIC function b1tab(w){var tcl=document.getElementById('b1-tcl'),tlh=document.getElementById('b1-tlh'),pcl=document.getElementById('b1-pcl'),plh=document.getElementById('b1-plh');if(w==='cl'){tcl.className='b1-v-tab b1-v-tab-cl-on';tlh.className='b1-v-tab b1-v-tab-lh-off';pcl.classList.add('b1-v-show');plh.classList.remove('b1-v-show');b1ovGroup('cl');}else{tcl.className='b1-v-tab b1-v-tab-cl-off';tlh.className='b1-v-tab b1-v-tab-lh-on';pcl.classList.remove('b1-v-show');plh.classList.add('b1-v-show');b1ovGroup('lh');}}
# MAGIC function b1acc(r,idx){var wasOpen=r.classList.contains('b1-v-aopen');r.parentElement.querySelectorAll('.b1-v-arow').forEach(function(x){x.classList.remove('b1-v-aopen');});if(!wasOpen){r.classList.add('b1-v-aopen');b1ovOne(idx);}else{var isCl=r.closest('.b1-v-pcl');b1ovGroup(isCl?'cl':'lh');}}
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Two Additional Pillars?</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Cloud WAFs stop at infrastructure.</strong> AWS, Azure, and GCP frameworks address compute, networking, and cost. They do not address data cataloging, data quality, AI asset governance, or cross-platform interoperability. These gaps are where lakehouse implementations fail.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> the five cloud pillars are the walls, roof, and wiring of a building. The two lakehouse pillars are the foundation and plumbing. Every other system depends on them, and you cannot retrofit them after the fact.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog spans both additions.</strong> It appears in the Governance pillar (cataloging, lineage, quality) and the Interoperability pillar (open formats, federated access, Delta Sharing). This is why the overview bar shows UC as the foundation under all seven pillars.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Governance vs. Security: A Common Confusion</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Data Governance</strong> focuses on ensuring data brings <em>business value</em> through cataloging, lineage, quality, and ownership. It answers: "Can we trust this data? Who owns it? How did it get here?"</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Security</strong> focuses on <em>protecting</em> the platform, workloads, and data from threats through access control, encryption, and network isolation. It answers: "Who can access this? Is it encrypted? Is the network secure?"</li>
# MAGIC           <li style="font-size: 14pt;">Both are separate pillars because they serve different stakeholders: governance serves data stewards and business owners, security serves platform administrators and compliance teams. Confusing them leads to gaps in one or both areas.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Using the Accordion Above</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Click through each pillar in the Cloud-Shared and Lakehouse-Specific tabs to review its one-line description and Databricks capabilities. The overview bar at the top highlights the active pillar in context of all seven.</li>
# MAGIC           <li style="font-size: 14pt;">In customer conversations, start with the five familiar cloud pillars (Cloud-Shared tab), then introduce the two lakehouse additions as "what the cloud WAF does not cover."</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/block/unity-catalog" style="color: #2574B5; font-size: 14pt;">Block</a> illustrates both in action: Unity Catalog provided centralized governance for data discovery and ownership across business units, while also enforcing GDPR/CCPA compliance through fine-grained access policies configured in a single location. &#x25C6;</li>
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
# MAGIC ## C. Applying the Framework
# MAGIC
# MAGIC The seven pillars define what a well-architected lakehouse looks like. This section covers when and how to apply the framework across different stages of the customer lifecycle.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. The Gain / Grow / Retain Lifecycle
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Well-Architected Framework is not a one-time assessment. It maps to the <strong>Gain / Grow / Retain</strong> customer lifecycle, with different activities appropriate at each stage. Early engagements focus on discovery and alignment. Mid-lifecycle accounts expand usage across teams and workloads. Mature accounts optimize ROI and deepen integration. Knowing which WAF activities to deploy at each stage is a core skill for Partner Solution Architects.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-gain-grow-retain ── -->
# MAGIC <style>
# MAGIC .c1-ggr-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .c1-ggr-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr 1fr;
# MAGIC   gap: 14px;
# MAGIC }
# MAGIC .c1-ggr-col {
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.08);
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .c1-ggr-col:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .c1-ggr-header {
# MAGIC   padding: 14px 16px;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .c1-ggr-h-gain { background: #1B5162; }
# MAGIC .c1-ggr-h-grow { background: #618794; }
# MAGIC .c1-ggr-h-retain { background: #00A972; }
# MAGIC .c1-ggr-stage {
# MAGIC   font-size: 16pt;
# MAGIC   font-weight: 700;
# MAGIC   line-height: 1.2;
# MAGIC }
# MAGIC .c1-ggr-sub {
# MAGIC   font-size: 14pt;
# MAGIC   opacity: 0.85;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .c1-ggr-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 16px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .c1-ggr-item {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC   padding: 6px 0;
# MAGIC   border-bottom: 1px solid #EEEDE9;
# MAGIC }
# MAGIC .c1-ggr-item:last-child {
# MAGIC   border-bottom: none;
# MAGIC }
# MAGIC .c1-ggr-bullet {
# MAGIC   color: #1B5162;
# MAGIC   font-weight: 700;
# MAGIC   margin-right: 6px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="c1-ggr-wrapper">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2;">WAF Across the Customer Lifecycle</div>
# MAGIC   <div class="c1-ggr-grid">
# MAGIC     <!-- Gain -->
# MAGIC     <div class="c1-ggr-col">
# MAGIC       <div class="c1-ggr-header c1-ggr-h-gain">
# MAGIC         <div class="c1-ggr-stage">Gain</div>
# MAGIC         <div class="c1-ggr-sub">Early-stage / at-risk accounts</div>
# MAGIC       </div>
# MAGIC       <div class="c1-ggr-body">
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Architecture Workshops</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Security Discussions</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Unity Catalog / Governance Strategy</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> White Space Discovery</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Grow -->
# MAGIC     <div class="c1-ggr-col">
# MAGIC       <div class="c1-ggr-header c1-ggr-h-grow">
# MAGIC         <div class="c1-ggr-stage">Grow</div>
# MAGIC         <div class="c1-ggr-sub">Mid-lifecycle expansion</div>
# MAGIC       </div>
# MAGIC       <div class="c1-ggr-body">
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Architecture Reassessment</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Advanced Governance / Lineage</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Security Maturity Reviews</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> QBRs and Executive Briefings</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Retain -->
# MAGIC     <div class="c1-ggr-col">
# MAGIC       <div class="c1-ggr-header c1-ggr-h-retain">
# MAGIC         <div class="c1-ggr-stage">Retain</div>
# MAGIC         <div class="c1-ggr-sub">Mature account optimization</div>
# MAGIC       </div>
# MAGIC       <div class="c1-ggr-body">
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> Workload Optimization</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> QBRs and Executive Briefings</div>
# MAGIC         <div class="c1-ggr-item"><span class="c1-ggr-bullet">&#x279C;</span> New Use Case Planning</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Gain: Early-Stage and At-Risk Accounts</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Architecture workshops</strong> enable discovery and alignment around use cases. These are your entry point for establishing strategic technical presence.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Security discussions</strong> uncover gaps and introduce Unity Catalog as a governance differentiator. For accounts at risk of underutilization, demonstrating unified governance can re-engage stakeholders.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>White space discovery</strong> identifies underutilized features (Lakeflow Spark Declarative Pipelines, MLflow, Databricks SQL) that could expand the customer's platform investment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> the Gain stage is like a home inspection before purchase. You are identifying structural issues, code violations, and upgrade opportunities before the buyer commits.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Grow: Mid-Lifecycle Expansion</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Architecture workshops</strong> reassess existing deployments as teams and workloads expand. What worked for 5 data engineers may not scale to 50.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Governance maturity</strong> advances with deeper Unity Catalog adoption: lineage tracking, AI asset governance, and domain-level ownership become priorities.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Quarterly business reviews (QBRs)</strong> and executive briefings provide structured progress tracking against the WAF roadmap.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Retain: Mature Account Optimization</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Workload optimization</strong> analyzes cost and performance trends, recommending features like Photon, liquid clustering, serverless, and predictive optimization.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Use case planning</strong> identifies new ML, BI, or streaming initiatives that reinforce continued investment and prevent competitive displacement.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; A large enterprise customer (tech sector) saved $600K annually after a WAF assessment uncovered that they were spending as much on storage as on compute. A 3-6-12 month architecture roadmap guided adoption of automated vacuuming, data archival policies, and lifecycle management, validating the WAF as both a technical and business advisory tool. &#x25C6;</li>
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
# MAGIC ## D. Implementation Methodology
# MAGIC
# MAGIC Knowing which pillars matter is only useful if you have a repeatable process for improving them. This section introduces the five-phase implementation cycle and the team roles that make it work.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. The Five-Phase Cycle
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">WAF implementation follows a <strong>continuous improvement cycle</strong> with five phases: Assess, Prioritize, Remediate, Educate, and Automate/Monitor/Iterate. This is not a one-time project. Like a health checkup, it should be repeated regularly as the customer's environment evolves. Each cycle produces a more mature, resilient, and cost-effective lakehouse.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-five-phase-cycle ── -->
# MAGIC <style>
# MAGIC .d1-cyc-wrapper { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; width: 100%; margin: 24px 0; }
# MAGIC .d1-cyc-ring { position: relative; width: 100%; max-width: 560px; aspect-ratio: 1; margin: 0 auto; }
# MAGIC /* Circular track */
# MAGIC .d1-cyc-track { position: absolute; top: 50%; left: 50%; width: 64%; height: 64%; transform: translate(-50%, -50%); border: 3px dashed #DCE0E2; border-radius: 50%; }
# MAGIC /* Animated dot traveling the track */
# MAGIC @keyframes d1-orbit { 0% { transform: rotate(0deg) translateX(180px) rotate(0deg); } 100% { transform: rotate(360deg) translateX(180px) rotate(-360deg); } }
# MAGIC .d1-cyc-dot { position: absolute; top: 50%; left: 50%; width: 10px; height: 10px; margin: -5px 0 0 -5px; background: #1B5162; border-radius: 50%; animation: d1-orbit 8s linear infinite; }
# MAGIC /* Nodes positioned around the circle */
# MAGIC .d1-cyc-node { position: absolute; width: 140px; text-align: center; }
# MAGIC .d1-cyc-circle { width: 90px; height: 90px; border-radius: 50%; margin: 0 auto 8px; display: flex; align-items: center; justify-content: center; flex-direction: column; box-shadow: 0 3px 12px rgba(27,49,57,0.18); border: 3px solid #fff; }
# MAGIC .d1-cyc-cnum { font-size: 22pt; font-weight: 800; color: rgba(255,255,255,0.4); line-height: 1; }
# MAGIC .d1-cyc-cname { font-size: 14pt; font-weight: 700; color: #fff; line-height: 1.1; }
# MAGIC .d1-cyc-label { font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.2; margin-bottom: 3px; }
# MAGIC .d1-cyc-desc { font-size: 14pt; color: #5A6F77; line-height: 1.4; }
# MAGIC .d1-cyc-c1 { background: #1B5162; }
# MAGIC .d1-cyc-c2 { background: #2272B4; }
# MAGIC .d1-cyc-c3 { background: #00A972; }
# MAGIC .d1-cyc-c4 { background: #E5A100; }
# MAGIC .d1-cyc-c5 { background: #618794; }
# MAGIC /* 5 positions: top-center, right, bottom-right, bottom-left, left */
# MAGIC .d1-cyc-n1 { top: 0; left: 50%; transform: translateX(-50%); }
# MAGIC .d1-cyc-n2 { top: 20%; right: 0; }
# MAGIC .d1-cyc-n3 { bottom: 2%; right: 7%; }
# MAGIC .d1-cyc-n4 { bottom: 2%; left: 7%; }
# MAGIC .d1-cyc-n5 { top: 20%; left: 0; }
# MAGIC /* Center label */
# MAGIC .d1-cyc-center { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; pointer-events: none; }
# MAGIC .d1-cyc-center-txt { font-size: 16pt; font-weight: 700; color: #1B5162; line-height: 1.3; }
# MAGIC .d1-cyc-center-sub { font-size: 14pt; color: #618794; margin-top: 4px; }
# MAGIC /* Curved arrows between nodes using CSS borders */
# MAGIC .d1-cyc-arrow { position: absolute; border: 2px solid #90A5B1; border-radius: 50%; }
# MAGIC .d1-cyc-arrowhead { position: absolute; width: 0; height: 0; }
# MAGIC </style>
# MAGIC <div class="d1-cyc-wrapper">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2;">WAF Implementation Cycle</div>
# MAGIC   <div class="d1-cyc-ring">
# MAGIC     <!-- Circular dashed track with orbiting dot -->
# MAGIC     <div class="d1-cyc-track"></div>
# MAGIC     <div class="d1-cyc-dot"></div>
# MAGIC     <!-- Center -->
# MAGIC     <div class="d1-cyc-center"><div class="d1-cyc-center-txt" style="font-size:16pt;">Continuous<br/>Improvement</div><div class="d1-cyc-center-sub" style="font-size:14pt;">Reassess regularly</div></div>
# MAGIC     <!-- 5 nodes -->
# MAGIC     <div class="d1-cyc-node d1-cyc-n1"><div class="d1-cyc-circle d1-cyc-c1"><div class="d1-cyc-cnum">1</div></div><div class="d1-cyc-label" style="font-size:14pt;">Assess</div><div class="d1-cyc-desc" style="font-size:14pt;">Evaluate against pillars; establish baseline maturity</div></div>
# MAGIC     <div class="d1-cyc-node d1-cyc-n2"><div class="d1-cyc-circle d1-cyc-c2"><div class="d1-cyc-cnum">2</div></div><div class="d1-cyc-label" style="font-size:14pt;">Prioritize</div><div class="d1-cyc-desc" style="font-size:14pt;">Rank gaps by criticality, urgency, and effort</div></div>
# MAGIC     <div class="d1-cyc-node d1-cyc-n3"><div class="d1-cyc-circle d1-cyc-c3"><div class="d1-cyc-cnum">3</div></div><div class="d1-cyc-label" style="font-size:14pt;">Remediate</div><div class="d1-cyc-desc" style="font-size:14pt;">Implement changes to address prioritized risks</div></div>
# MAGIC     <div class="d1-cyc-node d1-cyc-n4"><div class="d1-cyc-circle d1-cyc-c4"><div class="d1-cyc-cnum">4</div></div><div class="d1-cyc-label" style="font-size:14pt;">Educate</div><div class="d1-cyc-desc" style="font-size:14pt;">Train stakeholders to sustain improvements</div></div>
# MAGIC     <div class="d1-cyc-node d1-cyc-n5"><div class="d1-cyc-circle d1-cyc-c5"><div class="d1-cyc-cnum">5</div></div><div class="d1-cyc-label" style="font-size:14pt;">Automate</div><div class="d1-cyc-desc" style="font-size:14pt;">Monitor, alert, and iterate continuously</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Five Phases in Detail</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>1. Assess:</strong> evaluate the current customer environment against the seven pillars. Establish a baseline maturity assessment for each pillar and align findings to business objectives and platform usage patterns. This is where you discover the gaps.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>2. Prioritize:</strong> identify and rank high-risk gaps or architectural weaknesses by criticality, urgency, and effort. Not every gap needs immediate attention. Classify them to inform roadmap planning.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>3. Remediate:</strong> design and implement architectural changes that address the prioritized risks. This often involves deploying new platform features (Unity Catalog, Lakeflow Spark Declarative Pipelines, compute policies) or reconfiguring existing ones.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>4. Educate:</strong> train internal stakeholders to sustain improvements and prevent regressions. Provide documentation, enablement sessions, and access to technical artifacts. The goal is building long-term capability, not just a one-time fix.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>5. Automate, Monitor, and Iterate:</strong> embed automation and observability to make changes sustainable and scalable. Set up monitoring and alerting via system tables, schedule regular reassessments, and build a culture of continuous improvement.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Analogy: The Health Checkup Model</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Think of the WAF cycle like an annual physical exam. The <strong>Assess</strong> phase runs diagnostic tests. <strong>Prioritize</strong> identifies which results are concerning versus in normal range. <strong>Remediate</strong> is the treatment plan. <strong>Educate</strong> is the lifestyle guidance ("exercise more, eat better"). And <strong>Automate/Monitor</strong> is the ongoing fitness tracker that catches regressions early.</li>
# MAGIC           <li style="font-size: 14pt;">The key insight: you do not stop after one cycle. Each reassessment builds on the previous one, progressively raising the organization's architectural maturity.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Execution Team Roles</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Business Sponsor:</strong> provides oversight, ensures alignment to business priorities, and champions strategic growth efforts at the executive level.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Tech/Platform Owner:</strong> completes the framework assessment with the team, prioritizes improvements to the Data Intelligence Platform deployment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Developers:</strong> implement changes to close gaps in features and best practices, raise blockers and dependencies.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Project Manager:</strong> plans the implementation timeline, ensures appropriate resources, and addresses blockers and risks.</li>
# MAGIC           <li style="font-size: 14pt;">The Databricks DSA/SA collaborates with all four roles throughout execution, serving as the advisory bridge between platform capabilities and customer objectives.</li>
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
# MAGIC ## E. Pillar Best Practices
# MAGIC
# MAGIC Each pillar is decomposed into principles, and each principle into narrative best practices. This section shows the documentation hierarchy and the headline practices per pillar.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Documentation Structure and Headline Practices
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Well-Architected Lakehouse documentation follows a <strong>three-tier hierarchy</strong>: Pillars contain Principles (3-4 per pillar), and Principles contain Best Practices. Each best practice is a <strong>narrative recommendation</strong> with context, specific implementation guidance (bullet-point actions), and cross-references to detailed platform documentation. This structure helps practitioners understand what to do, see what "good" looks like, and access the specific steps through linked docs.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-pillar-hierarchy ── -->
# MAGIC <style>
# MAGIC .e1-v-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
# MAGIC .e1-v-heading { font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2; }
# MAGIC /* Three tiers as rows */
# MAGIC .e1-v-tiers { display: flex; flex-direction: column; gap: 0; }
# MAGIC .e1-v-tier { display: flex; align-items: stretch; border-bottom: 1px solid #EEEDE9; }
# MAGIC .e1-v-tier:last-child { border-bottom: none; }
# MAGIC .e1-v-tlabel { width: 120px; min-width: 120px; display: flex; align-items: center; justify-content: center; font-size: 14pt; font-weight: 700; color: #fff; text-align: center; padding: 14px 8px; }
# MAGIC .e1-v-tl1 { background: #1B5162; border-radius: 8px 0 0 0; }
# MAGIC .e1-v-tl2 { background: #2272B4; }
# MAGIC .e1-v-tl3 { background: #618794; border-radius: 0 0 0 8px; }
# MAGIC .e1-v-tbody { flex: 1; padding: 12px 16px; display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
# MAGIC .e1-v-tb1 { background: #F9F7F4; border-radius: 0 8px 0 0; }
# MAGIC .e1-v-tb2 { background: #fff; min-height: 50px; }
# MAGIC .e1-v-tb3 { background: #F9F7F4; border-radius: 0 0 8px 0; min-height: 50px; }
# MAGIC /* Clickable pills for pillars */
# MAGIC .e1-v-pill { padding: 8px 14px; border-radius: 6px; font-size: 14pt; font-weight: 600; cursor: pointer; user-select: none; transition: transform 0.15s, box-shadow 0.15s, opacity 0.15s; }
# MAGIC .e1-v-pill-p { background: #1B5162; color: #fff; }
# MAGIC .e1-v-pill-p.e1-v-dim { opacity: 0.4; }
# MAGIC .e1-v-pill-p.e1-v-act { opacity: 1; transform: translateY(-2px); box-shadow: 0 3px 10px rgba(27,49,57,0.2); }
# MAGIC /* Principle chips (populated by JS) */
# MAGIC .e1-v-chip { display: inline-block; padding: 6px 12px; border-radius: 5px; font-size: 14pt; font-weight: 600; color: #1B3139; background: #e3edf1; border: 1px solid #c8d8df; }
# MAGIC /* Best practice item */
# MAGIC .e1-v-bp { display: inline-block; padding: 6px 12px; border-radius: 5px; font-size: 14pt; color: #333; background: #fff; border: 1px solid #DCE0E2; }
# MAGIC .e1-v-hint { font-size: 14pt; color: #90A5B1; font-style: italic; }
# MAGIC </style>
# MAGIC <div class="e1-v-wrap">
# MAGIC   <div class="e1-v-heading">Documentation Hierarchy: Pillar &#x279C; Principle &#x279C; Best Practice</div>
# MAGIC   <div class="e1-v-tiers">
# MAGIC     <!-- Tier 1: Pillars (clickable) -->
# MAGIC     <div class="e1-v-tier">
# MAGIC       <div class="e1-v-tlabel e1-v-tl1" style="font-size:14pt;">Pillar</div>
# MAGIC       <div class="e1-v-tbody e1-v-tb1">
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-act" onclick="e1pick(0)">Operational Excellence</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(1)">Security</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(2)">Reliability</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(3)">Performance</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(4)">Cost</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(5)" style="background:#00A972;">Governance</div>
# MAGIC         <div class="e1-v-pill e1-v-pill-p e1-v-dim" onclick="e1pick(6)" style="background:#00A972;">Interop</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Tier 2: Principles (populated by click) -->
# MAGIC     <div class="e1-v-tier">
# MAGIC       <div class="e1-v-tlabel e1-v-tl2" style="font-size:14pt;">Principle</div>
# MAGIC       <div class="e1-v-tbody e1-v-tb2" id="e1-principles"></div>
# MAGIC     </div>
# MAGIC     <!-- Tier 3: Best Practice (sample, populated by click) -->
# MAGIC     <div class="e1-v-tier">
# MAGIC       <div class="e1-v-tlabel e1-v-tl3" style="font-size:14pt;">Best Practice</div>
# MAGIC       <div class="e1-v-tbody e1-v-tb3" id="e1-bp"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var E1D=[
# MAGIC {pr:["Optimize build and release","Automate deployments","Monitoring and alerting","Manage capacity and quotas"],bp:["Use IaC with Terraform","Standardize compute via policies","Use Lakeflow for ETL pipelines"]},
# MAGIC {pr:["Least-privilege access","Protect data in transit/at rest","Secure network and endpoints","Shared responsibility model","Compliance and privacy","Monitor system security"],bp:["SSO with SCIM provisioning","Row filters and column masks","PrivateLink for network isolation"]},
# MAGIC {pr:["Design for failure","Manage data quality","Design for autoscaling","Test recovery procedures","Automate deployments","Monitor systems"],bp:["ACID transactions for integrity","Automatic job retries","Rescued data columns for bad records"]},
# MAGIC {pr:["Use serverless architectures","Design workloads for performance","Performance testing in CI/CD","Monitor performance"],bp:["Serverless SQL warehouses","Photon vectorized execution","Liquid clustering for data layout"]},
# MAGIC {pr:["Choose optimal resources","Dynamically allocate resources","Monitor and control cost","Design cost-effective workloads"],bp:["Use job compute for batch","Up-to-date runtimes","Tagging for cost attribution"]},
# MAGIC {pr:["Unify data and AI management","Unify data and AI security","Establish data quality standards"],bp:["Design Unity Catalog structure","Track lineage to column level","Govern AI assets with data"]},
# MAGIC {pr:["Define integration standards","Open interfaces and formats","Simplify new use cases","Ensure data consistency"],bp:["Delta Sharing for cross-platform","Self-service deployment templates","Curated metadata and lineage"]}
# MAGIC ];
# MAGIC function e1pick(i){
# MAGIC   var pills=document.querySelectorAll('.e1-v-pill-p');
# MAGIC   pills.forEach(function(p,j){p.classList.remove('e1-v-act');p.classList.add('e1-v-dim');});
# MAGIC   pills[i].classList.remove('e1-v-dim');pills[i].classList.add('e1-v-act');
# MAGIC   var d=E1D[i],pr=document.getElementById('e1-principles'),bp=document.getElementById('e1-bp');
# MAGIC   pr.innerHTML=d.pr.map(function(p){return '<div class="e1-v-chip" style="font-size:14pt;">'+p+'</div>';}).join('');
# MAGIC   bp.innerHTML=d.bp.map(function(b){return '<div class="e1-v-bp" style="font-size:14pt;">'+b+'</div>';}).join('');
# MAGIC }
# MAGIC e1pick(0);
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Lakehouse-Specific Pillars: Principles and Headline Practices</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Data and AI Governance (3 principles):</strong> Unify data and AI management, Unify data and AI security, Establish data quality standards. Headline practices include designing Unity Catalog for your organization (choosing centralized, federated, or hybrid governance models), tracking data and AI lineage to column-level granularity, governing AI assets together with data in Unity Catalog, centralizing access control with row filters and column masks, and configuring audit logging for compliance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Interoperability and Usability (4 principles):</strong> Define standards for integration, Use open interfaces and open data formats, Simplify new use case implementation, Ensure data consistency and usability. Headline practices include adopting open formats (Delta Lake, Iceberg, Parquet) to prevent vendor lock-in, enabling self-service through preset deployment templates and shared environments, using Delta Sharing for cross-platform data access, and treating data as a product with managed schemas and curated metadata.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/hp-indigo" style="color: #2574B5; font-size: 14pt;">HP Indigo</a> reduced consumable traceability from 2-3 days to 60 minutes using Unity Catalog lineage, calling it "a real breakthrough" that connected manufacturing, field, and customer data end to end. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Cloud-Shared Pillars: Principles and Headline Practices</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Operational Excellence (4 principles):</strong> Optimize build and release processes, Automate deployments and workloads, Set up monitoring/alerting/logging, Manage capacity and quotas. 21 best practices covering dedicated lakehouse operations teams, IaC with Terraform, CI/CD and MLOps standardization, Declarative Automation Bundles, compute policies for T-shirt sizing, system tables monitoring, and automated ETL via Lakeflow Spark Declarative Pipelines.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Security, Privacy, Compliance (6 principles):</strong> Manage identity and access using least privilege, Protect data in transit and at rest, Secure your network and protect endpoints, Review the shared responsibility model, Meet compliance and data privacy requirements, Monitor system security. Practices cover SSO with SCIM provisioning, service principals, PrivateLink, row filters and column masks, and automated vulnerability scanning in CI/CD.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Reliability (6 principles):</strong> Design for failure, Manage data quality, Design for autoscaling, Test recovery procedures, Automate deployments and workloads, Monitor systems and workloads. Practices cover ACID transactions for data integrity, automatic task rescheduling, rescued data columns for malformed records, automatic job retries, disaster recovery via Terraform rebuilds, and production-grade model serving with autoscaling.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Performance and Cost Pillars</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Performance Efficiency (4 principles):</strong> Use serverless architectures, Design workloads for performance, Run performance testing in the scope of development, Monitor performance. Practices cover serverless SQL warehouses and compute, Photon vectorized execution, liquid clustering for data layout, predictive optimization for automatic table maintenance, and integrating performance baselines into CI/CD pipelines.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cost Optimization (4 principles):</strong> Choose optimal resources, Dynamically allocate resources, Monitor and control cost, Design cost-effective workloads. Practices cover using performance-optimized formats (Delta), job compute for non-interactive workloads, SQL warehouses with Photon for SQL, up-to-date runtimes, GPU-only-when-needed policies, auto-termination, tagging for cost attribution, and system tables for consumption monitoring.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/quartile" style="color: #2574B5; font-size: 14pt;">Quartile</a> reduced storage from 90TB to 18TB (80% reduction) by migrating to Delta Lake, using Parquet compaction and version control optimizations. &#x25C6;</li>
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
# MAGIC ### E2. Pillar Maturity: What to Look For
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">During WAF engagements, Partner SAs assess each pillar qualitatively by identifying what capabilities the customer has in place and where gaps remain. The table below shows <strong>illustrative findings</strong> for a hypothetical enterprise customer. Use this as a conversation model, not a scoring rubric: the goal is to surface strengths, growth areas, and quick wins for each pillar.</p>
# MAGIC
# MAGIC <!-- ── Visual: e2-maturity-guide ── -->
# MAGIC <style>
# MAGIC .e2-mg-wrapper { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; width: 100%; margin: 24px 0; }
# MAGIC .e2-mg-table { width: 100%; border-spacing: 0; font-size: 14pt; }
# MAGIC .e2-mg-table th { background: #1B3139; color: #fff; font-weight: 700; padding: 12px 14px; text-align: left; font-size: 14pt; }
# MAGIC .e2-mg-table th:first-child { border-radius: 8px 0 0 0; }
# MAGIC .e2-mg-table th:last-child { border-radius: 0 8px 0 0; }
# MAGIC .e2-mg-table td { padding: 12px 14px; border-bottom: 1px solid #EEEDE9; vertical-align: top; font-size: 14pt; color: #333; line-height: 1.5; }
# MAGIC .e2-mg-table tr:nth-child(even) td { background: #F9F7F4; }
# MAGIC .e2-mg-pillar { font-weight: 700; color: #1B3139; font-size: 14pt; }
# MAGIC .e2-mg-status { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 14pt; font-weight: 700; }
# MAGIC .e2-mg-strong { background: #e8f5e9; color: #2e7d32; }
# MAGIC .e2-mg-dev { background: #fff3e0; color: #e65100; }
# MAGIC .e2-mg-needs { background: #ffebee; color: #98102A; }
# MAGIC .e2-mg-note { margin-top: 12px; font-size: 14pt; color: #618794; font-style: italic; text-align: center; }
# MAGIC </style>
# MAGIC <div class="e2-mg-wrapper">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2;">Pillar Maturity: Illustrative Customer Findings</div>
# MAGIC   <table class="e2-mg-table">
# MAGIC     <thead><tr><th style="width:22%; font-size:14pt;">Pillar</th><th style="width:14%; font-size:14pt;">Maturity</th><th style="width:32%; font-size:14pt;">Capabilities in Place</th><th style="width:32%; font-size:14pt;">Growth Opportunities</th></tr></thead>
# MAGIC     <tbody>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Data Governance</span></td><td><span class="e2-mg-status e2-mg-dev">Developing</span></td><td style="font-size:14pt;">Unity Catalog adopted, data mesh intent, notebook quality checks</td><td style="font-size:14pt;">Systematic PII identification, AI asset governance automation</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Interoperability</span></td><td><span class="e2-mg-status e2-mg-strong">Strong</span></td><td style="font-size:14pt;">Tableau/Power BI integrated, Delta Lake standardized</td><td style="font-size:14pt;">Self-service productivity, simplified sharing workflows</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Operational Excellence</span></td><td><span class="e2-mg-status e2-mg-needs">Needs Attention</span></td><td style="font-size:14pt;">Terraform + Databricks API, Declarative Automation Bundles</td><td style="font-size:14pt;">Retire custom ops tools, adopt system tables monitoring</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Security</span></td><td><span class="e2-mg-status e2-mg-dev">Developing</span></td><td style="font-size:14pt;">PrivateLink, secrets management, token rotation</td><td style="font-size:14pt;">Run Security Analysis Tool (SAT), address masked data gaps</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Reliability</span></td><td><span class="e2-mg-status e2-mg-strong">Strong</span></td><td style="font-size:14pt;">Terraform-managed workspaces, CI/CD pipelines</td><td style="font-size:14pt;">Validate DR strategy, fault injection testing</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Performance</span></td><td><span class="e2-mg-status e2-mg-strong">Strong</span></td><td style="font-size:14pt;">Serverless SQL, Photon enabled</td><td style="font-size:14pt;">Liquid clustering for legacy tables, predictive optimization</td></tr>
# MAGIC       <tr><td><span class="e2-mg-pillar" style="font-size:14pt;">Cost Optimization</span></td><td><span class="e2-mg-status e2-mg-strong">Strong</span></td><td style="font-size:14pt;">Compute policies, system tables + Datadog</td><td style="font-size:14pt;">Automate cost reporting, efficiency reviews</td></tr>
# MAGIC     </tbody>
# MAGIC   </table>
# MAGIC   <div class="e2-mg-note" style="font-size:14pt;">Illustrative example for a hypothetical enterprise. Your assessments will vary based on customer maturity and priorities.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">How to Read the Maturity Levels</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Strong:</strong> the customer has adopted platform capabilities for this pillar and is using them in production. Growth areas are refinements, not gaps.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Developing:</strong> foundations are in place but adoption is incomplete or inconsistent across teams. The customer knows what to do but has not operationalized it.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Needs Attention:</strong> the customer has significant gaps that create risk. These pillars should be prioritized in the remediation roadmap.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Using This in Customer Engagements</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>This is a conversation model, not a scoring rubric.</strong> There is no official WAF scoring tool. Partner SAs assess maturity qualitatively through discovery workshops, architecture reviews, and stakeholder interviews.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> the assessment is like a home inspection report. The inspector notes "roof in good condition" and "electrical panel needs updating" without assigning numerical scores. The value is in identifying what needs attention and building a prioritized remediation plan.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Deliverable format:</strong> findings are typically captured in the WAF Readout Template (go/waf/slides) and presented alongside the WAF Checklist (go/waf/checklist), which maps best practices to pillar-level findings.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">From Findings to Roadmap</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The strategic value: a qualitative assessment transforms a technical review into an executive-ready deliverable that justifies investment, tracks progress, and positions Databricks as a strategic advisory partner.</li>
# MAGIC           <li style="font-size: 14pt;">Prioritize "Needs Attention" pillars for quick wins (0-30 days), then address "Developing" pillars with strategic fixes (30-90 days).</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/greenflex" style="color: #2574B5; font-size: 14pt;">GreenFlex</a> illustrates the operational impact: after adopting Unity Catalog for centralized governance, employee onboarding and data access granting dropped from approximately one week to a few minutes, and permissions tasks dropped from days to minutes. &#x25C6;</li>
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
# MAGIC ## F. Reference Architecture
# MAGIC
# MAGIC The pillars and best practices describe quality attributes. The reference architecture shows how the Databricks Data Intelligence Platform components connect end to end, providing the structural context for every WAF recommendation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. The Data Intelligence Platform Architecture
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Databricks reference architecture is organized along <strong>swim lanes</strong>: Source, Ingest, Transform, Query/Process, Serve, Analyse, and Integrate. Data flows left to right through the platform, stored in Delta Lake or Iceberg tables using the medallion architecture. Unity Catalog governs all layers. Think of it as a city map: the swim lanes are zones (residential, commercial, industrial), and the use case overlays are commute routes for different personas. Not every deployment uses every component, but the map shows how they all connect.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-reference-architecture ── -->
# MAGIC <style>
# MAGIC .f1-ra-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .f1-ra-lanes {
# MAGIC   display: flex;
# MAGIC   gap: 6px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .f1-ra-lane {
# MAGIC   flex: 1;
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 1px 4px rgba(0,0,0,0.08);
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .f1-ra-lane:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC   z-index: 1;
# MAGIC }
# MAGIC .f1-ra-lane-header {
# MAGIC   padding: 10px 8px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .f1-ra-lh-src { background: #90A5B1; }
# MAGIC .f1-ra-lh-ing { background: #618794; }
# MAGIC .f1-ra-lh-tfm { background: #1B5162; }
# MAGIC .f1-ra-lh-qry { background: #2272B4; }
# MAGIC .f1-ra-lh-srv { background: #00A972; }
# MAGIC .f1-ra-lh-ana { background: #FFAB00; }
# MAGIC .f1-ra-lh-int { background: #5A6F77; }
# MAGIC .f1-ra-lane-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 10px 8px;
# MAGIC   flex: 1;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .f1-ra-chip {
# MAGIC   display: inline-block;
# MAGIC   background: #fff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 4px;
# MAGIC   padding: 3px 7px;
# MAGIC   margin: 2px 1px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #1B3139;
# MAGIC   white-space: nowrap;
# MAGIC   transition: background 0.15s, border-color 0.15s;
# MAGIC }
# MAGIC .f1-ra-chip:hover {
# MAGIC   background: #e3f2fd;
# MAGIC   border-color: #1B5162;
# MAGIC }
# MAGIC .f1-ra-foundation {
# MAGIC   display: flex;
# MAGIC   gap: 8px;
# MAGIC   margin-top: 10px;
# MAGIC }
# MAGIC .f1-ra-found-bar {
# MAGIC   flex: 1;
# MAGIC   text-align: center;
# MAGIC   padding: 10px;
# MAGIC   border-radius: 6px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="f1-ra-wrapper">
# MAGIC   <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2;">Data Intelligence Platform Reference Architecture</div>
# MAGIC   <div class="f1-ra-lanes">
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-src">Source</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Files/Logs</span>
# MAGIC         <span class="f1-ra-chip">IoT</span>
# MAGIC         <span class="f1-ra-chip">RDBMS</span>
# MAGIC         <span class="f1-ra-chip">SaaS Apps</span>
# MAGIC         <span class="f1-ra-chip">Media</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-ing">Ingest</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Lakeflow Connect</span>
# MAGIC         <span class="f1-ra-chip">Auto Loader</span>
# MAGIC         <span class="f1-ra-chip">Streaming</span>
# MAGIC         <span class="f1-ra-chip">CDC</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-tfm">Transform</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Spark/Photon</span>
# MAGIC         <span class="f1-ra-chip">Declarative Pipelines</span>
# MAGIC         <span class="f1-ra-chip">SQL Warehouses</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-qry">Query/Process</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Databricks SQL</span>
# MAGIC         <span class="f1-ra-chip">Mosaic AI</span>
# MAGIC         <span class="f1-ra-chip">ML Runtimes</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-srv">Serve</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Model Serving</span>
# MAGIC         <span class="f1-ra-chip">Lakebase</span>
# MAGIC         <span class="f1-ra-chip">AI Gateway</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-ana">Analyse</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">AI/BI Dashboards</span>
# MAGIC         <span class="f1-ra-chip">Genie</span>
# MAGIC         <span class="f1-ra-chip">3rd Party BI</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f1-ra-lane">
# MAGIC       <div class="f1-ra-lane-header f1-ra-lh-int">Integrate</div>
# MAGIC       <div class="f1-ra-lane-body">
# MAGIC         <span class="f1-ra-chip">Delta Sharing</span>
# MAGIC         <span class="f1-ra-chip">Marketplace</span>
# MAGIC         <span class="f1-ra-chip">Clean Rooms</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Foundation bars -->
# MAGIC   <div class="f1-ra-foundation">
# MAGIC     <div class="f1-ra-found-bar" style="background: #1B5162; flex: 2;">Unity Catalog (Governance)</div>
# MAGIC     <div class="f1-ra-found-bar" style="background: #618794; flex: 1;">Lakeflow Jobs</div>
# MAGIC     <div class="f1-ra-found-bar" style="background: #1B3139; flex: 1;">DatabricksIQ</div>
# MAGIC   </div>
# MAGIC   <div style="margin-top: 6px; text-align: center;">
# MAGIC     <div class="f1-ra-found-bar" style="background: #90A5B1; display: inline-block; padding: 8px 40px; font-size: 14pt;">Delta Lake &nbsp;&#x2022;&nbsp; Apache Iceberg &nbsp;&#x2022;&nbsp; Cloud Object Storage (S3 / ADLS / GCS)</div>
# MAGIC   </div>
# MAGIC   <!-- Reference Architecture PDFs -->
# MAGIC   <div style="margin-top: 20px;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: #1B5162; margin-bottom: 10px;">Downloadable Reference Architectures (A3 PDF)</div>
# MAGIC     <div style="font-size: 14pt; color: #5A6F77; margin-bottom: 12px;">Each use case has a cloud-specific reference architecture PDF. Select a cloud to see download links.</div>
# MAGIC     <div style="display: flex; gap: 0; margin-bottom: 0;">
# MAGIC       <div class="f1-ra-ctab f1-ra-ctab-on" id="f1-ctab-aws" onclick="f1cloud('aws')" style="flex:1;padding:10px;text-align:center;font-size:14pt;font-weight:700;cursor:pointer;user-select:none;border:2px solid #E5A100;border-bottom:none;border-radius:8px 8px 0 0;background:#E5A100;color:#fff;">AWS</div>
# MAGIC       <div class="f1-ra-ctab" id="f1-ctab-azure" onclick="f1cloud('azure')" style="flex:1;padding:10px;text-align:center;font-size:14pt;font-weight:700;cursor:pointer;user-select:none;border:2px solid #DCE0E2;border-bottom:none;border-radius:8px 8px 0 0;background:#EEEDE9;color:#618794;">Azure</div>
# MAGIC       <div class="f1-ra-ctab" id="f1-ctab-gcp" onclick="f1cloud('gcp')" style="flex:1;padding:10px;text-align:center;font-size:14pt;font-weight:700;cursor:pointer;user-select:none;border:2px solid #DCE0E2;border-bottom:none;border-radius:8px 8px 0 0;background:#EEEDE9;color:#618794;">GCP</div>
# MAGIC     </div>
# MAGIC     <div id="f1-cpanel" style="border:2px solid #E5A100;border-top:none;border-radius:0 0 8px 8px;padding:16px;background:#fff;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;"></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var F1ARCHS=[
# MAGIC {n:"Platform Overview",aws:"https://docs.databricks.com/aws/en/assets/files/reference-architecture-databricks-on-aws-e987d77be185187a910f1698c6756f9d.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-architecture-databricks-on-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-architecture-databricks-on-gcp-a10d46d57e3285bc1b8a30ac568065f7.pdf"},
# MAGIC {n:"Lakeflow Connect",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-lakeflow-for-aws-2605b98a9e9823d02e2dd7f62338db9f.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-lakeflow-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-lakeflow-for-gcp-6c8ee79a526cf690d66f4bd8b796108e.pdf"},
# MAGIC {n:"Batch ETL",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-batch-for-aws-a0f0a9bf8e71359e768b66827c90f886.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-batch-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-batch-for-gcp-f1c14e0d4f1bdd61ad7dc18e8de73210.pdf"},
# MAGIC {n:"Streaming / CDC",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-streaming-cdc-for-aws-7d43b034bff8328b91dc674f07605768.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-streaming-cdc-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-streaming-cdc-for-gcp-32214787af39e6fec9290aab4beac3b4.pdf"},
# MAGIC {n:"ML and AI",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-ai-for-aws-76b82ef2bb04015c1e158be71cd0676b.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-ai-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-ai-for-gcp-f40b7c253cd436547fee65d6506433a3.pdf"},
# MAGIC {n:"Gen AI Agents",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-gen-ai-agent-for-aws-8a361de05ea97d90edadb49da56db423.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-gen-ai-agent-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-gen-ai-agent-for-gcp-bd1da6b404fcef91ea539f19c37efb20.pdf"},
# MAGIC {n:"BI / SQL Analytics",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-bi-for-aws-aa183b07d1d79f9cc1e06b942c60a2b3.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-bi-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-bi-for-gcp-a0ca37bd473b1cc26e1da8b5da48b13a.pdf"},
# MAGIC {n:"Business Apps",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-apps-for-aws-ad255cbd322ebac552d1c7241b1f31e0.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-apps-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-apps-for-gcp-a7fcabed235fbd72c5a081d17af3421f.pdf"},
# MAGIC {n:"Lakehouse Federation",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-lh-federation-for-aws-68fc07aca04c4c598969947bf6c003aa.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-lh-federation-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-lh-federation-for-gcp-14475784ac0d9016f0da1959ce9ade25.pdf"},
# MAGIC {n:"Catalog Federation",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-cat-federation-for-aws-1a8c5401842164b245a08b8348edef16.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-cat-federation-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-cat-federation-for-gcp-09c2ae577ff9837c3101c0066a9fc798.pdf"},
# MAGIC {n:"3rd-Party Sharing",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-3p-sharing-for-aws-398835fc7c52beac62c9ec563ca6b91d.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-3p-sharing-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-3p-sharing-for-gcp-a9348389000d64dde04bf872ea80b667.pdf"},
# MAGIC {n:"D2D Sharing",aws:"https://docs.databricks.com/aws/en/assets/files/reference-use-case-d2d-sharing-for-aws-a07005c6e8690e0f4ae84590394f029f.pdf",azure:"https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/reference-use-case-d2d-sharing-for-azure.pdf",gcp:"https://docs.databricks.com/gcp/en/assets/files/reference-use-case-d2d-sharing-for-gcp-b00d080d7c67fd1b54cc5333baa44098.pdf"}
# MAGIC ];
# MAGIC var f1cur='aws';
# MAGIC function f1cloud(c){
# MAGIC   f1cur=c;
# MAGIC   var colors={aws:'#E5A100',azure:'#2574B5',gcp:'#00A972'};
# MAGIC   ['aws','azure','gcp'].forEach(function(k){
# MAGIC     var t=document.getElementById('f1-ctab-'+k);
# MAGIC     if(k===c){t.style.background=colors[k];t.style.color='#fff';t.style.borderColor=colors[k];}
# MAGIC     else{t.style.background='#EEEDE9';t.style.color='#618794';t.style.borderColor='#DCE0E2';}
# MAGIC   });
# MAGIC   document.getElementById('f1-cpanel').style.borderColor=colors[c];
# MAGIC   var p=document.getElementById('f1-cpanel');
# MAGIC   p.innerHTML=F1ARCHS.map(function(a){return '<a href="'+a[c]+'" target="_blank" style="display:block;padding:10px 12px;background:#F9F7F4;border-radius:6px;font-size:14pt;color:#1B3139;text-decoration:none;border:1px solid #DCE0E2;transition:background 0.15s;">'+a.n+'</a>';}).join('');
# MAGIC }
# MAGIC f1cloud('aws');
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Architecture Layers</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Source:</strong> files, logs, sensors/IoT, RDBMS, business apps, and media. Lakehouse Federation enables SQL source integration without ETL. Catalog Federation integrates Hive Metastores or cloud first catalogs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Ingest:</strong> Lakeflow Connect for enterprise application connectors, Auto Loader for incremental cloud storage file processing, Structured Streaming for Kafka and event data, and CDC capabilities.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Transform and Query/Process:</strong> Apache Spark and Photon engines, Lakeflow Spark Declarative Pipelines for declarative ETL, SQL warehouses and workspace clusters for varied workloads, and specialized ML runtimes.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Serve:</strong> Databricks SQL for data warehousing, Model Serving for ML inference, Lakebase for Lakebase Postgres capabilities, and Unity AI Gateway for governing generative AI model access.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analyse and Collaborate:</strong> AI/BI Dashboards and Genie Spaces for analysis. Delta Sharing for partner data access. Databricks Marketplace for data product exchange. Clean Rooms for privacy-protected collaboration.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Cross-Cutting Capabilities</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Storage:</strong> data organized using the medallion architecture (Bronze/Silver/Gold) in cloud provider object storage as Delta files or Apache Iceberg tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Governance:</strong> Unity Catalog manages access control, lineage, auditing, and data quality monitoring across all workspaces and assets.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Intelligence Engine:</strong> DatabricksIQ combines generative AI with lakehouse unification to understand data semantics, powering intelligent search and code generation.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Orchestration:</strong> Lakeflow Jobs coordinates ETL pipelines, ML training runs, and data quality checks across the entire lifecycle.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Use Case Overlays</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The reference architecture supports <strong>twelve downloadable A3 PDFs</strong>: the platform overview plus eleven use case overlays (Lakeflow Connect, Batch ETL, Streaming/CDC, Traditional ML, Gen AI Agents, BI/SQL Analytics, Business Apps, Lakehouse Federation, Catalog Federation, Third-Party Sharing, and Databricks-to-Databricks Sharing). Use the cloud tabs above to download each one.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cloud-specific variants</strong> map the same architecture to AWS (S3, Kinesis, Bedrock), Azure (ADLS Gen2, Entra ID, Purview, Power BI), and GCP (GCS, Pub/Sub, Looker, Vertex AI). The core architecture is consistent across clouds; only the peripheral services differ.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/atlassian/security-lakehouse" style="color: #2574B5; font-size: 14pt;">Atlassian</a> used the reference architecture as a blueprint for "Project Banyan," a multi-business-unit initiative to build a unified Security Lakehouse. They adopted the Open Cybersecurity Schema Framework (OCSF) on Delta Lake, enabling interactive analysis of tens of billions of security events and extending log retention from 30 days to 12 months. &#x25C6;</li>
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
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture introduced the Well-Architected Lakehouse Framework as a structured approach to evaluating and improving lakehouse implementations. You started with the six guiding principles that set the architectural vision, then examined the seven pillars that formalize those principles into assessable dimensions. You learned how to apply the framework across the Gain/Grow/Retain customer lifecycle, and walked through the five-phase implementation cycle that turns assessments into action. The pillar best practices and documentation hierarchy give you the detailed guidance for each recommendation, and the reference architecture grounds everything in the concrete Databricks Data Intelligence Platform components.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Key takeaways from this lecture include:</p>
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>Six guiding principles set the vision:</strong> curate trusted data products, eliminate silos, democratize access, adopt governance, use open formats, and build to scale.</li>
# MAGIC   <li><strong>Seven pillars, two unique to the lakehouse:</strong> Data and AI Governance and Interoperability/Usability are lakehouse-specific additions to the five cloud-standard pillars. Governance is not just one pillar; it is also the foundational infrastructure (Unity Catalog) that spans all pillars.</li>
# MAGIC   <li><strong>Framework maps to the customer lifecycle:</strong> different WAF activities are appropriate at Gain (discovery), Grow (expansion), and Retain (optimization) stages.</li>
# MAGIC   <li><strong>Five-phase continuous improvement:</strong> Assess, Prioritize, Remediate, Educate, and Automate/Monitor/Iterate. This is not a one-time project but a recurring cycle.</li>
# MAGIC   <li><strong>Documentation hierarchy is your toolkit:</strong> Pillars contain Principles, Principles contain Best Practices, each with Why/How/Links. Use this structure in every customer engagement.</li>
# MAGIC   <li><strong>Reference architecture connects it all:</strong> the swim-lane architecture (Source through Analyse/Integrate) with eleven use case overlays provides the structural context for every recommendation.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Next:</strong> The Activity (Prescriptive Remediation Planning) puts these concepts into practice. You will recall or construct a customer scenario, identify the two most at-risk pillars, and draft a remediation plan with quick wins (0-30 days) and strategic fixes (30-90 days).</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333; font-size: 14pt;">
# MAGIC         <li style="font-size: 14pt;">Well-Architected Lakehouse Framework (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/well-architected" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/well-architected" style="color: #2574B5;">GCP</a>): Seven pillars overview with links to each pillar's detailed best practices</li>
# MAGIC         <li style="font-size: 14pt;">Guiding Principles (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/guiding-principles" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/guiding-principles" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/guiding-principles" style="color: #2574B5;">GCP</a>): The six foundational principles that shape lakehouse design decisions</li>
# MAGIC         <li style="font-size: 14pt;">Lakehouse Architecture Scope (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/scope" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/scope" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/scope" style="color: #2574B5;">GCP</a>): Platform domains, personas, and the full scope of the Data Intelligence Platform</li>
# MAGIC         <li style="font-size: 14pt;">Reference Architectures (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/reference" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/reference" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/reference" style="color: #2574B5;">GCP</a>): Downloadable A3 PDFs for cloud-agnostic and cloud-specific reference architectures</li>
# MAGIC         <li style="font-size: 14pt;">Introduction to the Well-Architected Lakehouse (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/" style="color: #2574B5;">GCP</a>): Entry point for all lakehouse architecture documentation including the deployment guide</li>
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
