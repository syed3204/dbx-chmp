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
# MAGIC # 4 Lecture - Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC As data estates grow across teams, regions, and clouds, a recurring challenge emerges: how do you enforce consistent governance without slowing down the people who need data? Traditional approaches rely on separate tools for access control, auditing, lineage, and discovery. Each tool covers one piece. None covers all of it. And the gaps between them become compliance risks, security blind spots, and operational bottlenecks.
# MAGIC
# MAGIC Unity Catalog is the unified governance layer built into Databricks. It enforces access controls, tracks lineage, maintains audit logs, and governs all data and AI assets through a single model. Rather than bolting governance on after the fact, Unity Catalog operates beneath every data interaction automatically.
# MAGIC
# MAGIC This lecture covers 6 sections that build on each other:
# MAGIC
# MAGIC - **A. Why Unified Governance** -- The problems that fragmented governance creates and what Unity Catalog replaces
# MAGIC - **B. Unity Catalog Architecture** -- The three-level namespace, metastore, catalogs, schemas, and asset types
# MAGIC - **C. Access Controls and ABAC** -- Privileges, securable objects, row/column security, and attribute-based access control
# MAGIC - **D. Lakehouse Federation** -- Querying external data sources through foreign catalogs without data migration
# MAGIC - **E. Delta Sharing and Marketplace** -- Open protocol sharing, Databricks-to-Databricks sharing, Marketplace, and Clean Rooms
# MAGIC - **F. Architecture Patterns** -- SDLC isolation, cross-region access, and multi-business-unit deployment patterns
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain what Unity Catalog is and how it provides unified governance across data, AI, and analytics assets
# MAGIC - Describe the three-level namespace (Catalog > Schema > Assets) and how it enables domain-level governance and environment isolation
# MAGIC - Demonstrate how centralized access controls work using SQL, the Catalog Explorer UI, and REST APIs, including ABAC with governed tags
# MAGIC - Explain how Lakehouse Federation extends governance to external data systems without data migration
# MAGIC - Describe how Delta Sharing enables secure, zero-copy data collaboration across organizations and clouds
# MAGIC - Compare common Unity Catalog architecture patterns for SDLC environments, cross-region access, and multi-business-unit deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Why Unified Governance
# MAGIC
# MAGIC Before examining Unity Catalog's architecture, it helps to understand the problem it was designed to solve. As organizations adopt open data formats (Delta Lake, Iceberg, Parquet), they gain storage flexibility, but they still face a fragmentation problem at the catalog layer. Different engines require different catalogs, and those catalogs do not interoperate cleanly.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. The Catalog Silo Problem
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Even when data is stored in open formats, the <strong>catalog</strong> that manages metadata determines which engines can read or write it. Each major platform ships its own catalog (AWS Glue, Snowflake Horizon, BigQuery Metastore), and these catalogs have limited interoperability. The result is that choosing a catalog locks you into a subset of engines, or forces you to maintain multiple catalogs in parallel.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-catalog-silos heatmap ── -->
# MAGIC <style>
# MAGIC .a1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; display: flex; gap: 20px; align-items: flex-start; flex-wrap: wrap; }
# MAGIC .a1-v-main { flex: 1; min-width: 0; }
# MAGIC .a1-v-callout { flex: 0 0 20%; min-width: 180px; border: 2px dashed #98102A; border-radius: 10px; padding: 20px 16px; color: #98102A; font-size: 14pt; font-weight: 600; line-height: 1.5; text-align: center; margin-top: 80px; }
# MAGIC /* Header bar */
# MAGIC .a1-v-hdr { background: #1B3139; color: #fff; padding: 14px 20px; border-radius: 10px 10px 0 0; font-size: 15pt; font-weight: 700; }
# MAGIC /* Section labels */
# MAGIC .a1-v-section { display: flex; margin-top: 4px; }
# MAGIC .a1-v-section-spacer { width: 12%; flex-shrink: 0; }
# MAGIC .a1-v-section-current { flex: 5; text-align: center; border: 1.5px solid #98102A; border-bottom: none; border-radius: 8px 8px 0 0; padding: 5px 0 2px; }
# MAGIC .a1-v-section-current-top { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .a1-v-section-current-sub { font-size: 14pt; color: #98102A; font-weight: 600; }
# MAGIC .a1-v-section-vision { flex: 1; text-align: center; border: 1.5px solid #1B5162; border-bottom: none; border-radius: 8px 8px 0 0; padding: 5px 0 2px; margin-left: 8px; }
# MAGIC .a1-v-section-vision-top { font-size: 14pt; font-weight: 700; color: #1B5162; }
# MAGIC /* Column headers */
# MAGIC .a1-v-colhdr { display: flex; background: #F9F7F4; border-bottom: 2px solid #DCE0E2; }
# MAGIC .a1-v-colhdr-eng { width: 12%; flex-shrink: 0; padding: 8px 10px; font-size: 14pt; font-weight: 700; color: #5A6F77; }
# MAGIC .a1-v-colhdr-cats { display: flex; }
# MAGIC .a1-v-catlabel { flex: 1; text-align: center; padding: 8px 2px; font-size: 14pt; font-weight: 700; color: #1B3139; line-height: 1.3; }
# MAGIC .a1-v-catlabel.a1-v-uc { color: #1B5162; }
# MAGIC /* Grid rows */
# MAGIC .a1-v-row { display: flex; align-items: stretch; border-bottom: 2px solid #fff; }
# MAGIC .a1-v-eng { width: 12%; flex-shrink: 0; display: flex; align-items: center; padding: 0 10px; font-size: 14pt; font-weight: 600; color: #0b2026; background: #F9F7F4; border-right: 2px solid #fff; }
# MAGIC .a1-v-cells { display: flex; }
# MAGIC .a1-v-cell { flex: 1; height: 44px; margin: 0 1px; transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
# MAGIC .a1-v-cell:hover { transform: scale(1.06); box-shadow: 0 2px 8px rgba(0,0,0,0.15); z-index: 1; position: relative; }
# MAGIC .a1-v-full { background: #00A972; }
# MAGIC .a1-v-part { background: #FFAB00; }
# MAGIC .a1-v-none { background: #E8453C; }
# MAGIC .a1-v-vision-cells { margin-left: 8px; }
# MAGIC /* Legend */
# MAGIC .a1-v-legend { display: flex; gap: 18px; justify-content: center; margin-top: 12px; font-size: 14pt; color: #5A6F77; align-items: center; }
# MAGIC .a1-v-leg-swatch { width: 16px; height: 16px; border-radius: 3px; display: inline-block; vertical-align: middle; margin-right: 5px; }
# MAGIC /* Source attribution */
# MAGIC .a1-v-source-box { margin-top: 14px; padding: 10px 16px; background: #F9F7F4; border: 1px solid #DCE0E2; border-radius: 6px; display: flex; align-items: center; gap: 8px; }
# MAGIC .a1-v-source-dot { width: 8px; height: 8px; border-radius: 50%; background: #1B5162; flex-shrink: 0; }
# MAGIC .a1-v-source-text { font-size: 14pt; color: #5A6F77; line-height: 1.4; }
# MAGIC .a1-v-source-text a { color: #2574B5; text-decoration: none; font-weight: 600; }
# MAGIC .a1-v-source-text a:hover { text-decoration: underline; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="a1-v-wrap">
# MAGIC <div class="a1-v-main">
# MAGIC   <div class="a1-v-hdr">Catalog Silos</div>
# MAGIC
# MAGIC   <!-- Section labels: Current State | Our Vision -->
# MAGIC   <div class="a1-v-section">
# MAGIC     <div class="a1-v-section-spacer"></div>
# MAGIC     <div class="a1-v-section-current">
# MAGIC       <div class="a1-v-section-current-top">Catalog</div>
# MAGIC       <div class="a1-v-section-current-sub">Current state</div>
# MAGIC     </div>
# MAGIC     <div class="a1-v-section-vision">
# MAGIC       <div class="a1-v-section-vision-top">Our Vision</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Column headers -->
# MAGIC   <div class="a1-v-colhdr">
# MAGIC     <div class="a1-v-colhdr-eng">Engine</div>
# MAGIC     <div class="a1-v-colhdr-cats" style="flex:5;">
# MAGIC       <div class="a1-v-catlabel">Glue<br/>Catalog</div>
# MAGIC       <div class="a1-v-catlabel">Snowflake<br/>Horizon</div>
# MAGIC       <div class="a1-v-catlabel">Polaris<br/>Catalog</div>
# MAGIC       <div class="a1-v-catlabel">Google<br/>Lakehouse</div>
# MAGIC       <div class="a1-v-catlabel a1-v-uc">Unity<br/>Catalog</div>
# MAGIC     </div>
# MAGIC     <div class="a1-v-colhdr-cats" style="flex:1;margin-left:8px;">
# MAGIC       <div class="a1-v-catlabel a1-v-uc">Unity<br/>Catalog</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Databricks: Glue=Full, Horizon=Some(federation reads), Polaris=Some(IRC reads), Google=Some(federation preview), UC=Full | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">Databricks</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC   <!-- AWS (EMR, Athena, Redshift): Glue=Full, Horizon=Some(IRC reads GA), Polaris=Some(Glue federation), Google=None, UC=Full(Glue federation+IRC) | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">AWS</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC   <!-- Fabric: Glue=None, Horizon=None, Polaris=None, Google=None, UC=Full(Mirroring GA) | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">Fabric</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC   <!-- Snowflake: Glue=None, Horizon=Full, Polaris=Full, Google=Some(preview), UC=Full(reads GA all, writes GA Azure) | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">Snowflake</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC   <!-- OSS (Spark, Trino, Flink): Glue=Full, Horizon=Some(IRC reads GA), Polaris=Full(native IRC), Google=Some(IRC), UC=Full(Unity REST+IRC) | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">OSS</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC   <!-- BigQuery: Glue=None, Horizon=None, Polaris=None, Google=Full, UC=Some(federation preview) | Vision=Full -->
# MAGIC   <div class="a1-v-row">
# MAGIC     <div class="a1-v-eng">BigQuery</div>
# MAGIC     <div class="a1-v-cells" style="flex:5;"><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-none"></div><div class="a1-v-cell a1-v-full"></div><div class="a1-v-cell a1-v-part"></div></div>
# MAGIC     <div class="a1-v-cells a1-v-vision-cells" style="flex:1;"><div class="a1-v-cell a1-v-full"></div></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Legend -->
# MAGIC   <div class="a1-v-legend">
# MAGIC     <span><span class="a1-v-leg-swatch" style="background:#00A972;"></span> Full support</span>
# MAGIC     <span><span class="a1-v-leg-swatch" style="background:#FFAB00;"></span> Some support</span>
# MAGIC     <span><span class="a1-v-leg-swatch" style="background:#E8453C;"></span> No access</span>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Source attribution -->
# MAGIC   <div class="a1-v-source-box" style="flex-direction:column;gap:6px;">
# MAGIC     <div style="display:flex;align-items:center;gap:8px;">
# MAGIC       <div class="a1-v-source-dot"></div>
# MAGIC       <div class="a1-v-source-text"><strong>Original source:</strong> <a href="https://www.databricks.com/blog/announcing-full-apache-iceberg-support-databricks">Announcing Full Apache Iceberg Support in Databricks</a> (June 2025)</div>
# MAGIC     </div>
# MAGIC     <div style="font-size: 14pt;color:#5A6F77;line-height:1.5;padding-left:16px;"><strong style="color:#1B5162;">Updated June 2026 with:</strong>
# MAGIC       <a href="https://docs.databricks.com/aws/en/external-access/integrations" style="color:#2574B5;text-decoration:none;font-weight:600;">UC Engine Integrations</a> |
# MAGIC       <a href="https://docs.snowflake.com/en/release-notes/2026/other/2026-04-06-iceberg-write-support-azure-unity-catalog" style="color:#2574B5;text-decoration:none;font-weight:600;">Snowflake Writes to UC (GA April 2026)</a> |
# MAGIC       <a href="https://docs.snowflake.com/en/release-notes/2026/other/2026-02-06-tables-iceberg-query-using-external-query-engine-snowflake-horizon-ga" style="color:#2574B5;text-decoration:none;font-weight:600;">Horizon External Engine Support (GA Feb 2026)</a> |
# MAGIC       <a href="https://blog.fabric.microsoft.com/en-us/blog/unified-by-design-mirroring-azure-databricks-unity-catalog-in-microsoft-fabric-now-generally-available" style="color:#2574B5;text-decoration:none;font-weight:600;">Fabric UC Mirroring (GA)</a> |
# MAGIC       <a href="https://aws.amazon.com/blogs/big-data/introducing-catalog-federation-for-apache-iceberg-tables-in-the-aws-glue-data-catalog/" style="color:#2574B5;text-decoration:none;font-weight:600;">AWS Glue Catalog Federation</a> |
# MAGIC       <a href="https://www.databricks.com/blog/interoperability-between-unity-catalog-and-google-bigquery-catalog-federation" style="color:#2574B5;text-decoration:none;font-weight:600;">BigQuery UC Federation (Preview)</a> |
# MAGIC       <a href="https://blog.fabric.microsoft.com/en-US/blog/fabcon-and-sqlcon-2026-whats-new-in-microsoft-onelake/" style="color:#2574B5;text-decoration:none;font-weight:600;">Google Lakehouse Iceberg REST</a>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Callout box -->
# MAGIC <div class="a1-v-callout">Data discovery and governance is often split across disparate catalogs</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why Open Formats Are Not Enough</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Format vs. catalog distinction:</strong> storing data in Delta Lake or Iceberg solves the storage format problem, but metadata management (table definitions, permissions, lineage) still depends on which catalog you use. Two teams using the same format but different catalogs cannot share metadata.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Vendor-specific catalogs create lock-in:</strong> Snowflake Horizon governs only Snowflake-managed assets. Google Lakehouse (formerly BigLake Metastore) governs only BigQuery. Apache Polaris provides an open-source Iceberg REST catalog, but Polaris tables must sync to Snowflake Horizon for full governance, splitting data across two catalogs. None of these extend to third-party engines without integration work.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog's differentiator:</strong> UC supports both the Iceberg REST Catalog API and its own Unity REST APIs, making it accessible from Databricks, Spark, Trino, Flink, and other engines. This breadth is why UC is the only catalog in the heatmap where every engine row has green or yellow support.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Fabric is the clearest example:</strong> Microsoft Fabric has zero interoperability with Glue, Snowflake Horizon, Polaris, or Google Lakehouse. The only external catalog Fabric connects to is Unity Catalog (via Mirroring, GA). For any Fabric customer needing cross-platform governance, UC is the only path.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Business Impact of Catalog Silos</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Duplicated governance effort:</strong> when each catalog has its own permission model, governance teams must configure and audit access in multiple places. Inconsistencies between catalogs become compliance risks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Fragmented lineage:</strong> if data flows from a Glue-managed table through a Databricks pipeline into a Snowflake table, no single catalog tracks the full lineage. Debugging data quality issues requires manual investigation across systems.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/kpmg/unity-catalog" style="color: #2574B5; font-size: 14pt;">KPMG</a> manages over 500TB under Unity Catalog with 850+ active users. They use Lakehouse Federation to bring data from on-premises SQL servers, cloud data warehouses, and Azure Data Lake Storage into a single governance model, replacing the fragmented approach of managing policies across separate systems. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What This Means for You as a Partner SA</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Discovery question:</strong> "How many catalogs or metadata stores does your organization maintain today?" The answer reveals governance fragmentation and positions UC as the consolidation path.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Open source angle:</strong> Unity Catalog is also available as an open-source project (<a href="https://github.com/unitycatalog/unitycatalog" style="color: #2574B5; font-size: 14pt;">github.com/unitycatalog</a>), which can address customer concerns about catalog lock-in.</li>
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
# MAGIC ### A2. What Unity Catalog Delivers
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Unity Catalog is not a traditional data catalog. It is the <strong>unified governance layer</strong> built into every Databricks workspace: one security model, one lineage graph, one audit surface covering all data and AI assets. Where traditional catalogs index metadata, UC enforces policy at query time, classifies sensitive data automatically, shares data without copies, and coordinates multi-engine access through open APIs.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-uc-hub ── -->
# MAGIC <style>
# MAGIC .a2-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC /* ── Central hub ── */
# MAGIC .a2-v-hub { background: linear-gradient(135deg, #1B3139, #1B5162); border-radius: 16px; padding: 20px 28px; text-align: center; max-width: 600px; margin: 0 auto 6px; color: #fff; box-shadow: 0 4px 20px rgba(27,49,57,0.25); }
# MAGIC .a2-v-hub-t { font-size: 20pt; font-weight: 700; letter-spacing: 0.3px; }
# MAGIC .a2-v-hub-s { font-size: 14pt; color: #a8c9d6; margin-top: 4px; }
# MAGIC /* ── Connector lines ── */
# MAGIC .a2-v-conn { display: flex; justify-content: center; height: 28px; }
# MAGIC .a2-v-conn-stem { width: 2px; background: #94b3be; position: relative; }
# MAGIC .a2-v-conn-stem::after { content:''; position: absolute; bottom: -5px; left: -4px; width: 0; height: 0; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 6px solid #1B5162; }
# MAGIC .a2-v-hbar { height: 2px; background: #94b3be; margin: 0 60px; }
# MAGIC .a2-v-drops { display: flex; justify-content: space-around; margin: 0 40px; height: 18px; }
# MAGIC .a2-v-drop { width: 2px; background: #94b3be; position: relative; }
# MAGIC .a2-v-drop::after { content:''; position: absolute; bottom: -4px; left: -3px; width: 0; height: 0; border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 5px solid #94b3be; }
# MAGIC /* ── Capability nodes ── */
# MAGIC .a2-v-nodes { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 4px 0 14px; }
# MAGIC .a2-v-node { background: #fff; border-radius: 10px; padding: 16px 14px 14px; text-align: center; border: 1.5px solid #DCE0E2; box-shadow: 0 2px 8px rgba(27,49,57,0.06); transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a2-v-node:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC /* Icon circle */
# MAGIC .a2-v-icon { width: 56px; height: 56px; border-radius: 50%; margin: 0 auto 10px; display: flex; align-items: center; justify-content: center; }
# MAGIC .a2-v-icon-inner { width: 20px; height: 20px; }
# MAGIC /* Shield icon for Security */
# MAGIC .a2-v-ic-sec { background: #1B5162; }
# MAGIC .a2-v-ic-sec .a2-v-icon-inner { width: 16px; height: 20px; background: #fff; border-radius: 8px 8px 50% 50%; }
# MAGIC /* Tag icon for Classification */
# MAGIC .a2-v-ic-tag { background: #2574B5; }
# MAGIC .a2-v-ic-tag .a2-v-icon-inner { width: 14px; height: 14px; background: #fff; transform: rotate(45deg); border-radius: 2px; }
# MAGIC /* Lineage icon for Discovery */
# MAGIC .a2-v-ic-lin { background: #618794; }
# MAGIC .a2-v-ic-lin .a2-v-icon-inner { display: flex; flex-direction: column; gap: 4px; align-items: center; }
# MAGIC .a2-v-ic-lin .a2-v-dot { width: 8px; height: 8px; border-radius: 50%; background: #fff; }
# MAGIC .a2-v-ic-lin .a2-v-bar { width: 2px; height: 4px; background: rgba(255,255,255,0.6); }
# MAGIC /* Share icon */
# MAGIC .a2-v-ic-shr { background: #00A972; }
# MAGIC .a2-v-ic-shr .a2-v-icon-inner { position: relative; width: 22px; height: 16px; }
# MAGIC .a2-v-ic-shr .a2-v-circ { position: absolute; width: 14px; height: 14px; border-radius: 50%; border: 2.5px solid #fff; background: transparent; }
# MAGIC .a2-v-ic-shr .a2-v-circ:first-child { left: 0; top: 0; }
# MAGIC .a2-v-ic-shr .a2-v-circ:last-child { right: 0; bottom: 0; }
# MAGIC /* Interop icon */
# MAGIC .a2-v-ic-int { background: #E5A100; }
# MAGIC .a2-v-ic-int .a2-v-icon-inner { display: flex; gap: 3px; align-items: center; }
# MAGIC .a2-v-ic-int .a2-v-link { width: 8px; height: 12px; border: 2.5px solid #fff; border-radius: 4px; }
# MAGIC /* AI icon */
# MAGIC .a2-v-ic-ai { background: #90A5B1; }
# MAGIC .a2-v-ic-ai .a2-v-icon-inner { width: 18px; height: 18px; background: transparent; border: 2.5px solid #fff; clip-path: polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%); }
# MAGIC /* Node title */
# MAGIC .a2-v-ntitle { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 8px; }
# MAGIC /* Keyword pills */
# MAGIC .a2-v-pills { display: flex; flex-wrap: wrap; gap: 6px; justify-content: center; }
# MAGIC .a2-v-pill { font-size: 14pt; font-weight: 600; padding: 4px 10px; border-radius: 14px; white-space: nowrap; }
# MAGIC .a2-v-pill-ga { font-size: 14pt; font-weight: 700; background: #00A972; color: #fff; padding: 2px 6px; border-radius: 8px; vertical-align: middle; margin-left: -2px; }
# MAGIC /* Per-node pill colors */
# MAGIC .a2-v-n1 .a2-v-pill { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC .a2-v-n2 .a2-v-pill { background: rgba(37,116,181,0.10); color: #2574B5; }
# MAGIC .a2-v-n3 .a2-v-pill { background: rgba(97,135,148,0.10); color: #4a6a76; }
# MAGIC .a2-v-n4 .a2-v-pill { background: rgba(0,169,114,0.10); color: #007a53; }
# MAGIC .a2-v-n5 .a2-v-pill { background: rgba(229,161,0,0.12); color: #8a6200; }
# MAGIC .a2-v-n6 .a2-v-pill { background: rgba(144,165,177,0.15); color: #4a5d66; }
# MAGIC /* ── Footer bars ── */
# MAGIC .a2-v-assets { background: #F9F7F4; border: 1.5px solid #DCE0E2; border-radius: 10px; padding: 12px 16px; margin-top: 14px; text-align: center; }
# MAGIC .a2-v-assets-label { font-size: 14pt; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: #5A6F77; margin-bottom: 6px; }
# MAGIC .a2-v-asset-pills { display: flex; flex-wrap: wrap; gap: 6px; justify-content: center; }
# MAGIC .a2-v-asset-pill { font-size: 14pt; font-weight: 600; padding: 4px 10px; border-radius: 14px; background: #fff; color: #1B3139; border: 1px solid #DCE0E2; }
# MAGIC .a2-v-open { background: #1B3139; color: #a8c9d6; padding: 12px 16px; font-size: 14pt; text-align: center; margin-top: 2px; border-radius: 10px; }
# MAGIC .a2-v-open strong { color: #fff; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="a2-v-wrap">
# MAGIC   <!-- Central hub -->
# MAGIC   <div class="a2-v-hub">
# MAGIC     <div class="a2-v-hub-t">Unity Catalog</div>
# MAGIC     <div class="a2-v-hub-s">Unified Governance for Data and AI</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Connector: stem down from hub -->
# MAGIC   <div class="a2-v-conn"><div class="a2-v-conn-stem"></div></div>
# MAGIC   <!-- Horizontal bar -->
# MAGIC   <div class="a2-v-hbar"></div>
# MAGIC   <!-- Drop lines to nodes -->
# MAGIC   <div class="a2-v-drops"><div class="a2-v-drop"></div><div class="a2-v-drop"></div><div class="a2-v-drop"></div></div>
# MAGIC
# MAGIC   <!-- Row 1: 3 capability nodes -->
# MAGIC   <div class="a2-v-nodes">
# MAGIC     <div class="a2-v-node a2-v-n1">
# MAGIC       <div class="a2-v-icon a2-v-ic-sec"><div class="a2-v-icon-inner"></div></div>
# MAGIC       <div class="a2-v-ntitle">Access Control</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">ABAC <span class="a2-v-pill-ga">GA</span></span>
# MAGIC         <span class="a2-v-pill">Row Filters</span>
# MAGIC         <span class="a2-v-pill">Column Masks</span>
# MAGIC         <span class="a2-v-pill">50 Privileges</span>
# MAGIC         <span class="a2-v-pill">Audit Logs</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a2-v-node a2-v-n2">
# MAGIC       <div class="a2-v-icon a2-v-ic-tag"><div class="a2-v-icon-inner"></div></div>
# MAGIC       <div class="a2-v-ntitle">Classification and Tags</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">Auto AI Scan <span class="a2-v-pill-ga">GA</span></span>
# MAGIC         <span class="a2-v-pill">Governed Tags <span class="a2-v-pill-ga">GA</span></span>
# MAGIC         <span class="a2-v-pill">PII / PHI Detection</span>
# MAGIC         <span class="a2-v-pill">Certification</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a2-v-node a2-v-n3">
# MAGIC       <div class="a2-v-icon a2-v-ic-lin"><div class="a2-v-icon-inner"><div class="a2-v-dot"></div><div class="a2-v-bar"></div><div class="a2-v-dot"></div><div class="a2-v-bar"></div><div class="a2-v-dot"></div></div></div>
# MAGIC       <div class="a2-v-ntitle">Discovery and Lineage</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">Catalog Explorer</span>
# MAGIC         <span class="a2-v-pill">Auto Lineage</span>
# MAGIC         <span class="a2-v-pill">Column-Level</span>
# MAGIC         <span class="a2-v-pill">AI Descriptions</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Drop lines to row 2 -->
# MAGIC   <div class="a2-v-drops"><div class="a2-v-drop"></div><div class="a2-v-drop"></div><div class="a2-v-drop"></div></div>
# MAGIC
# MAGIC   <!-- Row 2: 3 capability nodes -->
# MAGIC   <div class="a2-v-nodes">
# MAGIC     <div class="a2-v-node a2-v-n4">
# MAGIC       <div class="a2-v-icon a2-v-ic-shr"><div class="a2-v-icon-inner"><div class="a2-v-circ"></div><div class="a2-v-circ"></div></div></div>
# MAGIC       <div class="a2-v-ntitle">Data Sharing</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">Delta Sharing</span>
# MAGIC         <span class="a2-v-pill">Zero-Copy</span>
# MAGIC         <span class="a2-v-pill">Marketplace</span>
# MAGIC         <span class="a2-v-pill">Clean Rooms</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a2-v-node a2-v-n5">
# MAGIC       <div class="a2-v-icon a2-v-ic-int"><div class="a2-v-icon-inner"><div class="a2-v-link"></div><div class="a2-v-link"></div></div></div>
# MAGIC       <div class="a2-v-ntitle">Interoperability</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">Iceberg REST</span>
# MAGIC         <span class="a2-v-pill">Federation</span>
# MAGIC         <span class="a2-v-pill">Catalog Commits <span class="a2-v-pill-ga">GA</span></span>
# MAGIC         <span class="a2-v-pill">Managed Iceberg <span class="a2-v-pill-ga">GA</span></span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="a2-v-node a2-v-n6">
# MAGIC       <div class="a2-v-icon a2-v-ic-ai"><div class="a2-v-icon-inner"></div></div>
# MAGIC       <div class="a2-v-ntitle">AI Governance and Quality</div>
# MAGIC       <div class="a2-v-pills">
# MAGIC         <span class="a2-v-pill">Model Registry</span>
# MAGIC         <span class="a2-v-pill">AI Gateway</span>
# MAGIC         <span class="a2-v-pill">MLflow Traces</span>
# MAGIC         <span class="a2-v-pill">Data Quality</span>
# MAGIC         <span class="a2-v-pill">Metric Views</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Governed assets -->
# MAGIC   <div class="a2-v-assets">
# MAGIC     <div class="a2-v-assets-label">Governed Asset Types</div>
# MAGIC     <div class="a2-v-asset-pills">
# MAGIC       <span class="a2-v-asset-pill">Delta Tables</span>
# MAGIC       <span class="a2-v-asset-pill">Iceberg Tables</span>
# MAGIC       <span class="a2-v-asset-pill">Views</span>
# MAGIC       <span class="a2-v-asset-pill">MVs</span>
# MAGIC       <span class="a2-v-asset-pill">Streaming Tables</span>
# MAGIC       <span class="a2-v-asset-pill">Volumes</span>
# MAGIC       <span class="a2-v-asset-pill">Functions</span>
# MAGIC       <span class="a2-v-asset-pill">ML Models</span>
# MAGIC       <span class="a2-v-asset-pill">Notebooks</span>
# MAGIC       <span class="a2-v-asset-pill">Dashboards</span>
# MAGIC       <span class="a2-v-asset-pill">Genie Spaces</span>
# MAGIC       <span class="a2-v-asset-pill">Metric Views</span>
# MAGIC       <span class="a2-v-asset-pill">Shares</span>
# MAGIC       <span class="a2-v-asset-pill">Clean Rooms</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Open standards -->
# MAGIC   <div class="a2-v-open">
# MAGIC     <strong>Built on open standards:</strong> Iceberg REST Catalog API &bull; Delta Sharing Protocol &bull; Hive Metastore API &bull; Apache Polaris Compatible &bull; Open-Source UC Server
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Beyond a Traditional Data Catalog</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Traditional catalogs</strong> provide metadata search and basic tagging. Unity Catalog enforces access controls at query time, captures lineage automatically, classifies sensitive data with AI, and shares data securely across organizations. Think of a traditional catalog as a library card index: it tells you what exists and where to find it. UC is the library card index plus the lock on every door, the security camera system, the inter-library loan network, and an AI librarian that reads every book and flags the sensitive ones.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>AI asset governance:</strong> UC governs not just tables but also ML models registered in MLflow, AI Gateway endpoints, agent definitions, MLflow traces, and Metric Views (governed business KPIs). This is a differentiator against catalogs that cover only tabular data. As organizations deploy more AI workloads, governing the models, their inputs/outputs, and the business metrics they inform becomes as important as governing the data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Automatic lineage:</strong> UC captures runtime data lineage on clusters and SQL warehouses without manual instrumentation. Lineage spans tables, columns, dashboards, workflows, notebooks, Genie Spaces, and ML models. Users see lineage only for objects they are authorized to view.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Recent Governance Milestones (2026)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>ABAC (GA April 2026):</strong> Attribute-Based Access Control uses governed tags to define policies that auto-apply to any asset matching a tag value. One ABAC rule like "users tagged finance_team can access tables tagged pii_level=low" can cover thousands of tables without individual GRANT statements.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Automated Data Classification (GA):</strong> an agentic AI system scans new tables within 24 hours, detects PII, PHI, and financial data, and auto-applies governed tags. This closes the gap between data arriving and data being classified.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Catalog Commits (GA May 2026):</strong> UC becomes the system of coordination across engines. Instead of relying on cloud storage for commit coordination, UC brokers version-controlled access to catalog objects. This enables multi-table atomic transactions and reinforces UC's role as the single governance layer regardless of which engine reads or writes data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Managed Iceberg (GA May 2026):</strong> native Iceberg tables in UC with liquid clustering and predictive optimization. Any Iceberg-compatible engine can read and write through the Iceberg REST Catalog API.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What This Means for You as a Partner SA</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Positioning:</strong> UC is not "just a catalog." It is the governance, interoperability, and coordination layer that makes the lakehouse work. Lead with breadth: security + lineage + sharing + AI governance + open APIs, all in one. UC's open standards foundation (Iceberg REST Catalog API, Delta Sharing Protocol, Hive Metastore API, Apache Polaris compatibility) means customers avoid catalog lock-in while getting unified governance.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Default-on:</strong> all workspaces created after November 2023 have UC enabled by default. Legacy features (DBFS, Hive Metastore, no-isolation shared compute) are disabled by default on accounts created after December 2025.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/rbi" style="color: #2574B5; font-size: 14pt;">Raiffeisen Bank International</a> runs approximately 700 use cases with hundreds of concurrent users across risk, compliance, finance, and retail analytics, all governed by Unity Catalog. Their assessment: "There is no comparable platform in the bank that works at this scale with the level of auditing and governance Databricks provides." &#x25C6;</li>
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
# MAGIC ## B. Unity Catalog Architecture
# MAGIC
# MAGIC Section A established why unified governance matters and what Unity Catalog delivers. This section covers how UC organizes and isolates data through its hierarchical namespace, metastore model, and storage configuration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Three-Level Namespace
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Unity Catalog organizes assets using a hierarchical, three-level namespace: <strong><code>catalog.schema.object</code></strong>. This structure enables domain-level governance, environment isolation, and backward compatibility with the legacy Hive Metastore. Every asset you query, grant access to, or track lineage for lives in this hierarchy.</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-namespace-hierarchy ── -->
# MAGIC <style>
# MAGIC .b1-ns-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .b1-ns-level {
# MAGIC   display: flex;
# MAGIC   align-items: stretch;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .b1-ns-label {
# MAGIC   width: 12%;
# MAGIC   min-width: 100px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: flex-end;
# MAGIC   padding-right: 16px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #5A6F77;
# MAGIC   text-transform: uppercase;
# MAGIC   letter-spacing: 0.05em;
# MAGIC }
# MAGIC .b1-ns-boxes {
# MAGIC   display: flex;
# MAGIC   gap: 8px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .b1-ns-box {
# MAGIC   border-radius: 10px;
# MAGIC   padding: 14px 16px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   flex: 1;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .b1-ns-box:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .b1-ns-meta {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .b1-ns-cat {
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .b1-ns-cat-foreign {
# MAGIC   background: #618794;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .b1-ns-schema {
# MAGIC   background: #2272B4;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .b1-ns-asset {
# MAGIC   background: #F9F7F4;
# MAGIC   color: #333;
# MAGIC   border: 2px solid #DCE0E2;
# MAGIC }
# MAGIC .b1-ns-connector {
# MAGIC   width: 12%;
# MAGIC   min-width: 100px;
# MAGIC }
# MAGIC .b1-ns-example {
# MAGIC   background: #1B3139;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 14px 20px;
# MAGIC   margin-top: 16px;
# MAGIC   font-family: 'Menlo', 'Consolas', monospace;
# MAGIC   font-size: 14pt;
# MAGIC   color: #e0e0e0;
# MAGIC }
# MAGIC .b1-ns-kw { color: #7ecbf5; }
# MAGIC .b1-ns-str { color: #ce9178; }
# MAGIC .b1-ns-fn { color: #dcdcaa; }
# MAGIC </style>
# MAGIC <div class="b1-ns-wrapper">
# MAGIC   <div class="b1-ns-level">
# MAGIC     <div class="b1-ns-label">Metastore</div>
# MAGIC     <div class="b1-ns-boxes">
# MAGIC       <div class="b1-ns-box b1-ns-meta">Metastore (1 per cloud region)</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-ns-level">
# MAGIC     <div class="b1-ns-label">Level 1: Catalog</div>
# MAGIC     <div class="b1-ns-boxes">
# MAGIC       <div class="b1-ns-box b1-ns-cat">Standard Catalog</div>
# MAGIC       <div class="b1-ns-box b1-ns-cat-foreign">Foreign Catalog (Federation)</div>
# MAGIC       <div class="b1-ns-box b1-ns-cat-foreign">Sharing Catalog (Delta Sharing)</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-ns-level">
# MAGIC     <div class="b1-ns-label">Level 2: Schema</div>
# MAGIC     <div class="b1-ns-boxes">
# MAGIC       <div class="b1-ns-box b1-ns-schema">Schema (Database)</div>
# MAGIC       <div class="b1-ns-box b1-ns-schema">Schema</div>
# MAGIC       <div class="b1-ns-box b1-ns-schema">Schema</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-ns-level">
# MAGIC     <div class="b1-ns-label">Level 3: Assets</div>
# MAGIC     <div class="b1-ns-boxes">
# MAGIC       <div class="b1-ns-box b1-ns-asset">Tables</div>
# MAGIC       <div class="b1-ns-box b1-ns-asset">Views</div>
# MAGIC       <div class="b1-ns-box b1-ns-asset">Volumes</div>
# MAGIC       <div class="b1-ns-box b1-ns-asset">Functions</div>
# MAGIC       <div class="b1-ns-box b1-ns-asset">Models</div>
# MAGIC       <div class="b1-ns-box b1-ns-asset">Metric Views</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-ns-example">
# MAGIC     <span class="b1-ns-kw">SELECT</span> * <span class="b1-ns-kw">FROM</span> <span class="b1-ns-fn">retail_prod</span>.<span class="b1-ns-fn">churn</span>.<span class="b1-ns-fn">user_features</span>;
# MAGIC     &nbsp;&nbsp;&nbsp;
# MAGIC     <span style="color: #6A9955;">-- catalog.schema.table</span>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Each Level Explained</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Metastore:</strong> the top-level regional container for all metadata. One per cloud region. Can attach to multiple workspaces. Privilege grants at the metastore level apply across all attached workspaces, but metastore-level privileges do not inherit to child objects.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Catalog (Level 1):</strong> the primary unit of data organization. Groups schemas and enables domain-level governance. Three types: Standard (typical containers), Foreign (via Lakehouse Federation, read-only), and Sharing (via Delta Sharing).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Schema (Level 2):</strong> analogous to a traditional database. Serves as a container for a single use case, project, or team. Supports privilege inheritance to contained objects.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Assets (Level 3):</strong> Tables (managed, external, foreign), Views (standard and dynamic), Volumes (managed and external, for unstructured data), Functions (UDFs, stored procedures), Models (MLflow-registered models with aliases), and Metric Views (governed business KPIs). Note that Connections, External Locations, Storage Credentials, Shares, and Recipients are metastore-level securables that live outside the three-level namespace.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Table Types and Storage</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Managed tables:</strong> UC handles both governance and the underlying file storage lifecycle (auto-compaction, auto-optimize, faster metadata reads). Recommended for new implementations.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>External tables:</strong> UC manages governance only; storage remains at a user-specified path. Reserved for legacy migrations or when external tools need direct file access.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Storage locations</strong> can be set at three levels: Metastore (broadest default), Catalog (recommended), or Schema (most specific). More specific levels override general ones. Databricks recommends assigning managed storage at the catalog level for data isolation.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Hive Metastore Compatibility</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Legacy tables are surfaced as a <code>hive_metastore</code> catalog object for backward compatibility. These tables are read-compatible, but UC access controls and lineage are not supported for them.</li>
# MAGIC           <li style="font-size: 14pt;">Databricks recommends gradual migration from Hive Metastore to UC-managed tables. The <code>hive_metastore</code> catalog provides a bridge during the transition period.</li>
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
# MAGIC ## C. Access Controls and ABAC
# MAGIC
# MAGIC Section B introduced the namespace hierarchy where assets live. This section covers how access to those assets is governed: from privilege grants to fine-grained row/column security, and the newer Attribute-Based Access Control (ABAC) model that scales governance across large data estates.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Centralized Access Controls
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Unity Catalog provides centralized access control through three interfaces: <strong>ANSI SQL DCL</strong> (GRANT/REVOKE), the <strong>Catalog Explorer UI</strong>, and the <strong>REST API</strong>. Privileges follow a hierarchy: reading a table requires USE CATALOG + USE SCHEMA + SELECT. Grants on parent objects automatically apply to all current and future child objects.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-access-controls ── -->
# MAGIC <style>
# MAGIC .c1-ac-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC   display: flex;
# MAGIC   gap: 16px;
# MAGIC }
# MAGIC .c1-ac-card {
# MAGIC   flex: 1;
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .c1-ac-bar {
# MAGIC   padding: 14px 16px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .c1-ac-bar-sql { background: #1B5162; }
# MAGIC .c1-ac-bar-ui { background: #2272B4; }
# MAGIC .c1-ac-bar-api { background: #618794; }
# MAGIC .c1-ac-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 16px;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .c1-ac-code {
# MAGIC   background: #1B3139;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 12px 14px;
# MAGIC   font-family: 'Menlo', 'Consolas', monospace;
# MAGIC   font-size: 14pt;
# MAGIC   color: #e0e0e0;
# MAGIC   line-height: 1.6;
# MAGIC   margin: 8px 0;
# MAGIC }
# MAGIC .c1-kw { color: #7ecbf5; }
# MAGIC .c1-fn { color: #dcdcaa; }
# MAGIC .c1-str { color: #ce9178; }
# MAGIC .c1-ac-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC   margin-top: 8px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="c1-ac-wrapper">
# MAGIC   <div class="c1-ac-card">
# MAGIC     <div class="c1-ac-bar c1-ac-bar-sql">SQL (ANSI DCL)</div>
# MAGIC     <div class="c1-ac-body">
# MAGIC       <div class="c1-ac-code">
# MAGIC         <span class="c1-kw">GRANT</span> <span class="c1-fn">USE CATALOG</span>,<br/>
# MAGIC         &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="c1-fn">USE SCHEMA</span>,<br/>
# MAGIC         &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="c1-fn">SELECT</span><br/>
# MAGIC         <span class="c1-kw">ON CATALOG</span> <span class="c1-str">sales</span><br/>
# MAGIC         <span class="c1-kw">TO</span> <span class="c1-str">finance_team</span>;
# MAGIC       </div>
# MAGIC       <div class="c1-ac-desc">Standard SQL Data Control Language. Grants on a catalog inherit to all schemas and tables in it, including future objects.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c1-ac-card">
# MAGIC     <div class="c1-ac-bar c1-ac-bar-ui">Catalog Explorer (UI)</div>
# MAGIC     <div class="c1-ac-body">
# MAGIC       <div class="c1-ac-code" style="text-align: center; color: #90A5B1;">
# MAGIC         Permissions tab &#x2192;<br/>
# MAGIC         Select group &#x2192;<br/>
# MAGIC         Assign privileges &#x2192;<br/>
# MAGIC         Audit-friendly view
# MAGIC       </div>
# MAGIC       <div class="c1-ac-desc">Group-based access assignments with visual audit trail. Point-and-click management for non-SQL users.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c1-ac-card">
# MAGIC     <div class="c1-ac-bar c1-ac-bar-api">REST API</div>
# MAGIC     <div class="c1-ac-body">
# MAGIC       <div class="c1-ac-code">
# MAGIC         <span class="c1-kw">PATCH</span> /api/2.1/unity-catalog<br/>
# MAGIC         &nbsp;&nbsp;/permissions/<span class="c1-str">catalog</span><br/>
# MAGIC         &nbsp;&nbsp;/<span class="c1-str">sales</span>
# MAGIC       </div>
# MAGIC       <div class="c1-ac-desc">Programmatic ACL management through control plane APIs. Compute is not required. Enables infrastructure-as-code workflows.</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Privilege Model Concepts</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Core privileges:</strong> SELECT (read), MODIFY (write), CREATE TABLE/SCHEMA (create children), run (run functions), READ VOLUME/WRITE VOLUME (access volume files), APPLY TAG (metadata tags).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Usage privileges as gatekeepers:</strong> USE CATALOG and USE SCHEMA are prerequisites. Reading a table requires USE CATALOG + USE SCHEMA + SELECT. This prevents bypassing catalog/schema restrictions even if a user has been granted SELECT on a specific table.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Privilege inheritance:</strong> grants on parent objects automatically apply to all current and future child objects. A catalog-level SELECT grant covers all schemas, tables, views, and volumes in. Exception: metastore-level privileges do not inherit to children.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>BROWSE:</strong> enables metadata discovery without data access. MANAGE: delegates ownership capabilities. ALL PRIVILEGES: implies all applicable privileges (excluding EXTERNAL USE SCHEMA, EXTERNAL USE LOCATION, and MANAGE).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Ownership Model</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Every object has a single owner. Owners have all capabilities on owned objects without explicit privilege grants.</li>
# MAGIC           <li style="font-size: 14pt;">Ownership does not inherit downward: owning a catalog does not mean you own every table in it.</li>
# MAGIC           <li style="font-size: 14pt;">Databricks recommends assigning object ownership to groups rather than individuals, especially for production assets.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Identity Management</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Principals</strong> are users, groups, or service principals synced from your identity provider via SCIM. Groups should be defined in the identity provider rather than created ad hoc in Databricks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Service principals</strong> are recommended for production job automation. They provide a non-human identity that can own objects and receive grants independently of individual user accounts.</li>
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
# MAGIC ### C2. Row-Level Security, Column Masking, and ABAC
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Fine-grained access control goes beyond table-level privileges. Unity Catalog supports <strong>row filters</strong> (filtering which rows a user sees), <strong>column masks</strong> (redacting sensitive values), and <strong>ABAC</strong> (Attribute-Based Access Control) that scales these protections across an entire catalog or schema using governed tags. ABAC reached GA in April 2026 with cross-engine enforcement following in May 2026.</p>
# MAGIC
# MAGIC <!-- ── Visual: c2-abac interactive tabs ── -->
# MAGIC <style>
# MAGIC .c2-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC /* ── Radio inputs hidden ── */
# MAGIC .c2-v-radio { display: none; }
# MAGIC /* ── Tab bar (the 3 steps ARE the tabs) ── */
# MAGIC .c2-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .c2-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .c2-v-tab-num { display: inline-flex; align-items: center; justify-content: center; width: 36px; height: 36px; border-radius: 50%; font-size: 16pt; font-weight: 700; color: #fff; margin-bottom: 4px; }
# MAGIC .c2-v-tn1 { background: #93a8b0; }
# MAGIC .c2-v-tn2 { background: #a4b5c4; }
# MAGIC .c2-v-tn3 { background: #a0c4b0; }
# MAGIC .c2-v-tab-title { font-size: 15pt; font-weight: 700; display: block; }
# MAGIC .c2-v-tab-sub { font-size: 14pt; color: #888; display: block; margin-top: 2px; }
# MAGIC /* ── Panels ── */
# MAGIC .c2-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; min-height: 300px; }
# MAGIC /* ── Tab activation ── */
# MAGIC #c2-t1:checked ~ .c2-v-tabs .c2-v-tab1 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #c2-t1:checked ~ .c2-v-tabs .c2-v-tab1 .c2-v-tn1 { background: #fff; color: #1B5162; }
# MAGIC #c2-t1:checked ~ .c2-v-tabs .c2-v-tab1 .c2-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c2-t1:checked ~ .c2-v-p1 { display: block; border-color: #1B5162; }
# MAGIC #c2-t2:checked ~ .c2-v-tabs .c2-v-tab2 { background: #2574B5; color: #fff; border-color: #2574B5; }
# MAGIC #c2-t2:checked ~ .c2-v-tabs .c2-v-tab2 .c2-v-tn2 { background: #fff; color: #2574B5; }
# MAGIC #c2-t2:checked ~ .c2-v-tabs .c2-v-tab2 .c2-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c2-t2:checked ~ .c2-v-p2 { display: block; border-color: #2574B5; }
# MAGIC #c2-t3:checked ~ .c2-v-tabs .c2-v-tab3 { background: #00A972; color: #fff; border-color: #00A972; }
# MAGIC #c2-t3:checked ~ .c2-v-tabs .c2-v-tab3 .c2-v-tn3 { background: #fff; color: #00A972; }
# MAGIC #c2-t3:checked ~ .c2-v-tabs .c2-v-tab3 .c2-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #c2-t3:checked ~ .c2-v-p3 { display: block; border-color: #00A972; }
# MAGIC /* ── Shared table styles ── */
# MAGIC .c2-v-tbl { width: 100%; border-collapse: collapse; }
# MAGIC .c2-v-tbl th { padding: 10px 12px; font-size: 14pt; font-weight: 600; color: #1B3139; text-align: left; border-bottom: 2px solid #DCE0E2; background: #F9F7F4; }
# MAGIC .c2-v-tbl td { padding: 10px 12px; font-size: 14pt; color: #333; border-bottom: 1px solid #EEEDE9; font-family: 'Menlo','Consolas',monospace; }
# MAGIC .c2-v-tag { display: inline-block; font-size: 14pt; font-weight: 700; padding: 2px 8px; border-radius: 8px; vertical-align: middle; margin-left: 4px; }
# MAGIC .c2-v-tag-pii { background: #98102A; color: #fff; }
# MAGIC .c2-v-tag-dept { background: #1B5162; color: #fff; }
# MAGIC .c2-v-tag-dim { background: rgba(152,16,42,0.10); color: #98102A; }
# MAGIC .c2-v-tag-dim-dept { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC /* ── Highlight columns in tag view ── */
# MAGIC .c2-v-hl-pii { background: rgba(152,16,42,0.06); }
# MAGIC .c2-v-hl-dept { background: rgba(27,81,98,0.06); }
# MAGIC .c2-v-note { font-size: 14pt; color: #618794; text-align: center; margin-top: 16px; font-style: italic; }
# MAGIC /* ── Policy panel ── */
# MAGIC .c2-v-policies { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .c2-v-pol { flex: 1; min-width: 280px; border-radius: 10px; overflow: hidden; border: 2px solid #DCE0E2; }
# MAGIC .c2-v-pol-hdr { padding: 10px 16px; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .c2-v-pol-row { background: #2574B5; }
# MAGIC .c2-v-pol-mask { background: #7B4DB5; }
# MAGIC .c2-v-pol-body { padding: 16px; }
# MAGIC .c2-v-pol-rule { background: #1B3139; border-radius: 6px; padding: 12px 16px; font-family: 'Menlo','Consolas',monospace; font-size: 14pt; color: #e0e0e0; line-height: 1.6; margin-bottom: 12px; }
# MAGIC .c2-v-pol-kw { color: #7ecbf5; }
# MAGIC .c2-v-pol-str { color: #ce9178; }
# MAGIC .c2-v-pol-fn { color: #dcdcaa; }
# MAGIC .c2-v-pol-cmt { color: #6A9955; }
# MAGIC .c2-v-pol-explain { font-size: 14pt; color: #555; line-height: 1.5; }
# MAGIC .c2-v-pol-explain strong { color: #1B3139; }
# MAGIC /* ── Enforce panel ── */
# MAGIC .c2-v-results { display: flex; gap: 14px; flex-wrap: wrap; }
# MAGIC .c2-v-rpanel { flex: 1; min-width: 300px; border-radius: 10px; overflow: hidden; }
# MAGIC .c2-v-rp-full { border: 2px solid #1B5162; }
# MAGIC .c2-v-rp-masked { border: 2px solid #E5A100; }
# MAGIC .c2-v-rp-hdr { padding: 8px 14px; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .c2-v-rph-full { background: #1B5162; }
# MAGIC .c2-v-rph-masked { background: #E5A100; }
# MAGIC .c2-v-rp-sub { font-weight: 400; opacity: 0.85; }
# MAGIC .c2-v-masked { color: #98102A; font-weight: 700; }
# MAGIC .c2-v-filtered { background: repeating-linear-gradient(135deg, #fef3f3, #fef3f3 8px, #fff 8px, #fff 16px); }
# MAGIC .c2-v-filtered td { color: #98102A; font-style: italic; font-family: -apple-system, sans-serif; font-weight: 600; }
# MAGIC .c2-v-why { display: inline-block; font-size: 14pt; font-weight: 700; padding: 2px 7px; border-radius: 8px; background: rgba(152,16,42,0.10); color: #98102A; }
# MAGIC /* ── Scope bar ── */
# MAGIC .c2-v-scope { display: flex; gap: 0; border-radius: 8px; overflow: hidden; border: 1.5px solid #DCE0E2; margin-top: 16px; }
# MAGIC .c2-v-scope-item { flex: 1; display: flex; align-items: center; justify-content: center; gap: 8px; padding: 10px 10px; font-size: 14pt; font-weight: 600; color: #1B3139; background: #F9F7F4; border-right: 1px solid #DCE0E2; }
# MAGIC .c2-v-scope-item:last-child { border-right: none; }
# MAGIC .c2-v-scope-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
# MAGIC .c2-v-badge { font-size: 14pt; font-weight: 700; color: #fff; padding: 2px 7px; border-radius: 8px; }
# MAGIC .c2-v-badge-ga { background: #00A972; }
# MAGIC .c2-v-badge-new { background: #E5A100; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="c2-v-wrap">
# MAGIC <!-- Radio inputs (must be siblings of tabs and panels) -->
# MAGIC <input type="radio" name="c2tabs" id="c2-t1" class="c2-v-radio" checked>
# MAGIC <input type="radio" name="c2tabs" id="c2-t2" class="c2-v-radio">
# MAGIC <input type="radio" name="c2tabs" id="c2-t3" class="c2-v-radio">
# MAGIC
# MAGIC <!-- Tab bar -->
# MAGIC <div class="c2-v-tabs">
# MAGIC   <label for="c2-t1" class="c2-v-tab c2-v-tab1">
# MAGIC     <span class="c2-v-tab-num c2-v-tn1">1</span>
# MAGIC     <span class="c2-v-tab-title">Tag</span>
# MAGIC     <span class="c2-v-tab-sub">Steward or AI auto-scan</span>
# MAGIC   </label>
# MAGIC   <label for="c2-t2" class="c2-v-tab c2-v-tab2">
# MAGIC     <span class="c2-v-tab-num c2-v-tn2">2</span>
# MAGIC     <span class="c2-v-tab-title">Policy</span>
# MAGIC     <span class="c2-v-tab-sub">Governance admin creates rules</span>
# MAGIC   </label>
# MAGIC   <label for="c2-t3" class="c2-v-tab c2-v-tab3">
# MAGIC     <span class="c2-v-tab-num c2-v-tn3">3</span>
# MAGIC     <span class="c2-v-tab-title">Enforce</span>
# MAGIC     <span class="c2-v-tab-sub">Automatic at query time</span>
# MAGIC   </label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 1: TAG — source table with tags highlighted -->
# MAGIC <div class="c2-v-panel c2-v-p1">
# MAGIC   <div style="font-size: 14pt; font-weight: 700; color: #1B5162; margin-bottom: 12px;">hr.employees.profiles: governed tags applied to columns</div>
# MAGIC   <table class="c2-v-tbl">
# MAGIC     <tr>
# MAGIC       <th>Name</th>
# MAGIC       <th class="c2-v-hl-pii">SSN <span class="c2-v-tag c2-v-tag-pii">pii:ssn</span></th>
# MAGIC       <th class="c2-v-hl-pii">Salary <span class="c2-v-tag c2-v-tag-pii">pii:financial</span></th>
# MAGIC       <th class="c2-v-hl-dept">Dept <span class="c2-v-tag c2-v-tag-dept">dept</span></th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Alice Chen</td>
# MAGIC       <td class="c2-v-hl-pii">123-45-6789 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-pii">$142,000 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-dept">Engineering <span class="c2-v-tag c2-v-tag-dim-dept">dept</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Bob Patel</td>
# MAGIC       <td class="c2-v-hl-pii">987-65-4321 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-pii">$128,500 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-dept">Finance <span class="c2-v-tag c2-v-tag-dim-dept">dept</span></td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Carol Wu</td>
# MAGIC       <td class="c2-v-hl-pii">555-12-9876 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-pii">$156,000 <span class="c2-v-tag c2-v-tag-dim">pii</span></td>
# MAGIC       <td class="c2-v-hl-dept">Engineering <span class="c2-v-tag c2-v-tag-dim-dept">dept</span></td>
# MAGIC     </tr>
# MAGIC   </table>
# MAGIC   <div class="c2-v-note">Tags are applied by data stewards or detected automatically by AI Classification (12+ PII types, scanned within 24 hours). Tags on columns inherit from governed tag definitions at the account level.</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 2: POLICY — the two policy rules -->
# MAGIC <div class="c2-v-panel c2-v-p2">
# MAGIC   <div class="c2-v-policies">
# MAGIC     <div class="c2-v-pol">
# MAGIC       <div class="c2-v-pol-hdr c2-v-pol-row">Row Filter Policy</div>
# MAGIC       <div class="c2-v-pol-body">
# MAGIC         <div class="c2-v-pol-rule">
# MAGIC           <span class="c2-v-pol-kw">CREATE POLICY</span> <span class="c2-v-pol-fn">dept_filter</span><br>
# MAGIC           <span class="c2-v-pol-kw">ON CATALOG</span> <span class="c2-v-pol-str">hr</span><br>
# MAGIC           <span class="c2-v-pol-kw">ROW FILTER</span> <span class="c2-v-pol-fn">filter_by_dept</span><br>
# MAGIC           <span class="c2-v-pol-kw">TO</span> <span class="c2-v-pol-str">`account users`</span><br>
# MAGIC           &nbsp;&nbsp;<span class="c2-v-pol-kw">EXCEPT</span> <span class="c2-v-pol-str">`compliance team`</span><br>
# MAGIC           <span class="c2-v-pol-kw">FOR TABLES</span><br>
# MAGIC           <span class="c2-v-pol-kw">MATCH COLUMNS</span> <span class="c2-v-pol-fn">has_tag</span>(<span class="c2-v-pol-str">'dept'</span>);
# MAGIC         </div>
# MAGIC         <div class="c2-v-pol-explain"><strong>Effect:</strong> users only see rows where the dept column matches their own department. The compliance team is exempt and sees all rows.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="c2-v-pol">
# MAGIC       <div class="c2-v-pol-hdr c2-v-pol-mask">Column Mask Policy</div>
# MAGIC       <div class="c2-v-pol-body">
# MAGIC         <div class="c2-v-pol-rule">
# MAGIC           <span class="c2-v-pol-kw">CREATE POLICY</span> <span class="c2-v-pol-fn">mask_pii</span><br>
# MAGIC           <span class="c2-v-pol-kw">ON CATALOG</span> <span class="c2-v-pol-str">hr</span><br>
# MAGIC           <span class="c2-v-pol-kw">COLUMN MASK</span> <span class="c2-v-pol-fn">redact_value</span><br>
# MAGIC           <span class="c2-v-pol-kw">TO</span> <span class="c2-v-pol-str">`account users`</span><br>
# MAGIC           &nbsp;&nbsp;<span class="c2-v-pol-kw">EXCEPT</span> <span class="c2-v-pol-str">`compliance team`</span><br>
# MAGIC           <span class="c2-v-pol-kw">FOR TABLES</span><br>
# MAGIC           <span class="c2-v-pol-kw">MATCH COLUMNS</span> <span class="c2-v-pol-fn">has_tag</span>(<span class="c2-v-pol-str">'pii'</span>);
# MAGIC         </div>
# MAGIC         <div class="c2-v-pol-explain"><strong>Effect:</strong> any column tagged <strong>pii</strong> across all tables in the hr catalog is masked for non-exempt users. One policy protects every current and future PII column.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 3: ENFORCE — same query, two views -->
# MAGIC <div class="c2-v-panel c2-v-p3">
# MAGIC   <div style="font-size: 14pt; font-weight: 700; color: #00A972; margin-bottom: 12px;">Same query, same table, different results based on who you are</div>
# MAGIC   <div class="c2-v-results">
# MAGIC     <div class="c2-v-rpanel c2-v-rp-full">
# MAGIC       <div class="c2-v-rp-hdr c2-v-rph-full">Compliance Team <span class="c2-v-rp-sub">&#x2714; exempt</span></div>
# MAGIC       <table class="c2-v-tbl">
# MAGIC         <tr><th>Name</th><th>SSN</th><th>Salary</th><th>Dept</th></tr>
# MAGIC         <tr><td>Alice Chen</td><td>123-45-6789</td><td>$142,000</td><td>Engineering</td></tr>
# MAGIC         <tr><td>Bob Patel</td><td>987-65-4321</td><td>$128,500</td><td>Finance</td></tr>
# MAGIC         <tr><td>Carol Wu</td><td>555-12-9876</td><td>$156,000</td><td>Engineering</td></tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC     <div class="c2-v-rpanel c2-v-rp-masked">
# MAGIC       <div class="c2-v-rp-hdr c2-v-rph-masked">Engineering Analyst <span class="c2-v-rp-sub">policy applied</span></div>
# MAGIC       <table class="c2-v-tbl">
# MAGIC         <tr><th>Name</th><th>SSN</th><th>Salary</th><th>Dept</th></tr>
# MAGIC         <tr><td>Alice Chen</td><td class="c2-v-masked">***-**-6789</td><td class="c2-v-masked">$***,***</td><td>Engineering</td></tr>
# MAGIC         <tr class="c2-v-filtered"><td>Bob Patel</td><td>filtered</td><td></td><td>Finance <span class="c2-v-why">dept != Engineering</span></td></tr>
# MAGIC         <tr><td>Carol Wu</td><td class="c2-v-masked">***-**-9876</td><td class="c2-v-masked">$***,***</td><td>Engineering</td></tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c2-v-scope">
# MAGIC     <div class="c2-v-scope-item"><div class="c2-v-scope-dot" style="background:#1B5162;"></div> Databricks Compute <span class="c2-v-badge c2-v-badge-ga">GA</span></div>
# MAGIC     <div class="c2-v-scope-item"><div class="c2-v-scope-dot" style="background:#E5A100;"></div> External Engines (IRC) <span class="c2-v-badge c2-v-badge-new">May 2026</span></div>
# MAGIC     <div class="c2-v-scope-item"><div class="c2-v-scope-dot" style="background:#00A972;"></div> Delta Sharing <span class="c2-v-badge c2-v-badge-ga">GA</span></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Row-Level Security and Column Masking (Per-Table)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Row filters:</strong> SQL UDFs that return TRUE/FALSE for each row. Applied via <code>ALTER TABLE ... SET ROW FILTER</code>. Use the <code>IS_MEMBER</code> function to filter based on group membership.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Column masks:</strong> SQL UDFs that transform column values. Applied via <code>ALTER TABLE ... ALTER COLUMN ... SET MASK</code>. Can nullify, redact, or hash sensitive values.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Per-table limitation:</strong> without ABAC, these filters and masks must be applied table by table. In a data estate with hundreds of tables containing PII, this approach becomes manual, slow, and inconsistent.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ABAC: Governance at Scale</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>ABAC GA (April 2026):</strong> ABAC uses governed tags to dynamically enforce row filter and column mask policies, replacing per-table security configurations. One policy on a catalog can protect thousands of tables. Think of it like an airport security system: instead of posting a guard at every gate, you issue a badge (tag) and the scanners (policies) enforce access everywhere automatically.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Session user identity model (April 2026):</strong> policies now evaluate against the identity of the person running the query, not the view or function owner. This closed a loophole where views could bypass row filters.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cross-engine enforcement (May 2026):</strong> external engines reading UC managed tables via Iceberg REST now have ABAC row filters and column masks enforced server-side. The protection travels with the data regardless of which engine queries it.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Delta Sharing + ABAC (GA):</strong> providers can share ABAC-protected tables directly without creating separate filtered copies. Recipients can also apply their own local ABAC policies on shared data, with enforcement guaranteed at query time.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Data Classification (GA April 2026):</strong> an agentic AI system scans new tables within 24 hours, detects 12+ types of PII/PHI/financial data, and auto-applies governed tags. This closes the gap between data arriving and data being protected by ABAC.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Current Limitations</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Two policy types today (row filter and column mask). Grant/Deny policies are expected in a future release.</li>
# MAGIC           <li style="font-size: 14pt;">Cannot apply ABAC policies directly to views (but policies on underlying tables are respected when queried through views).</li>
# MAGIC           <li style="font-size: 14pt;">Time travel and CLONE operations are not supported on tables with active row filters or column masks. Principals needing these operations must be exempted via EXCEPT clauses.</li>
# MAGIC           <li style="font-size: 14pt;">Requires serverless compute or DBR 16.4+ on standard/dedicated compute.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/block/unity-catalog" style="color: #2574B5; font-size: 14pt;">Block</a> configured sub-group level access permissions in a single location, allowing data owners to make governance decisions rather than a centralized team imposing policies. Data sharing time dropped from days to seconds. &#x25C6;</li>
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
# MAGIC ## D. Lakehouse Federation
# MAGIC
# MAGIC Sections B and C covered how Unity Catalog organizes and secures assets in its own namespace. But data does not live in a single system. Lakehouse Federation extends UC governance to data that remains in external catalogs and databases, without requiring data migration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Querying External Data Sources
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Lakehouse Federation enables queries against external data sources without migration. It has two mechanisms with a critical difference: <strong>where does compute run?</strong> Catalog Federation reads foreign tables from object storage using Databricks compute. Query Federation pushes SQL via JDBC to the remote database. In both cases, Unity Catalog enforces access controls, captures lineage, and logs audit events.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-federation-comparison ── -->
# MAGIC <style>
# MAGIC .d1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC /* ── Two-column comparison ── */
# MAGIC .d1-v-cols { display: flex; gap: 16px; flex-wrap: wrap; }
# MAGIC .d1-v-col { flex: 1; min-width: 340px; border-radius: 10px; overflow: hidden; border: 2px solid #DCE0E2; }
# MAGIC .d1-v-col-hdr { padding: 12px 16px; font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .d1-v-col-hdr-cat { background: #1B5162; }
# MAGIC .d1-v-col-hdr-qry { background: #2574B5; }
# MAGIC .d1-v-col-body { padding: 16px; }
# MAGIC /* ── Flow diagrams ── */
# MAGIC .d1-v-flow { display: flex; align-items: center; gap: 6px; margin-bottom: 14px; justify-content: center; }
# MAGIC .d1-v-flow-box { padding: 8px 14px; border-radius: 8px; font-size: 14pt; font-weight: 600; text-align: center; }
# MAGIC .d1-v-flow-src { background: #F9F7F4; border: 1.5px solid #DCE0E2; color: #1B3139; }
# MAGIC .d1-v-flow-uc { background: linear-gradient(135deg, #1B3139, #1B5162); color: #fff; }
# MAGIC .d1-v-flow-store { background: #EEEDE9; border: 1.5px dashed #90A5B1; color: #5A6F77; }
# MAGIC .d1-v-flow-db { background: #F9F7F4; border: 1.5px solid #2574B5; color: #2574B5; }
# MAGIC .d1-v-flow-arrow { font-size: 18pt; color: #94b3be; }
# MAGIC .d1-v-flow-label { font-size: 14pt; color: #618794; text-align: center; margin-bottom: 12px; font-style: italic; }
# MAGIC /* ── Source pills ── */
# MAGIC .d1-v-src-label { font-size: 14pt; font-weight: 700; color: #5A6F77; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
# MAGIC .d1-v-pills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
# MAGIC .d1-v-pill { font-size: 14pt; font-weight: 600; padding: 4px 12px; border-radius: 12px; }
# MAGIC .d1-v-pill-cat { background: rgba(27,81,98,0.10); color: #1B5162; }
# MAGIC .d1-v-pill-qry { background: rgba(37,116,181,0.10); color: #2574B5; }
# MAGIC .d1-v-pill-both { background: rgba(229,161,0,0.12); color: #8a6200; border: 1.5px dashed #E5A100; }
# MAGIC /* ── Key characteristics ── */
# MAGIC .d1-v-chars { margin-top: 10px; }
# MAGIC .d1-v-char { display: flex; align-items: flex-start; gap: 8px; font-size: 14pt; color: #333; margin-bottom: 6px; }
# MAGIC .d1-v-char-dot { width: 8px; height: 8px; border-radius: 50%; margin-top: 7px; flex-shrink: 0; }
# MAGIC .d1-v-char-dot-cat { background: #1B5162; }
# MAGIC .d1-v-char-dot-qry { background: #2574B5; }
# MAGIC /* ── Shared governance bar ── */
# MAGIC .d1-v-gov { background: linear-gradient(135deg, #1B3139, #1B5162); border-radius: 10px; padding: 12px 20px; margin-top: 14px; display: flex; align-items: center; justify-content: center; gap: 24px; flex-wrap: wrap; }
# MAGIC .d1-v-gov-item { display: flex; align-items: center; gap: 6px; color: #fff; font-size: 14pt; font-weight: 600; }
# MAGIC .d1-v-gov-check { color: #00A972; font-size: 14pt; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="d1-v-wrap">
# MAGIC   <div class="d1-v-cols">
# MAGIC     <!-- Catalog Federation -->
# MAGIC     <div class="d1-v-col">
# MAGIC       <div class="d1-v-col-hdr d1-v-col-hdr-cat">Catalog Federation</div>
# MAGIC       <div class="d1-v-col-body">
# MAGIC         <div class="d1-v-flow">
# MAGIC           <div class="d1-v-flow-box d1-v-flow-src">Foreign Catalog</div>
# MAGIC           <span class="d1-v-flow-arrow">&#x279C;</span>
# MAGIC           <div class="d1-v-flow-box d1-v-flow-store">Object Storage<br><span style="font-size:14pt;font-weight:400;">(S3 / ADLS / GCS)</span></div>
# MAGIC           <span class="d1-v-flow-arrow">&#x279C;</span>
# MAGIC           <div class="d1-v-flow-box d1-v-flow-uc">Databricks<br>Compute</div>
# MAGIC         </div>
# MAGIC         <div class="d1-v-flow-label">Metadata from foreign catalog, data read directly from cloud storage</div>
# MAGIC         <div class="d1-v-src-label">Supported catalogs</div>
# MAGIC         <div class="d1-v-pills">
# MAGIC           <span class="d1-v-pill d1-v-pill-cat">AWS Glue</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-cat">External Hive</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-cat">Legacy Hive</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-both">Snowflake (Iceberg only)</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-cat">Salesforce Data 360</span>
# MAGIC         </div>
# MAGIC         <div class="d1-v-chars">
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-cat"></div> Compute runs on Databricks (faster, lower cost)</div>
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-cat"></div> Foreign catalogs are read-only in Databricks</div>
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-cat"></div> Best for: incremental migration, long-term hybrid</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Query Federation -->
# MAGIC     <div class="d1-v-col">
# MAGIC       <div class="d1-v-col-hdr d1-v-col-hdr-qry">Query Federation</div>
# MAGIC       <div class="d1-v-col-body">
# MAGIC         <div class="d1-v-flow">
# MAGIC           <div class="d1-v-flow-box d1-v-flow-uc">Databricks</div>
# MAGIC           <span class="d1-v-flow-arrow">&#x279C;</span>
# MAGIC           <div class="d1-v-flow-box" style="font-size:14pt;font-weight:600;color:#2574B5;">JDBC</div>
# MAGIC           <span class="d1-v-flow-arrow">&#x279C;</span>
# MAGIC           <div class="d1-v-flow-box d1-v-flow-db">Remote<br>Database</div>
# MAGIC         </div>
# MAGIC         <div class="d1-v-flow-label">Query pushed to remote database, executed on remote compute</div>
# MAGIC         <div class="d1-v-src-label">Supported databases</div>
# MAGIC         <div class="d1-v-pills">
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">PostgreSQL</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">MySQL</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">SQL Server</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Oracle</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Teradata</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">MariaDB</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Amazon Redshift</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Azure Synapse</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Google BigQuery</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-both">Snowflake (non-Iceberg)</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Salesforce Data 360</span>
# MAGIC           <span class="d1-v-pill d1-v-pill-qry">Databricks</span>
# MAGIC         </div>
# MAGIC         <div class="d1-v-chars">
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-qry"></div> Compute runs on the remote database</div>
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-qry"></div> Join pushdown GA for Redshift, Snowflake, BigQuery</div>
# MAGIC           <div class="d1-v-char"><div class="d1-v-char-dot d1-v-char-dot-qry"></div> Best for: ad hoc reporting, operational data access</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- UC governance applies to both -->
# MAGIC   <div class="d1-v-gov">
# MAGIC     <div class="d1-v-gov-item"><span class="d1-v-gov-check">&#x2714;</span> UC Access Controls</div>
# MAGIC     <div class="d1-v-gov-item"><span class="d1-v-gov-check">&#x2714;</span> Automatic Lineage</div>
# MAGIC     <div class="d1-v-gov-item"><span class="d1-v-gov-check">&#x2714;</span> Audit Logging</div>
# MAGIC     <div class="d1-v-gov-item"><span class="d1-v-gov-check">&#x2714;</span> SQL Syntax: <code style="color:#a8c9d6;background:rgba(255,255,255,0.1);padding:2px 6px;border-radius:4px;">catalog.schema.table</code></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Catalog Federation vs. Query Federation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Catalog Federation:</strong> UC crawls an external catalog (AWS Glue, External Hive, Legacy Hive, Salesforce Data 360, or Snowflake) to populate a foreign catalog. Queries run on Databricks compute reading directly from object storage. More cost-effective and performant. Ideal for incremental migrations or long-term hybrid models. <strong>Snowflake catalog federation reads only Snowflake-managed Iceberg tables</strong>; non-Iceberg Snowflake tables are not eligible and must use query federation instead.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Query Federation:</strong> UC pushes queries via JDBC to external databases (PostgreSQL, MySQL, SQL Server, Oracle, Teradata, MariaDB, Amazon Redshift, Azure Synapse, Google BigQuery, Snowflake, Salesforce Data 360, Databricks). Queries run on remote compute. Join pushdown is GA for Redshift, Snowflake, and BigQuery. Ideal for ad hoc reporting or POC scenarios accessing operational data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Foreign catalogs are read-only</strong> in Databricks, except for internal Hive metastore federation which supports both reads and writes with synchronized metadata updates.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Query Lifecycle (File-Based Sources)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">1. User sends query (SQL, Python, R, Scala)</li>
# MAGIC           <li style="font-size: 14pt;">2. Cluster checks namespace, metadata, and grants via UC control plane</li>
# MAGIC           <li style="font-size: 14pt;">3. UC assumes IAM Role / Managed Identity / Service Account</li>
# MAGIC           <li style="font-size: 14pt;">4. UC generates short-lived, scoped-down credentials (presigned URLs for S3, SAS tokens for ADLS)</li>
# MAGIC           <li style="font-size: 14pt;">5. Cluster requests data from cloud storage with temporary tokens</li>
# MAGIC           <li style="font-size: 14pt;">6. Data returned to cluster</li>
# MAGIC           <li style="font-size: 14pt;">7. Policy enforcement (row-level filters, column-level masking) applied on the compute layer</li>
# MAGIC           <li style="font-size: 14pt;">8. Results delivered to user</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Governance Over Federated Data</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">UC grants apply to foreign tables. Unity Catalog tracks lineage for foreign catalog tables and logs all access for auditing.</li>
# MAGIC           <li style="font-size: 14pt;">Use <code>REFRESH FOREIGN CATALOG</code>, <code>REFRESH FOREIGN SCHEMA</code>, or <code>REFRESH FOREIGN TABLE</code> to synchronize metadata changes from the external source.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/kpmg/unity-catalog" style="color: #2574B5; font-size: 14pt;">KPMG</a> uses Lakehouse Federation to federate data from on-premises SQL servers, cloud data warehouses, and Azure Data Lake Storage, providing their 850+ users a single place to administer data access policies across all workspaces. &#x25C6;</li>
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
# MAGIC ## E. Delta Sharing and Marketplace
# MAGIC
# MAGIC Lakehouse Federation (Section D) brings external data into UC for querying. Delta Sharing solves the complementary problem: sharing your data with external organizations securely, without copying it. This section covers the sharing protocol, Databricks Marketplace, and Clean Rooms.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Delta Sharing Protocol
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Delta Sharing is an open protocol developed by Databricks for secure data sharing across organizations regardless of computing platform. Part of the Linux Foundation, it uses <strong>presigned URLs</strong> to provide temporary access to files in object storage, meaning no data is replicated. In-region sharing incurs no egress costs.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-delta-sharing-arch ── -->
# MAGIC <style>
# MAGIC .e1-ds-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .e1-ds-flow {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   gap: 0;
# MAGIC }
# MAGIC .e1-ds-block {
# MAGIC   border-radius: 12px;
# MAGIC   padding: 18px 16px;
# MAGIC   text-align: center;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   gap: 8px;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .e1-ds-block:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .e1-ds-provider {
# MAGIC   flex: 1;
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .e1-ds-server {
# MAGIC   flex: 0 0 180px;
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .e1-ds-recipients {
# MAGIC   flex: 1;
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #DCE0E2;
# MAGIC   color: #333;
# MAGIC }
# MAGIC .e1-ds-title {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .e1-ds-sub {
# MAGIC   font-size: 14pt;
# MAGIC   opacity: 0.85;
# MAGIC }
# MAGIC .e1-ds-arrow {
# MAGIC   font-size: 20pt;
# MAGIC   color: #90A5B1;
# MAGIC   padding: 0 10px;
# MAGIC }
# MAGIC .e1-ds-badges {
# MAGIC   display: flex;
# MAGIC   gap: 6px;
# MAGIC   justify-content: center;
# MAGIC   flex-wrap: wrap;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .e1-ds-badge {
# MAGIC   background: rgba(0,0,0,0.1);
# MAGIC   border-radius: 12px;
# MAGIC   padding: 4px 10px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC }
# MAGIC .e1-ds-badge-light {
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 4px 10px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #5A6F77;
# MAGIC }
# MAGIC .e1-ds-comparison {
# MAGIC   display: flex;
# MAGIC   gap: 16px;
# MAGIC   margin-top: 20px;
# MAGIC }
# MAGIC .e1-ds-comp-card {
# MAGIC   flex: 1;
# MAGIC   border-radius: 8px;
# MAGIC   overflow: hidden;
# MAGIC   border: 2px solid #DCE0E2;
# MAGIC }
# MAGIC .e1-ds-comp-header {
# MAGIC   padding: 12px 14px;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .e1-ds-comp-h1 { background: #618794; }
# MAGIC .e1-ds-comp-h2 { background: #1B5162; }
# MAGIC .e1-ds-comp-body {
# MAGIC   padding: 12px 14px;
# MAGIC   background: #F9F7F4;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC }
# MAGIC .e1-ds-comp-body ul {
# MAGIC   margin: 4px 0 0 16px;
# MAGIC   padding: 0;
# MAGIC }
# MAGIC .e1-ds-comp-body li {
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="e1-ds-wrapper">
# MAGIC   <div class="e1-ds-flow">
# MAGIC     <div class="e1-ds-block e1-ds-provider">
# MAGIC       <div class="e1-ds-title">Data Provider</div>
# MAGIC       <div class="e1-ds-sub">UC-enabled workspace</div>
# MAGIC       <div class="e1-ds-badges">
# MAGIC         <span class="e1-ds-badge">Delta Lake</span>
# MAGIC         <span class="e1-ds-badge">Tables</span>
# MAGIC         <span class="e1-ds-badge">Views</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="e1-ds-arrow">&#x25B6;</div>
# MAGIC     <div class="e1-ds-block e1-ds-server">
# MAGIC       <div class="e1-ds-title">Delta Sharing Server</div>
# MAGIC       <div class="e1-ds-sub">Presigned URLs</div>
# MAGIC       <div class="e1-ds-sub">No data replication</div>
# MAGIC     </div>
# MAGIC     <div class="e1-ds-arrow">&#x25B6;</div>
# MAGIC     <div class="e1-ds-block e1-ds-recipients">
# MAGIC       <div class="e1-ds-title" style="color: #0b2026;">Data Recipients</div>
# MAGIC       <div class="e1-ds-badges">
# MAGIC         <span class="e1-ds-badge-light">Databricks</span>
# MAGIC         <span class="e1-ds-badge-light">Spark</span>
# MAGIC         <span class="e1-ds-badge-light">Pandas</span>
# MAGIC         <span class="e1-ds-badge-light">Power BI</span>
# MAGIC         <span class="e1-ds-badge-light">Any Platform</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="e1-ds-comparison">
# MAGIC     <div class="e1-ds-comp-card">
# MAGIC       <div class="e1-ds-comp-header e1-ds-comp-h1">Open Sharing</div>
# MAGIC       <div class="e1-ds-comp-body">
# MAGIC         <ul>
# MAGIC           <li>Recipients do <strong>not</strong> need Databricks</li>
# MAGIC           <li>Bearer tokens or OIDC federation</li>
# MAGIC           <li>Tabular data only</li>
# MAGIC           <li>Multi-cloud, multi-tool</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="e1-ds-comp-card">
# MAGIC       <div class="e1-ds-comp-header e1-ds-comp-h2">Databricks-to-Databricks</div>
# MAGIC       <div class="e1-ds-comp-body">
# MAGIC         <ul>
# MAGIC           <li>Both parties need UC-enabled workspaces</li>
# MAGIC           <li>No token management required</li>
# MAGIC           <li>Tables + views + volumes + notebooks + models</li>
# MAGIC           <li>In-region: no egress cost</li>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Open Sharing</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Who it is for:</strong> recipients who do not have Databricks. Any client that can read Parquet files (Spark, pandas, Power BI, Tableau, Java, Go) can consume shared data.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Authentication:</strong> bearer tokens (long-lived, max 1 year expiration as of December 2025) or OIDC federation (short-lived OAuth tokens, no credential management). IP address filtering can restrict recipient network access.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Asset scope:</strong> tabular data only (tables and views). Volumes, notebooks, and models require D2D sharing.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">D2D Sharing Workflow</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Provider side:</strong> (1) Create a share containing tables, views, volumes, notebooks, or models. (2) Create a recipient object using the recipient's sharing identifier from their metastore. (3) Grant the recipient access to the share.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Recipient side:</strong> (4) Receive share notification. (5) Create a catalog from the share (requires metastore admin or delegated privilege). (6) Grant access to other users. (7) Access shared data via Catalog Explorer, CLI, or SQL.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>History sharing:</strong> when enabled (default in DBR 16.2+ for new shares), performance is comparable to direct table access. This uses temporary scoped cloud storage credentials rather than presigned URLs, which is the newer and recommended mechanism for D2D sharing.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Security and Cost Model</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Presigned URLs</strong> provide temporary access to specific files in object storage. They are short-lived, scoped to specific data, and can be revoked centrally.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Token lifecycle:</strong> providers control token lifetime, can apply network-level access controls, and can revoke on demand.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cost:</strong> compute charged by Databricks (varies by recipient type). Storage and egress charged by the cloud vendor. In-region D2D sharing incurs no egress fees.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Real-World Sharing Use Cases</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/coastal-community-bank" style="color: #2574B5; font-size: 14pt;">Coastal Community Bank</a> uses Delta Sharing to power its Banking-as-a-Service partner ecosystem. They scaled from 40,000 to approximately 6 million customers and achieved 12x faster partner onboarding, reducing new data source ingestion from 1-2 months to 2 days. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/dc-octo" style="color: #2574B5; font-size: 14pt;">DC OCTO</a> uses Delta Sharing to share data across six government agencies for education-to-workforce records linkage, achieving a 50% cost reduction while maintaining governance and avoiding data duplication. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/kraken/utility-data-unity-catalog" style="color: #2574B5; font-size: 14pt;">Kraken</a> reduced time to share data with new utility clients from 1.5 weeks to 1 day, governing data across dozens of isolated client environments. &#x25C6;</li>
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
# MAGIC ### E2. Marketplace and Clean Rooms
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Delta Sharing is the foundation for two collaboration tools. <strong>Databricks Marketplace</strong> is an open exchange where providers publish data products for discovery and integration. <strong>Clean Rooms</strong> provide a privacy-preserving environment where multiple parties collaborate on sensitive data without seeing each other's raw data. Click each tab to explore.</p>
# MAGIC
# MAGIC <!-- ── Visual: e2-marketplace-cleanroom tabs ── -->
# MAGIC <style>
# MAGIC .e2-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .e2-v-radio { display: none; }
# MAGIC /* ── Tab bar ── */
# MAGIC .e2-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .e2-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .e2-v-tab-title { font-size: 16pt; font-weight: 700; display: block; }
# MAGIC .e2-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC /* ── Panels ── */
# MAGIC .e2-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC /* ── Activation ── */
# MAGIC #e2-t1:checked ~ .e2-v-tabs .e2-v-tab1 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #e2-t1:checked ~ .e2-v-tabs .e2-v-tab1 .e2-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #e2-t1:checked ~ .e2-v-p1 { display: block; border-color: #1B5162; }
# MAGIC #e2-t2:checked ~ .e2-v-tabs .e2-v-tab2 { background: #2574B5; color: #fff; border-color: #2574B5; }
# MAGIC #e2-t2:checked ~ .e2-v-tabs .e2-v-tab2 .e2-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #e2-t2:checked ~ .e2-v-p2 { display: block; border-color: #2574B5; }
# MAGIC /* ── Key metric banner ── */
# MAGIC .e2-v-metric { text-align: center; padding: 16px; border-radius: 10px; margin-bottom: 20px; }
# MAGIC .e2-v-metric-val { font-size: 22pt; font-weight: 800; color: #fff; }
# MAGIC .e2-v-metric-label { font-size: 14pt; color: rgba(255,255,255,0.85); margin-top: 4px; }
# MAGIC .e2-v-km-teal { background: linear-gradient(135deg, #1B5162, #2a7a94); }
# MAGIC .e2-v-km-blue { background: linear-gradient(135deg, #2574B5, #3a9ad9); }
# MAGIC /* ── Feature cards ── */
# MAGIC .e2-v-cards { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 16px; }
# MAGIC .e2-v-card { flex: 1; min-width: 200px; padding: 16px; border-radius: 10px; border: 1.5px solid #DCE0E2; background: #fff; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .e2-v-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .e2-v-card-teal { border-left: 4px solid #1B5162; }
# MAGIC .e2-v-card-blue { border-left: 4px solid #2574B5; }
# MAGIC .e2-v-card h4 { font-size: 14pt; margin-bottom: 6px; }
# MAGIC .e2-v-card-teal h4 { color: #1B5162; }
# MAGIC .e2-v-card-blue h4 { color: #2574B5; }
# MAGIC .e2-v-card p { font-size: 14pt; line-height: 1.5; color: #555; }
# MAGIC .e2-v-badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 14pt; font-weight: 700; color: #fff; margin-bottom: 8px; }
# MAGIC .e2-v-badge-teal { background: #1B5162; }
# MAGIC .e2-v-badge-blue { background: #2574B5; }
# MAGIC /* ── Flow diagram for Clean Rooms ── */
# MAGIC .e2-v-flow { display: flex; align-items: center; justify-content: center; gap: 6px; margin-bottom: 16px; flex-wrap: wrap; }
# MAGIC .e2-v-flow-box { padding: 10px 16px; border-radius: 8px; font-size: 14pt; font-weight: 600; text-align: center; }
# MAGIC .e2-v-flow-party { background: #F9F7F4; border: 1.5px solid #2574B5; color: #2574B5; }
# MAGIC .e2-v-flow-cr { background: linear-gradient(135deg, #1B3139, #2574B5); color: #fff; padding: 14px 20px; }
# MAGIC .e2-v-flow-arrow { font-size: 18pt; color: #94b3be; }
# MAGIC .e2-v-flow-label { font-size: 14pt; color: #618794; text-align: center; font-style: italic; margin-bottom: 14px; }
# MAGIC /* ── Footer ── */
# MAGIC .e2-v-footer { background: #F9F7F4; border: 1.5px solid #DCE0E2; border-radius: 8px; padding: 10px 16px; text-align: center; font-size: 14pt; color: #5A6F77; font-weight: 600; }
# MAGIC /* ── Info callout ── */
# MAGIC .e2-v-info { border-left: 4px solid #E5A100; background: #fffbf0; padding: 12px 16px; border-radius: 4px; margin-top: 14px; font-size: 14pt; color: #333; }
# MAGIC .e2-v-info strong { color: #8a6200; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="e2-v-wrap">
# MAGIC <input type="radio" name="e2tabs" id="e2-t1" class="e2-v-radio" checked>
# MAGIC <input type="radio" name="e2tabs" id="e2-t2" class="e2-v-radio">
# MAGIC
# MAGIC <div class="e2-v-tabs">
# MAGIC   <label for="e2-t1" class="e2-v-tab e2-v-tab1">
# MAGIC     <span class="e2-v-tab-title">Databricks Marketplace</span>
# MAGIC     <span class="e2-v-tab-sub">Open exchange for data and AI products</span>
# MAGIC   </label>
# MAGIC   <label for="e2-t2" class="e2-v-tab e2-v-tab2">
# MAGIC     <span class="e2-v-tab-title">Clean Rooms</span>
# MAGIC     <span class="e2-v-tab-sub">Privacy-safe multi-party collaboration</span>
# MAGIC   </label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 1: Marketplace -->
# MAGIC <div class="e2-v-panel e2-v-p1">
# MAGIC   <div class="e2-v-metric e2-v-km-teal">
# MAGIC     <div class="e2-v-metric-val">4 Shareable Product Types</div>
# MAGIC     <div class="e2-v-metric-label">An app store for data: browse, request, and install directly into your workspace</div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-cards">
# MAGIC     <div class="e2-v-card e2-v-card-teal">
# MAGIC       <span class="e2-v-badge e2-v-badge-teal">Data</span>
# MAGIC       <h4>Datasets</h4>
# MAGIC       <p>Tables and volumes shared via Delta Sharing. Consumers get live, zero-copy access: always fresh, no ETL.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-teal">
# MAGIC       <span class="e2-v-badge e2-v-badge-teal">AI</span>
# MAGIC       <h4>AI/ML Models</h4>
# MAGIC       <p>Pre-trained models and solution accelerators. Install into your UC Model Registry and deploy to endpoints.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-teal">
# MAGIC       <span class="e2-v-badge e2-v-badge-teal">Code</span>
# MAGIC       <h4>Notebooks and Git Repos</h4>
# MAGIC       <p>Solution accelerators and reference implementations. Clone into your workspace and customize.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-teal">
# MAGIC       <span class="e2-v-badge e2-v-badge-teal">Agents</span>
# MAGIC       <h4>MCP Servers</h4>
# MAGIC       <p>AI agent tool providers. Distribute and consume agent capabilities across organizations (added 2026).</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-cards" style="margin-bottom:0;">
# MAGIC     <div class="e2-v-card e2-v-card-teal" style="border-left-color:#00A972;">
# MAGIC       <h4 style="color:#00A972;">Public Marketplace</h4>
# MAGIC       <p>Open to any workspace. Free and commercial listings. Browse without a Databricks account.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-teal" style="border-left-color:#E5A100;">
# MAGIC       <h4 style="color:#8a6200;">Private Exchanges</h4>
# MAGIC       <p>Provider-curated, member-only listings for specific <strong>external</strong> partners. Not for internal distribution.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-footer" style="margin-top:14px;">Powered by Delta Sharing + Partner Connect</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 2: Clean Rooms -->
# MAGIC <div class="e2-v-panel e2-v-p2">
# MAGIC   <div class="e2-v-metric e2-v-km-blue">
# MAGIC     <div class="e2-v-metric-val">Zero Raw Data Exposure</div>
# MAGIC     <div class="e2-v-metric-label">Multiple parties collaborate on sensitive data without seeing each other's records</div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-flow">
# MAGIC     <div class="e2-v-flow-box e2-v-flow-party">Party A<br><span style="font-size:14pt;font-weight:400;">Retailer</span></div>
# MAGIC     <span class="e2-v-flow-arrow">&#x279C;</span>
# MAGIC     <div class="e2-v-flow-box e2-v-flow-cr">Clean Room<br><span style="font-size:14pt;font-weight:400;">Isolated serverless compute</span></div>
# MAGIC     <span class="e2-v-flow-arrow">&#x279C;</span>
# MAGIC     <div class="e2-v-flow-box e2-v-flow-party">Party B<br><span style="font-size:14pt;font-weight:400;">Advertiser</span></div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-flow-label">Both parties share data into the clean room via Delta Sharing. Only approved notebooks can execute.</div>
# MAGIC   <div class="e2-v-cards">
# MAGIC     <div class="e2-v-card e2-v-card-blue">
# MAGIC       <span class="e2-v-badge e2-v-badge-blue">Trust Model</span>
# MAGIC       <h4>No-Trust, Equal Privileges</h4>
# MAGIC       <p>All collaborators (including the creator) have equal access. No party has elevated control over the room.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-blue">
# MAGIC       <span class="e2-v-badge e2-v-badge-blue">Code Governance</span>
# MAGIC       <h4>Unanimous Notebook Approval</h4>
# MAGIC       <p>Every collaborator must approve a notebook before it can run. One party cannot execute unauthorized analysis.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-blue">
# MAGIC       <span class="e2-v-badge e2-v-badge-blue">Privacy</span>
# MAGIC       <h4>Schema-Only Visibility</h4>
# MAGIC       <p>Collaborators see column names and types from other parties, but never raw data. Only aggregated outputs are returned.</p>
# MAGIC     </div>
# MAGIC     <div class="e2-v-card e2-v-card-blue">
# MAGIC       <span class="e2-v-badge e2-v-badge-blue">Compliance</span>
# MAGIC       <h4>Full Audit Trail</h4>
# MAGIC       <p>All events logged to system tables and account audit logs. Supports GDPR, CCPA, and HIPAA workflows. Max 10 collaborators.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="e2-v-footer">Powered by Delta Sharing + Isolated Serverless Compute</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Marketplace for Partners</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Providers must have a Databricks account with a premium, UC-enabled workspace. They can apply through the Databricks Data Partner Program or use self-service signup for private exchanges. <strong>Private exchanges are for sharing with specific external partners, not for internal multi-region distribution within your own organization.</strong> For internal sharing, use Unity Catalog directly via workspace-to-catalog binding.</li>
# MAGIC           <li style="font-size: 14pt;">Consumers need a UC-enabled workspace to integrate data products, but the open marketplace can be browsed without a Databricks account.</li>
# MAGIC           <li style="font-size: 14pt;">Marketplace supports MCP server listings (added in 2026), meaning it now distributes AI agent tools in addition to data and models.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Clean Rooms: Architecture and Constraints</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Three components per clean room:</strong> (1) securable object in the creator's UC metastore, (2) isolated ephemeral central clean room managed by Databricks, (3) securable object in each collaborator's metastore. Data is shared exclusively with the central clean room via Delta Sharing.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Constraints:</strong> maximum 10 collaborators. Clean rooms are locked after creation (no new collaborators). If any collaborator deletes the clean room, the central clean room is voided.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Audit trail:</strong> clean room events are logged to system tables and account audit logs, providing regulatory compliance evidence (GDPR, CCPA, HIPAA).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Positioning for Customer Conversations</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Marketplace:</strong> position as "an app store for data" where consumers browse, request, and integrate data products directly into their workspace.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Clean Rooms:</strong> position for customers in regulated industries (financial services, healthcare, retail/advertising) where privacy requirements prevent direct data exchange between parties.</li>
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
# MAGIC ## F. Architecture Patterns
# MAGIC
# MAGIC The previous sections covered UC's governance capabilities. This section covers how organizations deploy those capabilities in practice: isolating environments for the software development lifecycle, sharing data across cloud regions, and governing multi-business-unit data estates.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. Architecture Patterns
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Unity Catalog deployments follow predictable patterns depending on the organization's needs. The three most common: <strong>SDLC isolation</strong> (separating dev/staging/production), <strong>cross-region access</strong> (sharing data between cloud regions), and <strong>multi-business-unit governance</strong> (isolating or sharing data between organizational units). Click each tab to explore the pattern.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-architecture-patterns-tabs ── -->
# MAGIC <style>
# MAGIC .f1-v-wrap { width: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px 0; }
# MAGIC .f1-v-radio { display: none; }
# MAGIC /* Tabs */
# MAGIC .f1-v-tabs { display: flex; gap: 6px; margin-bottom: 0; }
# MAGIC .f1-v-tab { flex: 1; text-align: center; padding: 14px 10px; border-radius: 10px 10px 0 0; border: 2px solid #DCE0E2; border-bottom: none; background: #e8ebed; color: #666; cursor: pointer; transition: all 0.15s; }
# MAGIC .f1-v-tab-title { font-size: 15pt; font-weight: 700; display: block; }
# MAGIC .f1-v-tab-sub { font-size: 14pt; display: block; margin-top: 2px; }
# MAGIC .f1-v-panel { display: none; background: #fff; border: 2px solid #DCE0E2; border-radius: 0 0 10px 10px; padding: 24px; }
# MAGIC /* Tab activation */
# MAGIC #f1-t1:checked ~ .f1-v-tabs .f1-v-tab1 { background: #1B5162; color: #fff; border-color: #1B5162; }
# MAGIC #f1-t1:checked ~ .f1-v-tabs .f1-v-tab1 .f1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1-t1:checked ~ .f1-v-p1 { display: block; border-color: #1B5162; }
# MAGIC #f1-t2:checked ~ .f1-v-tabs .f1-v-tab2 { background: #2574B5; color: #fff; border-color: #2574B5; }
# MAGIC #f1-t2:checked ~ .f1-v-tabs .f1-v-tab2 .f1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1-t2:checked ~ .f1-v-p2 { display: block; border-color: #2574B5; }
# MAGIC #f1-t3:checked ~ .f1-v-tabs .f1-v-tab3 { background: #00A972; color: #fff; border-color: #00A972; }
# MAGIC #f1-t3:checked ~ .f1-v-tabs .f1-v-tab3 .f1-v-tab-sub { color: rgba(255,255,255,0.7); }
# MAGIC #f1-t3:checked ~ .f1-v-p3 { display: block; border-color: #00A972; }
# MAGIC /* Shared styles */
# MAGIC .f1-v-metric { text-align: center; padding: 14px; border-radius: 10px; margin-bottom: 18px; }
# MAGIC .f1-v-metric-val { font-size: 20pt; font-weight: 800; color: #fff; }
# MAGIC .f1-v-metric-label { font-size: 14pt; color: rgba(255,255,255,0.85); margin-top: 4px; }
# MAGIC .f1-v-km1 { background: linear-gradient(135deg, #1B5162, #2a7a94); }
# MAGIC .f1-v-km2 { background: linear-gradient(135deg, #2574B5, #3a9ad9); }
# MAGIC .f1-v-km3 { background: linear-gradient(135deg, #00A972, #00c987); }
# MAGIC /* Environment columns */
# MAGIC .f1-v-envs { display: flex; gap: 12px; margin-bottom: 16px; }
# MAGIC .f1-v-env { flex: 1; border-radius: 10px; overflow: hidden; border: 2px solid #DCE0E2; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .f1-v-env:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .f1-v-env-hdr { padding: 10px; text-align: center; font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .f1-v-env-dev { background: #618794; }
# MAGIC .f1-v-env-stg { background: #2574B5; }
# MAGIC .f1-v-env-prd { background: #1B5162; }
# MAGIC .f1-v-env-row { padding: 8px 12px; text-align: center; border-bottom: 1px solid #EEEDE9; background: #F9F7F4; }
# MAGIC .f1-v-env-lbl { font-size: 14pt; color: #5A6F77; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
# MAGIC .f1-v-env-val { font-size: 14pt; color: #1B3139; font-weight: 600; font-family: 'Menlo','Consolas',monospace; }
# MAGIC /* Isolation dimensions */
# MAGIC .f1-v-dims { display: flex; gap: 8px; }
# MAGIC .f1-v-dim { flex: 1; border-radius: 8px; padding: 12px; text-align: center; border: 1.5px solid #DCE0E2; background: #fff; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .f1-v-dim:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.10); }
# MAGIC .f1-v-dim-icon { width: 36px; height: 36px; border-radius: 50%; margin: 0 auto 6px; display: flex; align-items: center; justify-content: center; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .f1-v-dim-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .f1-v-dim-desc { font-size: 14pt; color: #555; }
# MAGIC /* Cross-region approaches */
# MAGIC .f1-v-approaches { display: flex; gap: 10px; flex-wrap: wrap; }
# MAGIC .f1-v-approach { flex: 1; min-width: 200px; border-radius: 10px; padding: 14px; border: 1.5px solid #DCE0E2; background: #fff; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .f1-v-approach:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .f1-v-approach-num { display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; border-radius: 50%; font-size: 14pt; font-weight: 700; color: #fff; background: #2574B5; margin-bottom: 6px; }
# MAGIC .f1-v-approach h4 { font-size: 14pt; color: #1B3139; margin-bottom: 4px; }
# MAGIC .f1-v-approach p { font-size: 14pt; color: #555; line-height: 1.4; }
# MAGIC .f1-v-approach .f1-v-pill { display: inline-block; font-size: 14pt; font-weight: 600; padding: 2px 8px; border-radius: 10px; margin-top: 6px; }
# MAGIC .f1-v-pill-pro { background: rgba(0,169,114,0.10); color: #007a53; }
# MAGIC .f1-v-pill-con { background: rgba(152,16,42,0.10); color: #98102A; }
# MAGIC /* Multi-BU comparison */
# MAGIC .f1-v-models { display: flex; gap: 12px; flex-wrap: wrap; }
# MAGIC .f1-v-model { flex: 1; min-width: 220px; border-radius: 10px; overflow: hidden; border: 2px solid #DCE0E2; transition: transform 0.2s, box-shadow 0.2s; }
# MAGIC .f1-v-model:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .f1-v-model-hdr { padding: 10px 14px; font-size: 14pt; font-weight: 700; color: #fff; text-align: center; }
# MAGIC .f1-v-model-body { padding: 14px; font-size: 14pt; color: #333; line-height: 1.5; }
# MAGIC .f1-v-model-fit { padding: 8px 14px; background: #F9F7F4; font-size: 14pt; color: #5A6F77; font-weight: 600; text-align: center; border-top: 1px solid #EEEDE9; }
# MAGIC .f1-v-model-pills { display: flex; flex-wrap: wrap; gap: 5px; justify-content: center; margin-top: 8px; }
# MAGIC .f1-v-mp { font-size: 14pt; font-weight: 600; padding: 3px 10px; border-radius: 12px; }
# MAGIC .f1-v-egress { margin-top: 14px; padding: 10px 16px; background: #fffbf0; border: 1.5px solid #E5A100; border-radius: 8px; font-size: 14pt; color: #333; text-align: center; }
# MAGIC .f1-v-egress strong { color: #8a6200; }
# MAGIC .f1-sdlc-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .f1-sdlc-meta {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   text-align: center;
# MAGIC   padding: 12px;
# MAGIC   border-radius: 12px 12px 0 0;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .f1-sdlc-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr 1fr;
# MAGIC   gap: 0;
# MAGIC   border: 2px solid #DCE0E2;
# MAGIC   border-top: none;
# MAGIC   border-radius: 0 0 12px 12px;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .f1-sdlc-env {
# MAGIC   border-right: 1px solid #DCE0E2;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   transition: transform 0.2s;
# MAGIC }
# MAGIC .f1-sdlc-env:hover {
# MAGIC   transform: translateY(-2px);
# MAGIC   box-shadow: 0 4px 12px rgba(27,49,57,0.10);
# MAGIC   z-index: 1;
# MAGIC   position: relative;
# MAGIC }
# MAGIC .f1-sdlc-env:last-child {
# MAGIC   border-right: none;
# MAGIC }
# MAGIC .f1-sdlc-env-header {
# MAGIC   padding: 12px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .f1-h-dev { background: #618794; }
# MAGIC .f1-h-stg { background: #2272B4; }
# MAGIC .f1-h-prd { background: #1B5162; }
# MAGIC .f1-sdlc-row {
# MAGIC   padding: 10px 12px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   border-bottom: 1px solid #EEEDE9;
# MAGIC   background: #F9F7F4;
# MAGIC   color: #333;
# MAGIC }
# MAGIC .f1-sdlc-row-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   font-weight: 600;
# MAGIC   text-transform: uppercase;
# MAGIC   letter-spacing: 0.05em;
# MAGIC }
# MAGIC .f1-sdlc-dims {
# MAGIC   display: flex;
# MAGIC   gap: 12px;
# MAGIC   margin-top: 16px;
# MAGIC }
# MAGIC .f1-sdlc-dim {
# MAGIC   flex: 1;
# MAGIC   border-radius: 8px;
# MAGIC   border: 2px solid #DCE0E2;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .f1-sdlc-dim-header {
# MAGIC   background: #EEEDE9;
# MAGIC   padding: 10px 12px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC }
# MAGIC .f1-sdlc-dim-body {
# MAGIC   padding: 10px 12px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.5;
# MAGIC   background: #F9F7F4;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="f1-v-wrap">
# MAGIC <input type="radio" name="f1tabs" id="f1-t1" class="f1-v-radio" checked>
# MAGIC <input type="radio" name="f1tabs" id="f1-t2" class="f1-v-radio">
# MAGIC <input type="radio" name="f1tabs" id="f1-t3" class="f1-v-radio">
# MAGIC
# MAGIC <div class="f1-v-tabs">
# MAGIC   <label for="f1-t1" class="f1-v-tab f1-v-tab1"><span class="f1-v-tab-title">SDLC Isolation</span><span class="f1-v-tab-sub">Dev / Staging / Production</span></label>
# MAGIC   <label for="f1-t2" class="f1-v-tab f1-v-tab2"><span class="f1-v-tab-title">Cross-Region Access</span><span class="f1-v-tab-sub">4 approaches to share across regions</span></label>
# MAGIC   <label for="f1-t3" class="f1-v-tab f1-v-tab3"><span class="f1-v-tab-title">Multi-BU Governance</span><span class="f1-v-tab-sub">Distributed, Centralized, or Federated</span></label>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tab 1: SDLC Isolation -->
# MAGIC <div class="f1-v-panel f1-v-p1">
# MAGIC   <div class="f1-v-metric f1-v-km1">
# MAGIC     <div class="f1-v-metric-val">Minimum 2 Workspaces, Ideally 3</div>
# MAGIC     <div class="f1-v-metric-label">Separate environments by catalog, workspace, storage, and admin ownership</div>
# MAGIC   </div>
# MAGIC   <div class="f1-v-envs">
# MAGIC     <div class="f1-v-env"><div class="f1-v-env-hdr f1-v-env-dev">DEV</div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Workspace</div><div class="f1-v-env-val">ws-dev</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Catalog</div><div class="f1-v-env-val">analytics_dev</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Storage</div><div class="f1-v-env-val">s3://dev-bucket/</div></div></div>
# MAGIC     <div class="f1-v-env"><div class="f1-v-env-hdr f1-v-env-stg">STAGING</div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Workspace</div><div class="f1-v-env-val">ws-staging</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Catalog</div><div class="f1-v-env-val">analytics_stg</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Storage</div><div class="f1-v-env-val">s3://stg-bucket/</div></div></div>
# MAGIC     <div class="f1-v-env"><div class="f1-v-env-hdr f1-v-env-prd">PRODUCTION</div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Workspace</div><div class="f1-v-env-val">ws-prod</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Catalog</div><div class="f1-v-env-val">analytics_prd</div></div><div class="f1-v-env-row"><div class="f1-v-env-lbl">Storage</div><div class="f1-v-env-val">s3://prd-bucket/</div></div></div>
# MAGIC   </div>
# MAGIC   <div class="f1-v-dims">
# MAGIC     <div class="f1-v-dim"><div class="f1-v-dim-icon" style="background:#98102A;">1</div><div class="f1-v-dim-title">Admin Isolation</div><div class="f1-v-dim-desc">Delegate catalog ownership to BU-specific admins</div></div>
# MAGIC     <div class="f1-v-dim"><div class="f1-v-dim-icon" style="background:#1B5162;">2</div><div class="f1-v-dim-title">Catalog Binding</div><div class="f1-v-dim-desc">Restrict which workspaces can access which catalogs</div></div>
# MAGIC     <div class="f1-v-dim"><div class="f1-v-dim-icon" style="background:#E5A100;">3</div><div class="f1-v-dim-title">Storage Isolation</div><div class="f1-v-dim-desc">Separate cloud storage buckets per environment</div></div>
# MAGIC     <div class="f1-v-dim"><div class="f1-v-dim-icon" style="background:#00A972;">4</div><div class="f1-v-dim-title">Access Control</div><div class="f1-v-dim-desc">UC privileges + ABAC enforce who sees what data</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tab 2: Cross-Region Access -->
# MAGIC <div class="f1-v-panel f1-v-p2">
# MAGIC   <div class="f1-v-metric f1-v-km2">
# MAGIC     <div class="f1-v-metric-val">4 Approaches to Cross-Region Data Access</div>
# MAGIC     <div class="f1-v-metric-label">UC enforces one metastore per cloud region. These patterns bridge that boundary.</div>
# MAGIC   </div>
# MAGIC   <div class="f1-v-approaches">
# MAGIC     <div class="f1-v-approach"><div class="f1-v-approach-num">1</div><h4>D2D Delta Sharing</h4><p>Region 2 shares tables with Region 1's metastore. Region 1 creates a sharing catalog. Local ACLs enforced on top.</p><span class="f1-v-pill f1-v-pill-pro">Provider serves data</span></div>
# MAGIC     <div class="f1-v-approach"><div class="f1-v-approach-num">2</div><h4>Lakehouse Federation</h4><p>Region 1 represents Region 2 as a foreign catalog. Queries run via JDBC with predicate pushdown.</p><span class="f1-v-pill f1-v-pill-pro">Provider provides compute</span></div>
# MAGIC     <div class="f1-v-approach"><div class="f1-v-approach-num">3</div><h4>Cloudflare R2</h4><p>Clone data to R2 (zero egress). Share the R2-backed external table via Delta Sharing. No cross-region egress costs.</p><span class="f1-v-pill f1-v-pill-pro">Zero egress</span></div>
# MAGIC     <div class="f1-v-approach"><div class="f1-v-approach-num">4</div><h4>Remote Compute</h4><p>Connect directly to a cluster or SQL warehouse in Region 2 via JDBC/ODBC. No metastore integration in Region 1.</p><span class="f1-v-pill f1-v-pill-con">Egress both directions</span></div>
# MAGIC   </div>
# MAGIC   <div class="f1-v-egress"><strong>Note:</strong> Approaches 1, 2, and 4 incur egress charges when data leaves the source region. Approach 3 (Cloudflare R2) avoids egress entirely. Do not register the same external table in multiple metastores.</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tab 3: Multi-BU Governance -->
# MAGIC <div class="f1-v-panel f1-v-p3">
# MAGIC   <div class="f1-v-metric f1-v-km3">
# MAGIC     <div class="f1-v-metric-val">3 Governance Models</div>
# MAGIC     <div class="f1-v-metric-label">Choose based on regulatory requirements, organizational agility, and data sharing needs</div>
# MAGIC   </div>
# MAGIC   <div class="f1-v-models">
# MAGIC     <div class="f1-v-model"><div class="f1-v-model-hdr" style="background:#618794;">Distributed</div><div class="f1-v-model-body">Each BU owns and shares its own data. Central team provides platform operations. BUs are responsible for their own quality and access control.</div><div class="f1-v-model-pills"><span class="f1-v-mp" style="background:rgba(97,135,148,0.10);color:#4a6a76;">BU Autonomy</span><span class="f1-v-mp" style="background:rgba(97,135,148,0.10);color:#4a6a76;">Direct BU-to-BU Sharing</span></div><div class="f1-v-model-fit">Best for: agile orgs, domain expertise in BUs</div></div>
# MAGIC     <div class="f1-v-model"><div class="f1-v-model-hdr" style="background:#1B5162;">Centralized</div><div class="f1-v-model-body">Central BU is the single publishing authority. Other BUs are isolated from each other. Central team controls quality, access, and publishing.</div><div class="f1-v-model-pills"><span class="f1-v-mp" style="background:rgba(27,81,98,0.10);color:#1B5162;">Single Source of Truth</span><span class="f1-v-mp" style="background:rgba(27,81,98,0.10);color:#1B5162;">Tighter Control</span></div><div class="f1-v-model-fit">Best for: regulated industries, strict compliance</div></div>
# MAGIC     <div class="f1-v-model" style="border-color:#00A972;"><div class="f1-v-model-hdr" style="background:#00A972;">Federated (Recommended)</div><div class="f1-v-model-body">Central team sets guidelines and policies. BUs implement locally within guardrails. Balances agility with consistency. Self-service access prevents bottlenecks.</div><div class="f1-v-model-pills"><span class="f1-v-mp" style="background:rgba(0,169,114,0.10);color:#007a53;">Central Guidelines</span><span class="f1-v-mp" style="background:rgba(0,169,114,0.10);color:#007a53;">Local Implementation</span></div><div class="f1-v-model-fit">Best for: most enterprises (recommended default)</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">SDLC: Workspace-Catalog Binding</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Best practice: minimum 2 workspaces (dev+test, production), ideally 3 (dev, staging, production).</strong> Each workspace is bound to its corresponding catalog, with separate storage buckets and admin groups per environment. This is the most commonly recommended starting point for SDLC isolation.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Default behavior:</strong> all catalogs are accessible from any workspace attached to the same metastore. Binding must be explicitly configured to restrict this.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Isolation mode:</strong> set a catalog to ISOLATED mode, then assign specific workspaces. Non-assigned workspaces receive an error, overriding individual privilege grants.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Read-only binding:</strong> you can restrict bound workspaces to read-only operations, blocking all writes. This is useful for giving dev teams read access to production data without write risk.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Compliance use cases:</strong> SOX and GxP regulations often require environment isolation. Workspace-catalog binding provides auditable proof that production data is inaccessible from non-production environments.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Cross-Workspace vs. Siloed</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Cross-Workspace:</strong> a DEV workspace can access both DEV and STG catalogs, enabling promotional workflows where developers test against staging data before promoting to production.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Siloed:</strong> each workspace is bound only to its corresponding catalog. Ideal for regulated or production-critical scenarios where cross-environment access is prohibited.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Catalog Organization Patterns</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Environment-Based:</strong> dev/staging/prod catalogs (most common for SDLC)</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Domain-Based:</strong> sales/marketing/finance catalogs (organized by business function)</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Hybrid:</strong> combines both (e.g., sales_dev, sales_prod, marketing_dev, marketing_prod)</li>
# MAGIC           <li style="font-size: 14pt;">Databricks recommends limiting catalogs to 3-10 per metastore. Use schemas (not catalogs) for team-specific areas.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Cross-Region Access: Choosing an Approach</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>D2D Delta Sharing:</strong> the provider's metastore shares tables. The recipient creates a sharing catalog with read-only access. Local ACLs enforced on top. Best for governed, long-term cross-region access.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Lakehouse Federation via foreign catalog:</strong> Region 1 creates a foreign catalog pointing to Region 2. Queries run with predicate pushdown. Best for ad hoc querying without data replication.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cloudflare R2 (zero egress):</strong> clone data to Cloudflare R2 storage, share the R2-backed external table via Delta Sharing. Eliminates cross-region egress costs entirely. Best for high-volume, recurring cross-region transfers.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Remote compute (JDBC/ODBC):</strong> connect directly to a cluster or SQL warehouse in the remote region. No metastore integration. Simplest setup but incurs egress in both directions and no local lineage.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Key constraint:</strong> never register the same external table in more than one metastore. This creates conflicting metadata and breaks governance guarantees.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Multi-BU Governance: Real-World Examples</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/natura/unity-catalog" style="color: #2574B5; font-size: 14pt;">Natura</a> uses a federated hub-and-spoke model with 178 data products across 19 domains. The central data team (hub) defines rules and policies; business units (spokes) produce dashboards and products. Product owners manage permissions independently. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/kraken/utility-data-unity-catalog" style="color: #2574B5; font-size: 14pt;">Kraken</a> governs data across separate Databricks accounts (one per utility client), enforcing client data isolation, GDPR access controls, and SOC 1/2 compliance. System tables plus the UC API cover approximately 95% of their governance needs without third-party tools. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Single cloud-region:</strong> one metastore covering all BUs with workspace-to-catalog binding for isolation. Each BU gets its own workspaces and catalogs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Multiple cloud-regions:</strong> each region gets its own metastore. Use Delta Sharing or Foreign Catalogs to share data across regions.</li>
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
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture traced the path from fragmented catalog silos to unified governance with Unity Catalog. You started with the problem that open data formats alone do not solve: different catalogs restrict which engines can access your data, creating governance fragmentation. Unity Catalog addresses this as the unified governance layer built into Databricks, organizing assets through a three-level namespace (catalog.schema.object), enforcing access controls through SQL, UI, and API, and extending governance to external systems through Lakehouse Federation and Delta Sharing.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Key takeaways from this lecture include:</p>
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>Unified governance across all assets:</strong> UC governs tables, views, volumes, functions, ML models, notebooks, dashboards, and files. Four capability pillars: Security, Collaboration, Quality, and Insights.</li>
# MAGIC   <li><strong>Three-level namespace and isolation:</strong> Catalog > Schema > Assets provides domain-level governance. Workspace-catalog binding, admin isolation, and storage isolation enforce environment separation for SDLC compliance.</li>
# MAGIC   <li><strong>Access controls at scale:</strong> GRANT/REVOKE privileges with inheritance, fine-grained row/column security, and ABAC with governed tags and Data Classification for scaling governance across large data estates.</li>
# MAGIC   <li><strong>Lakehouse Federation:</strong> catalog federation and query federation extend UC governance to external data sources without migration. UC serves as the "catalog of catalogs."</li>
# MAGIC   <li><strong>Delta Sharing and Marketplace:</strong> open-protocol, zero-copy sharing across organizations, clouds, and platforms. Marketplace distributes data products; Clean Rooms enable privacy-safe multi-party collaboration.</li>
# MAGIC   <li><strong>Architecture patterns:</strong> SDLC isolation (dev/staging/prod), cross-region access (D2D sharing or federation), and multi-BU governance (distributed vs. centralized publishing) cover the most common deployment scenarios.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Next:</strong> The Lab (DBDEMOS 360 Intro) gives you hands-on practice with Unity Catalog capabilities in a live Databricks workspace.</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333; font-size: 14pt;">
# MAGIC         <li style="font-size: 14pt;">What is Unity Catalog? (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/" style="color: #2574B5;">GCP</a>): Platform overview covering the object model, capabilities, and interaction methods</li>
# MAGIC         <li style="font-size: 14pt;">Unity Catalog Best Practices (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/best-practices" style="color: #2574B5;">GCP</a>): Recommended patterns for identity management, privilege hierarchy, storage, and cross-region access</li>
# MAGIC         <li style="font-size: 14pt;">Attribute-Based Access Control (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/abac/" style="color: #2574B5;">GCP</a>): ABAC overview, core concepts, tutorial, and requirements</li>
# MAGIC         <li style="font-size: 14pt;">What is Lakehouse Federation? (<a href="https://docs.databricks.com/aws/en/query-federation/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/query-federation/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/query-federation/" style="color: #2574B5;">GCP</a>): Query federation and catalog federation documentation</li>
# MAGIC         <li style="font-size: 14pt;">What is Delta Sharing? (<a href="https://docs.databricks.com/aws/en/delta-sharing" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/delta-sharing/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/delta-sharing" style="color: #2574B5;">GCP</a>): Open sharing, D2D sharing, security model, and supported assets</li>
# MAGIC         <li style="font-size: 14pt;">Design Unity Catalog Architecture (<a href="https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/unity-catalog" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/deployment-guide/unity-catalog" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse-architecture/deployment-guide/unity-catalog" style="color: #2574B5;">GCP</a>): Deployment patterns, catalog organization, and isolation guidance</li>
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
