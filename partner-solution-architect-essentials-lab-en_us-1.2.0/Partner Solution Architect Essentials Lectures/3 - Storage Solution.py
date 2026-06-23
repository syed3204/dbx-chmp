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

# MAGIC %md-sandbox
# MAGIC # 3 Lecture: Storage Solution
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Open data lakes store data cheaply but offer no atomicity, no quality enforcement, and no consistency guarantees. A single failed write can corrupt an entire dataset, and concurrent readers may see partial results. Delta Lake was created to solve these problems by adding a transaction log on top of cloud object storage, giving data lakes the reliability and performance characteristics of a data warehouse without sacrificing openness or cost efficiency.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture covers the storage layer of the Databricks platform across six sections:</p>
# MAGIC
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>A. Delta Lake Fundamentals</strong>: ACID transactions, schema enforcement, time travel, and the transaction log architecture</li>
# MAGIC   <li><strong>B. The Medallion Architecture</strong>: Organizing lakehouse data into Bronze, Silver, and Gold layers for progressive data quality</li>
# MAGIC   <li><strong>C. Delta Lake Optimizations</strong>: Partitioning anti-patterns, Z-ordering limitations, and Liquid Clustering as the recommended approach</li>
# MAGIC   <li><strong>D. Predictive Optimization</strong>: AI-driven automatic OPTIMIZE, VACUUM, and ANALYZE for managed tables</li>
# MAGIC   <li><strong>E. UniForm and Managed Iceberg Tables</strong>: Solving table format fragmentation with multi-format interoperability</li>
# MAGIC   <li><strong>F. Lakebase</strong>: Fully managed PostgreSQL for OLTP workloads integrated with the Lakehouse</li>
# MAGIC </ul>
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC <br/>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333; font-size: 14pt;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333; font-size: 14pt;">
# MAGIC         <li style="font-size: 14pt;">Explain how Delta Lake brings data warehouse reliability (ACID transactions, schema enforcement, time travel) to data lake storage</li>
# MAGIC         <li style="font-size: 14pt;">Describe the Medallion Architecture (Bronze, Silver, Gold) and the role each layer plays in progressive data quality</li>
# MAGIC         <li style="font-size: 14pt;">Compare partitioning, Z-ordering, and Liquid Clustering, explaining why Liquid Clustering is the recommended data layout approach</li>
# MAGIC         <li style="font-size: 14pt;">Explain how Predictive Optimization automates table maintenance using telemetry and ML</li>
# MAGIC         <li style="font-size: 14pt;">Differentiate Delta Lake UniForm from Managed Iceberg tables for cross-platform interoperability</li>
# MAGIC         <li style="font-size: 14pt;">Describe Lakebase as a managed PostgreSQL database for OLTP workloads, including its use cases and limitations</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Delta Lake Fundamentals

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. What Is Delta Lake?
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Delta Lake is the default storage format for all tables on Databricks. It extends Apache Parquet with a file-based transaction log that provides ACID transactions, schema enforcement, time travel, and scalable metadata handling. Data files remain in open Parquet format on cloud object storage (S3, ADLS, GCS), so there is no vendor lock-in and no proprietary format to manage.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Delta Lake provides eight capabilities that bring data warehouse reliability to data lake storage:</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-eight-capabilities-grid ── -->
# MAGIC <style>
# MAGIC .a1-cap-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(4, 1fr);
# MAGIC   gap: 12px;
# MAGIC   margin: 24px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .a1-cap-card {
# MAGIC   background: #fff;
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 12px;
# MAGIC   text-align: center;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   align-items: center;
# MAGIC   gap: 8px;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s;
# MAGIC   cursor: default;
# MAGIC   position: relative;
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .a1-cap-card:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC   border-color: #00A972;
# MAGIC }
# MAGIC .a1-cap-card::before {
# MAGIC   content: "";
# MAGIC   position: absolute;
# MAGIC   top: 0;
# MAGIC   left: 0;
# MAGIC   right: 0;
# MAGIC   height: 4px;
# MAGIC   background: linear-gradient(90deg, #1B5162, #00A972);
# MAGIC }
# MAGIC .a1-cap-icon { font-size: 24px; color: #1B5162; }
# MAGIC .a1-cap-label { font-size: 14pt; font-weight: 600; color: #0b2026; line-height: 1.3; }
# MAGIC .a1-cap-desc { font-size: 14pt; color: #5A6F77; line-height: 1.4; }
# MAGIC </style>
# MAGIC <div class="a1-cap-grid">
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">ACID Transactions</div>
# MAGIC     <div class="a1-cap-desc">Atomic commits, optimistic concurrency</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Schema Enforcement</div>
# MAGIC     <div class="a1-cap-desc">Rejects bad data on write</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Time Travel</div>
# MAGIC     <div class="a1-cap-desc">Query any historical version</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">DML Operations</div>
# MAGIC     <div class="a1-cap-desc">MERGE, UPDATE, DELETE, INSERT</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Unified Batch/Streaming</div>
# MAGIC     <div class="a1-cap-desc">Same table for both workloads</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Scalable Metadata</div>
# MAGIC     <div class="a1-cap-desc">Transaction log with checkpointing</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Audit History</div>
# MAGIC     <div class="a1-cap-desc">DESCRIBE HISTORY for every change</div>
# MAGIC   </div>
# MAGIC   <div class="a1-cap-card">
# MAGIC     <div class="a1-cap-dot" style="width:10px;height:10px;border-radius:50%;background:#1B5162;"></div>
# MAGIC     <div class="a1-cap-label">Open Source</div>
# MAGIC     <div class="a1-cap-desc">Linux Foundation, Apache 2.0</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Delta Lake = Parquet + Transaction Log</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Open format at the core:</strong> Delta Lake stores data as Parquet files on cloud object storage. The transaction log is a series of JSON files in a <code>_delta_log/</code> directory. Both are open, readable by any system that implements the Delta protocol.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> think of the transaction log like a bank ledger. The ledger records every deposit and withdrawal sequentially. Even if the bank system crashes mid-transaction, the ledger ensures your balance is never corrupted. Delta Lake works the same way: data files are written first, then the log entry commits them atomically.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Default, not optional:</strong> unless you specify otherwise, every table created on Databricks is a Delta table. Organizations report query performance improvements of 10x to 100x over scanning raw Parquet files in a data lake.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Eight Capabilities</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>ACID Transactions:</strong> each SQL statement is an independent atomic transaction by default. Multiple statements across tables can be grouped using <code>BEGIN ATOMIC ... END;</code> syntax. The platform uses optimistic concurrency control with no read/write locks, eliminating deadlock risks.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Schema Enforcement:</strong> acts like a bouncer at a club. Data that does not match the table schema gets rejected on write, preventing corrupt or mismatched data from entering the table.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Time Travel:</strong> every write creates a new table version in the transaction log. You can query any previous version using <code>VERSION AS OF</code> or <code>TIMESTAMP AS OF</code>. Default retention is 7 days (the VACUUM threshold).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>DML Operations:</strong> MERGE, UPDATE, DELETE, and INSERT via SQL, Python, or Scala bring warehouse-like data manipulation to the data lake.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Proof Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/comcast" style="color: #2272B4; font-size: 14pt;">Comcast</a> achieved a 10x reduction in compute costs by adopting Delta Lake, replacing 640 machines with just 64 while improving performance. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/gousto" style="color: #2272B4; font-size: 14pt;">Gousto</a> reduced end-to-end ingestion time by 99.6%, taking a two-hour pipeline down to 15 seconds, with a 60% cost improvement. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/uscis" style="color: #2272B4; font-size: 14pt;">USCIS</a> achieved a 24x performance gain: the same query runs in 19 minutes instead of a full business day. &#x25C6;</li>
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
# MAGIC ### A2. Delta Lake Components
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Delta Lake operates across three layers. <strong>Delta Tables</strong> store data as Parquet files with a transaction log (<code>_delta_log/</code>) that records every commit atomically. The <strong>Delta Engine</strong> handles file management optimizations including ingestion-time clustering, liquid clustering, compaction, data skipping, and disk caching. The <strong>Storage Layer</strong> sits on cloud object storage (S3, ADLS, GCS), providing low-cost, scalable, and durable storage with consistency enforced by the transaction log.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-three-layer-architecture ── -->
# MAGIC <style>
# MAGIC .a2-v-wrap { width: 100% !important; max-width: 100% !important; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
# MAGIC .a2-v-layer { cursor: pointer; user-select: none; transition: box-shadow 0.2s; }
# MAGIC .a2-v-layer:active { box-shadow: inset 0 0 0 2px rgba(255,255,255,0.3); }
# MAGIC .a2-v-head { padding: 18px 24px; display: flex; align-items: center; gap: 14px; }
# MAGIC .a2-v-dot { width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }
# MAGIC .a2-v-title { font-size: 15pt; font-weight: 700; color: #fff; flex: 1; }
# MAGIC .a2-v-sub { font-size: 14pt; color: #DCE0E2; }
# MAGIC .a2-v-chev { width: 10px; height: 10px; border-right: 2px solid rgba(255,255,255,0.5); border-bottom: 2px solid rgba(255,255,255,0.5); transform: rotate(-45deg); transition: transform 0.2s; flex-shrink: 0; }
# MAGIC .a2-v-layer.a2-v-open .a2-v-chev { transform: rotate(45deg); }
# MAGIC .a2-v-body { max-height: 0; overflow: hidden; transition: max-height 0.3s ease, padding 0.3s ease; padding: 0 24px 0 50px; }
# MAGIC .a2-v-layer.a2-v-open .a2-v-body { max-height: 400px; padding: 0 24px 16px 50px; }
# MAGIC .a2-v-body ul { margin: 4px 0 0 0; padding-left: 18px; }
# MAGIC .a2-v-body li { font-size: 14pt; color: #F0F4F6; line-height: 1.6; margin-bottom: 6px; }
# MAGIC .a2-v-body li strong { color: #fff; font-size: 14pt; }
# MAGIC .a2-v-body li code { font-size: 14pt; background: rgba(0,0,0,0.2); padding: 1px 5px; border-radius: 3px; color: #F9F7F4; }
# MAGIC .a2-v-l1 { background: #1B5162; border-radius: 8px 8px 0 0; }
# MAGIC .a2-v-l2 { background: #618794; }
# MAGIC .a2-v-l3 { background: #1B3139; border-radius: 0 0 8px 8px; }
# MAGIC .a2-v-layer.a2-v-open { box-shadow: inset 4px 0 0 #FFAB00; }
# MAGIC </style>
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click each layer to expand its components</p>
# MAGIC <div class="a2-v-wrap">
# MAGIC   <!-- Layer 1: Delta Engine -->
# MAGIC   <div class="a2-v-layer a2-v-l1" onclick="a2tog(this)">
# MAGIC     <div class="a2-v-head"><div class="a2-v-dot" style="background:#FFAB00;"></div><div class="a2-v-title" style="font-size:15pt;">Delta Engine</div><div class="a2-v-sub" style="font-size:14pt;">Liquid Clustering, Compaction, Data Skipping, Auto-Optimized Writes, Disk Caching</div><div class="a2-v-chev"></div></div>
# MAGIC     <div class="a2-v-body">
# MAGIC       <ul>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Liquid Clustering:</strong> tree-based data layout that replaces partitioning and Z-ordering. Incremental, self-tuning, and skew-resistant.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Compaction:</strong> merges small files into optimally sized files (target ~1 GB) to reduce metadata overhead and improve read performance.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Data Skipping:</strong> file-level min/max statistics let the engine skip irrelevant files entirely, reading only what matches your query filters.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Auto-Optimized Writes:</strong> automatically coalesces small writes into larger files during ingestion, reducing the small file problem at the source.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Disk Caching:</strong> caches remote Parquet files on local SSD, reducing cloud storage read latency for repeated queries.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Layer 2: Delta Tables -->
# MAGIC   <div class="a2-v-layer a2-v-l2" onclick="a2tog(this)">
# MAGIC     <div class="a2-v-head"><div class="a2-v-dot" style="background:#F9F7F4;"></div><div class="a2-v-title" style="font-size:15pt;">Delta Tables</div><div class="a2-v-sub" style="font-size:14pt;">Parquet data files + Transaction Log (<code style="font-size:14pt;background:rgba(0,0,0,0.15);padding:1px 5px;border-radius:3px;color:#fff;">_delta_log/</code>) for ACID, versioning, and schema enforcement</div><div class="a2-v-chev"></div></div>
# MAGIC     <div class="a2-v-body">
# MAGIC       <ul>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Transaction Log:</strong> a series of JSON files recording every commit. The log is the single source of truth: if a file is not referenced in the log, it does not exist in the table.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Commit protocol:</strong> (1) read current version, (2) write data files to storage, (3) validate no conflicts and commit the log entry. Failed writes leave orphaned files that <code style="font-size:14pt;">VACUUM</code> cleans up.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Schema enforcement:</strong> rejects writes that do not match the table schema. Acts like a bouncer: data that does not conform gets rejected on write.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Time Travel:</strong> every write creates a new version. Query any previous state with <code style="font-size:14pt;">VERSION AS OF</code> or <code style="font-size:14pt;">TIMESTAMP AS OF</code>. Default retention is 7 days.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Layer 3: Cloud Object Storage -->
# MAGIC   <div class="a2-v-layer a2-v-l3" onclick="a2tog(this)">
# MAGIC     <div class="a2-v-head"><div class="a2-v-dot" style="background:#90A5B1;"></div><div class="a2-v-title" style="font-size:15pt;">Cloud Object Storage</div><div class="a2-v-sub" style="font-size:14pt;">Amazon S3, Azure ADLS, Google Cloud Storage: low-cost, scalable, durable</div><div class="a2-v-chev"></div></div>
# MAGIC     <div class="a2-v-body">
# MAGIC       <ul>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Decoupled from compute:</strong> storage scales independently. You pay for storage at cloud object rates regardless of how many (or few) compute resources are running.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">Consistency via the log:</strong> cloud object stores are eventually consistent for listings, but Delta's transaction log enforces strong consistency for reads and writes.</li>
# MAGIC         <li style="font-size:14pt;"><strong style="font-size:14pt;">No vendor lock-in:</strong> data files are standard Parquet. Any engine that reads Parquet can read the raw files. The Delta protocol adds transactions on top, not a proprietary format underneath.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function a2tog(el){var open=el.classList.contains('a2-v-open');document.querySelectorAll('.a2-v-layer').forEach(function(l){l.classList.remove('a2-v-open');});if(!open)el.classList.add('a2-v-open');}
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Transaction Log Internals</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Commit protocol:</strong> the three stages are: (1) read the current table version, (2) write data files to cloud storage, (3) validate no conflicts and commit the log entry. If a conflict is detected, the transaction retries automatically.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Failed writes are safe:</strong> if a write fails halfway through, the data files are written to storage but the transaction log entry is never committed. These orphaned files do not corrupt the table; VACUUM cleans them up.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> time travel is like version control (Git) for your data. You can check out any previous version at any time, which is useful for debugging, auditing, and reproducibility.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ACID Guarantees in Detail</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Atomicity:</strong> relies on the transaction log. Data files are written, then a log entry commits all file paths when complete. Untracked files from failed transactions do not corrupt the table.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Consistency:</strong> uses optimistic concurrency in three stages: reading the current version, writing data files, then validating and committing without conflicts.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Isolation:</strong> defaults to write-serializable isolation for writes and snapshot isolation for reads, balancing throughput with transactional integrity.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Durability:</strong> stems from cloud storage's inherent high availability. Transaction logs reside alongside data files in cloud object storage.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Managed Tables</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog managed tables</strong> are the default and recommended table type. Benefits include reduced storage and compute costs, faster query performance, automatic table maintenance, and automatic upgrades to the latest platform features.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>DROP safety:</strong> if you <code>DROP</code> a managed table, Databricks deletes the data after 8 days. <code>UNDROP TABLE</code> allows recovery in that window.</li>
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
# MAGIC ## B. The Medallion Architecture

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Bronze, Silver, and Gold Layers
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The <strong>medallion architecture</strong> is a data design pattern that organizes lakehouse data into three layers, with data quality increasing progressively from left to right. It is also called a "multi-hop" architecture. Each layer is stored as Delta tables with ACID guarantees, and the pattern is the recommended approach for organizing data transformations on Databricks.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Analogy:</strong> think of medallion like a water treatment plant. Bronze is raw intake water (unfiltered). Silver is the filtration stage (cleaned, safe to use). Gold is bottled water (packaged and ready for specific consumers).</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-medallion-flow ── -->
# MAGIC <style>
# MAGIC .b1-med-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .b1-med-flow {
# MAGIC   display: flex;
# MAGIC   align-items: stretch;
# MAGIC   gap: 0;
# MAGIC }
# MAGIC .b1-med-stage {
# MAGIC   flex: 1;
# MAGIC   padding: 20px 16px;
# MAGIC   text-align: center;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   justify-content: center;
# MAGIC   gap: 6px;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .b1-med-stage:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.15);
# MAGIC   z-index: 1;
# MAGIC }
# MAGIC @keyframes b1-med-flow { 0%,100% { opacity: 0.5; transform: translateX(0); } 50% { opacity: 1; transform: translateX(3px); } }
# MAGIC .b1-med-arrow {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   font-size: 24px;
# MAGIC   color: #618794;
# MAGIC   padding: 0 4px;
# MAGIC   animation: b1-med-flow 1.5s ease-in-out infinite;
# MAGIC }
# MAGIC .b1-med-src { background: #EEEDE9; border-radius: 8px 0 0 8px; }
# MAGIC .b1-med-bronze { background: #CD7F32; }
# MAGIC .b1-med-silver { background: #90A5B1; }
# MAGIC .b1-med-gold { background: #FFCC66; }
# MAGIC .b1-med-dest { background: #EEEDE9; border-radius: 0 8px 8px 0; }
# MAGIC .b1-med-title { font-size: 15pt; font-weight: 700; color: #0b2026; }
# MAGIC .b1-med-title-light { font-size: 15pt; font-weight: 700; color: #fff; }
# MAGIC .b1-med-sub { font-size: 14pt; color: #303F47; line-height: 1.4; }
# MAGIC .b1-med-sub-light { font-size: 14pt; color: #F9F7F4; line-height: 1.4; }
# MAGIC .b1-med-quality {
# MAGIC   margin-top: 8px;
# MAGIC   background: linear-gradient(to right, #98102A, #FFAB00, #00A972);
# MAGIC   border-radius: 4px;
# MAGIC   padding: 8px 16px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="b1-med-wrapper">
# MAGIC   <div class="b1-med-flow">
# MAGIC     <div class="b1-med-stage b1-med-src">
# MAGIC       <div class="b1-med-title">Sources</div>
# MAGIC       <div class="b1-med-sub">Batch, Streaming, Files, APIs</div>
# MAGIC     </div>
# MAGIC     <div class="b1-med-arrow">&#x25B6;</div>
# MAGIC     <div class="b1-med-stage b1-med-bronze">
# MAGIC       <div class="b1-med-title-light">Bronze</div>
# MAGIC       <div class="b1-med-sub-light">Raw Ingestion</div>
# MAGIC     </div>
# MAGIC     <div class="b1-med-arrow">&#x25B6;</div>
# MAGIC     <div class="b1-med-stage b1-med-silver">
# MAGIC       <div class="b1-med-title-light">Silver</div>
# MAGIC       <div class="b1-med-sub-light">Validated, Conformed</div>
# MAGIC     </div>
# MAGIC     <div class="b1-med-arrow">&#x25B6;</div>
# MAGIC     <div class="b1-med-stage b1-med-gold">
# MAGIC       <div class="b1-med-title">Gold</div>
# MAGIC       <div class="b1-med-sub">Enriched, Aggregated</div>
# MAGIC     </div>
# MAGIC     <div class="b1-med-arrow">&#x25B6;</div>
# MAGIC     <div class="b1-med-stage b1-med-dest">
# MAGIC       <div class="b1-med-title">Use Cases</div>
# MAGIC       <div class="b1-med-sub">BI, ML, Dashboards, AI</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="b1-med-quality">Data Quality &#x279C; Increasing Left to Right</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Bronze Layer (Raw Data)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Purpose:</strong> raw data landing zone that mirrors source system structures with no transformations applied. Includes metadata such as load timestamp, source file path, and process ID.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Best practice:</strong> store fields as string, VARIANT, or binary to protect against unexpected schema changes in upstream sources.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Sources:</strong> cloud storage (S3, GCS, ADLS), message buses (Kafka, Kinesis), and federated systems.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Intended users:</strong> data engineers, operations teams, compliance and audit.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Silver Layer (Validated Data)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Purpose:</strong> cleansed and conformed data providing an enterprise view. Operations include schema enforcement and type casting, null handling, deduplication, resolution of late-arriving data, and initial data modeling.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Reuse is the key benefit:</strong> one Silver table can serve as the source for multiple downstream Gold tables owned by different business units. For example, a Silver "Product Sales" table could flow into Gold tables for supply chain dashboards, payroll bonuses, and executive KPIs.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Intended users:</strong> data engineers, analysts, data scientists.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Gold Layer (Enriched Data)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Purpose:</strong> business-level aggregates optimized for analytics, reporting, and ML. Features dimensional modeling, aggregates tailored for specific business needs, and performance-optimized queries.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Multiple Gold tables are normal:</strong> each business unit or use case typically has its own Gold tables. HR, finance, and IT may each have distinct Gold layers.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Intended users:</strong> business analysts, BI developers, ML engineers, executives.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Proof Points</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/banco-bradesco/lakeflow-declarative-pipelines" style="color: #2272B4; font-size: 14pt;">Banco Bradesco</a> achieved a &gt;94% reduction in data latency (60-80 hours down to 3.5 hours) and doubled CRM campaign conversion rates using a medallion architecture powered by Spark Declarative Pipelines. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/ifood/lakeflow-declarative-pipelines" style="color: #2272B4; font-size: 14pt;">iFood</a> consolidated events into single tables per product across their medallion architecture, reducing pipeline maintenance by 70% and cutting costs by 67%. &#x25C6;</li>
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
# MAGIC ## C. Delta Lake Optimizations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Partitioning and Z-Ordering: The Legacy Approaches
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Hive-style partitioning</strong> physically separates data into directories based on column values. While it works for low-cardinality columns like date, it is frequently misused. Partitioning by high-cardinality columns (such as <code>customer_id</code>) creates thousands of tiny directories, each containing small files. This leads to high metadata operations overhead, slow reads, and skewed write amplification. Even after running <code>OPTIMIZE</code> for compaction, the data skew persists because partitions are rigid, static boundaries.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Z-ordering</strong> re-sorts data files so rows with similar values in selected columns are grouped together, enabling data skipping. However, Z-ordering has four significant drawbacks: (1) it is not incremental and rewrites up to 150 GB per run, (2) it requires manual tuning of column selection and frequency, (3) newly ingested data is not clustered until the next OPTIMIZE, and (4) it risks conflict with business transactions during the rewrite.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Analogy:</strong> Hive partitioning is like organizing a library by putting each author in a separate room. Works fine for 50 authors, but with 10,000 authors you have 10,000 tiny rooms, most nearly empty.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-partitioning-vs-zorder-challenges ── -->
# MAGIC <style>
# MAGIC .c1-chal-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   display: flex;
# MAGIC   gap: 16px;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .c1-chal-card {
# MAGIC   flex: 1;
# MAGIC   border-radius: 10px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .c1-chal-card:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .c1-chal-header {
# MAGIC   padding: 14px 18px;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .c1-chal-header-part { background: #98102A; }
# MAGIC .c1-chal-header-zord { background: #FFAB00; color: #0b2026; }
# MAGIC .c1-chal-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 16px 18px;
# MAGIC }
# MAGIC .c1-chal-item {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC   padding: 4px 0;
# MAGIC   display: flex;
# MAGIC   align-items: flex-start;
# MAGIC   gap: 8px;
# MAGIC }
# MAGIC .c1-chal-x { color: #FF3621; font-weight: 700; min-width: 20px; }
# MAGIC .c1-chal-warn { color: #FFAB00; font-weight: 700; min-width: 20px; }
# MAGIC </style>
# MAGIC <div class="c1-chal-wrapper">
# MAGIC   <div class="c1-chal-card">
# MAGIC     <div class="c1-chal-header c1-chal-header-part">Hive-Style Partitioning</div>
# MAGIC     <div class="c1-chal-body">
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-x">&#x2717;</span> Many small files from high-cardinality columns</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-x">&#x2717;</span> Data skew (some partitions much larger)</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-x">&#x2717;</span> Slow writes due to file movement</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-x">&#x2717;</span> Static, rigid boundaries</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-x">&#x2717;</span> Compaction helps file count, not skew</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="c1-chal-card">
# MAGIC     <div class="c1-chal-header c1-chal-header-zord">Z-Ordering</div>
# MAGIC     <div class="c1-chal-body">
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-warn">&#x26A0;</span> Not incremental: rewrites up to 150 GB</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-warn">&#x26A0;</span> Requires manual tuning of columns</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-warn">&#x26A0;</span> No immediate benefit for new data</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-warn">&#x26A0;</span> Risk of conflict with business writes</div>
# MAGIC       <div class="c1-chal-item"><span class="c1-chal-warn">&#x26A0;</span> Inconsistent performance over time</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When Partitioning Still Makes Sense</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Schema isolation:</strong> multi-tenant architectures where each tenant's data must be physically separated for security or regulatory reasons.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Regulatory deletion:</strong> GDPR or CCPA requirements where data for a region or customer must be physically deletable.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>If you partition:</strong> keep partition sizes between 1 GB and 1 TB, keep the total partition count in the low thousands, partition by date, and combine with Z-ordering on frequently filtered columns.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Databricks guidance:</strong> do not partition tables that contain less than a terabyte of data.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Z-Ordering Limitations</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Not incremental:</strong> each run rewrites up to 150 GB of data files, even if most data is already well-sorted.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Requires tuning:</strong> you must choose the right columns, balance them with partitioning, and schedule runs manually.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>No immediate benefit for new data:</strong> freshly ingested data is not clustered until the next OPTIMIZE run.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Conflict risk:</strong> the rewrite process can conflict with concurrent business transactions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> Z-ordering is like reorganizing your entire closet every weekend. It works, but it is expensive and disrupts everything while it runs.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Core Problem</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Both approaches require manual tuning, produce inconsistent performance over time, and create operational overhead. Liquid Clustering (covered next) addresses each of these challenges.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Comparison: Partitioning vs Z-Ordering vs Liquid Clustering</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Data skew:</strong> Partitioning (poor), Z-ordering (moderate), Liquid Clustering (resistant)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Requires tuning:</strong> Partitioning (yes), Z-ordering (yes), Liquid Clustering (no)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Incremental:</strong> Partitioning (no), Z-ordering (no), Liquid Clustering (yes)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Flexible keys:</strong> Partitioning (requires rewrite), Z-ordering (requires rewrite), Liquid Clustering (change without rewrite)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Conflict risk:</strong> Partitioning (moderate), Z-ordering (high), Liquid Clustering (low, uses row-level concurrency)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Small files:</strong> Partitioning (common), Z-ordering (possible), Liquid Clustering (managed via target file sizes)</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Immediate data skipping:</strong> Partitioning (partial), Z-ordering (no, waits for next OPTIMIZE), Liquid Clustering (via eager clustering on write)</li>
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
# MAGIC ### C2. The KD Tree: How Liquid Clustering Organizes Data
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Liquid Clustering uses a <strong>KD tree</strong> (k-dimensional tree) persisted in the Delta transaction log. With <code style="font-size:14pt;">CLUSTER BY (date, customer_id)</code>, the first level splits by date, the second by customer_id. Leaf nodes point to data files. The four diagrams below walk through the tree structure, data placement on write (eager clustering), and compaction via OPTIMIZE (lazy clustering).</p>
# MAGIC
# MAGIC <p style="font-size:14pt;color:#1B5162;font-weight:700;">KD Tree Structure</p>
# MAGIC <p style="font-size:14pt;color:#333;line-height:1.5;">The root splits at date 2023-02-06. Each branch splits further by customer_id. Leaf nodes (red borders) each point to a target-sized file (~1 GB). Customer C spans multiple leaves instead of one oversized partition. No data skew.</p>
# MAGIC
# MAGIC <div class="mermaid" id="kd-tree-1">
# MAGIC graph TD
# MAGIC A["Col 1: date"] -->|"&lt;= 2023-02-06"| B["Col 2: customer_id"]
# MAGIC A -->|"after 2023-02-06"| C["Col 2: customer_id"]
# MAGIC B -->|"&lt;= B"| D["Leaf1"]
# MAGIC B -->|"after B"| E["Col 1: date"]
# MAGIC C -->|"&lt;= C"| F["Leaf6"]
# MAGIC C -->|"after C"| G["Leaf7"]
# MAGIC E -->|"&lt;= 2023-02-05"| H["Leaf2"]
# MAGIC E -->|"after 2023-02-05"| I["Leaf3"]
# MAGIC E2["Col 2"] -->|"&lt;= C"| J["Leaf4"]
# MAGIC E2 -->|"after D"| K["Leaf5"]
# MAGIC I --> E2
# MAGIC classDef col1 fill:#F0F8FB,stroke:#1B5162,stroke-width:2px,color:#1B5162
# MAGIC classDef col2 fill:#F0FBF5,stroke:#00A972,stroke-width:2px,color:#00A972
# MAGIC classDef leaf fill:#fff,stroke:#98102A,stroke-width:2px,color:#98102A
# MAGIC class A,E col1
# MAGIC class B,C,E2 col2
# MAGIC class D,F,G,H,I,J,K leaf
# MAGIC </div>
# MAGIC
# MAGIC <details>
# MAGIC <summary style="cursor:pointer;list-style:none;user-select:none;padding:10px 16px;background:#F9F7F4;border:2px solid #DCE0E2;border-radius:8px;font-size:14pt;font-weight:700;color:#1B5162;">&#x25B6; Reading the Tree</summary>
# MAGIC <div style="padding:12px 0;">
# MAGIC <ul style="font-size:14pt;color:#333;line-height:1.7;">
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Teal nodes (Col 1: date)</strong> split on the date dimension. The root divides at 2023-02-06.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Green nodes (Col 2: customer_id)</strong> split on the customer dimension within each date range.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Red-bordered leaf nodes</strong> point to data files. Each leaf covers a specific range: Leaf1 covers dates &lt;= 2023-02-06 AND customer_id &lt;= B. Leaf6 covers dates after 2023-02-06 AND customer_id &lt;= C.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Sub-splits handle dense ranges.</strong> The Col 1 node below "after B" splits the date dimension again at 2023-02-05 to keep files at the target size when data for customers C-F is dense.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">No rigid boundaries.</strong> The tree decides what ranges to combine based on data volume. Consistent file sizes regardless of data distribution.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: false, theme: "base", themeVariables: { background: "#FFFFFF", lineColor: "#90A5B1" } });
# MAGIC await new Promise(r => requestAnimationFrame(r));
# MAGIC try { await mermaid.run({ querySelector: "#kd-tree-1" }); } catch(e) { await new Promise(r => setTimeout(r, 1000)); await mermaid.run({ querySelector: "#kd-tree-1" }); }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <p style="font-size:14pt;color:#1B5162;font-weight:700;">Eager Clustering: Data Placement on Write</p>
# MAGIC <p style="font-size:14pt;color:#333;line-height:1.5;">When a new batch exceeds the eager threshold (64 MB for 1 key, up to 1 GB for 4 keys), the optimizer reads the KD tree and writes data directly into the correct leaf positions. Data skipping works immediately. Leaf6 and Leaf7 (amber) receive the new 2023-02-07 data.</p>
# MAGIC
# MAGIC <div class="mermaid" id="kd-tree-2">
# MAGIC graph TD
# MAGIC NEW["New Data"] -.->|"Write"| A3
# MAGIC A3["Col 1: date"] -->|"&lt;= 2023-02-06"| B3["Col 2"]
# MAGIC A3 -->|"after 2023-02-06"| C3["Col 2"]
# MAGIC B3 --> D3["Leaf1"]
# MAGIC B3 --> E3["Col 1"]
# MAGIC C3 --> F3["Leaf6"]
# MAGIC C3 --> G3["Leaf7"]
# MAGIC E3 --> H3["Leaf2"]
# MAGIC E3 --> I3["Leaf3"]
# MAGIC classDef col1 fill:#F0F8FB,stroke:#1B5162,stroke-width:2px,color:#1B5162
# MAGIC classDef col2 fill:#F0FBF5,stroke:#00A972,stroke-width:2px,color:#00A972
# MAGIC classDef leaf fill:#fff,stroke:#98102A,stroke-width:2px,color:#98102A
# MAGIC classDef newdata fill:#FFF8E1,stroke:#E5A100,stroke-width:3px,color:#E5A100
# MAGIC classDef incoming fill:#FFF8E1,stroke:#E5A100,stroke-width:2px,stroke-dasharray:5 5,color:#E5A100
# MAGIC class A3,E3 col1
# MAGIC class B3,C3 col2
# MAGIC class D3,H3,I3 leaf
# MAGIC class F3,G3 newdata
# MAGIC class NEW incoming
# MAGIC </div>
# MAGIC
# MAGIC <details>
# MAGIC <summary style="cursor:pointer;list-style:none;user-select:none;padding:10px 16px;background:#F9F7F4;border:2px solid #DCE0E2;border-radius:8px;font-size:14pt;font-weight:700;color:#1B5162;">&#x25B6; Eager Clustering Details</summary>
# MAGIC <div style="padding:12px 0;">
# MAGIC <ul style="font-size:14pt;color:#333;line-height:1.7;">
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Threshold check:</strong> incoming batch exceeds 64 MB (1 key) to 1 GB (4 keys), so eager clustering activates during the write transaction.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Tree traversal:</strong> the optimizer reads the KD tree boundaries. New rows for 2023-02-07 go right at the root (after 2023-02-06), then split by customer_id into Leaf6 (&lt;= C) and Leaf7 (after C).</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Immediate data skipping:</strong> queries filtering on date = 2023-02-07 skip Leaf1 through Leaf5 entirely. No OPTIMIZE needed for the new data to be query-efficient.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Below threshold:</strong> small writes (under 64 MB) are stored as unclustered files. These accumulate until lazy clustering compacts them during the next OPTIMIZE.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: false, theme: "base", themeVariables: { background: "#FFFFFF", lineColor: "#90A5B1" } });
# MAGIC await new Promise(r => requestAnimationFrame(r));
# MAGIC try { await mermaid.run({ querySelector: "#kd-tree-2" }); } catch(e) { await new Promise(r => setTimeout(r, 1000)); await mermaid.run({ querySelector: "#kd-tree-2" }); }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <p style="font-size:14pt;color:#1B5162;font-weight:700;">Lazy Clustering: OPTIMIZE Compacts and Grows the Tree</p>
# MAGIC <p style="font-size:14pt;color:#333;line-height:1.5;">Small writes accumulate as unclustered fragments. When <code style="font-size:14pt;">OPTIMIZE</code> runs, the Classifier identifies nodes where <code style="font-size:14pt;">files_number</code> exceeds the threshold or <code style="font-size:14pt;">node_size</code> is below the threshold. The Optimizer rewrites only those nodes. If a leaf grows too large, the tree splits it into a new subtree. Below: Leaf7 has grown and splits into Col 1 with four new leaves (Leaf7-Leaf10), shown with dashed borders.</p>
# MAGIC
# MAGIC <div class="mermaid" id="kd-tree-3">
# MAGIC graph TD
# MAGIC A4["Col 1: date"] -->|"&lt;= 2023-02-06"| B4["Col 2"]
# MAGIC A4 -->|"after 2023-02-06"| C4["Col 2"]
# MAGIC B4 --> D4["Leaf1"]
# MAGIC B4 --> E4["Col 1"]
# MAGIC C4 --> F4["Leaf6"]
# MAGIC C4 --> G4["Col 1"]
# MAGIC G4 --> G4a["Col 2"]
# MAGIC G4 --> G4b["Col 2"]
# MAGIC G4a --> L7["Leaf7"]
# MAGIC G4a --> L8["Leaf8"]
# MAGIC G4b --> L9["Leaf9"]
# MAGIC G4b --> L10["Leaf10"]
# MAGIC E4 --> H4["Leaf2"]
# MAGIC E4 --> I4["Leaf3"]
# MAGIC classDef col1 fill:#F0F8FB,stroke:#1B5162,stroke-width:2px,color:#1B5162
# MAGIC classDef col2 fill:#F0FBF5,stroke:#00A972,stroke-width:2px,color:#00A972
# MAGIC classDef leaf fill:#fff,stroke:#98102A,stroke-width:2px,color:#98102A
# MAGIC classDef newleaf fill:#fff,stroke:#E5A100,stroke-width:2px,stroke-dasharray:5 5,color:#E5A100
# MAGIC classDef newsplit fill:#FFF8E1,stroke:#E5A100,stroke-width:2px,stroke-dasharray:5 5,color:#E5A100
# MAGIC class A4,E4 col1
# MAGIC class B4,C4 col2
# MAGIC class D4,F4,H4,I4 leaf
# MAGIC class G4 newsplit
# MAGIC class G4a,G4b newsplit
# MAGIC class L7,L8,L9,L10 newleaf
# MAGIC </div>
# MAGIC
# MAGIC <details>
# MAGIC <summary style="cursor:pointer;list-style:none;user-select:none;padding:10px 16px;background:#F9F7F4;border:2px solid #DCE0E2;border-radius:8px;font-size:14pt;font-weight:700;color:#1B5162;">&#x25B6; Lazy Clustering Details</summary>
# MAGIC <div style="padding:12px 0;">
# MAGIC <ul style="font-size:14pt;color:#333;line-height:1.7;">
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Classifier identifies candidates:</strong> nodes with too many small files (<code style="font-size:14pt;">files_number</code> above threshold) or undersized nodes (<code style="font-size:14pt;">node_size</code> below threshold) are selected for rewrite.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Optimizer rewrites only those nodes.</strong> Well-clustered files are never touched. This is incremental: only fragmented leaf nodes get rewritten.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Tree growth:</strong> when a leaf accumulates too much data, the tree splits it. Leaf7 in the previous step held all data for customer_id after C and date after 2023-02-06. After OPTIMIZE, it becomes a subtree with Col 1 and Col 2 splits, creating Leaf7 through Leaf10.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Row-level concurrency:</strong> OPTIMIZE cannot conflict with concurrent INSERT, UPDATE, or DELETE operations. 80%+ reduction in conflicts compared to Z-ordering.</li>
# MAGIC <li style="font-size:14pt;"><strong style="font-size:14pt;">Triggered by:</strong> manual <code style="font-size:14pt;">OPTIMIZE table_name;</code>, Predictive Optimization (automatic), or scheduled Lakeflow Jobs.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: false, theme: "base", themeVariables: { background: "#FFFFFF", lineColor: "#90A5B1" } });
# MAGIC await new Promise(r => requestAnimationFrame(r));
# MAGIC try { await mermaid.run({ querySelector: "#kd-tree-3" }); } catch(e) { await new Promise(r => setTimeout(r, 1000)); await mermaid.run({ querySelector: "#kd-tree-3" }); }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. How Liquid Clustering Works
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Liquid Clustering uses a <strong>KD tree</strong> (k-dimensional tree) persisted in the Delta transaction log to organize data files by clustering key values. Two clustering modes work together: <strong>Eager clustering</strong> organizes data at write time when the incoming batch exceeds size thresholds (64 MB to 1 GB depending on key count). <strong>Lazy clustering</strong> runs during <code style="font-size:14pt;">OPTIMIZE</code> to compact and reorganize files that were not clustered on write. The KD tree stores the boundary information so the engine only rewrites files that need reorganization.</p>
# MAGIC
# MAGIC <!-- ── Visual: c3-liquid-clustering-internals ── -->
# MAGIC <style>
# MAGIC .c3-v-wrap { width: 100% !important; max-width: 100% !important; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
# MAGIC .c3-v-heading { font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 2px solid #DCE0E2; }
# MAGIC /* Two-panel layout */
# MAGIC .c3-v-panels { display: flex; gap: 16px; }
# MAGIC .c3-v-left { flex: 1; }
# MAGIC .c3-v-right { flex: 1; }
# MAGIC /* Clickable flow steps */
# MAGIC .c3-v-step { padding: 14px 18px; border-radius: 8px; margin-bottom: 8px; cursor: pointer; user-select: none; display: flex; align-items: center; gap: 12px; transition: box-shadow 0.2s, background 0.2s; border: 2px solid transparent; }
# MAGIC .c3-v-step:active { transform: scale(0.98); }
# MAGIC .c3-v-step.c3-v-act { border-color: #1B5162; box-shadow: 0 3px 12px rgba(27,49,57,0.15); }
# MAGIC .c3-v-snum { width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14pt; font-weight: 800; color: #fff; flex-shrink: 0; }
# MAGIC .c3-v-stxt { flex: 1; }
# MAGIC .c3-v-sname { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .c3-v-ssub { font-size: 14pt; color: #5A6F77; margin-top: 2px; }
# MAGIC /* Detail panel */
# MAGIC .c3-v-detail { background: #F9F7F4; border-radius: 10px; padding: 20px; border-left: 5px solid #1B5162; min-height: 280px; }
# MAGIC .c3-v-dtitle { font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 8px; }
# MAGIC .c3-v-dbody { font-size: 14pt; color: #333; line-height: 1.7; }
# MAGIC .c3-v-dbody ul { margin: 8px 0 0 0; padding-left: 20px; }
# MAGIC .c3-v-dbody li { font-size: 14pt; margin-bottom: 6px; }
# MAGIC .c3-v-dbody li strong { font-size: 14pt; }
# MAGIC .c3-v-dbody code { font-size: 14pt; background: #EEEDE9; padding: 1px 5px; border-radius: 3px; }
# MAGIC /* Arrow between steps */
# MAGIC .c3-v-arrow { text-align: center; font-size: 14pt; color: #90A5B1; margin: -2px 0 -2px 14px; }
# MAGIC </style>
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click each step to see how Liquid Clustering processes data</p>
# MAGIC <div class="c3-v-wrap">
# MAGIC   <div class="c3-v-heading">Liquid Clustering: Under the Hood</div>
# MAGIC   <div class="c3-v-panels">
# MAGIC     <div class="c3-v-left">
# MAGIC       <div class="c3-v-step c3-v-act" onclick="c3pick(0)" style="background:#e8f0f3;"><div class="c3-v-snum" style="background:#1B5162;">1</div><div class="c3-v-stxt"><div class="c3-v-sname" style="font-size:14pt;">New Data Arrives</div><div class="c3-v-ssub" style="font-size:14pt;">Batch or streaming write</div></div></div>
# MAGIC       <div class="c3-v-arrow">&#x25BC;</div>
# MAGIC       <div class="c3-v-step" onclick="c3pick(1)" style="background:#F9F7F4;"><div class="c3-v-snum" style="background:#00A972;">2</div><div class="c3-v-stxt"><div class="c3-v-sname" style="font-size:14pt;">Eager Clustering</div><div class="c3-v-ssub" style="font-size:14pt;">Clustering on write</div></div></div>
# MAGIC       <div class="c3-v-arrow">&#x25BC;</div>
# MAGIC       <div class="c3-v-step" onclick="c3pick(2)" style="background:#F9F7F4;"><div class="c3-v-snum" style="background:#2272B4;">3</div><div class="c3-v-stxt"><div class="c3-v-sname" style="font-size:14pt;">KD Tree Metadata</div><div class="c3-v-ssub" style="font-size:14pt;">Persisted in the Delta log</div></div></div>
# MAGIC       <div class="c3-v-arrow">&#x25BC;</div>
# MAGIC       <div class="c3-v-step" onclick="c3pick(3)" style="background:#F9F7F4;"><div class="c3-v-snum" style="background:#618794;">4</div><div class="c3-v-stxt"><div class="c3-v-sname" style="font-size:14pt;">Lazy Clustering</div><div class="c3-v-ssub" style="font-size:14pt;">OPTIMIZE compacts and reorganizes</div></div></div>
# MAGIC       <div class="c3-v-arrow">&#x25BC;</div>
# MAGIC       <div class="c3-v-step" onclick="c3pick(4)" style="background:#F9F7F4;"><div class="c3-v-snum" style="background:#E5A100;">5</div><div class="c3-v-stxt"><div class="c3-v-sname" style="font-size:14pt;">Key Selection</div><div class="c3-v-ssub" style="font-size:14pt;">Choosing and changing clustering keys</div></div></div>
# MAGIC     </div>
# MAGIC     <div class="c3-v-right">
# MAGIC       <div class="c3-v-detail" id="c3-det"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var C3D=[
# MAGIC {t:"New Data Arrives",b:"<ul><li style='font-size:14pt;'><strong style='font-size:14pt;'>The Optimizer evaluates incoming data</strong> against the KD tree to determine where new rows belong in the clustering structure.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Size thresholds trigger eager clustering:</strong> if the write exceeds 64 MB (1 key) to 1 GB (4 keys), the data is clustered on write. Smaller writes are left unclustered for the next OPTIMIZE.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Unlike partitioning,</strong> there are no rigid directory boundaries. The optimizer groups rows into consistently sized files based on clustering key ranges.</li></ul>"},
# MAGIC {t:"Eager Clustering: On Write",b:"<ul><li style='font-size:14pt;'><strong style='font-size:14pt;'>Clustering happens during ingestion.</strong> The optimizer reads the KD tree, identifies which leaf nodes the new data belongs to, and writes files sorted by clustering key values.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Consistent target file sizes</strong> (not partition-bounded). Customer C with 10x more rows than Customer D still produces uniformly sized files. Data skew is gone.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>No rigid boundaries.</strong> The optimizer chooses which ranges of data to combine, so you get good data skipping immediately on new data (unlike Z-ordering which requires a separate OPTIMIZE).</li></ul>"},
# MAGIC {t:"KD Tree Metadata",b:"<ul><li style='font-size:14pt;'><strong style='font-size:14pt;'>The KD tree is persisted in the Delta transaction log.</strong> It records the clustering key boundary values at each node, mapping ranges to leaf nodes (data files).</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>How the tree works:</strong> each level splits on a different clustering key dimension. With <code style='font-size:14pt;'>CLUSTER BY (date, customer_id)</code>, the first level splits by date, the second by customer_id. Leaf nodes point to data files.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Both eager and lazy clustering use this tree.</strong> Eager clustering reads it to place new data. Lazy clustering reads it to identify which leaf nodes need compaction or reorganization.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Incremental by design:</strong> only leaf nodes with new or fragmented data get rewritten. Already well-clustered files are never touched.</li></ul>"},
# MAGIC {t:"Lazy Clustering: OPTIMIZE",b:"<ul><li style='font-size:14pt;'><strong style='font-size:14pt;'>Runs during <code style='font-size:14pt;'>OPTIMIZE</code></strong> (triggered manually, by Predictive Optimization, or via Lakeflow Jobs). Compacts small files and reorganizes data that was not eagerly clustered.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>The Classifier identifies candidates:</strong> leaf nodes with too many small files (files_number threshold) or undersized nodes (node_size threshold) are selected for rewrite.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>The Optimizer rewrites only those nodes.</strong> It merges small files, re-sorts by clustering keys, and produces target-sized output files. The KD tree is updated to reflect the new file layout.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Row-level concurrency:</strong> OPTIMIZE cannot conflict with concurrent INSERT, UPDATE, or DELETE operations. 80%+ reduction in conflicts compared to Z-ordering.</li></ul>"},
# MAGIC {t:"Choosing and Changing Keys",b:"<ul><li style='font-size:14pt;'><strong style='font-size:14pt;'>Maximum 4 clustering keys.</strong> Choose columns most frequently used in query <code style='font-size:14pt;'>WHERE</code> clauses. High-cardinality columns work well (unlike partitioning).</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Syntax:</strong> <code style='font-size:14pt;'>CREATE TABLE t (col0 INT, col1 STRING) CLUSTER BY (col0);</code></li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Change keys without rewriting:</strong> <code style='font-size:14pt;'>ALTER TABLE t CLUSTER BY (col0, col1);</code> updates the KD tree metadata. Existing files keep their old clustering. New writes and the next OPTIMIZE use the new keys.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>CLUSTER BY AUTO:</strong> <code style='font-size:14pt;'>CREATE TABLE t (col0 INT) CLUSTER BY AUTO;</code> lets Predictive Optimization analyze query workload and select the best keys automatically.</li><li style='font-size:14pt;'><strong style='font-size:14pt;'>Streaming support</strong> available in Runtime 16.0+. <code style='font-size:14pt;'>OPTIMIZE table_name FULL;</code> forces reclustering of all data.</li></ul>"}
# MAGIC ];
# MAGIC function c3pick(i){var steps=document.querySelectorAll('.c3-v-step');steps.forEach(function(s,j){s.classList.remove('c3-v-act');s.style.background=j===i?'#e8f0f3':'#F9F7F4';});steps[i].classList.add('c3-v-act');var d=C3D[i],det=document.getElementById('c3-det');det.innerHTML='<div class="c3-v-dtitle" style="font-size:15pt;">'+d.t+'</div><div class="c3-v-dbody" style="font-size:14pt;">'+d.b+'</div>';}
# MAGIC c3pick(0);
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Architecture: How the Pieces Connect</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Liquid metadata</strong> is the KD tree stored inside the Delta log. It tracks which clustering key ranges map to which data files. Both eager and lazy clustering read from and update this same tree.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Eager path:</strong> New Data &#x279C; Optimizer reads KD tree &#x279C; writes clustered files &#x279C; updates KD tree in Delta log. This happens inline during the write transaction.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Lazy path:</strong> Classifier reads KD tree &#x279C; identifies fragmented leaf nodes &#x279C; Optimizer rewrites those nodes &#x279C; updates KD tree. This runs during OPTIMIZE, on serverless compute if Predictive Optimization is enabled.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Analogy:</strong> the KD tree is like a library's card catalog. Eager clustering is a librarian shelving new books in the right section as they arrive. Lazy clustering is the weekend reorganization where the librarian consolidates undersized shelves and re-sorts misplaced books.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Why This Replaces Partitioning and Z-Ordering</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Partitioning problems solved:</strong> no more small files from high-cardinality columns, no data skew, no rigid directory boundaries. Liquid Clustering produces consistent file sizes regardless of data distribution.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Z-ordering problems solved:</strong> incremental (only rewrites fragmented nodes), no write amplification (not 150 GB per run), immediate data skipping on new data via eager clustering, and no conflict risk with business transactions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Five properties:</strong> Fast (faster writes, similar reads to well-tuned partitioned tables), Self-tuning (avoids over- and under-partitioning), Incremental (automatic partial clustering of new data), Skew-resistant (consistent file sizes and low write amplification), Flexible (change clustering columns without rewriting).</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Databricks Guidance</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Use Liquid Clustering for all new tables.</strong> Do not partition tables under 1 TB. If you have existing partitioned tables, migrate to Liquid Clustering with <code style="font-size: 14pt;">ALTER TABLE t CLUSTER BY (col);</code> and the next OPTIMIZE will begin reclustering.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/ifood/lakeflow-declarative-pipelines" style="color: #2272B4; font-size: 14pt;">iFood</a> replaced traditional partitioning with Liquid Clustering across their medallion architecture, reducing pipeline maintenance by 70% and cutting costs by 67%. &#x25C6;</li>
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
# MAGIC ## D. Predictive Optimization

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. AI-Driven Automatic Maintenance
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Managing Delta tables manually requires answering questions like: should I partition? When should I VACUUM? Which columns should I OPTIMIZE on? <strong>Predictive Optimization</strong> replaces these manual decisions with AI. Powered by DatabricksIQ, it analyzes workload telemetry and uses ML models to determine the optimal maintenance schedule for each table, then runs operations using serverless compute.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Three operations are automated:</p>
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>OPTIMIZE:</strong> triggers incremental clustering for enabled tables and improves query performance by optimizing file sizes</li>
# MAGIC   <li><strong>VACUUM:</strong> reduces storage costs by deleting data files no longer referenced by the table</li>
# MAGIC   <li><strong>ANALYZE:</strong> triggers incremental update of statistics to improve query planning</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Analogy:</strong> Predictive Optimization is like a smart thermostat for your data: it analyzes your usage patterns, adjusts automatically, and saves you money without you touching the controls.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-predictive-optimization-cycle ── -->
# MAGIC <style>
# MAGIC .d1-po-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   display: flex;
# MAGIC   gap: 12px;
# MAGIC   margin: 24px 0;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .d1-po-card {
# MAGIC   flex: 1;
# MAGIC   border-radius: 10px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .d1-po-card:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .d1-po-header {
# MAGIC   padding: 14px 16px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .d1-po-h1 { background: #1B5162; }
# MAGIC .d1-po-h2 { background: #618794; }
# MAGIC .d1-po-h3 { background: #00A972; }
# MAGIC .d1-po-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 14px 16px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.6;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC @keyframes d1-po-pulse { 0%,100% { opacity: 0.5; transform: translateX(0); } 50% { opacity: 1; transform: translateX(3px); } }
# MAGIC .d1-po-arrow {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   font-size: 24px;
# MAGIC   color: #618794;
# MAGIC   padding: 0 2px;
# MAGIC   animation: d1-po-pulse 1.5s ease-in-out infinite;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="d1-po-wrapper">
# MAGIC   <div class="d1-po-card">
# MAGIC     <div class="d1-po-header d1-po-h1">Engine Telemetry</div>
# MAGIC     <div class="d1-po-body">Collects query latencies, data sizes, access patterns, and workload shapes from managed tables</div>
# MAGIC   </div>
# MAGIC   <div class="d1-po-arrow">&#x25B6;</div>
# MAGIC   <div class="d1-po-card">
# MAGIC     <div class="d1-po-header d1-po-h2">ML Model Training</div>
# MAGIC     <div class="d1-po-body">DatabricksIQ evaluates table health, ranks maintenance priority, and selects optimal operations</div>
# MAGIC   </div>
# MAGIC   <div class="d1-po-arrow">&#x25B6;</div>
# MAGIC   <div class="d1-po-card">
# MAGIC     <div class="d1-po-header d1-po-h3">Automatic Maintenance</div>
# MAGIC     <div class="d1-po-body">OPTIMIZE (clustering + file sizing), VACUUM (delete unreferenced files), ANALYZE (update statistics)</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Enablement and Requirements</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Enabled by default</strong> for accounts created after November 2024. Existing accounts are being gradually enabled through August 2026.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Managed tables only:</strong> Predictive Optimization does not work on external tables or Delta Sharing recipient tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Inheritance model:</strong> settings cascade from account level to catalog level to schema level. Each level can enable, disable, or inherit from its parent.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Syntax:</strong> <code>ALTER CATALOG catalog_name ENABLE PREDICTIVE OPTIMIZATION;</code></li>
# MAGIC           <li style="font-size: 14pt;"><strong>Cost:</strong> operations run on serverless compute for jobs, billed through the serverless jobs SKU. No separate PO charge.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Automatic Statistics and Row-Level Concurrency</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Automatic statistics:</strong> gathered during data ingestion and proactively via background jobs. Eliminates the need to manually run ANALYZE. Benchmarks show +18% query performance on TPC-DS 3TB and +33% improvement on 100TB datasets.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Row-level concurrency:</strong> OPTIMIZE cannot conflict with other write operations (INSERT, UPDATE, DELETE). Provides 80%+ reduction in conflicts, resolved automatically. Enabled by default with deletion vectors for liquid-clustered and unpartitioned tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Monitoring:</strong> query <code>system.storage.predictive_optimization_operations_history</code> to see all operations, their costs, and impact.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">CLUSTER BY AUTO</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">With <code>CLUSTER BY AUTO</code>, Predictive Optimization analyzes historical query workload for the table to identify the best clustering keys. It is cost-aware about when changes justify the reclustering expense.</li>
# MAGIC           <li style="font-size: 14pt;">Millions of tables in production are already using automatic liquid clustering.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Proof Point</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <strong>Anker</strong> (consumer electronics): "Saved us 50% in annual storage costs while speeding up queries by &gt;2x." The system prioritized their largest and most-accessed tables based on usage telemetry. &#x25C6;</li>
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
# MAGIC ## E. UniForm and Managed Iceberg Tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. The Table Format Fragmentation Problem
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The lakehouse ecosystem is fragmented across three table formats: <strong>Delta Lake</strong> (used by Databricks and Microsoft Fabric), <strong>Apache Iceberg</strong> (used by AWS, Snowflake, and others), and <strong>Apache Hudi</strong> (used by some other platforms). All three share the same underlying data format (Parquet) but differ in their <strong>metadata layer</strong>, which tracks transactions, versions, schema changes, and partitioning. This creates platform silos where data written in one format is not directly readable by engines that expect another.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks offers two solutions: <strong>UniForm</strong> adds Iceberg-compatible metadata to Delta tables so external engines can read them. <strong>Managed Iceberg tables</strong> (GA May 2026) create built-in Iceberg tables governed by Unity Catalog that any Iceberg engine can read and write.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-uniform-vs-managed-iceberg ── -->
# MAGIC <style>
# MAGIC .e1-fmt-wrapper {
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   margin: 24px 0;
# MAGIC }
# MAGIC .e1-fmt-shared {
# MAGIC   background: #1B3139;
# MAGIC   border-radius: 8px 8px 0 0;
# MAGIC   padding: 16px;
# MAGIC   text-align: center;
# MAGIC   color: #fff;
# MAGIC   font-size: 15pt;
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .e1-fmt-shared-sub { font-size: 14pt; color: #90A5B1; font-weight: 400; margin-top: 4px; }
# MAGIC .e1-fmt-meta-row {
# MAGIC   display: flex;
# MAGIC   gap: 0;
# MAGIC }
# MAGIC .e1-fmt-meta {
# MAGIC   flex: 1;
# MAGIC   padding: 16px;
# MAGIC   text-align: center;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC }
# MAGIC .e1-fmt-meta-label { font-size: 14pt; font-weight: 700; margin-bottom: 4px; }
# MAGIC .e1-fmt-meta-desc { font-size: 14pt; color: #5A6F77; line-height: 1.4; }
# MAGIC .e1-fmt-m-delta { background: #F9F7F4; }
# MAGIC .e1-fmt-m-ice { background: #e3f2fd; }
# MAGIC .e1-fmt-m-uni { background: #e8f5e9; border-radius: 0 0 8px 8px; }
# MAGIC .e1-fmt-compare {
# MAGIC   display: flex;
# MAGIC   gap: 16px;
# MAGIC   margin-top: 16px;
# MAGIC }
# MAGIC .e1-fmt-col {
# MAGIC   flex: 1;
# MAGIC   border-radius: 10px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 2px 8px rgba(0,0,0,0.10);
# MAGIC   transition: transform 0.2s, box-shadow 0.2s;
# MAGIC   cursor: default;
# MAGIC }
# MAGIC .e1-fmt-col:hover {
# MAGIC   transform: translateY(-3px);
# MAGIC   box-shadow: 0 6px 16px rgba(27,49,57,0.13);
# MAGIC }
# MAGIC .e1-fmt-col-head {
# MAGIC   padding: 14px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .e1-fmt-col-uni { background: #1B5162; }
# MAGIC .e1-fmt-col-ice { background: #2272B4; }
# MAGIC .e1-fmt-col-body {
# MAGIC   background: #F9F7F4;
# MAGIC   padding: 14px 16px;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC }
# MAGIC .e1-fmt-col-body ul { margin: 0; padding-left: 20px; }
# MAGIC .e1-fmt-col-body li { margin-bottom: 6px; }
# MAGIC </style>
# MAGIC <div class="e1-fmt-wrapper">
# MAGIC   <div class="e1-fmt-shared">
# MAGIC     Shared Foundation: Apache Parquet Data Files
# MAGIC     <div class="e1-fmt-shared-sub">All table formats store data in the same columnar Parquet format; they differ only in the metadata layer</div>
# MAGIC   </div>
# MAGIC   <div class="e1-fmt-compare">
# MAGIC     <div class="e1-fmt-col">
# MAGIC       <div class="e1-fmt-col-head e1-fmt-col-uni">Delta UniForm</div>
# MAGIC       <div class="e1-fmt-col-body">
# MAGIC         <ul>
# MAGIC           <li><strong>Source of truth:</strong> Delta log</li>
# MAGIC           <li><strong>Iceberg metadata:</strong> generated asynchronously</li>
# MAGIC           <li><strong>External writes:</strong> not supported</li>
# MAGIC           <li><strong>External reads:</strong> read-only via Iceberg</li>
# MAGIC           <li><strong>Write overhead:</strong> &lt;5%</li>
# MAGIC           <li><strong>Use when:</strong> Databricks is the primary writer</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="e1-fmt-col">
# MAGIC       <div class="e1-fmt-col-head e1-fmt-col-ice">Managed Iceberg</div>
# MAGIC       <div class="e1-fmt-col-body">
# MAGIC         <ul>
# MAGIC           <li><strong>Source of truth:</strong> Iceberg metadata</li>
# MAGIC           <li><strong>Iceberg metadata:</strong> built-in</li>
# MAGIC           <li><strong>External writes:</strong> supported via Iceberg REST API</li>
# MAGIC           <li><strong>External reads:</strong> read/write from any engine</li>
# MAGIC           <li><strong>Format:</strong> full OSS Iceberg v2/v3</li>
# MAGIC           <li><strong>Use when:</strong> external engines need read/write</li>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">UniForm: Write Once, Read Anywhere</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>How it works:</strong> UniForm generates Iceberg metadata automatically and asynchronously after each Delta Lake commit. The Parquet data files remain unchanged. Each reader (Delta, Iceberg, Hudi) accesses the same table using its own metadata view.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Analogy:</strong> UniForm is like a multilingual subtitle system for a movie. The movie (Parquet data files) stays the same, but the subtitles (metadata) let viewers in different languages (Delta, Iceberg, Hudi) understand it.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Performance:</strong> less than 5% write overhead. Read performance from external engines is in +/-10% of built-in Iceberg tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Limitation:</strong> read-only for Iceberg clients. Delta is the source of truth; writes from external Iceberg engines are not supported.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Enablement:</strong> <code>CREATE TABLE T(c1 INT) TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'iceberg');</code> or <code>ALTER TABLE</code> on existing tables.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Managed Iceberg Tables: Full Read/Write</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>How it works:</strong> <code>CREATE TABLE catalog.schema.table_name (...) USING iceberg;</code> creates a built-in Iceberg table managed by Unity Catalog. Iceberg metadata is the source of truth.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Key difference from UniForm:</strong> external Iceberg engines can both read and write to these tables via the Iceberg REST Catalog API.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Managed Iceberg tables get:</strong> predictive optimization, liquid clustering, and governance through Unity Catalog.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Iceberg v3:</strong> GA on Databricks with deletion vectors, VARIANT type, and row lineage for managed Iceberg, UniForm, and foreign Iceberg tables.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Unity Catalog Iceberg GA (May 2026):</strong> As of May 2026, Unity Catalog Iceberg is GA. Managed Iceberg tables, foreign Iceberg tables, and v3 features including deletion vectors are all generally available. This eliminates format lock-in concerns entirely: you can create and manage Iceberg tables natively alongside Delta tables, all governed by Unity Catalog.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When to Use Which</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Use UniForm</strong> when Databricks is the primary write engine and you need read access from external Iceberg clients (Snowflake, Trino, Athena).</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Use Managed Iceberg</strong> when external engines need to both read and write, or when you want full OSS Iceberg compatibility.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Long-term direction:</strong> the goal is to unify Managed Delta and Managed Iceberg tables.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Catalog-Level Coordination</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Catalog Commits (GA May 2026):</strong> Catalog Commits enable Unity Catalog as the system of coordination, brokering access across engines with version control for catalog objects. This strengthens the multi-engine story for both Delta and Iceberg tables governed by UC.</li>
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
# MAGIC ## F. Lakebase

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### F1. Why Lakebase Exists and How It Works
# MAGIC
# MAGIC <p style="font-size: 14pt; color: #333; line-height: 1.7; max-width: 900px;">
# MAGIC The Lakehouse excels at analytical workloads, but many production systems require low-latency transactional reads and writes that Delta Lake alone cannot deliver. Lakebase fills this OLTP gap by embedding a fully managed PostgreSQL-compatible database directly into the Databricks platform, giving AI agents and applications sub-millisecond state management without leaving the Lakehouse ecosystem.
# MAGIC </p>
# MAGIC
# MAGIC <style>
# MAGIC .f1n-arch-row {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr auto 1fr auto 1fr auto 1fr;
# MAGIC   gap: 0;
# MAGIC   align-items: center;
# MAGIC   margin: 24px 0 16px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f1n-box {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 18px 14px;
# MAGIC   text-align: center;
# MAGIC   cursor: pointer;
# MAGIC   transition: background 0.2s;
# MAGIC   min-height: 70px;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .f1n-box-active {
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .f1n-box-active .f1n-box-title,
# MAGIC .f1n-box-active .f1n-box-sub {
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .f1n-box-title {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC }
# MAGIC .f1n-box-sub {
# MAGIC   font-size: 12pt;
# MAGIC   color: #618794;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .f1n-arrow {
# MAGIC   font-size: 24pt;
# MAGIC   color: #618794;
# MAGIC   text-align: center;
# MAGIC   padding: 0 8px;
# MAGIC }
# MAGIC .f1n-stats {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(3, 1fr);
# MAGIC   gap: 12px;
# MAGIC   margin: 20px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f1n-stat {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #00A972;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 12px;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .f1n-stat-num {
# MAGIC   font-size: 22pt;
# MAGIC   font-weight: 800;
# MAGIC   color: #00A972;
# MAGIC }
# MAGIC .f1n-stat-label {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .f1n-usecases {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(3, 1fr);
# MAGIC   gap: 12px;
# MAGIC   margin: 20px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f1n-usecase {
# MAGIC   background: #fff;
# MAGIC   border: 2px solid #618794;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 14px;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .f1n-usecase-title {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC }
# MAGIC .f1n-usecase-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   margin-top: 6px;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .f1n-detail {
# MAGIC   background: #EEEDE9;
# MAGIC   border-left: 4px solid #1B5162;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 16px 20px;
# MAGIC   margin: 12px 0 20px 0;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   display: none;
# MAGIC }
# MAGIC .f1n-detail-visible {
# MAGIC   display: block;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 4px 0 8px 0;">Click any component to explore its role in the architecture</p>
# MAGIC <div class="f1n-arch-row">
# MAGIC   <div class="f1n-box" id="f1n-b0" onclick="f1nSelect(0)">
# MAGIC     <div class="f1n-box-title">Delta Lake</div>
# MAGIC     <div class="f1n-box-sub">Analytical Storage</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-arrow">&#x2192;</div>
# MAGIC   <div class="f1n-box" id="f1n-b1" onclick="f1nSelect(1)">
# MAGIC     <div class="f1n-box-title">Sync Engine</div>
# MAGIC     <div class="f1n-box-sub">Bidirectional Bridge</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-arrow">&#x2192;</div>
# MAGIC   <div class="f1n-box" id="f1n-b2" onclick="f1nSelect(2)">
# MAGIC     <div class="f1n-box-title">Lakebase</div>
# MAGIC     <div class="f1n-box-sub">PostgreSQL OLTP</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-arrow">&#x2192;</div>
# MAGIC   <div class="f1n-box" id="f1n-b3" onclick="f1nSelect(3)">
# MAGIC     <div class="f1n-box-title">AI Agents / Apps</div>
# MAGIC     <div class="f1n-box-sub">Low-Latency Consumers</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="f1n-detail" id="f1n-detail"></div>
# MAGIC
# MAGIC <div class="f1n-stats">
# MAGIC   <div class="f1n-stat">
# MAGIC     <div class="f1n-stat-num">5x</div>
# MAGIC     <div class="f1n-stat-label">Faster writes vs. prior architecture</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-stat">
# MAGIC     <div class="f1n-stat-num">0.5 - 64 CU</div>
# MAGIC     <div class="f1n-stat-label">Autoscaling range with scale-to-zero</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-stat">
# MAGIC     <div class="f1n-stat-num">O(1)</div>
# MAGIC     <div class="f1n-stat-label">Branch creation (up to 500 branches)</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="f1n-usecases">
# MAGIC   <div class="f1n-usecase">
# MAGIC     <div class="f1n-usecase-title">Agent Memory</div>
# MAGIC     <div class="f1n-usecase-desc">Persistent short-term and long-term state for AI agents via checkpoint and conversation tables</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-usecase">
# MAGIC     <div class="f1n-usecase-title">Feature Serving</div>
# MAGIC     <div class="f1n-usecase-desc">Sub-millisecond reads for real-time feature lookups synced from the Lakehouse gold layer</div>
# MAGIC   </div>
# MAGIC   <div class="f1n-usecase">
# MAGIC     <div class="f1n-usecase-title">Application Backend</div>
# MAGIC     <div class="f1n-usecase-desc">Full CRUD operations for Databricks Apps with native OAuth and pgvector support</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var f1nData = {
# MAGIC   0: "<strong>Delta Lake</strong> provides the analytical foundation: columnar Parquet files with ACID transactions, time travel, and schema enforcement. It handles batch and streaming workloads at scale but is not designed for point lookups or high-frequency transactional writes that OLTP applications demand.",
# MAGIC   1: "<strong>Sync Engine</strong> bridges the two worlds. It reads Delta change data feed (CDF) or snapshots and writes into Lakebase, or reads the Lakebase WAL and writes back to Delta history tables. Three modes are available: snapshot (bulk refresh), triggered (on-demand), and continuous (near real-time streaming).",
# MAGIC   2: "<strong>Lakebase</strong> is a fully managed PostgreSQL-compatible database running on Databricks infrastructure. It delivers 5x faster writes than prior architectures through a 94% WAL reduction, autoscales from 0.5 to 64 CU, supports scale-to-zero (GA May 2026), and provides O(1) branching for up to 500 branches per database.",
# MAGIC   3: "<strong>AI Agents and Apps</strong> connect to Lakebase using standard PostgreSQL drivers (psycopg2, SQLAlchemy, JDBC). Agents use it for conversational memory and checkpointing. Apps use it for CRUD backends. All connections are secured through Databricks OAuth with Unity Catalog governance."
# MAGIC };
# MAGIC function f1nSelect(idx) {
# MAGIC   for (var i = 0; i < 4; i++) {
# MAGIC     var el = document.getElementById('f1n-b' + i);
# MAGIC     if (i === idx) { el.classList.add('f1n-box-active'); }
# MAGIC     else { el.classList.remove('f1n-box-active'); }
# MAGIC   }
# MAGIC   var detail = document.getElementById('f1n-detail');
# MAGIC   detail.innerHTML = f1nData[idx];
# MAGIC   detail.classList.add('f1n-detail-visible');
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Architecture: Managed PostgreSQL on Databricks</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Lakebase runs PostgreSQL 16.x under the hood, fully managed by Databricks with no patching or infrastructure overhead</li>
# MAGIC           <li style="font-size: 14pt;">Governed by Unity Catalog: databases, schemas, and tables appear as securables with standard GRANT/REVOKE controls</li>
# MAGIC           <li style="font-size: 14pt;">Accessible via any PostgreSQL-compatible driver (psycopg2, asyncpg, JDBC, ODBC) using Databricks OAuth tokens</li>
# MAGIC           <li style="font-size: 14pt;">Supports pgvector for embedding storage and similarity search, enabling RAG patterns without an external vector database</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Write Performance: 5x Improvement</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The storage engine achieves a 94% reduction in write-ahead log (WAL) volume compared to standard PostgreSQL</li>
# MAGIC           <li style="font-size: 14pt;">This translates to 5x faster write throughput for transactional workloads</li>
# MAGIC           <li style="font-size: 14pt;">Optimized for the mixed read/write patterns typical of AI agent state management and application backends</li>
# MAGIC           <li style="font-size: 14pt;">Sub-millisecond point reads enable real-time feature serving and session lookups</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Autoscaling: 0.5 to 64 CU with Scale-to-Zero</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Compute Units (CU) scale automatically based on workload, from a minimum of 0.5 CU to a maximum of 64 CU</li>
# MAGIC           <li style="font-size: 14pt;">Scale-to-zero reached General Availability in May 2026, eliminating costs during idle periods</li>
# MAGIC           <li style="font-size: 14pt;">Cold start from zero takes seconds, not minutes, making it practical for bursty agent workloads</li>
# MAGIC           <li style="font-size: 14pt;">Scaling decisions are transparent and logged in system tables for observability</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Branching: O(1) Copy-on-Write</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Creating a branch is an O(1) metadata operation regardless of database size, completing in under a second</li>
# MAGIC           <li style="font-size: 14pt;">Each database supports up to 500 branches for parallel development, testing, and experimentation</li>
# MAGIC           <li style="font-size: 14pt;">Branches are full read/write copies that share storage through copy-on-write, so a 100GB database branch consumes near-zero additional storage initially</li>
# MAGIC           <li style="font-size: 14pt;">Ideal for CI/CD pipelines, A/B testing agent configurations, and safe schema migrations</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Use Cases in Production</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Agent memory:</strong> LangGraph and similar frameworks use Lakebase as a checkpoint store for conversational state and long-term user preferences</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Feature serving:</strong> Gold-layer features synced to Lakebase for sub-millisecond lookup by real-time scoring endpoints</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Application backends:</strong> Databricks Apps use Lakebase for CRUD operations with native OAuth, no external database required</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Operational dashboards:</strong> High-concurrency point queries from BI tools that need faster response than a SQL warehouse provides</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <p style="font-size: 12pt; color: #618794; margin-top: 16px; border-top: 1px solid #EEEDE9; padding-top: 12px;">
# MAGIC         Sources: Databricks Lakebase documentation (2026), Data+AI Summit 2025 keynote, Lakebase GA announcement (May 2026)
# MAGIC         </p>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child { transition: transform 0.2s ease; display: inline-block; }
# MAGIC details[open] summary span:first-child { transform: rotate(90deg); }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### F2. Data Flow: Bidirectional Lakehouse Integration
# MAGIC
# MAGIC <p style="font-size: 14pt; color: #333; line-height: 1.7; max-width: 900px;">
# MAGIC As introduced in F1, Lakebase connects to the Lakehouse through a bidirectional sync engine. This section explores how data moves in each direction, the three sync modes available, and the practical limits that shape architectural decisions.
# MAGIC </p>
# MAGIC
# MAGIC <style>
# MAGIC .f2d-dirs {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(2, 1fr);
# MAGIC   gap: 12px;
# MAGIC   margin: 24px 0 16px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f2d-dir-btn {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 14px;
# MAGIC   text-align: center;
# MAGIC   cursor: pointer;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC }
# MAGIC .f2d-dir-active {
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC }
# MAGIC .f2d-flow {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr auto 1fr auto 1fr auto 1fr;
# MAGIC   gap: 0;
# MAGIC   align-items: center;
# MAGIC   margin: 16px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f2d-flow-box {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #1B5162;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 10px;
# MAGIC   text-align: center;
# MAGIC   min-height: 60px;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .f2d-flow-title {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC }
# MAGIC .f2d-flow-sub {
# MAGIC   font-size: 12pt;
# MAGIC   color: #618794;
# MAGIC   margin-top: 4px;
# MAGIC }
# MAGIC .f2d-flow-arrow {
# MAGIC   font-size: 24pt;
# MAGIC   color: #618794;
# MAGIC   text-align: center;
# MAGIC   padding: 0 8px;
# MAGIC }
# MAGIC .f2d-modes {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(3, 1fr);
# MAGIC   gap: 12px;
# MAGIC   margin: 20px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f2d-mode {
# MAGIC   background: #fff;
# MAGIC   border: 2px solid #618794;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 14px;
# MAGIC   text-align: center;
# MAGIC   cursor: pointer;
# MAGIC }
# MAGIC .f2d-mode-active {
# MAGIC   border-color: #00A972;
# MAGIC   background: #F9F7F4;
# MAGIC }
# MAGIC .f2d-mode-name {
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #1B3139;
# MAGIC }
# MAGIC .f2d-mode-throughput {
# MAGIC   font-size: 14pt;
# MAGIC   color: #00A972;
# MAGIC   font-weight: 700;
# MAGIC   margin-top: 6px;
# MAGIC }
# MAGIC .f2d-mode-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   margin-top: 6px;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .f2d-cdf {
# MAGIC   background: #EEEDE9;
# MAGIC   border-left: 4px solid #E5A100;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 16px 20px;
# MAGIC   margin: 12px 0 20px 0;
# MAGIC   font-size: 14pt;
# MAGIC   color: #333;
# MAGIC   line-height: 1.7;
# MAGIC   display: none;
# MAGIC }
# MAGIC .f2d-cdf-visible {
# MAGIC   display: block;
# MAGIC }
# MAGIC .f2d-forward-panel {
# MAGIC   display: block;
# MAGIC }
# MAGIC .f2d-reverse-panel {
# MAGIC   display: none;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click a direction to see how data flows between the lakehouse and Lakebase</p>
# MAGIC <div class="f2d-dirs">
# MAGIC   <div class="f2d-dir-btn f2d-dir-active" id="f2d-fwd" onclick="f2dDir('fwd')">Lakehouse &#x2192; Lakebase (Forward Sync)</div>
# MAGIC   <div class="f2d-dir-btn" id="f2d-rev" onclick="f2dDir('rev')">Lakebase &#x2192; Lakehouse (Reverse Sync)</div>
# MAGIC </div>
# MAGIC
# MAGIC <div id="f2d-forward-panel" class="f2d-forward-panel">
# MAGIC   <div class="f2d-flow">
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">Gold Layer</div>
# MAGIC       <div class="f2d-flow-sub">Delta Tables</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">Synced Table</div>
# MAGIC       <div class="f2d-flow-sub">CDF / Snapshot</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box" style="border-color: #00A972;">
# MAGIC       <div class="f2d-flow-title" style="color: #00A972;">Lakebase</div>
# MAGIC       <div class="f2d-flow-sub">PostgreSQL Tables</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">Consumers</div>
# MAGIC       <div class="f2d-flow-sub">Agents / Apps / APIs</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="f2d-modes">
# MAGIC     <div class="f2d-mode" id="f2d-m0" onclick="f2dMode(0)">
# MAGIC       <div class="f2d-mode-name">Snapshot</div>
# MAGIC       <div class="f2d-mode-throughput">Bulk Refresh</div>
# MAGIC       <div class="f2d-mode-desc">Full table replacement on each run. Best for small reference tables or initial loads.</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-mode" id="f2d-m1" onclick="f2dMode(1)">
# MAGIC       <div class="f2d-mode-name">Triggered</div>
# MAGIC       <div class="f2d-mode-throughput">On-Demand</div>
# MAGIC       <div class="f2d-mode-desc">Incremental sync using CDF, executed on API call or schedule. Good for hourly or daily refreshes.</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-mode" id="f2d-m2" onclick="f2dMode(2)">
# MAGIC       <div class="f2d-mode-name">Continuous</div>
# MAGIC       <div class="f2d-mode-throughput">Near Real-Time</div>
# MAGIC       <div class="f2d-mode-desc">Streaming pipeline that processes CDF changes as they arrive. Lowest latency for operational workloads.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div id="f2d-reverse-panel" class="f2d-reverse-panel">
# MAGIC   <div class="f2d-flow">
# MAGIC     <div class="f2d-flow-box" style="border-color: #00A972;">
# MAGIC       <div class="f2d-flow-title" style="color: #00A972;">Lakebase</div>
# MAGIC       <div class="f2d-flow-sub">PostgreSQL WAL</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">wal2delta</div>
# MAGIC       <div class="f2d-flow-sub">WAL Consumer</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">History Table</div>
# MAGIC       <div class="f2d-flow-sub">Delta Format</div>
# MAGIC     </div>
# MAGIC     <div class="f2d-flow-arrow">&#x2192;</div>
# MAGIC     <div class="f2d-flow-box">
# MAGIC       <div class="f2d-flow-title">Analytics</div>
# MAGIC       <div class="f2d-flow-sub">SQL / ML / BI</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="f2d-cdf f2d-cdf-visible">
# MAGIC     <strong style="color: #E5A100;">Reverse Sync via wal2delta:</strong> Lakebase streams its write-ahead log (WAL) into Delta history tables in the Lakehouse. This enables analytical queries, ML training, and BI dashboards to operate on transactional data without querying Lakebase directly. The history table preserves the full change stream, making it suitable for auditing and time-series analysis. CDF (Change Data Feed) on the Delta side then enables downstream consumers to process only the incremental changes.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function f2dDir(dir) {
# MAGIC   var fwd = document.getElementById('f2d-fwd');
# MAGIC   var rev = document.getElementById('f2d-rev');
# MAGIC   var fp = document.getElementById('f2d-forward-panel');
# MAGIC   var rp = document.getElementById('f2d-reverse-panel');
# MAGIC   if (dir === 'fwd') {
# MAGIC     fwd.classList.add('f2d-dir-active');
# MAGIC     rev.classList.remove('f2d-dir-active');
# MAGIC     fp.style.display = 'block';
# MAGIC     rp.style.display = 'none';
# MAGIC   } else {
# MAGIC     rev.classList.add('f2d-dir-active');
# MAGIC     fwd.classList.remove('f2d-dir-active');
# MAGIC     fp.style.display = 'none';
# MAGIC     rp.style.display = 'block';
# MAGIC   }
# MAGIC }
# MAGIC function f2dMode(idx) {
# MAGIC   for (var i = 0; i < 3; i++) {
# MAGIC     var el = document.getElementById('f2d-m' + i);
# MAGIC     if (i === idx) { el.classList.add('f2d-mode-active'); }
# MAGIC     else { el.classList.remove('f2d-mode-active'); }
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Sync Modes in Depth</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong>Snapshot:</strong> Replaces the entire Lakebase table on each sync. Simple but expensive for large tables. Best for reference data under 1GB.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Triggered:</strong> Uses Delta CDF to identify changed rows and applies only the deltas. Invoked via API call, pipeline trigger, or cron schedule.</li>
# MAGIC           <li style="font-size: 14pt;"><strong>Continuous:</strong> A persistent streaming job that processes CDF changes with sub-minute latency. Highest cost but lowest data staleness.</li>
# MAGIC           <li style="font-size: 14pt;">All three modes are configured per-table, so a single Lakebase database can mix sync strategies based on freshness requirements</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Limits and Guardrails</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Maximum synced table size: 8TB per table</li>
# MAGIC           <li style="font-size: 14pt;">Maximum synced tables per database: 20 (as of GA)</li>
# MAGIC           <li style="font-size: 14pt;">Source tables must have Change Data Feed enabled for triggered and continuous modes</li>
# MAGIC           <li style="font-size: 14pt;">Schema evolution on the source requires a full re-sync; additive columns propagate automatically but type changes do not</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">CDF and wal2delta: The Reverse Path</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">wal2delta reads the Lakebase write-ahead log and produces Delta-formatted history tables in Unity Catalog</li>
# MAGIC           <li style="font-size: 14pt;">History tables capture every INSERT, UPDATE, and DELETE as append-only rows with operation metadata</li>
# MAGIC           <li style="font-size: 14pt;">Downstream consumers use Delta CDF on the history table to build incremental ETL pipelines</li>
# MAGIC           <li style="font-size: 14pt;">This enables a closed loop: analytics in Delta, transactions in Lakebase, changes flowing both ways</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Type Mapping: PostgreSQL to Delta</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">INTEGER/BIGINT map to INT/BIGINT in Delta; SERIAL types become BIGINT with metadata</li>
# MAGIC           <li style="font-size: 14pt;">VARCHAR/TEXT map to STRING; JSONB maps to STRING (query with from_json or parse downstream)</li>
# MAGIC           <li style="font-size: 14pt;">TIMESTAMP WITH TIME ZONE maps to TIMESTAMP_NTZ in Delta (UTC normalized)</li>
# MAGIC           <li style="font-size: 14pt;">pgvector VECTOR type maps to ARRAY&lt;FLOAT&gt; in Delta, preserving dimensionality in column metadata</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Superhuman Story: Real-Time Email Intelligence</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Superhuman uses Lakebase to serve AI-generated email summaries and priority scores to their desktop and mobile clients</li>
# MAGIC           <li style="font-size: 14pt;">Gold-layer ML features sync via continuous mode for sub-second freshness</li>
# MAGIC           <li style="font-size: 14pt;">The reverse path streams user interaction data back to Delta for model retraining and A/B test analysis</li>
# MAGIC           <li style="font-size: 14pt;">Before Lakebase, this required a separate RDS instance and custom CDC pipeline, adding operational cost and latency</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <p style="font-size: 12pt; color: #618794; margin-top: 16px; border-top: 1px solid #EEEDE9; padding-top: 12px;">
# MAGIC         Sources: Databricks Lakebase sync documentation (2026), Data+AI Summit 2025 breakout sessions, Superhuman engineering blog
# MAGIC         </p>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </details>
# MAGIC <style>
# MAGIC details summary span:first-child { transition: transform 0.2s ease; display: inline-block; }
# MAGIC details[open] summary span:first-child { transform: rotate(90deg); }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### F3. Building on Lakebase: AI Agents and Apps
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">With the data flow patterns from F2 established, this section examines how AI agents and Databricks Apps use Lakebase as their operational backbone. The two primary patterns are agent memory (short-term checkpointing and long-term preferences) and full application backends with OAuth-secured CRUD operations.</p>
# MAGIC
# MAGIC <!-- ── Visual: f3a-tabbed ── -->
# MAGIC <style>
# MAGIC .f3a-tabs {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 0;
# MAGIC   margin: 24px 0 0 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f3a-tab {
# MAGIC   padding: 14px 20px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   cursor: pointer;
# MAGIC   border: 2px solid #EEEDE9;
# MAGIC   border-bottom: none;
# MAGIC   border-radius: 10px 10px 0 0;
# MAGIC   background: #F9F7F4;
# MAGIC   color: #618794;
# MAGIC }
# MAGIC .f3a-tab-active {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   border-color: #1B3139;
# MAGIC }
# MAGIC .f3a-panel {
# MAGIC   border: 2px solid #1B3139;
# MAGIC   border-radius: 0 0 10px 10px;
# MAGIC   padding: 24px;
# MAGIC   margin: 0 0 24px 0;
# MAGIC   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC }
# MAGIC .f3a-panel-hidden { display: none; }
# MAGIC
# MAGIC /* Tab 1: Agent Memory */
# MAGIC .f3a-mem-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 14px;
# MAGIC   margin-bottom: 16px;
# MAGIC }
# MAGIC .f3a-mem-card {
# MAGIC   border: 2px solid #EEEDE9;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px;
# MAGIC   cursor: pointer;
# MAGIC   transition: border-color 0.2s;
# MAGIC }
# MAGIC .f3a-mem-card:active { border-color: #1B5162; }
# MAGIC .f3a-mem-title { font-size: 14pt; font-weight: 700; color: #1B3139; margin-bottom: 4px; }
# MAGIC .f3a-mem-sub { font-size: 12pt; color: #618794; margin-bottom: 10px; }
# MAGIC .f3a-mem-detail { font-size: 13pt; color: #333; line-height: 1.6; }
# MAGIC .f3a-lb-box {
# MAGIC   background: #1B3139;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 16px 20px;
# MAGIC   text-align: center;
# MAGIC   margin-top: 8px;
# MAGIC }
# MAGIC .f3a-lb-title { font-size: 14pt; font-weight: 700; color: #fff; margin-bottom: 10px; }
# MAGIC .f3a-pill-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(4, 1fr);
# MAGIC   gap: 8px;
# MAGIC }
# MAGIC .f3a-pill {
# MAGIC   background: #1B5162;
# MAGIC   color: #fff;
# MAGIC   border-radius: 20px;
# MAGIC   padding: 6px 10px;
# MAGIC   font-size: 12pt;
# MAGIC   text-align: center;
# MAGIC   cursor: pointer;
# MAGIC }
# MAGIC .f3a-pill:active { background: #00A972; }
# MAGIC .f3a-pill-tip { font-size: 12pt; color: #90A5B1; text-align: center; margin-top: 10px; min-height: 20px; }
# MAGIC
# MAGIC /* Tab 2: Competitive Matrix */
# MAGIC .f3a-comp-table {
# MAGIC   width: 100%;
# MAGIC   border-collapse: collapse;
# MAGIC   font-size: 14pt;
# MAGIC }
# MAGIC .f3a-comp-table th {
# MAGIC   background: #1B3139;
# MAGIC   color: #fff;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 700;
# MAGIC   padding: 16px 20px;
# MAGIC   text-align: left;
# MAGIC }
# MAGIC .f3a-comp-table th.f3a-hl { background: #1B5162; }
# MAGIC .f3a-comp-table td {
# MAGIC   padding: 14px 20px;
# MAGIC   border-bottom: 2px solid #EEEDE9;
# MAGIC   font-size: 14pt;
# MAGIC   vertical-align: middle;
# MAGIC }
# MAGIC .f3a-comp-table td.f3a-hl { background: #F0F8FB; font-weight: 700; color: #1B3139; font-size: 14pt; }
# MAGIC .f3a-comp-table td.f3a-cap { font-weight: 700; color: #1B3139; background: #F9F7F4; font-size: 14pt; }
# MAGIC .f3a-comp-table tr:hover td { background: #F9F7F4; }
# MAGIC .f3a-yes { color: #00A972; font-weight: 700; font-size: 14pt; }
# MAGIC .f3a-no { color: #98102A; font-weight: 700; font-size: 14pt; }
# MAGIC .f3a-partial { color: #E5A100; font-weight: 700; font-size: 14pt; }
# MAGIC .f3a-note { font-size: 14pt; color: #618794; margin-top: 20px; line-height: 1.7; padding: 14px 0; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tab Buttons -->
# MAGIC <p style="font-size: 12pt; color: #90A5B1; font-style: italic; margin: 8px 0 4px 0;">Click the tabs to switch between Agent Memory Architecture and Competitive Comparison</p>
# MAGIC <div class="f3a-tabs">
# MAGIC   <div class="f3a-tab f3a-tab-active" id="f3a-tab-mem" onclick="f3aSwitch('mem')">Agent Memory Architecture</div>
# MAGIC   <div class="f3a-tab" id="f3a-tab-comp" onclick="f3aSwitch('comp')">Competitive Comparison</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 1: Agent Memory -->
# MAGIC <div class="f3a-panel" id="f3a-panel-mem">
# MAGIC
# MAGIC   <div class="f3a-mem-grid">
# MAGIC     <div class="f3a-mem-card" style="border-left: 4px solid #00A972;" onclick="f3aPill('cp')">
# MAGIC       <div class="f3a-mem-title">Short-Term Memory</div>
# MAGIC       <div class="f3a-mem-sub">Thread-scoped conversation checkpoints</div>
# MAGIC       <div class="f3a-mem-detail"><strong style="font-size: 13pt;">CheckpointSaver</strong> writes agent state after every LangGraph node execution. Each conversation thread gets its own checkpoint chain. Model Serving endpoints can resume or fork from any prior checkpoint (time travel).</div>
# MAGIC     </div>
# MAGIC     <div class="f3a-mem-card" style="border-left: 4px solid #2574B5;" onclick="f3aPill('ds')">
# MAGIC       <div class="f3a-mem-title">Long-Term Memory</div>
# MAGIC       <div class="f3a-mem-sub">User-scoped persistent knowledge</div>
# MAGIC       <div class="f3a-mem-detail"><strong style="font-size: 13pt;">DatabricksStore</strong> persists user facts, preferences, and summaries across sessions. Retrieval uses <strong style="font-size: 13pt;">pgvector</strong> semantic search, so agents surface relevant context by similarity, not just exact match.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="text-align: center; font-size: 18pt; color: #618794; margin: 4px 0;">&#x2193;</div>
# MAGIC
# MAGIC   <div class="f3a-lb-box">
# MAGIC     <div class="f3a-lb-title">Lakebase (PostgreSQL 17)</div>
# MAGIC     <div class="f3a-pill-grid">
# MAGIC       <div class="f3a-pill" onclick="f3aPill('cp')">CheckpointSaver</div>
# MAGIC       <div class="f3a-pill" onclick="f3aPill('acp')">AsyncCheckpointSaver</div>
# MAGIC       <div class="f3a-pill" onclick="f3aPill('ds')">DatabricksStore</div>
# MAGIC       <div class="f3a-pill" onclick="f3aPill('ads')">AsyncDatabricksStore</div>
# MAGIC     </div>
# MAGIC     <div class="f3a-pill-tip" id="f3a-pill-tip">Click an API class above to see its role</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="f3a-note">All four classes accept <code style="font-size: 12pt;">instance_name</code> as the primary constructor parameter. Connection pooling and credential rotation are handled internally. Async variants support non-blocking I/O on Model Serving endpoints.</div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- Panel 2: Competitive Comparison -->
# MAGIC <div class="f3a-panel f3a-panel-hidden" id="f3a-panel-comp">
# MAGIC
# MAGIC   <table class="f3a-comp-table">
# MAGIC     <thead>
# MAGIC       <tr>
# MAGIC         <th style="width: 22%;">Capability</th>
# MAGIC         <th class="f3a-hl" style="width: 18%;">Lakebase</th>
# MAGIC         <th style="width: 18%;">RDS / Aurora</th>
# MAGIC         <th style="width: 22%;">Snowflake pg_lake</th>
# MAGIC         <th style="width: 20%;">Neon (standalone)</th>
# MAGIC       </tr>
# MAGIC     </thead>
# MAGIC     <tbody>
# MAGIC       <tr><td class="f3a-cap">UC Governance</td><td class="f3a-hl"><span class="f3a-yes">Native</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-no">No</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">Synced Tables / CDF</td><td class="f3a-hl"><span class="f3a-yes">Yes</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-partial">Via Iceberg</span></td><td><span class="f3a-no">No</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">Scale-to-Zero</td><td class="f3a-hl"><span class="f3a-yes">GA (May 2026)</span></td><td><span class="f3a-partial">Min 0.5 ACU</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-yes">Yes</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">O(1) Branching</td><td class="f3a-hl"><span class="f3a-yes">500 max</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-yes">Yes</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">pgvector</td><td class="f3a-hl"><span class="f3a-yes">0.8.0</span></td><td><span class="f3a-yes">Yes</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-yes">Yes</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">Databricks Apps OAuth</td><td class="f3a-hl"><span class="f3a-yes">Native</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-no">No</span></td><td><span class="f3a-no">No</span></td></tr>
# MAGIC       <tr><td class="f3a-cap">Postgres Engine</td><td class="f3a-hl">Neon (PG17)</td><td>AWS (PG16)</td><td>Crunchy (pg_lake)</td><td>Neon (PG17)</td></tr>
# MAGIC     </tbody>
# MAGIC   </table>
# MAGIC
# MAGIC   <div class="f3a-note"><strong style="font-size: 12pt; color: #1B5162;">Key distinction:</strong> Lakebase replaces the Postgres storage layer with Neon inside the Databricks platform. Snowflake pg_lake extends Postgres to query Iceberg data from outside. "If you want Postgres inside your lakehouse, Lakebase. If you want Postgres to query the lake, pg_lake." (Source: <a href="https://thebuild.com/blog/2026/05/12/snowflake-postgres-lakebase-horizondb-picking-the-lock-in-you-want/" style="font-size: 12pt; color: #2574B5;">thebuild.com</a>)</div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function f3aSwitch(tab) {
# MAGIC   var tabs = ['mem', 'comp'];
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     var t = tabs[i];
# MAGIC     var tabEl = document.getElementById('f3a-tab-' + t);
# MAGIC     var panelEl = document.getElementById('f3a-panel-' + t);
# MAGIC     if (t === tab) {
# MAGIC       tabEl.className = 'f3a-tab f3a-tab-active';
# MAGIC       panelEl.className = 'f3a-panel';
# MAGIC     } else {
# MAGIC       tabEl.className = 'f3a-tab';
# MAGIC       panelEl.className = 'f3a-panel f3a-panel-hidden';
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC var f3aPillData = {
# MAGIC   cp: 'CheckpointSaver: Synchronous LangGraph checkpoint writer. Saves thread state after each node. Primary param: instance_name.',
# MAGIC   acp: 'AsyncCheckpointSaver: Async variant for non-blocking I/O. Use with async LangGraph graphs on Model Serving.',
# MAGIC   ds: 'DatabricksStore: Synchronous long-term memory store. Persists user-scoped facts with optional pgvector semantic retrieval.',
# MAGIC   ads: 'AsyncDatabricksStore: Async variant. All four classes accept instance_name and handle connection pooling internally.'
# MAGIC };
# MAGIC function f3aPill(key) {
# MAGIC   var tip = document.getElementById('f3a-pill-tip');
# MAGIC   if (tip) { tip.textContent = f3aPillData[key] || ''; }
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Agent Memory: Short-Term Checkpointing</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">LangGraph assigns a <strong style="font-size: 14pt;">thread_id</strong> to each conversation. After every graph node executes, the checkpoint saver writes the full state to a Lakebase row keyed by <code style="font-size: 14pt;">thread_id + checkpoint_id</code>.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Time travel:</strong> Model Serving endpoints can retrieve any prior checkpoint and resume or fork the graph at that point, without replaying earlier steps.</li>
# MAGIC           <li style="font-size: 14pt;">Four API classes in <code style="font-size: 14pt;">databricks_langchain</code>: <strong style="font-size: 14pt;">CheckpointSaver</strong>, <strong style="font-size: 14pt;">AsyncCheckpointSaver</strong>, <strong style="font-size: 14pt;">DatabricksStore</strong>, <strong style="font-size: 14pt;">AsyncDatabricksStore</strong>. All accept <code style="font-size: 14pt;">instance_name</code> as primary parameter.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Agent Memory: Long-Term Knowledge Store</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">DatabricksStore</strong> persists user-scoped facts, preferences, and summaries across sessions. Scope is separate from thread-scoped checkpoints.</li>
# MAGIC           <li style="font-size: 14pt;">Retrieval uses <strong style="font-size: 14pt;">pgvector</strong> semantic search: the agent embeds a query and retrieves closest stored memories by cosine similarity.</li>
# MAGIC           <li style="font-size: 14pt;">Because the store lives in Lakebase under Unity Catalog, access is governed by existing column-level and row-level permissions.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Databricks Apps Integration</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">app.yaml</strong> declares the Lakebase connection under a named resource block. The runtime injects host and credentials as environment variables.</li>
# MAGIC           <li style="font-size: 14pt;">OAuth tokens expire after <strong style="font-size: 14pt;">1 hour</strong>. The <strong style="font-size: 14pt;">OAuthConnection</strong> class handles rotation transparently.</li>
# MAGIC           <li style="font-size: 14pt;">Source: <a href="https://docs.databricks.com/aws/en/oltp/projects/tutorial-databricks-apps-autoscaling" style="font-size: 14pt; color: #2574B5;">Databricks Apps + Lakebase tutorial</a></li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Competitive Positioning</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">vs RDS/Aurora:</strong> Aurora Serverless v2 has a minimum floor of 0.5 ACU and no branching. Lakebase adds UC governance, Synced Tables, and scale-to-zero.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">vs Snowflake pg_lake:</strong> Different architecture. pg_lake extends Postgres to query Iceberg from outside. Lakebase replaces the Postgres storage layer with Databricks-native infrastructure.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">vs Neon (standalone):</strong> Same Neon engine. Lakebase adds UC governance, Synced Tables, CDF, and Apps integration that standalone Neon lacks.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Stories and Adoption</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/blog/how-nops-rebuilt-their-cloud-optimization-platform-databricks-lakebase-and-why-other-isvs" style="font-size: 14pt; color: #2574B5;">nOps</a> rebuilt their cloud optimization platform on Lakebase from RDS. The platform manages $4B in cloud spend. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/yipitdata/ai" style="font-size: 14pt; color: #2574B5;">YipitData</a> runs an AI agent pipeline processing 1 million records per hour with 92-95% accuracy and 20x coverage expansion. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/superhuman/lakebase" style="font-size: 14pt; color: #2574B5;">Superhuman</a> reduced on-call burden from 3 days per shift to approximately 1 hour (20x improvement) while serving 200K QPS across 40M daily users. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/blog/announcing-databricks-lakebase-launch-partners" style="font-size: 14pt; color: #2574B5;">Replit</a> reached production in 3 weeks and reported 10x developer velocity. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">Since June 2025, Lakebase adoption has grown at 2x the rate of Databricks SQL data warehousing over its comparable early period.</li>
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
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">This lecture covered the storage layer of the Databricks platform from the ground up. You started with Delta Lake as the foundation: ACID transactions, schema enforcement, and time travel bring data warehouse reliability to open cloud storage. The medallion architecture organizes that storage into Bronze, Silver, and Gold layers with progressive data quality. Delta Lake optimizations evolved from manual partitioning and Z-ordering to Liquid Clustering, which provides incremental, self-tuning data layout with no manual intervention. Predictive Optimization takes this further by using AI to automate OPTIMIZE, VACUUM, and ANALYZE operations on managed tables. UniForm and Managed Iceberg tables solve cross-platform interoperability, enabling Delta tables to be read by any Iceberg engine. And Lakebase extends the platform with fully managed PostgreSQL for OLTP workloads that require sub-second latency.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Key takeaways from this lecture include:</p>
# MAGIC <ul style="font-size: 14pt; line-height: 1.7; color: #333;">
# MAGIC   <li><strong>Delta Lake is the default:</strong> every table on Databricks is a Delta table unless specified otherwise. The transaction log provides ACID, time travel, and schema enforcement on top of open Parquet files.</li>
# MAGIC   <li><strong>Medallion organizes data quality:</strong> Bronze (raw), Silver (validated), and Gold (enriched) layers separate concerns across ingestion, transformation, and consumption.</li>
# MAGIC   <li><strong>Liquid Clustering replaces partitioning and Z-ordering:</strong> it is incremental, self-tuning, skew-resistant, and flexible. Use <code>CLUSTER BY AUTO</code> to let Predictive Optimization select keys.</li>
# MAGIC   <li><strong>Predictive Optimization automates maintenance:</strong> AI-driven OPTIMIZE, VACUUM, and ANALYZE on managed tables using serverless compute.</li>
# MAGIC   <li><strong>UniForm enables cross-platform reads; Managed Iceberg enables cross-platform writes:</strong> choose based on whether external engines need read-only or read/write access.</li>
# MAGIC   <li><strong>Lakebase brings OLTP to the Lakehouse:</strong> fully managed PostgreSQL (GA on AWS, February 2026) built on the Neon engine with 5x faster writes, autoscaling with scale-to-zero (GA May 2026), and copy-on-write branching. Bidirectional data flow via Synced Tables (lakehouse to Lakebase) and CDF via wal2delta (Lakebase to lakehouse). Serves as the memory layer for AI agents and the transactional backend for Databricks Apps.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;"><strong>Next:</strong> The Activity gives you hands-on practice applying these storage concepts to real customer scenarios.</p>
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333; font-size: 14pt;">
# MAGIC         <li style="font-size: 14pt;">What is Delta Lake? (<a href="https://docs.databricks.com/aws/en/delta/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/delta/" style="color: #2574B5;">GCP</a>): Overview of Delta Lake as the default storage layer on Databricks</li>
# MAGIC         <li style="font-size: 14pt;">Medallion architecture (<a href="https://docs.databricks.com/aws/en/lakehouse/medallion" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/lakehouse/medallion" style="color: #2574B5;">GCP</a>): Bronze, Silver, and Gold layer definitions and ingestion patterns</li>
# MAGIC         <li style="font-size: 14pt;">Use liquid clustering (<a href="https://docs.databricks.com/aws/en/delta/clustering" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/clustering" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/delta/clustering" style="color: #2574B5;">GCP</a>): Syntax, key selection guidelines, and comparison to partitioning</li>
# MAGIC         <li style="font-size: 14pt;">Predictive optimization (<a href="https://docs.databricks.com/aws/en/optimizations/predictive-optimization" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/optimizations/predictive-optimization" style="color: #2574B5;">GCP</a>): AI-driven table maintenance for managed tables</li>
# MAGIC         <li style="font-size: 14pt;">UniForm: Iceberg reads (<a href="https://docs.databricks.com/aws/en/delta/uniform" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/uniform" style="color: #2574B5;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/delta/uniform" style="color: #2574B5;">GCP</a>): Multi-format interoperability for Delta tables</li>
# MAGIC         <li style="font-size: 14pt;">Lakebase Postgres (<a href="https://docs.databricks.com/aws/en/oltp/" style="color: #2574B5;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/oltp/" style="color: #2574B5;">Azure</a>): Managed PostgreSQL for OLTP workloads on Databricks (GCP not yet available)</li>
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
