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
# MAGIC # 8 Lecture: Machine Learning and Generative AI
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Machine learning and generative AI are reshaping how organizations extract value from data, but taking models from experimentation to production remains one of the most persistent challenges in enterprise technology. The Databricks platform addresses this by unifying data preparation, model development, and production deployment under a single governance framework.
# MAGIC
# MAGIC This module walks through the Databricks ML and AI stack: the platform architecture that supports both classic ML and generative AI, the lifecycle management tools that move models from notebooks to production endpoints, and the security framework that helps organizations adopt AI responsibly.
# MAGIC
# MAGIC This lecture covers 8 sections:
# MAGIC
# MAGIC - **A. The ML Platform:** Mosaic AI, ML Runtime, and the three focus areas that support the full data-to-AI lifecycle
# MAGIC - **B. MLflow:** Experiment tracking, model packaging, model registry, and the MLflow 3 GenAI capabilities
# MAGIC - **C. Generative AI on Databricks:** Four approaches to building GenAI applications, from prompt engineering to pre-training
# MAGIC - **D. RAG Architecture and Vector Search:** Embeddings, retrieval, and the compound AI systems that power knowledge-grounded applications
# MAGIC - **E. AI Agents and Tools:** Agent architecture, chains vs. agents, MCP integration, and prototyping with AI Playground
# MAGIC - **F. Model Serving and Production:** Endpoint types, AI Gateway governance, and the path from development to production
# MAGIC - **G. Agentic Machine Learning:** Genie Code for conversational, transparent ML development across the full lifecycle
# MAGIC - **H. AI Security:** The Databricks AI Security Framework (DASF) for risk management across AI deployments
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Describe the three focus areas of the Databricks ML platform (data accessibility, team productivity, lifecycle standardization) and how they support the full ML lifecycle
# MAGIC - Explain the core components of MLflow (Tracking, Models, Model Registry) and how MLflow 3 extends them for GenAI with tracing, evaluation, and prompt management
# MAGIC - Compare the four GenAI application approaches (Prompt Engineering, RAG, Fine-Tuning, Pre-Training) and identify when each is appropriate
# MAGIC - Describe how RAG works on Databricks, including the roles of Vector Search, embedding models, and agents in building retrieval-augmented applications
# MAGIC - Explain how Mosaic AI Model Serving provides a unified interface for custom models, foundation models, and external models, with AI Gateway as the governance layer
# MAGIC - Identify the key elements of the Databricks AI Security Framework (DASF), including its 12 AI system components, risk categories, and approach to selecting mitigation controls

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The ML Platform
# MAGIC
# MAGIC The architecture supporting the full data-to-AI lifecycle on Databricks rests on three focus areas that work together to take data teams from raw data to production models.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Three Focus Areas
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">The Databricks ML platform is built on three pillars: <strong>high-quality data, readily accessible</strong> through Spark and Delta Lake; <strong>increased data team productivity</strong> through ML Workspace and Runtime; and <strong>standardized ML lifecycle</strong> through MLflow. Together, these pillars support the full spectrum from classic ML through deep learning to generative AI and agentic systems.</p>
# MAGIC
# MAGIC <!-- ── Visual: a1-three-pillars ── -->
# MAGIC <style>
# MAGIC .a1p-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a1p-row { display: flex; gap: 16px; align-items: stretch; }
# MAGIC .a1p-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a1p-card:hover { transform: translateY(-4px); box-shadow: 0 8px 24px rgba(27,49,57,0.16); }
# MAGIC .a1p-accent { height: 7px; flex-shrink: 0; }
# MAGIC .a1p-body { padding: 20px 18px; flex: 1; background: #fff; }
# MAGIC .a1p-pill { display: inline-block; font-size: 14pt; font-weight: 700; letter-spacing: 0.7px; text-transform: uppercase; padding: 2px 10px; border-radius: 20px; margin-bottom: 10px; }
# MAGIC .a1p-title { font-size: 15pt; font-weight: 700; color: #1B3139; margin-bottom: 8px; }
# MAGIC .a1p-desc { font-size: 14pt; color: #444; line-height: 1.6; margin-bottom: 8px; }
# MAGIC .a1p-items { font-size: 14pt; color: #333; line-height: 1.7; margin: 8px 0 0 16px; padding: 0; }
# MAGIC .a1p-items li { margin-bottom: 4px; }
# MAGIC .a1p-detail { max-height: 0; overflow: hidden; transition: max-height 0.3s; font-size: 14pt; color: #618794; padding: 0 4px; }
# MAGIC .a1p-card:hover .a1p-detail { max-height: 80px; }
# MAGIC .a1p-gov { background: #1B3139; border-radius: 10px; padding: 16px 24px; margin-top: 16px; text-align: center; box-shadow: 0 3px 12px rgba(27,49,57,0.15); }
# MAGIC .a1p-gov-text { font-size: 14pt; font-weight: 600; color: #fff; }
# MAGIC </style>
# MAGIC <div class="a1p-wrap">
# MAGIC <div class="a1p-row">
# MAGIC   <div class="a1p-card"><div class="a1p-accent" style="background:#1B5162;"></div><div class="a1p-body"><span class="a1p-pill" style="background:#e8f0fe;color:#2b6cb0;">Pillar 1</span><div class="a1p-title">Accessible Data</div><div class="a1p-desc">High-quality data from any source, discoverable and ready for ML</div><ul class="a1p-items"><li>Apache Spark for distributed processing</li><li>Delta Lake for ACID transactions</li><li>Feature Store for ML-ready features</li></ul><div class="a1p-detail">Like a restaurant kitchen: Spark and Delta prepare the raw ingredients</div></div></div>
# MAGIC   <div class="a1p-card"><div class="a1p-accent" style="background:#618794;"></div><div class="a1p-body"><span class="a1p-pill" style="background:#f0f2f4;color:#4a5568;">Pillar 2</span><div class="a1p-title">Team Productivity</div><div class="a1p-desc">Use preferred tools, languages, and frameworks on one platform</div><ul class="a1p-items"><li>ML Runtime with pre-configured libraries</li><li>GPU support and distributed training</li><li>Collaborative notebooks (Python, R, Scala, SQL)</li></ul><div class="a1p-detail">One environment for data engineers, scientists, and analysts</div></div></div>
# MAGIC   <div class="a1p-card"><div class="a1p-accent" style="background:#2272B4;"></div><div class="a1p-body"><span class="a1p-pill" style="background:#e8f0f8;color:#2272B4;">Pillar 3</span><div class="a1p-title">Lifecycle Standardization</div><div class="a1p-desc">Move models from experimentation to production with consistency</div><ul class="a1p-items"><li>MLflow for tracking and registry</li><li>Model Serving for deployment</li><li>Lakehouse Monitoring for drift detection</li></ul><div class="a1p-detail">Only ~54% of AI projects make it to production (Gartner)</div></div></div>
# MAGIC </div>
# MAGIC <div class="a1p-gov"><div class="a1p-gov-text">Unity Catalog: Unified Governance Across Data, Features, Models, and Functions</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 1: Accessible Data via Spark and Delta Lake</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Spark provides distributed data processing that handles datasets too large for a single machine. Delta Lake adds ACID transactions, schema enforcement, and time travel on top of cloud object storage.</li>
# MAGIC           <li style="font-size: 14pt;">The Feature Store lets teams create, manage, and serve ML features with automated pipelines and feature discovery, so work done by one team can be reused by others.</li>
# MAGIC           <li style="font-size: 14pt;">Think of it like a restaurant kitchen: Spark and Delta prepare the raw ingredients (data), and the Feature Store is the mise en place station that has everything chopped, measured, and ready for the chef (data scientist) to use.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 2: Team Productivity via ML Workspace and Runtime</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">ML Runtime</strong> provides pre-configured clusters with scikit-learn, XGBoost, PyTorch, LightGBM, MLflow, and other ML libraries, plus optimized GPU drivers. This is not just "Python with extra packages"; it includes framework-specific performance tuning and distributed training libraries (Ray, TorchDistributor, DeepSpeed).</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">AI Runtime</strong> is a newer option offering serverless GPU compute for deep learning training and inference, removing the need to configure cluster hardware manually.</li>
# MAGIC           <li style="font-size: 14pt;">Collaborative notebooks support Python, R, Scala, and SQL, so data engineers, data scientists, and analysts can work in the same environment.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Pillar 3: Lifecycle Standardization via MLflow</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">MLflow tracks experiments, packages models, manages model versions in Unity Catalog, and deploys models to serving endpoints. It provides the connective tissue between experimentation and production.</li>
# MAGIC           <li style="font-size: 14pt;">Lakehouse Monitoring adds drift detection, data quality profiling, and alerting for deployed models.</li>
# MAGIC           <li style="font-size: 14pt;">On average, only about 54% of AI projects make it from pilot to production (Gartner, 2022). A standardized lifecycle is what closes that gap.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Role of Mosaic AI</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Mosaic AI</strong> is the umbrella term for AI capabilities on the Databricks Data Intelligence Platform, spanning GenAI app development, the agent framework, model serving, and evaluation. It is not a separate product; it is the AI layer of the platform you already use.</li>
# MAGIC           <li style="font-size: 14pt;">Unity Catalog provides unified governance across data, features, models, and functions. This means the same access controls, lineage tracking, and audit logging that govern your Delta tables also govern your ML models and AI agents.</li>
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
# MAGIC ### A2. ML Runtime Capabilities
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">ML Runtime provides a pre-configured environment with optimized ML frameworks, distributed training, automated model selection, and GPU acceleration. These four capability areas cover the tools needed to go from data to trained model.</p>
# MAGIC
# MAGIC <!-- ── Visual: a2-runtime-grid ── -->
# MAGIC <style>
# MAGIC .a2r-wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .a2r-card { border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .a2r-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .a2r-accent { height: 6px; }
# MAGIC .a2r-header { background: #1B3139; padding: 14px 18px; text-align: center; }
# MAGIC .a2r-htxt { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .a2r-body { background: #fff; padding: 14px 18px; }
# MAGIC .a2r-body ul { margin: 0; padding: 0 0 0 18px; font-size: 14pt; color: #333; line-height: 1.7; }
# MAGIC .a2r-body li { font-size: 14pt; }
# MAGIC </style>
# MAGIC <div class="a2r-wrap">
# MAGIC   <div class="a2r-card"><div class="a2r-accent" style="background:#1B5162;"></div><div class="a2r-header"><div class="a2r-htxt">ML Frameworks</div></div><div class="a2r-body"><ul><li>PyTorch, scikit-learn, Keras</li><li>XGBoost, LightGBM</li><li>SHAP for model explainability</li></ul></div></div>
# MAGIC   <div class="a2r-card"><div class="a2r-accent" style="background:#2272B4;"></div><div class="a2r-header"><div class="a2r-htxt">Distributed Training</div></div><div class="a2r-body"><ul><li>Apache Spark for data-parallel workloads</li><li>TorchDistributor, DeepSpeed</li><li>Ray on Databricks</li></ul></div></div>
# MAGIC   <div class="a2r-card"><div class="a2r-accent" style="background:#00A972;"></div><div class="a2r-header"><div class="a2r-htxt">Genie Code / Agentic ML</div></div><div class="a2r-body"><ul><li>Conversational ML development</li><li>Unity Catalog-grounded</li><li>Agent Mode (GA May 2026)</li></ul></div></div>
# MAGIC   <div class="a2r-card"><div class="a2r-accent" style="background:#E5A100;"></div><div class="a2r-header"><div class="a2r-htxt">Hardware Accelerators</div></div><div class="a2r-body"><ul><li>NVIDIA CUDA, AMD GPUs</li><li>AI Runtime (serverless GPU)</li><li>Pre-configured GPU drivers</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">ML Runtime vs. Standard Runtime</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Standard Runtime works for basic data preparation and simple ML tasks. ML Runtime is recommended because it includes pre-installed ML libraries, optimized GPU drivers, and distributed training support that would otherwise require manual installation and configuration.</li>
# MAGIC           <li style="font-size: 14pt;">ML Runtime is not a separate product. It is a Databricks Runtime variant that adds ML-specific packages and optimizations on top of the standard runtime.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Distributed Training</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">TorchDistributor enables multi-node PyTorch training directly from a Databricks notebook. DeepSpeed provides memory-efficient training for large models. Ray on Databricks supports distributed computing for both training and hyperparameter tuning.</li>
# MAGIC           <li style="font-size: 14pt;">For customers who need distributed deep learning training without managing cluster hardware, AI Runtime offers serverless GPU compute. This is a newer option introduced in 2025.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Full ML Support Spectrum</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The platform supports the full range of ML and AI workloads: classic ML (classification, regression, forecasting), deep learning (PyTorch, distributed training), generative AI (LLM fine-tuning, RAG, agents), and production deployment (model serving, monitoring, governance).</li>
# MAGIC           <li style="font-size: 14pt;">This breadth matters because organizations rarely have just one type of ML workload. A retail customer might run demand forecasting (classic ML), product recommendations (deep learning), and a customer service chatbot (GenAI) on the same platform.</li>
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
# MAGIC ## B. MLflow
# MAGIC
# MAGIC Section A established the three pillars of the ML platform. The third pillar, lifecycle standardization, is delivered through MLflow. This section covers how MLflow tracks experiments, packages models, manages model versions, and how MLflow 3 extends these capabilities for generative AI.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. MLflow Overview
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">MLflow is an open-source platform for managing the end-to-end lifecycle of ML models, from experiment tracking through deployment. It was developed by Databricks and has grown to 30M+ monthly downloads. On Databricks, MLflow is pre-installed on ML Runtime and integrated with Unity Catalog for enterprise governance.</p>
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">MLflow operates on four core components that map to stages of the ML lifecycle:</p>
# MAGIC
# MAGIC <!-- ── Visual: b1-mlflow-lifecycle ── -->
# MAGIC <style>
# MAGIC .b1m-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b1m-flow { display: flex; align-items: stretch; gap: 0; justify-content: center; }
# MAGIC .b1m-stage { flex: 1; flex: 1; background: #fff; border: 2px solid #DCE0E2; border-radius: 12px; padding: 22px 16px; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 8px; transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s; cursor: default; }
# MAGIC .b1m-stage:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.15); border-color: #1B5162; }
# MAGIC .b1m-arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 0 6px; min-width: 50px; }
# MAGIC .b1m-arrow .b1m-line { width: 36px; height: 3px; position: relative; overflow: hidden; border-radius: 2px; background: #c8e6c9; }
# MAGIC .b1m-arrow .b1m-line::before { content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%; background: linear-gradient(90deg, transparent, #1B5162, transparent); animation: b1mFlow 1.5s linear infinite; }
# MAGIC @keyframes b1mFlow { 0% { left: -100%; } 100% { left: 100%; } }
# MAGIC .b1m-arrow .b1m-head { color: #1B5162; font-size: 14px; }
# MAGIC .b1m-icon { font-size: 30px; color: #1B5162; }
# MAGIC .b1m-title { font-size: 14pt; font-weight: 700; color: #0b2026; }
# MAGIC .b1m-comp { font-size: 14pt; color: #618794; font-weight: 600; margin-top: 2px; }
# MAGIC .b1m-desc { font-size: 14pt; color: #444; line-height: 1.5; margin-top: 4px; }
# MAGIC </style>
# MAGIC <div class="b1m-wrap">
# MAGIC   <div class="b1m-flow">
# MAGIC     <div class="b1m-stage"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="b1m-title">Prep Data</div><div class="b1m-comp">MLflow Tracking</div><div class="b1m-desc">Log parameters, metrics, artifacts, and data lineage</div></div>
# MAGIC     <div class="b1m-arrow"><div class="b1m-line"></div><span class="b1m-head">&#x25B6;</span></div>
# MAGIC     <div class="b1m-stage"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="b1m-title">Build Model</div><div class="b1m-comp">MLflow Models</div><div class="b1m-desc">Standard packaging format with flavors for any framework</div></div>
# MAGIC     <div class="b1m-arrow"><div class="b1m-line"></div><span class="b1m-head">&#x25B6;</span></div>
# MAGIC     <div class="b1m-stage"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="b1m-title">Register</div><div class="b1m-comp">Model Registry</div><div class="b1m-desc">Centralized versioning with aliases in Unity Catalog</div></div>
# MAGIC     <div class="b1m-arrow"><div class="b1m-line"></div><span class="b1m-head">&#x25B6;</span></div>
# MAGIC     <div class="b1m-stage"><div style="width:10px;height:10px;border-radius:50%;background:#1B5162;flex-shrink:0;"></div><div class="b1m-title">Deploy</div><div class="b1m-comp">Model Serving</div><div class="b1m-desc">REST endpoints, batch scoring, Spark UDF</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">MLflow Tracking</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">An <strong>experiment</strong> is a top-level grouping (like a project folder). A <strong>run</strong> is a single execution in that experiment. Each run logs parameters, metrics, artifacts, and model objects.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Autologging</strong> with <code>mlflow.autolog()</code> captures training data from major ML frameworks with no manual configuration: one line of code logs parameters, metrics, data lineage, the model, and environment details.</li>
# MAGIC           <li style="font-size: 14pt;">Think of MLflow Tracking as a lab notebook for data scientists: it records every experiment with its inputs, results, and outputs so any experiment can be reproduced or compared.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">MLflow Models and Flavors</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">A <strong>flavor</strong> is MLflow's framework-specific model serialization format. The same model can be saved and loaded in different ways depending on the deployment target: sklearn flavor for scikit-learn, pytorch flavor for PyTorch, pyfunc for generic Python.</li>
# MAGIC           <li style="font-size: 14pt;">The MLModel file in each model directory defines which flavors are available, plus the conda.yaml and requirements.txt needed to recreate the environment.</li>
# MAGIC           <li style="font-size: 14pt;">Flavors work like file format adapters: the same model can be consumed as a Python function, a Spark UDF, a REST endpoint, or a Docker container.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Model Registry in Unity Catalog</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Models are registered using the three-level namespace: <code>catalog.schema.model</code>. Databricks recommends using the namespace as an environment indicator (e.g., <code>prod.ml_team.iris_model</code>).</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Aliases</strong> (e.g., "Champion", "Challenger") provide mutable references to specific model versions, replacing the deprecated stage-based lifecycle. Updating which version is in production requires only reassigning the alias, with no downstream code changes.</li>
# MAGIC           <li style="font-size: 14pt;">Think of aliases as DNS for models: the name "Champion" always points to the current best version, and you can redirect it to a new version without changing any consuming application.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Managed vs. Open-Source MLflow</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">MLflow is open source and can run anywhere. Databricks provides a managed version that adds enterprise security, disaster recovery, Unity Catalog integration, and built-in model serving.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/mondelez" style="color: #2574B5; font-size: 14pt;">Mondelez</a> uses MLflow to manage close to 20,000 registered models with ~3,000 in production. SKU recommendation models deliver a 2-4% increase in store-level topline sales. &#x25C6;</li>
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
# MAGIC ### B2. MLflow 3 for GenAI
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">MLflow 3 was redesigned for generative AI while maintaining backward compatibility with MLflow 2.x. The central innovation is <strong>LoggedModel</strong>, which promotes models to first-class objects with their own lifecycle metadata rather than treating them as byproducts of training runs. Five new capability areas address the unique requirements of GenAI applications.</p>
# MAGIC
# MAGIC <!-- ── Visual: b2-mlflow3-capabilities ── -->
# MAGIC <style>
# MAGIC .b2c-wrap { display: flex; gap: 12px; margin: 24px 0; align-items: stretch; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .b2c-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .b2c-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .b2c-accent { height: 6px; flex-shrink: 0; }
# MAGIC .b2c-hdr { padding: 14px 14px 10px; text-align: center; background: #fff; }
# MAGIC .b2c-title { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .b2c-body { background: #F9F7F4; padding: 12px 14px; flex: 1; }
# MAGIC .b2c-body ul { margin: 0; padding: 0 0 0 16px; font-size: 14pt; color: #333; line-height: 1.6; }
# MAGIC .b2c-body li { font-size: 14pt; }
# MAGIC </style>
# MAGIC <div class="b2c-wrap">
# MAGIC   <div class="b2c-card"><div class="b2c-accent" style="background:#1B5162;"></div><div class="b2c-hdr"><div class="b2c-title">Tracing</div></div><div class="b2c-body"><ul><li>End-to-end observability</li><li>20+ framework integrations</li><li>One-line enable: <code>mlflow.autolog()</code></li></ul></div></div>
# MAGIC   <div class="b2c-card"><div class="b2c-accent" style="background:#2272B4;"></div><div class="b2c-hdr"><div class="b2c-title">Evaluation</div></div><div class="b2c-body"><ul><li>Built-in LLM judges</li><li>Scorers: correctness, relevance, recall</li><li>Side-by-side version comparison</li></ul></div></div>
# MAGIC   <div class="b2c-card"><div class="b2c-accent" style="background:#618794;"></div><div class="b2c-hdr"><div class="b2c-title">Monitoring</div></div><div class="b2c-body"><ul><li>Reuse offline scorers in production</li><li>Configurable sampling</li><li>AI judges on live traces</li></ul></div></div>
# MAGIC   <div class="b2c-card"><div class="b2c-accent" style="background:#00A972;"></div><div class="b2c-hdr"><div class="b2c-title">Prompt Registry</div></div><div class="b2c-body"><ul><li>UC-governed prompt versioning</li><li>Aliases for A/B testing</li><li>Lineage tracking</li></ul></div></div>
# MAGIC   <div class="b2c-card"><div class="b2c-accent" style="background:#1B3139;"></div><div class="b2c-hdr"><div class="b2c-title">Classic ML/DL</div></div><div class="b2c-body"><ul><li>LoggedModel as first-class object</li><li>Deployment Jobs via Lakeflow</li><li>Full backward compatibility</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Tracing: Observability for GenAI</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">MLflow Tracing captures intermediate steps, inputs, outputs, and latency for LLM calls, RAG pipelines, agents, and any GenAI application. Think of it as X-ray vision for your AI application: you can see every intermediate step inside a complex agent chain.</li>
# MAGIC           <li style="font-size: 14pt;">Automatic instrumentation is available for 20+ frameworks including OpenAI, LangChain, LlamaIndex, Anthropic, and DSPy. Enable with <code>mlflow.autolog()</code>.</li>
# MAGIC           <li style="font-size: 14pt;">Each trace contains <strong>spans</strong> (individual operations like an LLM call or tool invocation), visualized as a tree in the notebook cell output. Exception details and call trees are captured for debugging.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Evaluation: Measuring GenAI Quality</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">GenAI outputs are open-ended, so traditional metrics (accuracy, F1) are not sufficient. MLflow provides <strong>scorers</strong> that measure dimensions like correctness, document recall, relevance, and retrieval groundedness.</li>
# MAGIC           <li style="font-size: 14pt;">The evaluation UI allows side-by-side comparison of two agent versions, showing diff of prompts, parameters, traces, and performance metrics. This helps teams identify which version to promote to production.</li>
# MAGIC           <li style="font-size: 14pt;">The same scorers used in offline evaluation can be reused for production monitoring, ensuring consistent quality measurement across the development and deployment lifecycle.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Prompt Registry</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The Prompt Registry integrates with Unity Catalog for centralized prompt versioning, governance, and A/B testing. Register prompts with <code>mlflow.genai.register_prompt()</code>, set production aliases, and load prompts in applications.</li>
# MAGIC           <li style="font-size: 14pt;">This is version control for prompts, similar to how Git tracks code changes. You can tag, alias, and roll back prompt versions while maintaining full lineage tracking.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Trace Storage in Unity Catalog (Public Preview April 2026)</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">MLflow traces can now be stored in Unity Catalog in OpenTelemetry format, with access governed through standard UC permissions. This unifies ML observability with data governance.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">LoggedModel: The MLflow 3 Innovation</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">In MLflow 2.x, models were byproducts of training runs. In MLflow 3, LoggedModel promotes models to dedicated objects with their own lifecycle metadata: metrics, parameters, traces, and version history travel with the model.</li>
# MAGIC           <li style="font-size: 14pt;">Think of it as giving a model its own passport. Instead of being identified by which training run it came from, it carries its own identity everywhere it goes.</li>
# MAGIC           <li style="font-size: 14pt;">MLflow 3 maintains backward compatibility: existing runs, models, and traces from MLflow 2.x are readable by MLflow 3 clients.</li>
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
# MAGIC ## C. Generative AI on Databricks
# MAGIC
# MAGIC With the ML platform and MLflow lifecycle covered, we now turn to generative AI. This section introduces the four approaches to building GenAI applications, organized from lowest to highest complexity and compute requirements.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Four Approaches to GenAI
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Databricks supports four approaches to building generative AI applications, each at a different point on the complexity/compute spectrum. Choosing the right approach depends on your data, quality requirements, and available resources.</p>
# MAGIC
# MAGIC <!-- ── Visual: c1-genai-spectrum ── -->
# MAGIC <style>
# MAGIC .c1s-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .c1s-bar { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; margin-bottom: 8px; }
# MAGIC .c1s-label { font-size: 14pt; color: #618794; font-weight: 600; }
# MAGIC .c1s-gradient { flex: 1; height: 6px; margin: 0 12px; border-radius: 3px; background: linear-gradient(90deg, #00A972, #FFAB00, #FF5F46, #98102A); }
# MAGIC .c1s-cards { display: flex; gap: 14px; align-items: stretch; }
# MAGIC .c1s-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .c1s-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(27,49,57,0.15); }
# MAGIC .c1s-hdr { padding: 16px 16px 12px; text-align: center; }
# MAGIC .c1s-title { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .c1s-body { background: #fff; padding: 14px 16px; flex: 1; font-size: 14pt; color: #333; line-height: 1.6; }
# MAGIC .c1s-tool { font-size: 14pt; color: #618794; font-weight: 600; margin-top: 8px; padding-top: 8px; border-top: 1px solid #DCE0E2; }
# MAGIC .c1s-detail { max-height: 0; overflow: hidden; transition: max-height 0.3s; font-size: 14pt; color: #618794; margin-top: 4px; }
# MAGIC .c1s-card:hover .c1s-detail { max-height: 50px; }
# MAGIC </style>
# MAGIC <div class="c1s-wrap">
# MAGIC   <div class="c1s-bar"><div class="c1s-label">Low Complexity</div><div class="c1s-gradient"></div><div class="c1s-label">High Complexity</div></div>
# MAGIC   <div class="c1s-cards">
# MAGIC     <div class="c1s-card"><div class="c1s-hdr" style="background:#00A972;"><div class="c1s-title">Prompt Engineering</div></div><div class="c1s-body">Craft specialized prompts using the LLM's internal knowledge. Quality depends on prompt design and model capability.<div class="c1s-tool">Tool: AI Playground</div><div class="c1s-detail">Start here. No model modification needed.</div></div></div>
# MAGIC     <div class="c1s-card"><div class="c1s-hdr" style="background:#2272B4;"><div class="c1s-title">RAG</div></div><div class="c1s-body">Combine an LLM with external knowledge retrieval via vector search. The most popular GenAI pattern on Databricks.<div class="c1s-tool">Tool: Vector Search + Agent Framework</div><div class="c1s-detail">Like an open-book exam for your LLM.</div></div></div>
# MAGIC     <div class="c1s-card"><div class="c1s-hdr" style="background:#FFAB00;"><div class="c1s-title">Fine-Tuning</div></div><div class="c1s-body">Adapt a pre-trained LLM to specific datasets or domains. Requires thousands of domain-specific training examples.<div class="c1s-tool">Tool: AI Runtime</div><div class="c1s-detail">Changes model behavior and expertise.</div></div></div>
# MAGIC     <div class="c1s-card"><div class="c1s-hdr" style="background:#98102A;"><div class="c1s-title">Pre-Training</div></div><div class="c1s-body">Train an LLM from scratch. Requires billions to trillions of tokens and massive compute resources. Rarely done by customers.<div class="c1s-tool">Tool: Mosaic Research</div><div class="c1s-detail">Orders of magnitude more expensive.</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Prompt Engineering</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Prompt components include instruction, context, enter/question, and output format. A well-crafted prompt with system instructions and few-shot examples can solve many production use cases without any model modification.</li>
# MAGIC           <li style="font-size: 14pt;">Limitations: output quality is constrained by the model's pre-trained knowledge. For organization-specific data (customer records, internal policies), the model cannot answer accurately from prompts alone.</li>
# MAGIC           <li style="font-size: 14pt;">On Databricks, prompt engineering can be done interactively via AI Playground or through data-driven optimization using MLflow Prompt Optimization.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">RAG: Retrieval Augmented Generation</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">RAG is like an open-book exam: the LLM can look up answers in a knowledge base (via vector search), producing better responses than relying on its pre-trained knowledge alone.</li>
# MAGIC           <li style="font-size: 14pt;">RAG works with both unstructured data (PDFs, documentation, wikis) and structured data (Delta tables, APIs). It is the most practical GenAI approach for most enterprise use cases because it grounds responses in current, organization-specific data.</li>
# MAGIC           <li style="font-size: 14pt;">Section D covers RAG architecture in detail.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Fine-Tuning vs. Pre-Training</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Fine-tuning</strong> adapts an existing model to a new domain. It changes the model's behavior and expertise. Use it when you need the model to speak in a specific style or understand domain-specific terminology, and you have thousands of labeled examples.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Pre-training</strong> builds a model from scratch. This is orders of magnitude more expensive and is typically done by research organizations or large technology companies.</li>
# MAGIC           <li style="font-size: 14pt;">These approaches are complementary, not mutually exclusive. A fine-tuned model can be combined with RAG to get both domain expertise and access to current data.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">When to Use Which Approach</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Start with prompt engineering. If the model's internal knowledge is insufficient, add RAG for external data. If you need the model to behave differently (style, format, domain expertise), consider fine-tuning.</li>
# MAGIC           <li style="font-size: 14pt;">The four approaches form a learning curve: use prompt engineering to understand what the model can do, RAG to extend it with your data, and fine-tuning to customize its behavior.</li>
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
# MAGIC ## D. RAG Architecture and Vector Search
# MAGIC
# MAGIC RAG is the most commonly implemented GenAI pattern on Databricks. This section covers how it works: from embedding documents into vector representations, through similarity search, to generating context-grounded responses.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. RAG Architecture
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">RAG operates in two phases. First, documents are split into chunks, converted to vector embeddings, and stored in a vector index. Second, when a user asks a question, the question is embedded, similar chunks are retrieved via vector search, and those chunks are passed as context to the LLM, which generates a grounded response. Storage-optimized endpoints (GA May 2026) now support over one billion vectors, making production-scale RAG more practical.</p>
# MAGIC
# MAGIC <!-- ── Visual: d1-rag-architecture ── -->
# MAGIC <style>
# MAGIC .d1r-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .d1r-phase { border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10); margin-bottom: 10px; }
# MAGIC .d1r-phdr { padding: 12px 18px; display: flex; align-items: center; gap: 10px; }
# MAGIC .d1r-pnum { width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14pt; font-weight: 800; color: #fff; flex-shrink: 0; }
# MAGIC .d1r-plabel { font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .d1r-psub { font-size: 14pt; color: rgba(255,255,255,0.7); margin-left: auto; }
# MAGIC .d1r-flow { display: flex; align-items: stretch; gap: 0; background: #fff; padding: 16px 18px; }
# MAGIC .d1r-step { flex: 1; border-radius: 10px; padding: 16px 14px; text-align: center; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
# MAGIC .d1r-step:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.12); }
# MAGIC .d1r-name { font-size: 14pt; font-weight: 700; }
# MAGIC .d1r-desc { font-size: 14pt; line-height: 1.4; }
# MAGIC .d1r-arr { display: flex; align-items: center; justify-content: center; padding: 0 6px; min-width: 40px; flex-shrink: 0; }
# MAGIC .d1r-arr-line { width: 28px; height: 3px; position: relative; overflow: hidden; border-radius: 2px; }
# MAGIC .d1r-arr-line::before { content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%; animation: d1rFlow 1.5s linear infinite; }
# MAGIC @keyframes d1rFlow { 0% { left: -100%; } 100% { left: 100%; } }
# MAGIC .d1r-arr::after { content: ''; width: 0; height: 0; border-top: 7px solid transparent; border-bottom: 7px solid transparent; flex-shrink: 0; }
# MAGIC .d1r-bridge { display: flex; align-items: center; justify-content: center; height: 32px; gap: 10px; }
# MAGIC .d1r-bridge-line { width: 3px; height: 100%; border-radius: 2px; background: #00A972; }
# MAGIC .d1r-bridge-label { font-size: 14pt; font-weight: 600; color: #00A972; }
# MAGIC </style>
# MAGIC <div class="d1r-wrap">
# MAGIC   <!-- Phase 1 -->
# MAGIC   <div class="d1r-phase">
# MAGIC     <div class="d1r-phdr" style="background:#1B5162;"><div class="d1r-pnum" style="background:#0B2026;">1</div><div class="d1r-plabel">Ingestion</div><div class="d1r-psub">Offline, batch process</div></div>
# MAGIC     <div class="d1r-flow">
# MAGIC       <div class="d1r-step" style="background:#F9F7F4;border:2px solid #DCE0E2;"><div class="d1r-name" style="color:#0b2026;">Source Documents</div><div class="d1r-desc" style="color:#5A6F77;">PDFs, docs, wikis, Delta tables</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#c8e6c9;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#F9F7F4;border:2px solid #DCE0E2;"><div class="d1r-name" style="color:#0b2026;">Chunking</div><div class="d1r-desc" style="color:#5A6F77;">Split into smaller text segments</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#c8e6c9;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#1B3139;border:2px solid #1B3139;"><div class="d1r-name" style="color:#fff;">Embedding Model</div><div class="d1r-desc" style="color:#90A5B1;">Convert text to vector representations</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#c8e6c9;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#1B5162;border:2px solid #1B5162;"><div class="d1r-name" style="color:#fff;">Vector Index</div><div class="d1r-desc" style="color:#DCE0E2;">Stored in Mosaic AI Vector Search</div></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Bridge -->
# MAGIC   <div class="d1r-bridge"><div class="d1r-bridge-line"></div><div class="d1r-bridge-label">Vector Index connects both phases</div><div class="d1r-bridge-line"></div></div>
# MAGIC   <!-- Phase 2 -->
# MAGIC   <div class="d1r-phase">
# MAGIC     <div class="d1r-phdr" style="background:#2272B4;"><div class="d1r-pnum" style="background:#1B5162;">2</div><div class="d1r-plabel">Query</div><div class="d1r-psub">Online, per request</div></div>
# MAGIC     <div class="d1r-flow">
# MAGIC       <div class="d1r-step" style="background:#F9F7F4;border:2px solid #DCE0E2;"><div class="d1r-name" style="color:#0b2026;">User Question</div><div class="d1r-desc" style="color:#5A6F77;">"What is the return policy?"</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#bbdefb;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#1B3139;border:2px solid #1B3139;"><div class="d1r-name" style="color:#fff;">Embed Query</div><div class="d1r-desc" style="color:#90A5B1;">Same embedding model as Phase 1</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#bbdefb;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#1B5162;border:2px solid #1B5162;"><div class="d1r-name" style="color:#fff;">Similarity Search</div><div class="d1r-desc" style="color:#DCE0E2;">Find relevant chunks in index</div></div>
# MAGIC       <div class="d1r-arr"><div class="d1r-arr-line" style="background:#bbdefb;"></div></div>
# MAGIC       <div class="d1r-step" style="background:#F9F7F4;border:2px solid #00A972;"><div class="d1r-name" style="color:#0b2026;">LLM + Context</div><div class="d1r-desc" style="color:#006644;">Generate grounded answer</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Embeddings and Similarity Search</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Embeddings are numerical vector representations of text that capture semantic meaning. Similar text produces vectors that are close together in vector space. A query about "canine behavior" will find documents about "dogs" even without that exact word.</li>
# MAGIC           <li style="font-size: 14pt;">Think of embeddings as GPS coordinates for meaning. Just as nearby GPS coordinates indicate nearby physical locations, nearby embedding vectors indicate similar semantic content.</li>
# MAGIC           <li style="font-size: 14pt;">Vector Search uses the HNSW (Hierarchical Navigable Small World) algorithm for approximate nearest neighbor search with L2 distance metrics.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Mosaic AI Vector Search</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Vector Search is built into the Data Intelligence Platform and integrated with Unity Catalog governance. Four index types are available:
# MAGIC             <ul>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Delta Sync with managed embeddings:</strong> Databricks handles embedding generation and index updates automatically</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Delta Sync with self-managed embeddings:</strong> You provide pre-calculated embeddings</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Direct Vector Access:</strong> Manual REST API updates for custom pipelines</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Full-Text Search (Beta):</strong> BM25 keyword scoring without embeddings</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Hybrid search</strong> combines vector-based semantic search with keyword-based exact-match search using Reciprocal Rank Fusion (RRF), giving the best of both approaches.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Storage-optimized endpoints (GA May 2026):</strong> support over one billion vectors with 10-20x faster indexing and lower cost than standard endpoints. This makes production-scale RAG applications more practical and affordable.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Retrieval quality evaluation (Beta April 2026):</strong> built-in retrieval quality evaluation measures and compares the relevance of different search strategies, helping teams optimize their RAG pipelines systematically.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The Full RAG Stack on Databricks</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The full deployment stack is layered: Chat Application (Databricks Apps), REST API, RAG Chain (Model Serving with LangChain or Python), Models (GenAI Model Serving), Data and Vector Serving (Feature Serving + Vector Search), Data Prep (Lakeflow Jobs, SDP), and Lakehouse (Unity Catalog, Delta Tables, Volumes).</li>
# MAGIC           <li style="font-size: 14pt;">Each layer is governed by Unity Catalog, from the source Delta tables through the vector index to the model serving endpoint.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customer Examples</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/lippert" style="color: #2574B5; font-size: 14pt;">Lippert</a> built a RAG-powered customer support agent that improved from 33% to 84% accuracy in weeks and is expected to augment 50% of 1 million+ annual calls, projecting $2.1 million in savings. &#x25C6;</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/grainger" style="color: #2574B5; font-size: 14pt;">Grainger</a> uses Mosaic AI Vector Search to power a RAG-based search function for call center agents, replacing their previous keyword-based approach. &#x25C6;</li>
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
# MAGIC ## E. AI Agents and Tools
# MAGIC
# MAGIC RAG chains follow a fixed pipeline: retrieve, augment, generate. Agents extend this by adding flexible, LLM-driven decision-making about which tools to use and in what order. This section covers agent architecture, tool integration, and prototyping.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. What Is an Agent?
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">An agent is an AI-driven system that can autonomously perceive, decide, and act in an environment to achieve goals. Unlike a chain (which follows a fixed pipeline), an agent uses an LLM as its reasoning engine to dynamically decide which tools to call and in what order. The model is not the agent; the agent is the orchestration system that includes the model, tools, memory, and planning.</p>
# MAGIC
# MAGIC <!-- ── Visual: e1-agent-tabs ── -->
# MAGIC <style>
# MAGIC .e1a-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .e1a-wrap input[type="radio"] { display: none; }
# MAGIC .e1a-tabs { display: flex; gap: 4px; margin-bottom: 0; }
# MAGIC .e1a-tab { flex: 1; text-align: center; padding: 14px 8px; font-size: 14pt; font-weight: 700; color: #1B3139; cursor: pointer; border: 2px solid #E0E0E0; border-bottom: none; border-radius: 8px 8px 0 0; background: #fff; transition: all 0.2s; user-select: none; position: relative; }
# MAGIC .e1a-tab::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 5px; border-radius: 6px 6px 0 0; }
# MAGIC .e1a-tab[for="e1r1"]::before { background: #1B5162; }
# MAGIC .e1a-tab[for="e1r2"]::before { background: #2272B4; }
# MAGIC .e1a-tab[for="e1r3"]::before { background: #00A972; }
# MAGIC #e1r1:checked ~ .e1a-tabs .e1a-tab[for="e1r1"] { border-color: #1B5162; background: #E8F0ED; }
# MAGIC #e1r2:checked ~ .e1a-tabs .e1a-tab[for="e1r2"] { border-color: #2272B4; background: #E8F0F8; }
# MAGIC #e1r3:checked ~ .e1a-tabs .e1a-tab[for="e1r3"] { border-color: #00A972; background: #E8F5E9; }
# MAGIC .e1a-panels { border: 1px solid #E8E3DC; border-radius: 0 0 8px 8px; background: #fff; }
# MAGIC .e1a-panel { display: none; padding: 24px 28px; }
# MAGIC #e1r1:checked ~ .e1a-panels .e1a-p1 { display: block; }
# MAGIC #e1r2:checked ~ .e1a-panels .e1a-p2 { display: block; }
# MAGIC #e1r3:checked ~ .e1a-panels .e1a-p3 { display: block; }
# MAGIC </style>
# MAGIC <div class="e1a-wrap">
# MAGIC <input type="radio" name="e1grp" id="e1r1" checked>
# MAGIC <input type="radio" name="e1grp" id="e1r2">
# MAGIC <input type="radio" name="e1grp" id="e1r3">
# MAGIC <div class="e1a-tabs">
# MAGIC   <label class="e1a-tab" for="e1r1">Chain vs Agent</label>
# MAGIC   <label class="e1a-tab" for="e1r2">Agent Architecture</label>
# MAGIC   <label class="e1a-tab" for="e1r3">Building Agents</label>
# MAGIC </div>
# MAGIC <div class="e1a-panels">
# MAGIC   <!-- Tab 1: Chain vs Agent -->
# MAGIC   <div class="e1a-panel e1a-p1">
# MAGIC     <div style="display:flex;gap:14px;margin-bottom:14px;">
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;border:2px solid #DCE0E2;">
# MAGIC         <div style="background:#618794;padding:14px 16px;"><div style="font-size:15pt;font-weight:700;color:#fff;">Chain (Fixed Pipeline)</div></div>
# MAGIC         <div style="background:#fff;padding:14px 16px;">
# MAGIC           <div style="display:flex;align-items:center;gap:6px;margin-bottom:10px;flex-wrap:wrap;">
# MAGIC             <span style="font-size:14pt;background:#F9F7F4;padding:6px 14px;border-radius:8px;border:1px solid #DCE0E2;">Retrieve docs</span><span style="font-size:14pt;color:#618794;">&#x279C;</span>
# MAGIC             <span style="font-size:14pt;background:#F9F7F4;padding:6px 14px;border-radius:8px;border:1px solid #DCE0E2;">Build prompt</span><span style="font-size:14pt;color:#618794;">&#x279C;</span>
# MAGIC             <span style="font-size:14pt;background:#F9F7F4;padding:6px 14px;border-radius:8px;border:1px solid #DCE0E2;">Generate response</span>
# MAGIC           </div>
# MAGIC           <div style="font-size:14pt;color:#5A6F77;">Fixed steps, fixed order. Every request takes the same path. Simpler and more predictable.</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;border:2px solid #1B5162;">
# MAGIC         <div style="background:#1B5162;padding:14px 16px;"><div style="font-size:15pt;font-weight:700;color:#fff;">Agent (Dynamic Reasoning)</div></div>
# MAGIC         <div style="background:#fff;padding:14px 16px;">
# MAGIC           <div style="display:flex;align-items:center;gap:6px;margin-bottom:10px;flex-wrap:wrap;">
# MAGIC             <span style="font-size:14pt;background:#E8F0ED;padding:6px 14px;border-radius:8px;border:1px solid #1B5162;">Reason</span><span style="font-size:14pt;color:#1B5162;">&#x279C;</span>
# MAGIC             <span style="font-size:14pt;background:#E8F0ED;padding:6px 14px;border-radius:8px;border:1px solid #1B5162;">Select tool</span><span style="font-size:14pt;color:#1B5162;">&#x279C;</span>
# MAGIC             <span style="font-size:14pt;background:#E8F0ED;padding:6px 14px;border-radius:8px;border:1px solid #1B5162;">Execute</span><span style="font-size:14pt;color:#00A972;">&#x21BB;</span>
# MAGIC             <span style="font-size:14pt;background:#E8F0ED;padding:6px 14px;border-radius:8px;border:1px solid #1B5162;">Iterate</span>
# MAGIC           </div>
# MAGIC           <div style="font-size:14pt;color:#5A6F77;">LLM decides next step based on results. Flexible, iterative, non-deterministic.</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="font-size:14pt;color:#618794;font-style:italic;">A chain is like an assembly line: fixed steps, fixed order. An agent is like a project manager who selects the next action based on results from each stage.</div>
# MAGIC   </div>
# MAGIC   <!-- Tab 2: Agent Architecture -->
# MAGIC   <div class="e1a-panel e1a-p2">
# MAGIC     <div style="display:flex;gap:16px;align-items:stretch;">
# MAGIC       <!-- Left: Input -->
# MAGIC       <div style="width:120px;flex-shrink:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;">
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#0b2026;">User Request</div>
# MAGIC         <div style="font-size:14pt;color:#1B5162;">&#x279C;</div>
# MAGIC       </div>
# MAGIC       <!-- Center: Agent core -->
# MAGIC       <div style="flex:1;border:3px solid #1B5162;border-radius:14px;padding:18px 20px;background:#F9F7F4;">
# MAGIC         <div style="background:#1B3139;border-radius:10px;padding:14px 18px;text-align:center;margin-bottom:14px;"><div style="font-size:16pt;font-weight:700;color:#fff;">LLM (Reasoning Engine)</div><div style="font-size:14pt;color:#90A5B1;">Examines request, plans, selects tools, iterates</div></div>
# MAGIC         <div style="display:flex;gap:10px;">
# MAGIC           <div style="flex:1;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.08);">
# MAGIC             <div style="background:#1B5162;padding:8px 12px;font-size:14pt;font-weight:700;color:#fff;">Planning</div>
# MAGIC             <div style="background:#fff;padding:10px 12px;font-size:14pt;color:#444;">Determines next steps dynamically based on results</div>
# MAGIC           </div>
# MAGIC           <div style="flex:1;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.08);">
# MAGIC             <div style="background:#2272B4;padding:8px 12px;font-size:14pt;font-weight:700;color:#fff;">Memory</div>
# MAGIC             <div style="background:#fff;padding:10px 12px;font-size:14pt;color:#444;">Short-term (thread) and cross-session (user) state</div>
# MAGIC           </div>
# MAGIC           <div style="flex:1;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.08);">
# MAGIC             <div style="background:#00A972;padding:8px 12px;font-size:14pt;font-weight:700;color:#fff;">Tools</div>
# MAGIC             <div style="background:#fff;padding:10px 12px;font-size:14pt;color:#444;">UC functions, Vector Search, Genie, APIs, code execution</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div style="text-align:center;margin-top:10px;font-size:14pt;font-weight:600;color:#E5A100;">&#x21BB; Observe &#x279C; Reason &#x279C; Act &#x279C; Repeat until satisfied</div>
# MAGIC       </div>
# MAGIC       <!-- Right: Output -->
# MAGIC       <div style="width:120px;flex-shrink:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;">
# MAGIC         <div style="font-size:14pt;color:#1B5162;">&#x279C;</div>
# MAGIC         <div style="font-size:14pt;font-weight:700;color:#0b2026;">Response</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="display:flex;gap:10px;margin-top:14px;">
# MAGIC       <div style="flex:1;background:#F9F7F4;border-radius:8px;padding:10px 14px;border-left:4px solid #1B5162;font-size:14pt;color:#333;"><strong style="color:#1B3139;">MCP:</strong> open standard connecting agents to tools via Unity AI Gateway. Four server types: Managed, External, Custom, Client.</div>
# MAGIC       <div style="flex:1;background:#F9F7F4;border-radius:8px;padding:10px 14px;border-left:4px solid #00A972;font-size:14pt;color:#333;"><strong style="color:#1B3139;">UC Governance:</strong> tools registered in Unity Catalog inherit access policies, lineage tracking, and usage logging.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Tab 3: Building Agents -->
# MAGIC   <div class="e1a-panel e1a-p3">
# MAGIC     <div style="display:flex;gap:14px;margin-bottom:14px;">
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(27,49,57,0.08);cursor:default;transition:transform 0.15s;"><div style="background:#00A972;padding:12px 14px;display:flex;align-items:center;gap:8px;"><div style="width:8px;height:8px;border-radius:50%;background:#fff;flex-shrink:0;"></div><div style="font-size:14pt;font-weight:700;color:#fff;">AI Playground</div></div><div style="background:#fff;padding:12px 14px;"><ul style="margin:0;padding:0 0 0 16px;font-size:14pt;color:#444;line-height:1.55;"><li style="font-size:14pt;">Low-code UI for rapid prototyping</li><li style="font-size:14pt;">Create tool-calling agents without code</li><li style="font-size:14pt;">Built-in LLM judges for quality</li><li style="font-size:14pt;">Export Python code when ready</li></ul></div></div>
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(27,49,57,0.08);cursor:default;transition:transform 0.15s;"><div style="background:#1B5162;padding:12px 14px;display:flex;align-items:center;gap:8px;"><div style="width:8px;height:8px;border-radius:50%;background:#fff;flex-shrink:0;"></div><div style="font-size:14pt;font-weight:700;color:#fff;">Agent Bricks</div></div><div style="background:#fff;padding:12px 14px;"><ul style="margin:0;padding:0 0 0 16px;font-size:14pt;color:#444;line-height:1.55;"><li style="font-size:14pt;">Auto-optimized agent building</li><li style="font-size:14pt;">Specify problem and data; system optimizes</li><li style="font-size:14pt;">Agent Learning on Human Feedback (ALHF)</li><li style="font-size:14pt;">Supervisor Agent nesting (GA May 2026)</li></ul></div></div>
# MAGIC       <div style="flex:1;border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(27,49,57,0.08);cursor:default;transition:transform 0.15s;"><div style="background:#2272B4;padding:12px 14px;display:flex;align-items:center;gap:8px;"><div style="width:8px;height:8px;border-radius:50%;background:#fff;flex-shrink:0;"></div><div style="font-size:14pt;font-weight:700;color:#fff;">Code Frameworks</div></div><div style="background:#fff;padding:12px 14px;"><ul style="margin:0;padding:0 0 0 16px;font-size:14pt;color:#444;line-height:1.55;"><li style="font-size:14pt;">OpenAI Agents SDK, LangGraph, LangChain</li><li style="font-size:14pt;">Custom Python</li><li style="font-size:14pt;">ResponsesAgent wraps any framework</li><li style="font-size:14pt;">Deploy via Model Serving + Apps</li></ul></div></div>
# MAGIC     </div>
# MAGIC     <div style="font-size:14pt;color:#618794;font-style:italic;">&#x25C6; <a href="https://www.databricks.com/customers/vale/agent-bricks" style="color:#2574B5;font-size:14pt;">Vale</a> built a finance AI agent in three days using Agent Bricks, supporting 100+ users across 430+ insurance policies. &#x25C6;</div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Chain vs Agent Tab: The Core Distinction</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Chain = fixed pipeline:</strong> a RAG chain follows a predetermined sequence (retrieve documents, build prompt, generate response). Every request takes the same path. Simpler to build, debug, and predict. Use chains when the task is well-defined and the steps never vary.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Agent = dynamic reasoning:</strong> the LLM examines the user's request, reasons about which tools to call, executes those tools, evaluates the results, and iterates until it reaches a satisfactory answer. More flexible but introduces non-deterministic behavior.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Analogy:</strong> a chain is an assembly line where every product follows the same stations in the same order. An agent is a project manager who selects the next action based on what each stage reveals.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Architecture Tab: Components, Tools, and MCP</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Three components:</strong> the agent core contains Planning (determines next steps dynamically), Memory (short-term thread state and cross-session user state), and Tools (UC-governed functions and APIs). The LLM is the reasoning engine that orchestrates all three.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Tool categories:</strong> data retrieval and analysis (vector indexes, structured queries, web search), state modification (external API calls, notifications), and logic execution (sandboxed code). Tools registered in UC inherit access policies, lineage tracking, and usage logging.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Model Context Protocol (MCP):</strong> an open standard from Anthropic for connecting agents to tools. Think of it as a universal power adapter: one standard interface instead of custom connectors per tool. On Databricks, MCP servers operate through Unity AI Gateway with four types: Managed (Vector Search, Genie, SQL, UC functions), External (third-party via proxy), Custom (Databricks Apps), and Client (Claude, Cursor, MCP Inspector).</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">The agent loop:</strong> Observe (receive input or tool result), Reason (decide next action), Act (call a tool or generate a response), Repeat until satisfied. This loop is what separates agents from chains.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Building Agents Tab: From Prototype to Production</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">AI Playground:</strong> low-code UI for rapid prototyping. Create tool-calling agents without writing code, assess quality with built-in LLM judges, and export Python code for production deployment.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Agent Bricks:</strong> auto-optimized agent building. Specify your problem and data, the system optimizes for quality vs cost, and continuous improvement happens through Agent Learning on Human Feedback (ALHF). Supervisor Agent nesting (GA May 2026) enables hierarchical multi-agent orchestration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Code frameworks:</strong> OpenAI Agents SDK, LangChain, LangGraph, and custom Python. <code style="font-size: 14pt;">ResponsesAgent</code> wraps any framework for consistent deployment through AI Playground, Agent Evaluation, and Databricks Apps.</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/vale/agent-bricks" style="color: #2574B5; font-size: 14pt;">Vale</a> built a finance AI agent in three days using Agent Bricks, supporting 100+ users across 430+ insurance policies and 10,000+ tax laws, with one initiative identifying ~$33 million in recoverable value. &#x25C6;</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Recent GA Additions</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Supervisor Agent nesting (GA May 2026):</strong> add another Supervisor Agent as a subagent tool for hierarchical multi-agent orchestration.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Custom MCP for Supervisor Agents (GA April 2026):</strong> custom MCP servers and Databricks Apps can be added as subagent tools, extending agent capabilities with external services.</li>
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
# MAGIC ## F. Model Serving and Production
# MAGIC
# MAGIC Building and evaluating models is only half the challenge. This section covers how models move from notebooks to production endpoints and the governance layer that manages access and security.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. Mosaic AI Model Serving
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Model Serving provides a unified interface to deploy, govern, and query AI models via REST API. It supports three model types, auto-scales (including to zero), and delivers 25K+ QPS with sub-50ms overhead latency. All three model types share the same serving infrastructure and the same API format.</p>
# MAGIC
# MAGIC <!-- ── Visual: f1-model-serving-types ── -->
# MAGIC <style>
# MAGIC .f1m-wrap { display: flex; gap: 16px; margin: 24px 0; align-items: stretch; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .f1m-card { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); display: flex; flex-direction: column; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .f1m-card:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.13); }
# MAGIC .f1m-accent { height: 7px; flex-shrink: 0; }
# MAGIC .f1m-hdr { padding: 16px 18px; text-align: center; background: #fff; }
# MAGIC .f1m-title { font-size: 15pt; font-weight: 700; color: #1B3139; }
# MAGIC .f1m-subtitle { font-size: 14pt; color: #618794; margin-top: 4px; }
# MAGIC .f1m-body { background: #F9F7F4; padding: 16px 18px; flex: 1; }
# MAGIC .f1m-body ul { margin: 0; padding: 0 0 0 18px; font-size: 14pt; color: #333; line-height: 1.7; }
# MAGIC .f1m-body li { font-size: 14pt; }
# MAGIC </style>
# MAGIC <div class="f1m-wrap">
# MAGIC   <div class="f1m-card"><div class="f1m-accent" style="background:#1B5162;"></div><div class="f1m-hdr"><div class="f1m-title">Custom Models</div><div class="f1m-subtitle">Your models, your code</div></div><div class="f1m-body"><ul><li>Any Python model in MLflow format</li><li>scikit-learn, XGBoost, PyTorch, HuggingFace</li><li>Serverless CPU and GPU</li><li>Registered in Unity Catalog</li></ul></div></div>
# MAGIC   <div class="f1m-card"><div class="f1m-accent" style="background:#2272B4;"></div><div class="f1m-hdr"><div class="f1m-title">Foundation Models</div><div class="f1m-subtitle">Databricks-hosted LLMs</div></div><div class="f1m-body"><ul><li>Meta Llama, Qwen, Mistral, and more</li><li>Pay-per-token (experimentation)</li><li>Provisioned throughput (production)</li><li>OpenAI-compatible API</li></ul></div></div>
# MAGIC   <div class="f1m-card"><div class="f1m-accent" style="background:#618794;"></div><div class="f1m-hdr"><div class="f1m-title">External Models</div><div class="f1m-subtitle">Third-party LLM providers</div></div><div class="f1m-body"><ul><li>OpenAI, Anthropic, Google, and more</li><li>Centralized credential management</li><li>Unified API across providers</li><li>Governed via AI Gateway</li></ul></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Model Serving is Like a Restaurant with Three Kitchens</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The <strong>in-house kitchen</strong> (custom models) serves dishes you designed and trained yourself. The <strong>partner restaurant</strong> (foundation models) provides curated dishes hosted by Databricks. The <strong>food delivery service</strong> (external models) brings in dishes from third-party providers. AI Gateway is the front-of-house manager who handles permissions, monitors orders, and enforces rules for all three.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Foundation Model APIs: Two Pricing Models</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Pay-per-token</strong> is ideal for experimentation, with no infrastructure commitment. Think of it like a taxi meter: you pay for what you use.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Provisioned throughput</strong> is recommended for production, with dedicated capacity and performance guarantees. This is like a private car service: guaranteed availability at a fixed rate. It also supports fine-tuned models.</li>
# MAGIC           <li style="font-size: 14pt;">Available foundation models include Meta Llama (3.3, 4 Maverick), Anthropic Claude (Opus, Sonnet, Haiku families), OpenAI GPT series, Google Gemini, and Alibaba Qwen. Recent additions to Foundation Model APIs include OpenAI GPT-5.5 (April 2026), Anthropic Claude Opus 4.7 and 4.8 (April/May 2026), and Google Gemini 3.5 Flash (May 2026), all available as Databricks-hosted pay-per-token models.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Unity AI Gateway</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">AI Gateway is the central governance layer for agents, LLM endpoints, MCP servers, and coding agents. It provides:
# MAGIC             <ul>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Rate limiting:</strong> QPM/TPM constraints per endpoint, per user, per group</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">AI Guardrails:</strong> Safety filters to prevent unsafe content, PII detection to block or mask sensitive data</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Payload logging:</strong> Requests and responses logged to inference tables (Delta) for auditing</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Traffic splitting:</strong> Route traffic percentages to different models for A/B testing, with fallback on errors</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Usage tracking:</strong> Cost observability via system tables</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li style="font-size: 14pt;">AI Gateway also integrates with external coding tools including Cursor, Gemini CLI, Codex CLI, and Claude Code.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">MCP governance (Beta April 2026):</strong> Unity AI Gateway enforces access control, monitors usage, and audits activity across all MCP interactions, bringing governance to the agent tool layer.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Performance and Security</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Model Serving supports 25K+ QPS with sub-50ms overhead latency, auto-scaling (including down to zero), and 99.5% availability. Data is encrypted at rest (AES-256) and in transit (TLS 1.2+).</li>
# MAGIC           <li style="font-size: 14pt;">&#x25C6; <a href="https://www.databricks.com/customers/scribd/genai" style="color: #2574B5; font-size: 14pt;">Scribd</a> cut LLM running costs by more than 90% using Databricks Model Serving with batch inference, processing 1 million+ files daily without managing a GPU fleet. &#x25C6;</li>
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
# MAGIC ## G. Agentic Machine Learning
# MAGIC
# MAGIC Section F covered how trained models reach production endpoints. But most ML projects stall before they get there: repetitive data prep, manual feature engineering, and fragmented tooling slow teams down. Genie Code addresses this by bringing an agentic approach to the ML development workflow itself.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. Genie Code for ML Development
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Traditional ML development involves repetitive manual work across data prep, feature engineering, model training, tuning, and deployment. Genie Code (formerly Databricks Assistant, renamed March 2026) replaces this with an agentic approach: a conversational AI partner that plans and executes the full ML lifecycle through natural language, generating transparent, editable notebook code at every step.</p>
# MAGIC
# MAGIC <!-- ── Visual: f2-gc-workflow ── -->
# MAGIC <style>
# MAGIC .f2-gc-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .f2-gc-compare { display: flex; gap: 20px; align-items: flex-start; margin-bottom: 20px; }
# MAGIC .f2-gc-side { flex: 1; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10), 0 1px 3px rgba(27,49,57,0.06); }
# MAGIC .f2-gc-side-hdr { padding: 14px 18px; text-align: center; }
# MAGIC .f2-gc-side-hdr h4 { margin: 0; font-size: 14pt; font-weight: 700; color: #fff; }
# MAGIC .f2-gc-side-body { background: #fff; padding: 16px 18px; }
# MAGIC .f2-gc-step-list { list-style: none; margin: 0; padding: 0; }
# MAGIC .f2-gc-step-list li { font-size: 14pt; color: #333; padding: 8px 0; border-bottom: 1px solid #f0f0f0; display: flex; align-items: center; gap: 10px; }
# MAGIC .f2-gc-step-list li:last-child { border-bottom: none; }
# MAGIC .f2-gc-num { display: inline-flex; align-items: center; justify-content: center; width: 26px; height: 26px; border-radius: 50%; font-size: 14pt; font-weight: 700; color: #fff; flex-shrink: 0; }
# MAGIC .f2-gc-cards { display: flex; flex-wrap: wrap; gap: 12px; justify-content: center; }
# MAGIC .f2-gc-cap { flex: 1 1 170px; flex: 1; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(27,49,57,0.10); transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .f2-gc-cap:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.15); }
# MAGIC .f2-gc-cap-accent { height: 5px; }
# MAGIC .f2-gc-cap-body { padding: 14px 14px; background: #fff; text-align: center; }
# MAGIC .f2-gc-cap-title { font-size: 14pt; font-weight: 700; color: #1B3139; }
# MAGIC .f2-gc-cap-desc { font-size: 14pt; color: #618794; margin-top: 4px; }
# MAGIC </style>
# MAGIC <div class="f2-gc-wrap">
# MAGIC   <div class="f2-gc-compare">
# MAGIC     <div class="f2-gc-side">
# MAGIC       <div class="f2-gc-side-hdr" style="background: #98102A;"><h4>Traditional ML</h4></div>
# MAGIC       <div class="f2-gc-side-body">
# MAGIC         <ul class="f2-gc-step-list">
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">1</span> Data discovery and profiling</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">2</span> Data cleaning and preparation</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">3</span> Feature engineering</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">4</span> Model selection and training</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">5</span> Hyperparameter tuning</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">6</span> Evaluation and comparison</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">7</span> Registration and deployment</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#98102A;">8</span> Monitoring setup</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="f2-gc-side">
# MAGIC       <div class="f2-gc-side-hdr" style="background: #00A972;"><h4>Agentic ML with Genie Code</h4></div>
# MAGIC       <div class="f2-gc-side-body">
# MAGIC         <ul class="f2-gc-step-list">
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#00A972;">1</span> Describe your goal in natural language</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#00A972;">2</span> Genie Code plans the workflow</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#00A972;">3</span> Generates and runs notebook code</li>
# MAGIC           <li style="font-size: 14pt;"><span class="f2-gc-num" style="background:#00A972;">4</span> Iterates based on your feedback</li>
# MAGIC         </ul>
# MAGIC         <div style="margin-top: 12px; padding: 10px 14px; background: #e8f5e9; border-radius: 8px; text-align: center; font-size: 14pt; color: #2e7d32; font-weight: 600;">Transparent, editable code at every step</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="f2-gc-cards">
# MAGIC     <div class="f2-gc-cap"><div class="f2-gc-cap-accent" style="background:#1B5162;"></div><div class="f2-gc-cap-body"><div class="f2-gc-cap-title">Data Discovery</div><div class="f2-gc-cap-desc">Lineage-aware search across Unity Catalog</div></div></div>
# MAGIC     <div class="f2-gc-cap"><div class="f2-gc-cap-accent" style="background:#2272B4;"></div><div class="f2-gc-cap-body"><div class="f2-gc-cap-title">Exploratory Analysis</div><div class="f2-gc-cap-desc">Natural language profiling and insights</div></div></div>
# MAGIC     <div class="f2-gc-cap"><div class="f2-gc-cap-accent" style="background:#618794;"></div><div class="f2-gc-cap-body"><div class="f2-gc-cap-title">Model Training</div><div class="f2-gc-cap-desc">Iterative training with domain expertise</div></div></div>
# MAGIC     <div class="f2-gc-cap"><div class="f2-gc-cap-accent" style="background:#00A972;"></div><div class="f2-gc-cap-body"><div class="f2-gc-cap-title">Model Deployment</div><div class="f2-gc-cap-desc">Register, serve, monitor in one conversation</div></div></div>
# MAGIC     <div class="f2-gc-cap"><div class="f2-gc-cap-accent" style="background:#E5A100;"></div><div class="f2-gc-cap-body"><div class="f2-gc-cap-title">Customization</div><div class="f2-gc-cap-desc">MCP, Instructions, Skills for context</div></div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">What Genie Code Is</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Genie Code is an autonomous AI partner for data teams, fluent in data engineering, data science, ML, and dashboards. It is designed specifically for Databricks and grounded in Unity Catalog metadata, schemas, table lineage, and platform APIs.</li>
# MAGIC           <li style="font-size: 14pt;">Unlike legacy AutoML, which was a batch process that returned trial notebooks after completion, Genie Code is transparent and conversational. It generates readable, editable notebook code at every step, so you can see, modify, and learn from everything it produces.</li>
# MAGIC           <li style="font-size: 14pt;">Genie Code addresses five core ML needs: data discovery, exploratory data analysis, model training, model deployment, and model monitoring.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Platform Integration</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Feature Store:</strong> Genie Code reads and writes features directly with schema and lineage awareness, so feature engineering happens within the governed catalog.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">MLflow:</strong> Experiments, parameters, and metrics are auto-logged. Genie Code generates the appropriate MLflow tracking calls in the notebook code it produces.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Model Registry:</strong> Models can be registered, transitioned between aliases, and versioned through natural language instructions.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Model Serving:</strong> Genie Code configures and deploys serving endpoints, completing the path from training to production within a single conversation.</li>
# MAGIC           <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Agent Mode (GA May 2026):</strong> Plans and executes complex multi-step workflows end to end, running cells with your approval and iteratively fixing errors.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Customizing Genie Code for ML</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Four customization layers, in order of increasing complexity:
# MAGIC             <ul>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Unity Catalog metadata (automatic, always on):</strong> Table descriptions, column metadata, and lineage provide context without any setup.</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Instructions (markdown files):</strong> Coding standards, preferred libraries, and naming conventions. Available at workspace level or user level, with a 20,000-character limit.</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">MCP connections:</strong> Connect to external documentation, APIs, and data sources using Model Context Protocol, an open standard for AI tool integration.</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Skills (on-demand bundles):</strong> Task-specific procedures, examples, and automation scripts. Activated in Agent mode when a matching task is detected.</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">From AutoML to Agentic ML</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">AutoML was glass-box but batch-oriented: you submitted a dataset and got back trial notebooks. Genie Code is conversational and iterative: you describe your goal, and Genie Code plans, executes, and iterates with you in real time.</li>
# MAGIC           <li style="font-size: 14pt;">The key shift: "AutoML runs a pipeline for me" becomes "Genie Code writes and runs the pipeline with me, transparently." The data scientist stays in the loop at every stage.</li>
# MAGIC           <li style="font-size: 14pt;">With proper context configured (MCP for data access, Instructions for standards, Skills for workflows), a data scientist can go from raw data to a production endpoint in a single conversation.</li>
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
# MAGIC ## H. AI Security
# MAGIC
# MAGIC Deploying AI introduces a new class of security risks beyond traditional IT security. The Databricks AI Security Framework (DASF) provides a structured approach to identifying and mitigating these risks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### H1. The Databricks AI Security Framework (DASF)
# MAGIC
# MAGIC <p style="font-size: 14pt; line-height: 1.6; color: #333;">Security is the top concern for organizations adopting AI, with 46% citing it as a primary worry (451 Research, 2023). The Databricks AI Security Framework (DASF) is an actionable framework designed to create an end-to-end risk profile for AI deployments. It identifies 12 AI system components, catalogs risks across those components, and prescribes mitigation controls. The framework follows a four-step implementation flow.</p>
# MAGIC
# MAGIC <!-- ── Visual: g1-dasf-flow ── -->
# MAGIC <style>
# MAGIC .g1d-wrap { width: 100%; margin: 24px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
# MAGIC .g1d-flow { display: flex; align-items: stretch; gap: 0; justify-content: center; }
# MAGIC .g1d-step { flex: 1; flex: 1; border-radius: 10px; padding: 18px 14px; text-align: center; display: flex; flex-direction: column; align-items: center; gap: 6px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .g1d-step:hover { transform: translateY(-3px); box-shadow: 0 6px 16px rgba(27,49,57,0.15); }
# MAGIC .g1d-num { font-size: 22px; font-weight: 700; color: rgba(255,255,255,0.5); }
# MAGIC .g1d-title { font-size: 14pt; font-weight: 700; color: #fff; line-height: 1.3; }
# MAGIC .g1d-desc { font-size: 14pt; color: rgba(255,255,255,0.8); margin-top: 4px; }
# MAGIC .g1d-s3 .g1d-title, .g1d-s3 .g1d-num { color: #0b2026; }
# MAGIC .g1d-s3 .g1d-desc { color: rgba(11,32,38,0.7); }
# MAGIC .g1d-arr { display: flex; align-items: center; padding: 0 4px; min-width: 40px; }
# MAGIC .g1d-arr .g1d-line { width: 28px; height: 3px; position: relative; overflow: hidden; border-radius: 2px; background: #bbdefb; }
# MAGIC .g1d-arr .g1d-line::before { content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%; background: linear-gradient(90deg, transparent, #1B5162, transparent); animation: g1dFlow 1.5s linear infinite; }
# MAGIC @keyframes g1dFlow { 0% { left: -100%; } 100% { left: 100%; } }
# MAGIC .g1d-arr .g1d-head { color: #1B5162; font-size: 14px; }
# MAGIC .g1d-stats { display: flex; gap: 16px; margin-top: 22px; justify-content: center; }
# MAGIC .g1d-stat { background: #fff; border: 2px solid #DCE0E2; border-radius: 10px; padding: 14px 24px; text-align: center; min-width: 150px; transition: transform 0.2s, box-shadow 0.2s; cursor: default; }
# MAGIC .g1d-stat:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(27,49,57,0.12); border-color: #1B5162; }
# MAGIC .g1d-stat-num { font-size: 26pt; font-weight: 700; color: #1B5162; }
# MAGIC .g1d-stat-label { font-size: 14pt; color: #5A6F77; margin-top: 4px; }
# MAGIC </style>
# MAGIC <div class="g1d-wrap">
# MAGIC   <div class="g1d-flow">
# MAGIC     <div class="g1d-step" style="background:#1B5162;"><div class="g1d-num">1</div><div class="g1d-title">Identify Use Case</div><div class="g1d-desc">Datasets, stakeholders, compliance</div></div>
# MAGIC     <div class="g1d-arr"><div class="g1d-line"></div><span class="g1d-head">&#x25B6;</span></div>
# MAGIC     <div class="g1d-step" style="background:#2272B4;"><div class="g1d-num">2</div><div class="g1d-title">Identify Deployment Model</div><div class="g1d-desc">ML, LLM, RAG, Agents, etc.</div></div>
# MAGIC     <div class="g1d-arr"><div class="g1d-line"></div><span class="g1d-head">&#x25B6;</span></div>
# MAGIC     <div class="g1d-step g1d-s3" style="background:#FFAB00;"><div class="g1d-num">3</div><div class="g1d-title">Select Applicable Risks</div><div class="g1d-desc">Not all risks apply to every project</div></div>
# MAGIC     <div class="g1d-arr"><div class="g1d-line"></div><span class="g1d-head">&#x25B6;</span></div>
# MAGIC     <div class="g1d-step" style="background:#00A972;"><div class="g1d-num">4</div><div class="g1d-title">Select Controls and Implement</div><div class="g1d-desc">Map to Databricks capabilities</div></div>
# MAGIC   </div>
# MAGIC   <div class="g1d-stats">
# MAGIC     <div class="g1d-stat"><div class="g1d-stat-num">12</div><div class="g1d-stat-label">AI System Components</div></div>
# MAGIC     <div class="g1d-stat"><div class="g1d-stat-num">97</div><div class="g1d-stat-label">Identified Risks (v3.0)</div></div>
# MAGIC     <div class="g1d-stat"><div class="g1d-stat-num">73</div><div class="g1d-stat-label">Mitigation Controls (v3.0)</div></div>
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
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">The 12 AI System Components</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The 12 components span four operational stages:
# MAGIC             <ul>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Data Operations (1-4):</strong> Raw Data, Data Preparation, Dataset Management, Data Cataloging</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Model Operations (5-8):</strong> Algorithms, Model Evaluation, Model Development, Model Governance</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Serving Infrastructure (9-10):</strong> Prompting and RAG, Serving Infrastructure</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Operations and Platform (11-12):</strong> Monitoring, Operational Security</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC           <li style="font-size: 14pt;">DASF is like a building safety code for AI systems: it identifies structural components, catalogs what can go wrong, and prescribes safety measures. Just as not every building code applies to every type of building, not every DASF risk applies to every AI deployment.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Risk Categories and Evolution</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">The original DASF v1.0 identified 55 risks: 20 traditional (found in non-AI systems) and 35 novel (AI-specific). Novel risks include prompt injection, model inversion, LLM hallucinations, data poisoning, and model extraction.</li>
# MAGIC           <li style="font-size: 14pt;">DASF v3.0 (March 2026) expanded to 97 risks and 73 controls with the addition of agentic AI security guidance, addressing new risks like tool misuse, memory manipulation, and chain-of-thought attacks.</li>
# MAGIC           <li style="font-size: 14pt;">Not all risks apply to every project. A RAG deployment might face 15-25 relevant risks, not all 97. The deployment model selection in Step 2 scopes which risks are relevant.</li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Mapping Controls to Databricks</strong>
# MAGIC         <ul style="margin: 8px 0 12px 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">Controls are defined generically (applicable to any platform) but include implementation guidelines for the Databricks Data Intelligence Platform:
# MAGIC             <ul>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Unity Catalog:</strong> Access controls, data governance, lineage tracking</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">AI Gateway:</strong> Guardrails, rate limiting, payload logging</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Model Serving:</strong> Container isolation, request validation</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Lakehouse Monitoring:</strong> Drift detection, alerting</li>
# MAGIC               <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Lakeflow Jobs:</strong> CI/CD security, environment separation</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC         <strong style="color: #1B5162; font-size: 14pt;">Industry Standards Alignment</strong>
# MAGIC         <ul style="margin: 8px 0 0 0; color: #333; line-height: 1.7; font-size: 14pt;">
# MAGIC           <li style="font-size: 14pt;">DASF risks are mapped to MITRE ATLAS, OWASP Top 10 for LLMs, and NIST AML Taxonomy. This helps organizations align DASF adoption with existing compliance and security programs.</li>
# MAGIC           <li style="font-size: 14pt;">DASF is a risk management framework, not a compliance standard. It is Databricks-originated but platform-agnostic in its risk and control definitions.</li>
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
# MAGIC This lecture covered the Databricks ML and AI stack from platform architecture to production security:
# MAGIC
# MAGIC - The <strong>ML Platform</strong> provides three pillars (accessible data, team productivity, lifecycle standardization) with Unity Catalog governance across all AI assets
# MAGIC - <strong>MLflow</strong> manages the full model lifecycle, and MLflow 3 extends it for GenAI with tracing, evaluation, prompt management, and the LoggedModel concept
# MAGIC - <strong>Four GenAI approaches</strong> (prompt engineering, RAG, fine-tuning, pre-training) address different complexity levels, with RAG as the most common enterprise pattern
# MAGIC - <strong>Vector Search</strong> powers RAG by converting documents to embeddings and retrieving relevant context via similarity search
# MAGIC - <strong>AI agents</strong> extend chains with dynamic, LLM-driven tool orchestration, connected through MCP and governed by Unity Catalog
# MAGIC - <strong>Model Serving</strong> provides a unified endpoint for custom, foundation, and external models, with AI Gateway for governance. <strong>Genie Code</strong> replaces legacy AutoML with an agentic, conversational approach to the full ML lifecycle
# MAGIC - <strong>DASF</strong> provides a structured framework for identifying and mitigating AI-specific security risks across 12 system components
# MAGIC
# MAGIC <div style="border-left: 4px solid #607d8b; background: #eceff1; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <img src="../Includes/images/icons/link-icon.png" height="24" style="vertical-align: middle;">
# MAGIC     <div>
# MAGIC       <strong style="color: #37474f; font-size: 1.1em;">Additional Context</strong>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>AI and Machine Learning on Databricks (<a href="https://docs.databricks.com/aws/en/machine-learning/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/machine-learning/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/machine-learning/">GCP</a>): Overview of the ML platform, training, deployment, and governance</li>
# MAGIC         <li>MLflow on Databricks (<a href="https://docs.databricks.com/aws/en/mlflow/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/mlflow/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/mlflow/">GCP</a>): MLflow tracking, models, registry, and GenAI features</li>
# MAGIC         <li>Generative AI Apps on Databricks (<a href="https://docs.databricks.com/aws/en/generative-ai/guide/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/generative-ai/guide/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/generative-ai/guide/">GCP</a>): Agent framework, RAG, tools, and MCP integration</li>
# MAGIC         <li>Vector Search (<a href="https://docs.databricks.com/aws/en/vector-search/vector-search">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/vector-search/vector-search">Azure</a> | <a href="https://docs.databricks.com/gcp/en/vector-search/vector-search">GCP</a>): Index types, endpoints, hybrid search, and retrieval quality</li>
# MAGIC         <li>Model Serving (<a href="https://docs.databricks.com/aws/en/machine-learning/model-serving">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving">Azure</a> | <a href="https://docs.databricks.com/gcp/en/machine-learning/model-serving">GCP</a>): Custom, foundation, and external model deployment</li>
# MAGIC         <li>Unity AI Gateway (<a href="https://docs.databricks.com/aws/en/ai-gateway/">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-gateway/">GCP</a>): Governance, rate limiting, guardrails, and traffic splitting</li>
# MAGIC         <li>Genie Code (<a href="https://docs.databricks.com/aws/en/genie/genie-code.html">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/genie/genie-code">Azure</a> | <a href="https://docs.databricks.com/gcp/en/genie/genie-code.html">GCP</a>): Agentic ML development, Agent Mode, and customization</li>
# MAGIC         <li>DASF Whitepaper: <a href="https://www.databricks.com/resources/whitepaper/databricks-ai-security-framework-dasf">AI Security Framework</a> for risk management across AI deployments</li>
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
