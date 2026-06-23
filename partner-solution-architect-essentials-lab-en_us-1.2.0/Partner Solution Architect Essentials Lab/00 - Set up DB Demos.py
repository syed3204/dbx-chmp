# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Partner Solution Architect Essentials Lab

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 14pt;">Select Compute</strong>
# MAGIC <p style="font-size: 14pt; margin: 8px 0 0 0; color: #333;">Before starting this notebook, select the required compute environment listed below.</p>
# MAGIC <ul style="font-size: 14pt; margin: 12px 0 0 16px; color: #333;">
# MAGIC <li style="font-size: 14pt;"><strong style="font-size: 14pt;">Serverless Compute, Version 5</strong>: <a href="https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version" style="color: #2574B5;">How to select an environment version</a></li>
# MAGIC </ul>
# MAGIC <p style="font-size: 14pt; margin: 8px 0 0 0; color: #333;"><strong style="font-size: 14pt;">NOTE:</strong> This notebook was <strong style="font-size: 14pt;">developed and tested using Serverless V5</strong>. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated. Do not use Serverless GPU.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Initialize Your Environment
# MAGIC
# MAGIC Run the cell below to set up the environment. This creates your per-user Unity Catalog schema, SDP pipeline, dashboards, and workflow job, then launches the setup job to load data and train models.

# COMMAND ----------

# MAGIC %run "./Includes/00 - Run Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Monitor Your Setup Job</strong>
# MAGIC <div style="color:#333;">
# MAGIC <p>The setup job has been submitted. To monitor progress:</p>
# MAGIC <ol>
# MAGIC <li>Open the <strong>Jobs & Pipelines</strong> page from the left sidebar.</li>
# MAGIC <li>Find your job (named <code>psae_retail_c360_init_&lt;your_username&gt;</code>).</li>
# MAGIC <li>Click into the job to see task-level progress.</li>
# MAGIC </ol>
# MAGIC <p>You can proceed to the next notebooks once <strong>Task 2 (start_sdp_pipeline)</strong> completes. The remaining tasks finish in the background.</p>
# MAGIC <p>All resources are created under your personal schema (<code>dbacademy.&lt;your_username&gt;</code>), so your work does not affect other students.</p>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Review Job Details
# MAGIC
# MAGIC The setup job runs 7 tasks sequentially and should complete in 15-25 minutes:
# MAGIC
# MAGIC <table style="border-collapse: collapse; width: 100%; font-size: 14px;">
# MAGIC <tr style="border-bottom: 2px solid #ddd;">
# MAGIC   <th style="text-align: left; padding: 8px; width: 5%;">#</th>
# MAGIC   <th style="text-align: left; padding: 8px; width: 30%;">Task</th>
# MAGIC   <th style="text-align: left; padding: 8px;">What it does</th>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">1</td>
# MAGIC   <td style="padding: 8px;"><code>load_dataset</code></td>
# MAGIC   <td style="padding: 8px;">Loads sample data into a UC Volume</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">2</td>
# MAGIC   <td style="padding: 8px;"><code>start_sdp_pipeline</code></td>
# MAGIC   <td style="padding: 8px;">Runs bronze/silver/gold pipeline with ML predictions</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">3</td>
# MAGIC   <td style="padding: 8px;"><code>init_dashboard_data</code></td>
# MAGIC   <td style="padding: 8px;">Prepares tables for DBSQL dashboards</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">4</td>
# MAGIC   <td style="padding: 8px;"><code>create_feature_and_train_model</code></td>
# MAGIC   <td style="padding: 8px;">Trains a churn classification model</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">5</td>
# MAGIC   <td style="padding: 8px;"><code>register_churn_model</code></td>
# MAGIC   <td style="padding: 8px;">Registers model to Unity Catalog with "prod" alias</td>
# MAGIC </tr>
# MAGIC <tr style="border-bottom: 1px solid #eee;">
# MAGIC   <td style="padding: 8px;">6</td>
# MAGIC   <td style="padding: 8px;"><code>running_inference</code></td>
# MAGIC   <td style="padding: 8px;">Batch inference + deploys model serving endpoint</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC   <td style="padding: 8px;">7</td>
# MAGIC   <td style="padding: 8px;"><code>create_ai_functions</code></td>
# MAGIC   <td style="padding: 8px;">Creates SQL AI functions for churn prediction</td>
# MAGIC </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. General Introduction and Instructions
# MAGIC
# MAGIC In this section, you will work with the `lakehouse-retail-c360` notebooks, which include pipelines, dashboards, ML models, and other assets. Your objectives are:
# MAGIC
# MAGIC 1. **Explore and Run the Notebooks**
# MAGIC    - Open each notebook in the `lakehouse-retail-c360` folder and review its contents.
# MAGIC    - Use Genie Code to understand the current contents.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Tip: Genie Code Shortcuts</strong>
# MAGIC <div style="color:#333;">Use <code>/doc</code> to add comments to a code cell, or <code>/explain</code> to have Genie Code explain the code.</div>
# MAGIC </div>
# MAGIC
# MAGIC 2. **Complete the Assigned Tasks**
# MAGIC    - Follow the instructions provided in each notebook and in **this** document.
# MAGIC    - Capture the required evidence (output, screenshots, or descriptions) and add them to this notebook where requested.
# MAGIC      The output of the other executed cells will also act as evidence during assessment.
# MAGIC
# MAGIC 3. **Submit Your Work**
# MAGIC    - Once you have finished all tasks, submit the entire folder containing:
# MAGIC      - This notebook with all of your evidence.
# MAGIC      - All notebooks in the `lakehouse-retail-c360` folder.
# MAGIC    - Reviewers check for completeness and accuracy.
# MAGIC
# MAGIC **Next Steps**
# MAGIC Proceed to the subsequent notebooks to begin your hands-on activities.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Important Note</strong>
# MAGIC <div style="color:#333;">The setup job occasionally requires troubleshooting or debugging to ensure successful execution. If you encounter errors, take the time to debug and resolve the issues. Share solutions and insights with classmates.</div>
# MAGIC </div>
