# Databricks notebook source
# MAGIC %md
# MAGIC # Image Evidence Test
# MAGIC
# MAGIC This notebook tests the upload + display pattern for learner screenshot submissions.
# MAGIC Each evidence slot uses two cells: an upload cell and a display cell.

# COMMAND ----------

# MAGIC %run ./Images/Image_Display_Function

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Evidence: Activity 01 - Pipeline Screenshot
# MAGIC Drag and drop your screenshot below. It will be saved automatically as `evidence-01.png`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run this cell. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

upload_evidence("evidence-01.png")

# COMMAND ----------

show_evidence("evidence-01.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Evidence: Activity 02 - Alert Configuration
# MAGIC Drag and drop your screenshot below. It will be saved automatically as `evidence-02.png`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run this cell. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

upload_evidence("evidence-02.png")

# COMMAND ----------

show_evidence("evidence-02.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Evidence: Activity 03 - Dashboard with Genie

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run this cell. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

upload_evidence("evidence-03.png")

# COMMAND ----------

show_evidence("evidence-03.png")
