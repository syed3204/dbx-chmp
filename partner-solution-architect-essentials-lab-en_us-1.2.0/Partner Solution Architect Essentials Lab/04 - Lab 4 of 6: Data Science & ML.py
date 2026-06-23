# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Lab 4 of 6: Data Science and Machine Learning
# MAGIC
# MAGIC Run the Data Science and ML notebooks, then complete the activities below. You will train, evaluate, and tune machine learning models.
# MAGIC
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Model Deployment</strong>
# MAGIC <div style="color:#333;">The setup job has trained a LightGBM churn prediction model and registered it in Unity Catalog. A shared model serving endpoint (<code>psae_customer_churn_endpoint</code>) is available for all users to query.</div>
# MAGIC </div>
# MAGIC
# MAGIC Once you have completed the notebooks, proceed to the associated activities.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Modify Model Configuration Using Genie Code
# MAGIC
# MAGIC Use **Genie Code** (the Databricks AI assistant) to help you modify the model training configuration and experiment with different approaches to improve performance.
# MAGIC
# MAGIC
# MAGIC ### A1. Review the Trained Model
# MAGIC
# MAGIC 1. **Open Notebook 04.1:**
# MAGIC    - Navigate to `lakehouse-retail-c360/04-Data-Science-ML/04.1-churn-prediction`.
# MAGIC    - Review the LightGBM model training code, feature preparation, and logged metrics.
# MAGIC
# MAGIC 2. **Open Notebook 04.2:**
# MAGIC    - Navigate to `lakehouse-retail-c360/04-Data-Science-ML/04.2-model-evaluation`.
# MAGIC    - Review the model evaluation, confusion matrix, and registration to Unity Catalog.
# MAGIC
# MAGIC
# MAGIC ### A2. Use Genie Code to Modify the Model
# MAGIC
# MAGIC 1. **Open Notebook 04.1** and use **Genie Code** to make changes to the model configuration.
# MAGIC    - Click the **Genie Code** button (or use the keyboard shortcut) to open the AI assistant.
# MAGIC    - Try prompts such as:
# MAGIC      - _"Replace LightGBM with a Random Forest classifier and remove the LightGBM-specific callbacks"_
# MAGIC      - _"Add cross-validation with 5 folds"_
# MAGIC      - _"Increase the number of estimators to 500 and reduce the learning rate to 0.05"_
# MAGIC      - _"Add a new feature column that calculates the ratio of order_count to session_count"_
# MAGIC
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Troubleshooting</strong>
# MAGIC <div style="color:#333;">Genie Code suggestions may not always run without errors. If a change breaks the notebook, review the error, undo the change, and try a different prompt or adjust the generated code manually. This is a normal part of iterating with AI-assisted coding.</div>
# MAGIC </div>
# MAGIC
# MAGIC 2. **Run the modified notebook** and observe the new performance metrics.
# MAGIC
# MAGIC 3. **Compare results** by checking the MLflow experiment:
# MAGIC    - Multiple runs will appear in the experiment, each with different configurations.
# MAGIC    - Compare F1 scores, accuracy, and other metrics across runs.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Tip</strong>
# MAGIC <div style="color:#333;">Genie Code can help you understand existing code as well. Try selecting a code block and asking <em>"Explain this code"</em> or <em>"What does this cell do?"</em></div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ### A3. Analyze Runs in the MLflow Experiment UI
# MAGIC
# MAGIC 1. **Navigate to Notebook 04.1:**
# MAGIC    - On the right side of the notebook, find the flask icon indicating the **MLflow Experiment** and click on it.
# MAGIC    - Review the details of the runs you performed with different model configurations.
# MAGIC
# MAGIC 2. **Switch to the "Charts" View:**
# MAGIC    - Go to the **"Experiments"** section by clicking back one level in the breadcrumbs at the top of the screen.
# MAGIC    - Change the view to **"Charts"** to visualize the comparison between model performance across runs.
# MAGIC
# MAGIC 3. **Save** a screenshot of the chart view comparing your model runs.
# MAGIC
# MAGIC ### A4. Verification: Screenshot of Model Comparison
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Verification</strong>
# MAGIC <div style="color:#333;">Click the <strong>Upload Screenshot</strong> button in the widget below, navigate to your saved screenshot, and select <strong>Open</strong>. Then run the <code>show_evidence</code> code cell to verify the image appears correctly.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Images/Image_Display_Function

# COMMAND ----------

upload_evidence("evidence-04-experiments.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows the MLflow Experiment Charts view comparing your model runs.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-04-experiments.png", width=600)
