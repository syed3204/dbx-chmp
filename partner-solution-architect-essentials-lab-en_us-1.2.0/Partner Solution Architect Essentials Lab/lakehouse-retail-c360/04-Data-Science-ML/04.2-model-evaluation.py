# Databricks notebook source
dbutils.widgets.dropdown("shap_enabled", "false", ["true", "false"], "Compute SHAP feature importance")

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Evaluation and Registration
# MAGIC
# MAGIC This notebook loads the best model from the churn prediction experiment, evaluates its performance, and registers it to Unity Catalog for production use.

# COMMAND ----------

# MAGIC %pip install mlflow==3.1.0 lightgbm scikit-learn<1.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Best Model Run

# COMMAND ----------

import mlflow
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score, accuracy_score, classification_report, confusion_matrix

mlflow.set_registry_uri('databricks-uc')

xp_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/psae_churn_prediction_{schema}"
mlflow.set_experiment(xp_path)

# Find the best run by validation F1 score
runs_df = mlflow.search_runs(filter_string="metrics.val_f1_score > 0", order_by=["metrics.val_f1_score DESC"])
best_run = runs_df.iloc[0]
best_run_id = best_run["run_id"]
print(f"Best run: {best_run_id}")
print(f"Val F1: {best_run['metrics.val_f1_score']:.4f}")
print(f"Test F1: {best_run['metrics.test_f1_score']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate on Feature Store Data

# COMMAND ----------

model = mlflow.sklearn.load_model(f"runs:/{best_run_id}/model")

# Load features for evaluation
features_df = spark.table(f"{catalog}.{db}.churn_user_features").toPandas()
target_col = "churn"

y = features_df[target_col].astype(int)
X = features_df.drop(columns=[target_col, "user_id"])

# Handle categorical and datetime columns
for col in X.select_dtypes(include=['object', 'category']).columns:
    X[col] = X[col].astype('category').cat.codes
for col in X.select_dtypes(include=['datetime', 'datetime64']).columns:
    X[col] = X[col].astype(np.int64) // 10**9
X = X.fillna(0)

y_pred = model.predict(X)
print("Classification Report:")
print(classification_report(y, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion Matrix

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay

fig, ax = plt.subplots(figsize=(6, 6))
ConfusionMatrixDisplay.from_predictions(y, y_pred, ax=ax, cmap="Blues")
ax.set_title("Churn Prediction Confusion Matrix")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance (SHAP)

# COMMAND ----------

shap_enabled = dbutils.widgets.get("shap_enabled") == "true"
if shap_enabled:
    try:
        import shap
        explainer = shap.TreeExplainer(model)
        sample = X.sample(n=min(200, len(X)), random_state=42)
        shap_values = explainer.shap_values(sample)
        shap.summary_plot(shap_values[1] if isinstance(shap_values, list) else shap_values, sample)
    except ImportError:
        print("SHAP not available. Install with: %pip install shap")
else:
    print("SHAP disabled. Set shap_enabled=true to compute feature importance.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model to Unity Catalog

# COMMAND ----------

model_name = "dbdemos_customer_churn"
client = mlflow.tracking.MlflowClient()

try:
    existing = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
    print(f"Model already registered: {catalog}.{db}.{model_name}@prod (version {existing.version})")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
        print("Registering model to Unity Catalog...")
        registered = mlflow.register_model(f"runs:/{best_run_id}/model", f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(
            name=f"{catalog}.{db}.{model_name}",
            alias="prod",
            version=registered.version
        )
        print(f"Registered: {catalog}.{db}.{model_name}@prod (version {registered.version})")
    else:
        # Model exists but needs updating -- register new version
        registered = mlflow.register_model(f"runs:/{best_run_id}/model", f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(
            name=f"{catalog}.{db}.{model_name}",
            alias="prod",
            version=registered.version
        )
        print(f"Updated: {catalog}.{db}.{model_name}@prod (version {registered.version})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The model is now registered in Unity Catalog with the `@prod` alias. Next:
# MAGIC - [Deploy for batch and real-time inference]($./04.3-running-inference)
