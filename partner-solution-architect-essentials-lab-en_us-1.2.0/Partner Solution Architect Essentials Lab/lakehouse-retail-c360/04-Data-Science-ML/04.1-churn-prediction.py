# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Training
# MAGIC
# MAGIC This notebook trains a LightGBM classification model to predict customer churn using features from the SDP pipeline.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Load and explore the churn features data
# MAGIC 2. Create a feature table for model training
# MAGIC 3. Train a LightGBM classifier with MLflow tracking
# MAGIC 4. Evaluate model performance

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="color: #c62828; font-size: 14pt;">Serverless Compute Version 5 Required</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This notebook must run on <strong>Serverless Compute, Version 5</strong>. If you are on a different version, click the environment selector at the top right of the notebook and switch to Version 5 before running any cells.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install mlflow==3.1.0 lightgbm scikit-learn<1.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration
# MAGIC
# MAGIC Review the churn features table produced by the SDP pipeline.

# COMMAND ----------

churn_dataset = spark.table("churn_features")
display(churn_dataset)

# COMMAND ----------

import seaborn as sns
g = sns.PairGrid(churn_dataset.sample(0.01).toPandas()[['age_group','total_amount','order_count']], diag_sharey=False)
g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Preparation
# MAGIC
# MAGIC Drop columns not useful for modeling and save to the Feature Store.

# COMMAND ----------

dataset = churn_dataset.pandas_api()
dataset.describe()
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'last_event'])
# Fill nulls (CSV ingestion may produce nulls in numeric columns)
for col in dataset.columns:
    if col in ['canal', 'country', 'platform', 'user_id']:
        dataset[col] = dataset[col].fillna('UNKNOWN')
    elif col == 'last_transaction':
        pass
    else:
        dataset[col] = dataset[col].fillna(0)

# COMMAND ----------

features_df = dataset.to_spark()
features_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{db}.churn_user_features")
features = spark.table(f"{catalog}.{db}.churn_user_features")
print(f"Wrote {features.count()} rows to {catalog}.{db}.churn_user_features")
display(features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train LightGBM Classifier
# MAGIC
# MAGIC Train a churn classification model using LightGBM with MLflow experiment tracking.

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

df = features.toPandas()
target_col = "churn"

# Separate features and target
y = df[target_col].astype(int)
X = df.drop(columns=[target_col, "user_id"])

# Handle categorical and datetime columns
for col in X.select_dtypes(include=['object', 'category']).columns:
    X[col] = X[col].astype('category').cat.codes

for col in X.select_dtypes(include=['datetime', 'datetime64']).columns:
    X[col] = X[col].astype(np.int64) // 10**9  # Convert to unix timestamp

X = X.fillna(0)

# Train/validation/test split
X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp)

print(f"Train: {len(X_train)}, Validation: {len(X_val)}, Test: {len(X_test)}")

# COMMAND ----------

import mlflow
import lightgbm
from lightgbm import LGBMClassifier
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score

mlflow.set_registry_uri('databricks-uc')

xp_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/psae_churn_prediction_{schema}"
mlflow.set_experiment(xp_path)

with mlflow.start_run(run_name="lgbm_churn_classifier") as run:
    # Train LightGBM
    model = LGBMClassifier(
        n_estimators=200,
        max_depth=8,
        learning_rate=0.1,
        num_leaves=31,
        random_state=42
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lightgbm.early_stopping(10), lightgbm.log_evaluation(0)]
    )

    # Evaluate
    y_pred_val = model.predict(X_val)
    y_pred_test = model.predict(X_test)

    val_f1 = f1_score(y_val, y_pred_val)
    test_f1 = f1_score(y_test, y_pred_test)

    # Log metrics
    mlflow.log_metric("val_f1_score", val_f1)
    mlflow.log_metric("val_accuracy", accuracy_score(y_val, y_pred_val))
    mlflow.log_metric("test_f1_score", test_f1)
    mlflow.log_metric("test_accuracy", accuracy_score(y_test, y_pred_test))
    mlflow.log_metric("test_precision", precision_score(y_test, y_pred_test))
    mlflow.log_metric("test_recall", recall_score(y_test, y_pred_test))

    # Log model
    mlflow.sklearn.log_model(model, artifact_path="model")

    print(f"Validation F1: {val_f1:.4f}")
    print(f"Test F1:       {test_f1:.4f}")
    print(f"Run ID:        {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance
# MAGIC
# MAGIC Review which features have the most impact on churn prediction.

# COMMAND ----------

import matplotlib.pyplot as plt

feature_importance = pd.DataFrame({
    'feature': X_train.columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=True)

plt.figure(figsize=(10, 8))
plt.barh(feature_importance['feature'], feature_importance['importance'])
plt.xlabel('Importance')
plt.title('Feature Importance for Churn Prediction')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The model is logged in MLflow. Next:
# MAGIC - [Register and deploy the model]($./04.2-model-evaluation)
# MAGIC - [Run batch and real-time inferences]($./04.3-running-inference)
