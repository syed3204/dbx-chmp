# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook
# MAGIC Copies bundled dataset files to the per-user UC volume and registers a placeholder churn model.

# COMMAND ----------

# MAGIC %pip install mlflow==3.1.0 cloudpickle==3.0.0 numpy==1.26.4 pandas==2.2.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../Includes/_resources/00-global-setup-v2

# COMMAND ----------

import sys
major, minor = sys.version_info[:2]
print(f"Python version: {major}.{minor}")

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False, volume_name)
volume_path = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import os

# Resolve the bundled data path relative to this notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# This notebook is at lakehouse-retail-c360/_resources/01-load-data
# Bundled data is at Includes/data/ (sibling of lakehouse-retail-c360 under course folder)
course_folder = notebook_path.rsplit("/lakehouse-retail-c360/", 1)[0]
data_source = f"file:/Workspace{course_folder}/Includes/data"

# Copy all data folders directly to the volume
# Clean each destination first to avoid mixing old files with new ones
for subfolder in ["events", "orders", "users", "ml_features"]:
    src = f"{data_source}/{subfolder}"
    dst = f"{volume_path}/{subfolder}"
    # Remove any existing files to prevent format mixing (e.g., old JSON + new CSV)
    try:
        dbutils.fs.rm(dst, recurse=True)
    except:
        pass
    dbutils.fs.cp(src, dst, recurse=True)
    file_count = len(dbutils.fs.ls(dst))
    print(f"Copied {file_count} files to {dst}")

print("Data load complete.")

# COMMAND ----------

import mlflow
from mlflow.models.signature import ModelSignature
import cloudpickle
from unittest import mock
import numpy as np

class ChurnEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        return model_input['user_id'].apply(lambda x: random.randint(0, 1)).astype('int32')

mlflow.set_registry_uri('databricks-uc')
model_name = "dbdemos_customer_churn"
client = mlflow.tracking.MlflowClient()

try:
    latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
    print(f"Model already registered: {catalog}.{db}.{model_name}@prod")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
        print("Model doesn't exist - registering placeholder model")
        DBDemos.init_experiment_for_batch("lakehouse-retail-c360", "customer_churn_mock")
        churn_model = ChurnEmptyModel()
        import pandas as pd
        signature = ModelSignature.from_dict({
            'inputs': '[{"name": "user_id", "type": "string"}, {"name": "age_group", "type": "long"}, {"name": "canal", "type": "string"}, {"name": "country", "type": "string"}, {"name": "gender", "type": "long"}, {"name": "order_count", "type": "long"}, {"name": "total_amount", "type": "long"}, {"name": "total_item", "type": "long"}, {"name": "last_transaction", "type": "datetime"}, {"name": "platform", "type": "string"}, {"name": "event_count", "type": "long"}, {"name": "session_count", "type": "long"}, {"name": "days_since_creation", "type": "long"}, {"name": "days_since_last_activity", "type": "long"}, {"name": "days_last_event", "type": "long"}]',
            'outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "int32", "shape": [-1]}}]'
        })
        with mlflow.start_run(run_name="mockup_model") as run, mock.patch("mlflow.utils.environment.PYTHON_VERSION", DBDemos.get_python_version_mlflow()):
            model_info = mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=churn_model,
                signature=signature,
                pip_requirements=[f'mlflow=={mlflow.__version__}', f'pandas=={pd.__version__}', f'numpy=={np.__version__}', f'cloudpickle=={cloudpickle.__version__}']
            )
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
        print(f"Registered placeholder model: {catalog}.{db}.{model_name}@prod")
    else:
        print(f"ERROR: couldn't access model: {e}")

# COMMAND ----------

dbutils.notebook.exit("data loaded")
