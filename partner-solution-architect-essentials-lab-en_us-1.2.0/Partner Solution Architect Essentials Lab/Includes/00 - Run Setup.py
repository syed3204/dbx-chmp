# Databricks notebook source
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"Notebook path: {notebook_path}")

# %run returns the caller's path (00 - Set up DB Demos), not this notebook's path
# Strip just the notebook name to get the course folder
course_path = notebook_path.rsplit("/", 1)[0]
demo_folder = course_path + "/lakehouse-retail-c360"

print(f"Course path: {course_path}")
print(f"Demo folder: {demo_folder}")

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
import re
schema = re.sub(r'[^a-zA-Z0-9_]', '_', username.split("@")[0])
catalog = "dbacademy"

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
spark.sql(f"USE `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS c360")
print(f"Ready: {catalog}.{schema} with volume c360")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pipeline_name = f"psae_sdp_churn_{schema}"

# Drop stale event log table if it exists (prevents conflict on pipeline re-creation)
spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.psae_event_log")

existing = [p for p in w.pipelines.list_pipelines() if p.name == pipeline_name]
if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"Pipeline already exists: {pipeline_name} ({pipeline_id})")
else:
    result = w.api_client.do("POST", "/api/2.0/pipelines", body={
        "name": pipeline_name,
        "serverless": True,
        "libraries": [{"glob": {"include": f"{demo_folder}/01-Data-ingestion/01.1-SDP-SQL/transformations/**"}}],
        "catalog": catalog,
        "schema": schema,
        "configuration": {"schema": schema},
        "continuous": False,
        "development": True,
        "channel": "PREVIEW",
        "root_path": f"{demo_folder}/01-Data-ingestion/01.1-SDP-SQL",
        "event_log": {"catalog": catalog, "schema": schema, "name": "psae_event_log"},
        "environment": {"dependencies": ["mlflow==3.1.0"]}
    })
    pipeline_id = result["pipeline_id"]
    print(f"Created pipeline: {pipeline_name} ({pipeline_id})")

# COMMAND ----------

from databricks.sdk.service.jobs import (
    Task, NotebookTask, PipelineTask, TaskDependency,
    JobEnvironment, Source
)
from databricks.sdk.service.compute import Environment

job_name = f"psae_retail_c360_init_{schema}"

existing_jobs = [j for j in w.jobs.list() if j.settings.name == job_name]
if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"Job already exists: {job_name} ({job_id})")
else:
    job = w.jobs.create(
        name=job_name,
        max_concurrent_runs=1,
        environments=[
            JobEnvironment(
                environment_key="default",
                spec=Environment(
                    environment_version="5",
                    dependencies=["mlflow==3.1.0", "lightgbm", "scikit-learn<1.6"]
                )
            )
        ],
        tasks=[
            Task(
                task_key="load_dataset",
                notebook_task=NotebookTask(notebook_path=f"{demo_folder}/_resources/01-load-data", source=Source.WORKSPACE),
                environment_key="default"
            ),
            Task(
                task_key="start_sdp_pipeline",
                pipeline_task=PipelineTask(pipeline_id=pipeline_id, full_refresh=True),
                depends_on=[TaskDependency(task_key="load_dataset")]
            ),
            Task(
                task_key="init_dashboard_data",
                notebook_task=NotebookTask(notebook_path=f"{demo_folder}/_resources/00-prep-data-db-sql", source=Source.WORKSPACE),
                depends_on=[TaskDependency(task_key="start_sdp_pipeline")],
                environment_key="default"
            ),
            Task(
                task_key="create_feature_and_train_model",
                notebook_task=NotebookTask(notebook_path=f"{demo_folder}/04-Data-Science-ML/04.1-churn-prediction", source=Source.WORKSPACE),
                depends_on=[TaskDependency(task_key="init_dashboard_data")],
                environment_key="default"
            ),
            Task(
                task_key="register_churn_model",
                notebook_task=NotebookTask(
                    notebook_path=f"{demo_folder}/04-Data-Science-ML/04.2-model-evaluation",
                    source=Source.WORKSPACE,
                    base_parameters={"shap_enabled": "false"}
                ),
                depends_on=[TaskDependency(task_key="create_feature_and_train_model")],
                environment_key="default"
            ),
            Task(
                task_key="running_inference",
                notebook_task=NotebookTask(notebook_path=f"{demo_folder}/04-Data-Science-ML/04.3-running-inference", source=Source.WORKSPACE),
                depends_on=[TaskDependency(task_key="register_churn_model")],
                environment_key="default"
            ),
            Task(
                task_key="create_ai_functions",
                notebook_task=NotebookTask(notebook_path=f"{demo_folder}/05-Generative-AI/05.1-Agent-Functions-Creation", source=Source.WORKSPACE),
                depends_on=[TaskDependency(task_key="running_inference")],
                environment_key="default"
            )
        ]
    )
    job_id = job.job_id
    print(f"Created job: {job_name} ({job_id})")

# COMMAND ----------

import requests

dashboard_folder = f"/Workspace{demo_folder}/_resources/dashboards"
dashboard_files = {
    "churn-universal": "Retail - Customer Churn - Universal",
    "churn-prediction": "Retail Churn Prediction",
    "sdp-quality-stat": "SDP Data Quality Stats"
}

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

for file_key, base_name in dashboard_files.items():
    dashboard_name = f"{base_name} - {schema}"
    json_path = f"{dashboard_folder}/{file_key}.lvdash.json"

    try:
        with open(json_path) as f:
            dashboard_def = f.read()

        # Replace hardcoded catalog/schema references in dashboard JSON
        dashboard_def = dashboard_def.replace("main__build", catalog)
        dashboard_def = dashboard_def.replace('"main"', f'"{catalog}"')
        dashboard_def = dashboard_def.replace("dbdemos_retail_c360", schema)

        # Check if dashboard already exists for this user
        existing_dashboards = requests.get(
            f"https://{host}/api/2.0/lakeview/dashboards",
            headers=headers,
            params={"page_size": 100}
        ).json()

        found = next((d for d in existing_dashboards.get("dashboards", []) if d.get("display_name") == dashboard_name), None)

        if found:
            print(f"Dashboard already exists: {dashboard_name}")
        else:
            resp = requests.post(
                f"https://{host}/api/2.0/lakeview/dashboards",
                headers=headers,
                json={
                    "display_name": dashboard_name,
                    "serialized_dashboard": dashboard_def,
                    "parent_path": f"/Workspace/Users/{username}"
                }
            )
            if resp.status_code == 200:
                print(f"Created dashboard: {dashboard_name}")
            else:
                print(f"Dashboard {dashboard_name}: {resp.status_code} - {resp.text[:200]}")
    except FileNotFoundError:
        print(f"Dashboard JSON not found: {json_path} (skipping)")
    except Exception as e:
        print(f"Dashboard {dashboard_name} error: {e}")

# COMMAND ----------

from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# Check if job has a successful completed run already
runs = list(w.jobs.list_runs(job_id=job_id, limit=5))
has_successful_run = any(
    r.state
    and r.state.life_cycle_state == RunLifeCycleState.TERMINATED
    and r.state.result_state == RunResultState.SUCCESS
    for r in runs
)
has_active_run = any(
    r.state
    and r.state.life_cycle_state in (RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING)
    for r in runs
)

_host = spark.conf.get("spark.databricks.workspaceUrl")
_job_url = f"https://{_host}/jobs/{job_id}"

if has_successful_run:
    print(f"Job {job_name} has already completed successfully. Skipping re-run.")
    print(f"Job URL: {_job_url}")
    print("To force a re-run, go to the job page and trigger it manually.")
elif has_active_run:
    print(f"Job {job_name} is already running.")
    print(f"Monitor progress: {_job_url}")
else:
    run = w.jobs.run_now(job_id)
    print(f"Started job run: {job_name} (run_id: {run.run_id})")
    print(f"Monitor progress: {_job_url}")
    print(f"The job runs 7 tasks sequentially and should complete in 15-25 minutes.")
