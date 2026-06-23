# ----------------------------------------
# Registering python UDF to a SQL function
# ----------------------------------------
# This notebook loads the predict_churn model from MLflow registry
# and registers it as a SQL function for use in the SDP pipeline.
#
# While this code could be embedded in the SQL notebook, it won't be executed
# by the SDP engine (since SQL notebooks only process SQL cells).
# Therefore, this companion Python notebook must be included in your SDP libraries.

import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                     Stage/version
#                                                                                   Model name               |
#                                                                                       |                    |
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow==3.1.0", "lightgbm", "scikit-learn<1.6"])


user_credential = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
model_schema = spark.conf.get("schema")

predict_churn_udf = mlflow.pyfunc.spark_udf(spark, f"models:/dbacademy.{model_schema}.dbdemos_customer_churn@prod", "long", env_manager='local')
spark.udf.register("predict_churn", predict_churn_udf)
