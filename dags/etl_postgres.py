import pytz
import yaml
from datetime import datetime
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from resources.scripts.extract import extract_table
from resources.scripts.load import load_table

with open("dags/resources/config/postgres_config.yaml", "r") as f:
   config = yaml.safe_load(f)

@dag(
   schedule_interval = None,
   start_date        = datetime(2025, 1, 12, tzinfo=pytz.timezone("Asia/Jakarta")),
   catchup           = False,
   params            = {
      table: Param("incremental", description="incremental / full", enum=["full", "incremental"])
      for table in config["batch_ingestion"]
   }
)

def etl_postgres():
   start_task          = EmptyOperator(task_id="start_task")
   end_task            = EmptyOperator(task_id="end_task")
   wait_etl_task        = EmptyOperator(task_id="wait_etl_task")
   wait_datamart_task = EmptyOperator(task_id="wait_datamart_task")

   for table in config.get("batch_ingestion", []):
      extract = task(extract_table, task_id=f"extract.{table}")
      load    = task(load_table, task_id=f"load.{table}")

      start_task >> extract(table) >> load(table) >> wait_etl_task


#    for filepath in config.get("datamart", []):
#        datamart = SQLExecuteQueryOperator(
#            task_id = f"datamart.{filepath.split('/')[-1]}",
#            conn_id = "postgres_dw",
#            sql     = filepath,
#        )

#        wait_etl_task >> datamart >> wait_datamart_task


#    for filepath in config.get("analytic", []):
#        analytic = SQLExecuteQueryOperator(
#            task_id = f"analytic.{filepath.split('/')[-1]}",
#            conn_id = "postgres_dw",
#            sql     = filepath,
#        )

#        wait_datamart_task >> analytic >> end_task

etl_postgres()