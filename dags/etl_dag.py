import os
import pytz
import pyarrow.parquet as pq

from datetime import datetime, timedelta

from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = str(os.path.basename(__file__).replace(".py", ""))

CURRENT_DATETIME = datetime.now(pytz.timezone('Asia/Bangkok'))

BRONZE_ZONE_DIR: str = "bronze"
SILVER_ZONE_DIR: str = "silver"
GOLD_ZONE_DIR: str = "gold"

DATABASE_NAME = "warehouse"
TABLE_NAME = "customers"

YEAR = CURRENT_DATETIME.year
MONTH = CURRENT_DATETIME.month
DAY = CURRENT_DATETIME.day

destination_dir = f"data/{GOLD_ZONE_DIR}/table={TABLE_NAME}/year={YEAR}/month={MONTH}/day={DAY}/"

DESTINATION_ENDPOINT = f'postgresql://airflow:airflow@postgres:5432/{DATABASE_NAME}'
ENGINE = create_engine(DESTINATION_ENDPOINT)


def load():
    year = CURRENT_DATETIME.year
    month = CURRENT_DATETIME.month
    day = CURRENT_DATETIME.day
    source_dir = "data/{}/table={}/year={}/month={}/day={}/".format(GOLD_ZONE_DIR, TABLE_NAME, year, month, day)
    df = pq.read_table(source_dir).to_pandas()

    df.to_sql(name=TABLE_NAME, con=ENGINE, if_exists="append", index=False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

with DAG(dag_id=DAG_ID,
         description='ETL for ST',
         schedule="@once",
         default_args=default_args,
         catchup=False,
         tags=["st", "exam"]) as dag:
    execute_etl_task = SparkSubmitOperator(
        task_id="etl_task",
        application="scripts/main.py",
        application_args=["--source", "data/transaction.csv",
                          "--destination", destination_dir],
        conn_id="spark_cnx",
        verbose=True,
        conf={
            "spark.master": "spark://spark:7077",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.3"
        }
    )

    ###
    # in order to load for small tables
    ###
    # load_task = PythonOperator(
    #     task_id="load_task",
    #     python_callable=load
    # )

    execute_etl_task
