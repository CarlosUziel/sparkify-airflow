from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from sql_queries import STAGING_TABLES
from utils import process_config

user_config, dwh_config = (
    process_config(Path(__file__).parents[2].joinpath("_user.cfg")),
    process_config(Path(__file__).parents[2].joinpath("dwh.cfg")),
)

default_args = {
    "owner": "Sparkify",
    "start_date": datetime(2022, 11, 1),
}

dag = DAG(
    "SPARKIFY_S3_to_Redshift",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@monthly",
)

start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)


# 1. Staging tables
stage_tables_tasks = {
    table_name: S3ToRedshiftOperator(
        task_id=f"stage_{table_name.split('_')[-1]}",
        dag=dag,
        schema=dwh_config.get("DWH", "DWH_DB"),
        table=table_name,
        s3_bucket=dwh_config.get("S3", "BUCKET_NAME"),
        s3_key=dwh_config.get(
            "S3", table_name.split("_")[-1]
        ),  # assign from a task factory
        redshift_conn_id="aws_redshift",
        aws_conn_id="aws_credentials",
        column_list=[col.split(" ")[0] for col in table_args],
        autocommit=True,
        method="REPLACE",
    )
    for table_name, table_args in STAGING_TABLES.items()
}

for task in stage_tables_tasks.values():
    start_operator.set_downstream(task)

# 3. Load users, song, etc. (one task each)
# RedshiftSQLOperator()

# 4. Data quality on all loaded tables
# one task for each table to be checked!

# load_songplays_table = LoadFactOperator(task_id="Load_songplays_fact_table", dag=dag)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id="Load_user_dim_table", dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id="Load_song_dim_table", dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id="Load_artist_dim_table", dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id="Load_time_dim_table", dag=dag
# )

# run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks", dag=dag)

end_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

start_operator >> end_operator
