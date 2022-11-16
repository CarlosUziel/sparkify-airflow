from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# amazon aws provider reference:
#   https://github.com/apache/airflow/tree/main/airflow/providers/amazon/aws

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# set variables/connections programatically:
#   https://stackoverflow.com/questions/69086208/programatically-set-connections-variables-in-airflow

default_args = {
    "owner": "Sparkify",
    "start_date": datetime(2019, 1, 12),
}

dag = DAG(
    "SPARKIFY_S3_to_Redshift",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)

start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)


# 1. stage events
# S3ToRedshiftOperator()

# 1. stage songs
# S3ToRedshiftOperator()

# 2. Load songplays
# RedshiftSQLOperator()

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
