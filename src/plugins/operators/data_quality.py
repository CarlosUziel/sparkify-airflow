import logging

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    def __init__(self, conn_id: str, table: str, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        redshift_hook = RedshiftSQLHook(self.conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed. {self.table} returned no results"
            )
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(
                f"Data quality check failed. {self.table} contained 0 rows"
            )
        logging.info(
            f"Data quality on table {self.table} check passed with"
            f" {records[0][0]} records"
        )
