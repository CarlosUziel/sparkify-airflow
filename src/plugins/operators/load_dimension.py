from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator


class LoadDimensionOperator(RedshiftSQLOperator):
    ui_color = "#80BD9E"

    def __init__(self, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
