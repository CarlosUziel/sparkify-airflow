from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    def __init__(
        self,
        # Define your operators params (with defaults) here
        # Example:
        # conn_id = your-connection-name
        *args,
        **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info("DataQualityOperator not implemented yet")
