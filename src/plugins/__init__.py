from __future__ import absolute_import, division, print_function

import helpers
import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
    ]
    helpers = [helpers.SqlQueries]
