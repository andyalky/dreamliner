from airflow.plugins_manager import AirflowPlugin

from dreamliner.operators.mssql_schema_to_postgres_operator import MsSqlSchemaToPostgresOperator
from dreamliner.operators.mssql_to_s3_operator import MsSqlToS3Operator
from dreamliner.operators.s3_to_postgres_operator import S3ToPostgresOperator

from dreamliner.hooks.S3_hook import S3Hook

class DreamlinerPlugin(AirflowPlugin):
    name = "dreamliner"
    operators = [
        MsSqlSchemaToPostgresOperator,
        MsSqlToS3Operator,
        S3ToPostgresOperator,
    ]
    hooks = [
        S3Hook,
    ]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []