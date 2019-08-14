from airflow import DAG

from dreamliner.operators.mssql_schema_to_postgres_operator import MsSqlSchemaToPostgresOperator
from dreamliner.operators.mssql_to_s3_operator import MsSqlToS3Operator
from dreamliner.operators.s3_to_postgres_operator import S3ToPostgresOperator

from airflow.operators.dummy_operator import DummyOperator


from datetime import datetime, date, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('test_etl', catchup=False, default_args=default_args)

SOURCE_CONN_ID = 'cmp_source'
STORAGE_CONN_ID = 'cmp_s3'
TARGET_CONN_ID = 'cmp_dw'

STORAGE_DIRECTORY = 'cmpetl'



transfer_definitions = [
    {
        'schema': 'public', 
        'table': 'rpt_campaignrun', 
        'query_string': 'SELECT * FROM CampaignRun',
        'query_merge_field': None,
    },
    {
        'schema': 'public', 
        'table': 'rpt_workflowrun', 
        'query_string': 'SELECT * FROM WorkflowRun',
        'query_merge_field': None,
    },                  
]

"""
    {
        'schema': 'public', 
        'table': 'rpt_campaign', 
        'query_string': 'SELECT * FROM Campaign WHERE modificationDate >= {0}',
        'query_merge_field': 'modificationDate',
    },     
"""


t0 = DummyOperator(task_id='start-etl', dag=dag)
tx = DummyOperator(task_id='end-etl', dag=dag)

for transfer in transfer_definitions:

    #Sync where possible for trailing 30 days
    #from_date = '{{ds - macros.timedelta(days=30)}}'    
    #from_date = datetime.strptime('{{ds}}', "%Y-%m-%d") - timedelta(days=30)
    #from_date = '{{macros.ds_add(ds, -30)}}'
    from_date = '{{ds}}'

    key_path = '{{macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d")}}'
    key_name = '{0}-{1}'.format(transfer['table'], '{{ds_nodash}}')

    t1 = MsSqlSchemaToPostgresOperator(
        task_id = 'create-{0}_{1}'.format(transfer['schema'], transfer['table']),
        dag = dag,
        source_conn_id = SOURCE_CONN_ID,
        target_conn_id = TARGET_CONN_ID,
        target_schema = 'public',
        target_table = transfer['table'],
        query_string = transfer['query_string'].format(from_date), #Formats the query to include the FROM_DATE if applicable
        query_merge_field = transfer['query_merge_field'],           
    )

    t2 = MsSqlToS3Operator(
        task_id = 'extract-{0}_{1}'.format(transfer['schema'], transfer['table']),
        dag = dag,
        source_conn_id = SOURCE_CONN_ID,
        s3_conn_id = STORAGE_CONN_ID,
        s3_bucket = STORAGE_DIRECTORY,
        s3_key = 'Campaigner/{0}/{1}/{2}'.format(transfer['table'], key_path, key_name),        
        query_string = transfer['query_string'].format(from_date), #Formats the query to include the FROM_DATE if applicable
        query_merge_field = transfer['query_merge_field'],
        #query_lookback_date

    )

    t3 = S3ToPostgresOperator(
        task_id = 'load-{0}_{1}'.format(transfer['schema'], transfer['table']),
        dag = dag,        
        s3_conn_id = STORAGE_CONN_ID,
        s3_bucket = STORAGE_DIRECTORY,
        s3_key = 'Campaigner/{0}/{1}/{2}'.format(transfer['table'], key_path, key_name),
        query_merge_field = transfer['query_merge_field'],
        target_conn_id = TARGET_CONN_ID,            
        target_schema = 'public',
        target_table = transfer['table'],
    )

    t0 >> t1 >> t2 >> t3 >> tx