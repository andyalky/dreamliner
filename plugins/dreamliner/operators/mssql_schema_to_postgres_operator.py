from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker

class MsSqlSchemaToPostgresOperator(BaseOperator):

    #Allow for query definitions to have custom dates
    #template_fields = ('query_string', )

    @apply_defaults
    def __init__(self,
                 source_conn_id,
                 target_conn_id,
                 target_schema,
                 target_table,
                 query_string,
                 query_merge_field=None,                     
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        #Create connections to our source and target db
        self.source_db = MsSqlHook(source_conn_id)
        self.target_db = PostgresHook(target_conn_id)

        #If there are additional parameters in the query, then take care of them
        self.query = query_string 

        #Get our target db schema and table
        self.target_schema = target_schema
        self.target_table = target_table 


    def execute(self, context):
        if self.table_exists():
            return "Table Already Exists"
        else:
            #Get the query that defines the source table schema
            source_table_definition = self.get_source_definition(self.query)

            target_table_definition = self.map_query_to_definition(source_table_definition)

            self.create_target_table(self.target_schema, self.target_table, target_table_definition)

            return "Table Created"

    def table_exists(self):
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{0}' and table_name = '{1}'
        """.format(self.target_schema, self.target_table)

        num_rows_in_table = self.target_db.get_records(query)[0][0]

        if num_rows_in_table >= 1:
            return True
        else:
            return False

    def get_source_definition(self, query):

        source_table_schema_query = """
            IF OBJECT_ID('tempdb..#tmp_schema') IS NOT NULL DROP TABLE #tmp_schema;

            SELECT *
            INTO #tmp_schema
            FROM ({0}) Subquery
            WHERE 1 = 0;

            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM tempdb.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME LIKE '#tmp_schema%';
        """.format(query)

        return self.source_db.get_records(source_table_schema_query)

    def map_query_to_definition(self, definition):
        data_type_mapping = {
            'smallint': 'INTEGER',
            'varchar': 'VARCHAR',
            'text': 'VARCHAR',
            'int': 'INTEGER',
            'float': 'FLOAT',
            'money': 'FLOAT',
            'datetime': 'TIMESTAMP',
            'bit': 'BOOLEAN',
            'char': 'VARCHAR',
            'tinyint': 'INTEGER',
            'smalldatetime': 'TIMESTAMP',
            'real': 'FLOAT',
            'nvarchar': 'VARCHAR',
            'uniqueidentifier': 'UUID',
            'bigint': 'BIGINT',
        }

        #Lambda function to take the max character length and properly format it if it exists
        character_maximum_length_mapping = lambda x: '' if x in ('', None, 'NULL', -1) else '({0})'.format(x)

        is_nullable_mapping = {
            'YES': '',
            'NO': 'NOT NULL',
        }        

        return ',\n'.join('{0} {1}{2} {3}'.format(x[0].lower(), data_type_mapping[x[1]], character_maximum_length_mapping(x[2]), is_nullable_mapping[x[3]]) for x in definition)

    def create_target_table(self, target_schema, target_table, target_definition):
        create_table_query = "CREATE TABLE {0}.{1} ( {2} );".format(target_schema, target_table, target_definition)
        self.target_db.run(create_table_query)