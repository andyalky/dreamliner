from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

from dreamliner.hooks.S3_hook import S3Hook

from airflow.utils.decorators import apply_defaults

import smart_open
from contextlib import closing #Needed to work around a bug with PostgresHook's copy_expert function

class S3ToPostgresOperator(BaseOperator):

    #Allow for S3 Key Paths to have custom dates
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(
            self,
            s3_conn_id,
            s3_bucket,
            s3_key,
            target_conn_id,         
            target_schema,
            target_table,
            query_merge_field=None,            
            *args, 
            **kwargs):
        super().__init__(*args, **kwargs)

        #Define our connections
        self.storage = S3Hook(aws_conn_id=s3_conn_id)
        self.target_db = PostgresHook(postgres_conn_id=target_conn_id)        

        self.staging_table = '{schema}._staging_{table}'.format(schema=target_schema, table=target_table)
        self.production_table = '{schema}.{table}'.format(schema=target_schema, table=target_table)

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.query_merge_field = query_merge_field

        #Get the session and endpoint url
        self.session = self.storage.get_session()
        self.endpoint_url = self.storage.get_endpoint_url()         

    def execute(self, context):        
        #Create a staging table
        self.create_staging_table()

        #Stream the file from S3 into our staging table    
        self.copy_file_to_staging()

        #Merge the staging table to the production table
        self.merge_staging_to_production()

        #Drop the staging table
        self.drop_staging_table()

        #Vacuum/clean up the production table
        self.vacuum_production_table()


    #If there's an existing staging table, drop it and recreate it.
    def create_staging_table(self):
        query = """
            DROP TABLE IF EXISTS {staging}; 
            CREATE TABLE {staging} (LIKE {production});
        """.format(staging=self.staging_table, production=self.production_table)
        self.target_db.run(query)            

    def copy_file_to_staging(self):
        #Get parameters to pass to the smart_open open function
        transport_params = {
            'session': self.session,
            'resource_kwargs': {
                'endpoint_url': self.endpoint_url,
            }
        }

        storage_uri = 's3://{bucket}/{key}.tsv.gz'.format(bucket=self.s3_bucket, key=self.s3_key)

        #Can't use copy expert directly because of a bug with postgreshook
        with smart_open.open(storage_uri, 'rb', transport_params=transport_params) as infile:
            with closing(self.target_db.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    query = """
                        COPY {staging} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER '\t')
                    """.format(staging=self.staging_table)
                    cur.copy_expert(query, infile)
                    conn.commit()

    def merge_staging_to_production(self):
        if self.query_merge_field is not None:
            query = """
                DELETE FROM {production}
                WHERE {query_merge_field} IN (
                    SELECT {query_merge_field}
                    FROM {staging}
                    GROUP BY 1
                );
                INSERT INTO {production}
                SELECT * FROM {staging};
            """.format(staging=self.staging_table, production=self.production_table, query_merge_field=self.query_merge_field)
            self.target_db.run(query)
        else:
            query = """
                DELETE FROM {production};
                INSERT INTO {production}
                SELECT * FROM {staging};
            """.format(staging=self.staging_table, production=self.production_table)
            self.target_db.run(query)

    def drop_staging_table(self):
        query = "DROP TABLE {staging};".format(staging=self.staging_table)
        self.target_db.run(query)            

    def vacuum_production_table(self):
        #Need to fix issues w/ vacuuming production tables
        pass
        """
        with closing(self.target_db.get_conn()) as conn:

            try:
                conn.set_isolation_level(0)
                conn.autocommit = True
                query = "VACUUM VERBOSE {production};".format(production=self.production_table)
                self.target_db.run(query)
            except Exception as e:
                print('Could not vaccum table! Error details below. Skipping Vacuum')
                print(e)
            finally:
                conn.set_isolation_level(1)
                conn.autocommit = False
        """      