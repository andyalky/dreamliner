from airflow.models import BaseOperator
from airflow.hooks.mssql_hook import MsSqlHook

from dreamliner.hooks.S3_hook import S3Hook

from airflow.utils.decorators import apply_defaults

import smart_open
import csv

class MsSqlToS3Operator(BaseOperator):

    #Allow for query definitions and S3 Key Paths to have custom dates
    template_fields = ('query_string', 's3_key')

    @apply_defaults
    def __init__(self,
                 source_conn_id,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,               
                 query_string,
                 query_merge_field=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        #Define our connections +
        #Create connections to our DB and our storage   
        self.source_conn_id = source_conn_id
        self.s3_conn_id = s3_conn_id
        self.source_hook = self.source_hook = MsSqlHook(self.source_conn_id)
        self.s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        #Get our DB query + merge fields
        self.query_string = query_string
        self.query_merge_field = query_merge_field

        #Get our S3 Bucket and Key
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        #Get the session and endpoint url
        self.session = self.s3_hook.get_session()
        self.endpoint_url = self.s3_hook.get_endpoint_url()        

    def execute(self, context):
        #Query the source for records
        records = self.get_records_from_source()

        #Write the records to our storage
        self.write_file_to_storage(records)

    def get_records_from_source(self):
        #Get the records from our source database
        query = self.query_string
        return self.source_hook.get_records(query)

    def write_file_to_storage(self, records):
        #Get parameters to pass to the smart_open open function
        transport_params = {
            'session': self.session,
            'resource_kwargs': {
                'endpoint_url': self.endpoint_url,
            }
        }

        #Construct the storage URI
        storage_uri = 's3://%s/%s.tsv.gz' % (self.s3_bucket, self.s3_key)

        #Write records to S3
        with smart_open.open(storage_uri, 'w', transport_params=transport_params) as fout:
            file_writer = csv.writer(fout, delimiter='\t', lineterminator='\n')
            file_writer.writerows(records)