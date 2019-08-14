from airflow.hooks import S3_hook

#Custom S3 Hook
#Includes a method to get the endpoint URL (under the "host" key in the Extra Config section of Airflow connections)
class S3Hook(S3_hook.S3Hook):
    
    def get_endpoint_url(self, region_name=None):
        _, endpoint_url = self._get_credentials(region_name)
        return endpoint_url