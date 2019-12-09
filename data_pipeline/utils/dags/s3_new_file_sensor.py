from urllib.parse import urlparse
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class S3NewFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,
                 bucket_key,
                 bucket_name=None,
                 *args,
                 **kwargs):
        super(S3NewFileSensor, self).__init__(*args, **kwargs)
        # Parse
        if bucket_name is None:
            parsed_url = urlparse(bucket_key)
            if parsed_url.netloc == '':
                raise AirflowException('Please provide a bucket_name')
            else:
                bucket_name = parsed_url.netloc
                if parsed_url.path[0] == '/':
                    bucket_key = parsed_url.path[1:]
                else:
                    bucket_key = parsed_url.path
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def poke(self, context):
        self.log.info('Poking for key : s3://%s/%s', self.bucket_name, self.bucket_key)
        if self.wildcard_match:
            return hook.check_for_wildcard_key(self.bucket_key,
                                               self.bucket_name)
        else:
            return hook.check_for_key(self.bucket_key, self.bucket_name)
