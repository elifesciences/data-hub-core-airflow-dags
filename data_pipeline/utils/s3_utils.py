

from io import StringIO
import boto3
import pandas as pd


def write_dataframe_to_s3_bucket(
    df_name: pd.DataFrame,
    bucket: str,
    object_name: str
):
    csv_buffer = StringIO()
    df_name.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, object_name).put(Body=csv_buffer.getvalue())
    csv_buffer.truncate(0)
