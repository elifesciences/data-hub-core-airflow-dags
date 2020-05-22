import os
import logging
import json
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from apiclient import discovery
from google.oauth2 import service_account
from googleapiclient.discovery_cache.base import Cache

from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig
from data_pipeline.utils.data_store.bq_data_service import (
    create_or_extend_table_schema,
    load_file_into_bq
)
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    standardize_field_name
)
from data_pipeline.google_analytics.etl_state import (
    update_state
)


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


LOGGER = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DATE_RANGE = "date_range"


def get_gcp_cred_file_location():
    return os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


# pylint: disable=no-member
def initialize_analytics_reporting():
    g_cred_loc = get_gcp_cred_file_location()
    try:
        credentials = service_account.Credentials.from_service_account_file(
            g_cred_loc, scopes=SCOPES
        )
    except OSError as err:
        LOGGER.error(
            "Google application credentials file not found at  %s : %s",
            g_cred_loc, err,
        )
        raise

    analytics_reporting = discovery.build(
        "analyticsreporting", "v4", credentials=credentials, cache=MemoryCache()
    )

    return analytics_reporting


def get_report(
        analytics_reporting,
        view_id: str,
        date_ranges: list,
        metrics: list,
        dimensions: list,
        page_token: str = None,
        page_size: int = 5000
):
    return analytics_reporting.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'pageToken': page_token,
                    'dateRanges': date_ranges,
                    'metrics': metrics,
                    'dimensions': dimensions,
                    'pageSize': page_size
                }
            ]
        }
    ).execute()


def transform_response_to_bq_compatible_record(response):
    for report in response.get('reports', []):
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])
        metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            bq_json_formatted_record = {}
            dimensions = row.get('dimensions', [])
            date_range_values = row.get('metrics', [])

            for header, dimension in zip(dimension_headers, dimensions):
                bq_json_formatted_record[standardize_field_name(header)] = dimension
            for i, values in enumerate(date_range_values):
                bq_json_formatted_record[DATE_RANGE] = str(i)
                for metricHeader, value in zip(metric_headers, values.get('values')):
                    bq_json_formatted_record[standardize_field_name(metricHeader.get('name'))] = value

            yield bq_json_formatted_record


def etl_google_analytics(
        ga_config: GoogleAnalyticsConfig,
        start_date: datetime,
        end_date: datetime = None,
):
    analytics = initialize_analytics_reporting()
    # response has a limit of 10000 result per query
    # intelligiently downloading the data
    # using filters with high cardinality
    end_date = end_date or datetime.now()
    from_date = start_date.strftime("%Y-%m-%d")
    to_date = end_date.strftime("%Y-%m-%d")
    dimensions = [
        {
            'name':dim_name
        } for dim_name in ga_config.dimensions
    ]
    metrics = [
        {
            'expression': metric_name
        } for metric_name in ga_config.metrics
    ]
    page_token = None
    while True:

        response = get_report(
            analytics_reporting=analytics,
            date_ranges=[{'startDate': from_date, 'endDate': to_date}],
            view_id=ga_config.ga_view_id,
            metrics=metrics,
            dimensions=dimensions,
            page_token=page_token
        )
        reports = response.get('reports', [])
        # api can return multiple reports, however, this implementation consides
        # single report
        page_token = reports[0].get('nextPageToken') if len(reports) == 1 else None

        transformed_response = transform_response_to_bq_compatible_record(
            response
        )
        with NamedTemporaryFile() as temp_file:
            temp_file_name = temp_file.name
            write_result_to_file(
                json_list=transformed_response,
                full_temp_file_location=temp_file_name,
                write_mode="w"
            )
            load_written_data_to_bq(
                ga_config=ga_config,
                file_location=temp_file_name
            )
        #update_state(
        #    start_date,
        #    ga_config.state_s3_bucket_name,
        #    ga_config.state_s3_object_name
        #)
        if not page_token:
            break


def write_result_to_file(
        json_list,
        full_temp_file_location: str,
        write_mode: str = 'a'
):
    with open(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
            write_file.write("\n")


def load_written_data_to_bq(
        ga_config: GoogleAnalyticsConfig,
        file_location: str
):
    if os.path.getsize(file_location) > 0:
        create_or_extend_table_schema(
            ga_config.gcp_project,
            ga_config.dataset,
            ga_config.table,
            file_location
        )

        load_file_into_bq(
            filename=file_location,
            table_name=ga_config.table,
            dataset_name=ga_config.dataset,
            project_name=ga_config.gcp_project
        )
