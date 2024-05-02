import os
import logging
from typing import Iterable, Optional, Sequence
from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import datetime, timedelta

from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig
from data_pipeline.utils.data_store.bq_data_service import (
    create_or_extend_table_schema,
    load_file_into_bq
)
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    standardize_field_name
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string
)
from data_pipeline.google_analytics.etl_state import (
    get_stored_state_date_or_default_start_date,
    update_state
)
from data_pipeline.utils.data_store.google_analytics import (
    DEFAULT_PAGE_SIZE,
    GoogleAnalyticsClient
)
from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file

LOGGER = logging.getLogger(__name__)

GA_DATE_RANGE_KEY = "date_range"


def transform_response_to_bq_compatible_record(
        response
) -> Iterable[dict]:
    LOGGER.info('response: %r', response)
    for report in response.get('reports', []):
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])
        metric_headers = column_header.get('metricHeader', {}).get(
            'metricHeaderEntries', []
        )

        for row in report.get('data', {}).get('rows', []):
            bq_json_formatted_record = {}
            dimensions = row.get('dimensions', [])
            date_range_values = row.get('metrics', [])

            for header, dimension in zip(dimension_headers, dimensions):
                bq_json_formatted_record[
                    standardize_field_name(header)
                ] = dimension
            for i, values in enumerate(date_range_values):
                bq_json_formatted_record[GA_DATE_RANGE_KEY] = str(i)
                for metric_header, value in zip(
                        metric_headers, values.get('values')
                ):
                    bq_json_formatted_record[
                        standardize_field_name(metric_header.get('name'))
                    ] = value

            yield bq_json_formatted_record


# pylint: disable=too-many-arguments
def iter_get_report_pages(
        analytics: GoogleAnalyticsClient,
        metrics: Sequence[dict],
        dimensions: Sequence[dict],
        ga_config: GoogleAnalyticsConfig,
        from_date: str,
        to_date: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE
):
    LOGGER.info('metrics: %r', metrics)
    LOGGER.info('dimensions: %r', dimensions)
    LOGGER.info('from_date: %r', from_date)
    LOGGER.info('to_date: %r', to_date)
    page_token = None
    while True:
        response = analytics.get_report(
            date_ranges=[{'startDate': from_date, 'endDate': to_date}],
            view_id=ga_config.ga_view_id,
            metrics=metrics,
            dimensions=dimensions,
            page_token=page_token,
            page_size=page_size
        )

        reports = response.get('reports', [])
        # api can return multiple reports, however,
        # this implementation considers single report
        page_token = (
            reports[0].get('nextPageToken')
            if len(reports) == 1 else None
        )

        yield response
        if not page_token:
            break


def etl_google_analytics_for_date_range(
        ga_config: GoogleAnalyticsConfig,
        start_date: datetime,
        end_date: datetime
):
    current_timestamp_as_string = get_current_timestamp_as_string()
    analytics = GoogleAnalyticsClient()
    from_date = start_date.strftime("%Y-%m-%d")
    to_date = end_date.strftime("%Y-%m-%d")
    dimensions = [
        {
            'name': dim_name
        } for dim_name in ga_config.dimensions
    ]
    metrics = [
        {
            'expression': metric_name
        } for metric_name in ga_config.metrics
    ]

    for paged_report_response in iter_get_report_pages(
            analytics, metrics, dimensions, ga_config, from_date, to_date
    ):
        process_paged_report_response(
            paged_report_response, ga_config,
            current_timestamp_as_string
        )
    update_state(
        start_date,
        ga_config.state_s3_bucket_name,
        ga_config.state_s3_object_name
    )


def etl_google_analytics(
        ga_config: GoogleAnalyticsConfig,
        externally_selected_start_date: Optional[datetime] = None,
        externally_selected_end_date: Optional[datetime] = None
):
    start_date = (
        externally_selected_start_date
        or get_stored_state_date_or_default_start_date(ga_config)
    )
    end_date = (
        externally_selected_end_date
        or ga_config.end_date
        or (start_date + timedelta(days=0))
    )
    LOGGER.info('start_date: %r', start_date)
    LOGGER.info('end_date: %r', end_date)
    etl_google_analytics_for_date_range(
        ga_config=ga_config,
        start_date=start_date,
        end_date=end_date
    )


def process_paged_report_response(
        paged_report_response: dict,
        ga_config: GoogleAnalyticsConfig,
        current_timestamp_as_string: str
):
    transformed_response_with_provenance = (
        add_provenance(
            transform_response_to_bq_compatible_record(
                paged_report_response
            ),
            ga_config.import_timestamp_field_name,
            current_timestamp_as_string,
            ga_config.record_annotations
        )
    )
    with TemporaryDirectory() as tmp_dir:
        full_temp_file_location = str(
            Path(tmp_dir, "downloaded_jsonl_data")
        )
        write_jsonl_to_file(
            json_list=transformed_response_with_provenance,
            full_temp_file_location=full_temp_file_location,
        )
        load_written_data_to_bq(
            ga_config=ga_config,
            file_location=full_temp_file_location
        )


def add_provenance(
        ga_records: Iterable[dict],
        timestamp_field_name: str,
        current_etl_time: str,
        record_annotation: dict
):
    for record in ga_records:
        provenance = {
            'provenance': {
                timestamp_field_name: current_etl_time,
                'annotation': record_annotation
            }
        }

        yield {
            **record,
            **provenance
        }


def load_written_data_to_bq(
        ga_config: GoogleAnalyticsConfig,
        file_location: str
):
    if os.path.getsize(file_location) > 0:
        create_or_extend_table_schema(
            ga_config.gcp_project,
            ga_config.dataset,
            ga_config.table,
            file_location,
            quoted_values_are_strings=False
        )

        load_file_into_bq(
            filename=file_location,
            table_name=ga_config.table,
            dataset_name=ga_config.dataset,
            project_name=ga_config.gcp_project
        )
