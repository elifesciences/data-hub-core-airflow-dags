import logging
from typing import Iterable, Optional, Sequence, Tuple
from datetime import datetime, timedelta

from data_pipeline.google_analytics.ga_config import GoogleAnalyticsConfig
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
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
    GoogleAnalyticsClient
)
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.progress import ProgressMonitor
from data_pipeline.utils.web_api import requests_retry_session

LOGGER = logging.getLogger(__name__)


GA_DATE_RANGE_INDEX_KEY = "date_range_index"


def load_ga_bq_record_iterable_to_bq(
    json_iterable: Iterable[dict],
    ga_config: GoogleAnalyticsConfig
):
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=ga_config.gcp_project,
        dataset_name=ga_config.dataset,
        table_name=ga_config.table,
        json_list=json_iterable
    )


def iter_bq_compatible_record_for_response(
    response: dict
) -> Iterable[dict]:
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
            for date_range_index, values in enumerate(date_range_values):
                bq_json_formatted_record[GA_DATE_RANGE_INDEX_KEY] = date_range_index
                for metric_header, value in zip(
                        metric_headers, values.get('values')
                ):
                    bq_json_formatted_record[
                        standardize_field_name(metric_header.get('name'))
                    ] = value

            yield bq_json_formatted_record


def transform_response_to_bq_compatible_record(
    response: dict
) -> Iterable[dict]:
    if not response:
        raise AssertionError('Response is empty')
    yield {
        'records': list(iter_bq_compatible_record_for_response(response)),
        'raw_response': response
    }


# pylint: disable=too-many-arguments
def iter_get_report_pages(
    analytics: GoogleAnalyticsClient,
    metrics: Sequence[dict],
    dimensions: Sequence[dict],
    ga_config: GoogleAnalyticsConfig,
    from_date: str,
    to_date: Optional[str] = None
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
            page_size=ga_config.page_size
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


def get_provenance_containing_dict(
    timestamp_field_name: str,
    current_etl_time: str,
    response_timestamp_as_string: str,
    ga_config: GoogleAnalyticsConfig,
    start_date_str: str,
    end_date_str: str
) -> dict:
    return {
        'provenance': {
            timestamp_field_name: current_etl_time,
            'response_timestamp': response_timestamp_as_string,
            'view_id': ga_config.ga_view_id,
            'annotation': ga_config.record_annotations,
            'dimension_names': ga_config.dimensions,
            'metrics_names': ga_config.metrics,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'page_size': ga_config.page_size
        }
    }


def add_provenance(
    ga_records: Iterable[dict],
    provenance_containing_dict: dict
) -> Iterable[dict]:
    for record in ga_records:
        yield remove_key_with_null_value({
            **record,
            **provenance_containing_dict
        })


def iter_bq_records_for_paged_report_response(
    paged_report_response: dict,
    ga_config: GoogleAnalyticsConfig,
    current_timestamp_as_string: str,
    response_timestamp_as_string: str,
    start_date_str: str,
    end_date_str: str
) -> Iterable[dict]:
    if ga_config.log_response:
        LOGGER.info('paged_report_response: %r', paged_report_response)
    provenance_containing_dict = get_provenance_containing_dict(
        timestamp_field_name=ga_config.import_timestamp_field_name,
        current_etl_time=current_timestamp_as_string,
        response_timestamp_as_string=response_timestamp_as_string,
        ga_config=ga_config,
        start_date_str=start_date_str,
        end_date_str=end_date_str
    )
    yield from (
        add_provenance(
            transform_response_to_bq_compatible_record(
                paged_report_response
            ),
            provenance_containing_dict=provenance_containing_dict
        )
    )


def get_total_count_for_response(response: dict) -> Optional[int]:
    reports = response.get('reports', [])
    if len(reports) == 1:
        report = reports[0]
        return report.get('data', {}).get('rowCount')
    return None


def get_page_count_for_record_count(
    total_count: int,
    page_size: int
) -> int:
    return (total_count + page_size - 1) // page_size


def iter_bq_records_for_paged_report_response_iterable(
    paged_report_response_iterable: Iterable[dict],
    ga_config: GoogleAnalyticsConfig,
    current_timestamp_as_string: str,
    start_date_str: str,
    end_date_str: str
) -> Iterable[dict]:
    progress_monitor = ProgressMonitor(message_prefix='Processed response page: ')
    for paged_report_response in paged_report_response_iterable:
        response_timestamp_as_string = get_current_timestamp_as_string()
        total_count = get_total_count_for_response(paged_report_response)
        if total_count:
            progress_monitor.set_total(get_page_count_for_record_count(
                total_count=total_count,
                page_size=ga_config.page_size
            ))
        yield from iter_bq_records_for_paged_report_response(
            paged_report_response,
            ga_config=ga_config,
            current_timestamp_as_string=current_timestamp_as_string,
            response_timestamp_as_string=response_timestamp_as_string,
            start_date_str=start_date_str,
            end_date_str=end_date_str
        )
        progress_monitor.increment()
        LOGGER.info('%s', progress_monitor)


def etl_google_analytics_for_date_range(
    ga_config: GoogleAnalyticsConfig,
    start_date: datetime,
    end_date: datetime
):
    with requests_retry_session() as session:
        current_timestamp_as_string = get_current_timestamp_as_string()
        analytics = GoogleAnalyticsClient(
            session=session
        )
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

        paged_report_response_iterable = iter_get_report_pages(
            analytics, metrics, dimensions, ga_config, from_date, to_date
        )
        transformed_response_with_provenance_iterable = (
            iter_bq_records_for_paged_report_response_iterable(
                paged_report_response_iterable,
                ga_config=ga_config,
                current_timestamp_as_string=current_timestamp_as_string,
                start_date_str=from_date,
                end_date_str=to_date
            )
        )
        load_ga_bq_record_iterable_to_bq(
            transformed_response_with_provenance_iterable,
            ga_config=ga_config
        )

        new_state_date = end_date + timedelta(days=1)
        LOGGER.info('Updating state to: %r', new_state_date)
        update_state(
            new_state_date,
            ga_config.state_s3_bucket_name,
            ga_config.state_s3_object_name
        )


def iter_start_end_date_for_batch(
    ga_config: GoogleAnalyticsConfig,
    start_date: datetime,
    end_date: datetime
) -> Iterable[Tuple[datetime, datetime]]:
    if not ga_config.batch_date_interval:
        yield start_date, end_date
        return
    batch_start_date = start_date
    while batch_start_date <= end_date:
        batch_end_date = min(
            end_date,
            batch_start_date + ga_config.batch_date_interval - timedelta(days=1)
        )
        yield batch_start_date, batch_end_date
        batch_start_date = batch_end_date + timedelta(days=1)


def etl_google_analytics_for_date_range_in_batches(
    ga_config: GoogleAnalyticsConfig,
    start_date: datetime,
    end_date: datetime
):
    # Note: end date is inclusive (therefore add one)
    total_days = (end_date - start_date).days + 1
    progress_monitor = ProgressMonitor(message_prefix='Processed batch days: ', total=total_days)
    for batch_start_date, batch_end_date in iter_start_end_date_for_batch(
        ga_config=ga_config,
        start_date=start_date,
        end_date=end_date
    ):
        batch_days = (batch_end_date - batch_start_date).days + 1
        LOGGER.info(
            'Processing batch: %r to %r (%d days)',
            batch_start_date, batch_end_date, batch_days
        )
        etl_google_analytics_for_date_range(
            ga_config=ga_config,
            start_date=batch_start_date,
            end_date=batch_end_date
        )
        progress_monitor.increment(batch_days)
        LOGGER.info('%s', progress_monitor)


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
    if start_date > end_date:
        LOGGER.info('Start date after end date. Nothing to process.')
        return
    etl_google_analytics_for_date_range_in_batches(
        ga_config=ga_config,
        start_date=start_date,
        end_date=end_date
    )
