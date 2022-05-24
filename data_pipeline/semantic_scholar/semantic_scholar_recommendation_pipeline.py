import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping, NamedTuple, Optional, Sequence

import requests

from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.web_api import requests_retry_session
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.pipeline_utils import (
    get_response_json_with_provenance_from_api,
    iter_dict_for_bigquery_source_config_with_exclusion
)
from data_pipeline.semantic_scholar.semantic_scholar_pipeline import (
    get_progress_message,
    get_request_params_for_source_config
)
from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarMatrixConfig,
    SemanticScholarSourceConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_recommendation_config import (
    SemanticScholarRecommendationConfig
)


LOGGER = logging.getLogger(__name__)


class ExcludableListItem(NamedTuple):
    doi: str
    is_excluded: bool = False


@dataclass(frozen=True)
class ExcludableListWithMeta:
    list_key: str
    item_list: Sequence[ExcludableListItem]
    list_meta: Mapping[str, Any] = field(default_factory=dict)


def get_order_preserving_doi_list_by_events(
    item_list: Sequence[ExcludableListItem],
    return_exclude_list: bool = False
) -> Sequence[str]:
    doi_list = []
    for item in item_list:
        doi = item.doi
        if item.is_excluded == return_exclude_list:
            if doi not in doi_list:
                doi_list.append(doi)
        else:
            if doi in doi_list:
                doi_list.remove(doi)
    return doi_list


def get_paper_ids_for_dois(doi_list: Iterable[str]) -> Sequence[str]:
    return [f'DOI:{doi}' for doi in doi_list]


def get_list_item_for_dict(list_item_dict: dict) -> ExcludableListItem:
    return ExcludableListItem(
        doi=list_item_dict['doi'],
        is_excluded=list_item_dict['is_excluded']
    )


def get_list_with_meta_for_dict(list_dict: dict) -> ExcludableListWithMeta:
    return ExcludableListWithMeta(
        list_key=list_dict['list_key'],
        list_meta=list_dict['list_meta'],
        item_list=[
            get_list_item_for_dict(list_item_dict)
            for list_item_dict in list_dict['list']
        ]
    )


def iter_list_for_matrix_config(
    matrix_config: SemanticScholarMatrixConfig
) -> Iterable[ExcludableListWithMeta]:
    LOGGER.debug('matrix_config: %r', matrix_config)
    variable_config = matrix_config.variables['list']
    iterable = iter_dict_for_bigquery_source_config_with_exclusion(
        variable_config.include.bigquery,
        key_field_name='list_key',
        exclude_bigquery_source_config=(
            variable_config.exclude.bigquery if variable_config.exclude else None
        )
    )
    return map(get_list_with_meta_for_dict, iterable)


def get_recommendation_response_json_from_api(
    excludable_list_with_meta: ExcludableListWithMeta,
    source_config: SemanticScholarSourceConfig,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None,
    progress_message: Optional[str] = None
) -> dict:
    LOGGER.debug('list_: %r', excludable_list_with_meta)
    url = source_config.api_url
    params = get_request_params_for_source_config(source_config)
    json_data = {
        'positivePaperIds': get_paper_ids_for_dois(
            get_order_preserving_doi_list_by_events(
                excludable_list_with_meta.item_list,
                return_exclude_list=False
            )
        ),
        'negativePaperIds': get_paper_ids_for_dois(
            get_order_preserving_doi_list_by_events(
                excludable_list_with_meta.item_list,
                return_exclude_list=True
            )
        )
    }
    return get_response_json_with_provenance_from_api(
        url,
        params=params,
        method='POST',
        json_data=json_data,
        provenance=provenance,
        session=session,
        raise_on_status=False,
        progress_message=progress_message
    )


def iter_recommendation_data(
    list_iterable: Iterable[str],
    source_config: SemanticScholarSourceConfig,
    provenance: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None
) -> Iterable[dict]:
    for index, list_ in enumerate(list_iterable):
        progress_message = get_progress_message(index, list_iterable)
        yield get_recommendation_response_json_from_api(
            list_,
            source_config=source_config,
            provenance=provenance,
            session=session,
            progress_message=progress_message
        )


def fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
    config: SemanticScholarRecommendationConfig
):
    LOGGER.info('config: %r', config)
    batch_size = config.batch_size
    provenance = {'imported_timestamp': datetime.utcnow().isoformat()}
    list_iterable = iter_list_for_matrix_config(config.matrix)
    with requests_retry_session(
        status_forcelist=(500, 502, 503, 504, 429),
        raise_on_redirect=False,  # avoid raising exception, instead we will save response as is
        raise_on_status=False
    ) as session:
        data_iterable = iter_recommendation_data(
            list_iterable,
            source_config=config.source,
            provenance=provenance,
            session=session
        )
    for batch_data_iterable in iter_batches_iterable(data_iterable, batch_size):
        batch_data_list = list(batch_data_iterable)
        LOGGER.debug('batch_data_list: %r', batch_data_list)
        LOGGER.info('loading batch into bigquery: %d', len(batch_data_list))
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_list
        )
