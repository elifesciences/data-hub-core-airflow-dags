from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, List, Callable

from data_pipeline.utils.data_store.bq_data_service import (
    create_or_xtend_table_and_load_file
)
from data_pipeline.utils.pipeline_file_io import (
    IntermediateBatchFileProcessing,
    iter_multi_write_jsonl_to_file_process_batch,
    WriteIterRecordsInBQConfig,
    iter_write_jsonl_to_file_process_batch
)


def batch_load_iter_record_to_bq(
        iter_processed_tweets: Iterable,
        gcp_project: str, dataset: str, table: str,
        batch_size: int = 2000,
) -> Iterable[dict]:
    write_file_to_bq = partial(
        create_or_xtend_table_and_load_file,
        gcp_project=gcp_project,
        dataset=dataset,
        table=table, quoted_values_are_strings=True
    )
    batch_file_processor = IntermediateBatchFileProcessing(
        write_file_to_bq, batch_size
    )
    with TemporaryDirectory() as tmp_dir:
        f_path = str(Path(tmp_dir, 'downloaded'))
        iter_written_records = iter_write_jsonl_to_file_process_batch(
            full_temp_file_location=f_path,
            iter_jsonl=iter_processed_tweets,
            batch_processing=batch_file_processor, write_mode='w'
        )
        for written_record in iter_written_records:
            yield written_record


def batch_load_multi_iter_record_to_bq(
        iter_multi_record: Iterable,
        write_to_bq_conf_list: List[WriteIterRecordsInBQConfig],
        gcp_project: str, dataset: str,
        batch_size: int = 2000,
) -> Iterable[dict]:
    with TemporaryDirectory() as tmp_dir:
        for ind, write_to_bq_conf in enumerate(write_to_bq_conf_list):
            write_to_bq_conf_list[ind] = prep_write_to_bq_conf(
                write_to_bq_conf, gcp_project, dataset,
                str(Path(tmp_dir, 'downloaded_' + str(ind))),
                create_or_xtend_table_and_load_file,
                batch_size
            )
        iter_written_records = iter_multi_write_jsonl_to_file_process_batch(
            iter_multi_record,
            write_to_bq_conf_list
        )
        for written_record in iter_written_records:
            yield written_record


# pylint: disable=too-many-arguments
def prep_write_to_bq_conf(
        write_record_to_file_conf: WriteIterRecordsInBQConfig,
        gcp_project: str,
        dataset: str,
        file_path: str,
        bq_table_load_func: Callable,
        batch_size: int
):
    write_file_to_bq = partial(
        bq_table_load_func,
        gcp_project=gcp_project,
        dataset=dataset,
        table=write_record_to_file_conf.bq_table,
        quoted_values_are_strings=True
    )
    batch_file_processor = IntermediateBatchFileProcessing(
        write_file_to_bq, batch_size
    )
    write_record_to_file_conf.batch_processing = batch_file_processor
    write_record_to_file_conf.file_location = file_path
    write_record_to_file_conf.write_mode = 'w'
    return write_record_to_file_conf
