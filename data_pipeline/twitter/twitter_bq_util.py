from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

from data_pipeline.utils.data_store.bq_data_service import create_or_xtend_table_and_load_file
from data_pipeline.utils.pipeline_file_io import IntermediateBatchFileProcessing, iter_write_jsonl_to_file_process_batch


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
