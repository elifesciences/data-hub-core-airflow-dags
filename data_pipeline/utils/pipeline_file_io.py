import os
from contextlib import ExitStack, contextmanager
from typing import IO, Union, List
from typing import Iterable, Callable
import json

import yaml


class IntermediateBatchFileProcessing:
    def __init__(
            self,
            intermediate_batch_processor: Callable,
            batch_size: int,
            truncate_file_after_processing: bool = True,
            initial_record_processing_index: int = 0
    ):
        self.intermediate_batch_processor = intermediate_batch_processor
        self.batch_size = batch_size
        self.truncate_file_after_processing = truncate_file_after_processing
        self.current_record_in_batch_index = initial_record_processing_index

    def process_batch(
            self, writer: IO,
            process_file_instantly: bool = False
    ):
        if (
                self.current_record_in_batch_index == self.batch_size
                or process_file_instantly
        ):
            writer.flush()
            self.intermediate_batch_processor(writer.name)
            self.current_record_in_batch_index = 0

            if self.truncate_file_after_processing:
                writer.seek(0)
                writer.truncate()
        else:
            self.current_record_in_batch_index += 1


class WriteIterRecordsInBQConfig:
    def __init__(
            self,
            bq_table: str = None,
            file_location: str = None,
            batch_processing: IntermediateBatchFileProcessing = None,
            write_mode: str = 'a'
    ):
        self.bq_table = bq_table
        self.batch_processing = batch_processing
        self.file_location = file_location
        self.write_mode = write_mode


@contextmanager
def get_multiple_opened_files(
        file_opening_conf_list: List[WriteIterRecordsInBQConfig]
):
    with ExitStack() as stack:
        opened_files = [
            stack.enter_context(
                open(
                    file_opening_conf.file_location,
                    file_opening_conf.write_mode
                )
            )
            for file_opening_conf in file_opening_conf_list
        ]
        yield opened_files


def iter_write_jsonl_to_file_process_batch(
        iter_jsonl,
        full_temp_file_location: str,
        write_mode: str = 'a',
        batch_processing: IntermediateBatchFileProcessing = None,
) -> Iterable[dict]:
    yield from iter_multi_write_jsonl_to_file_process_batch(
        iter_jsonl,
        [
            WriteIterRecordsInBQConfig(
                write_mode=write_mode,
                batch_processing=batch_processing,
                file_location=full_temp_file_location
            )
        ]
    )


def iter_multi_write_jsonl_to_file_process_batch(
        iter_multi_record: Iterable,
        iter_write_to_bq: List[WriteIterRecordsInBQConfig]
) -> Iterable[dict]:

    with get_multiple_opened_files(
            iter_write_to_bq
    ) as file_writer:
        for record in iter_multi_record:
            for ind, iter_write in enumerate(iter_write_to_bq):
                record_element = (
                    record
                    if len(iter_write_to_bq) == 1
                    else record[ind]
                )
                write_file_batch_process(
                    record_element,
                    file_writer[ind],
                    iter_write.batch_processing
                )
            yield record

        for ind, iter_write in enumerate(iter_write_to_bq):
            write_file_batch_process(
                opened_file_writer=file_writer[ind],
                batch_processing=iter_write.batch_processing
            )


def write_file_batch_process(
        record: dict = None,
        opened_file_writer=None,
        batch_processing: IntermediateBatchFileProcessing = None
):
    if record:
        opened_file_writer.write(json.dumps(record))
        opened_file_writer.write("\n")
    if batch_processing:

        opened_file_writer.flush()
        batch_processing.process_batch(opened_file_writer)


def write_jsonl_to_file(
        json_list: Iterable,
        full_temp_file_location: str,
        write_mode: str = 'w',
        batch_processor: IntermediateBatchFileProcessing = None,
):
    with open(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
            write_file.write("\n")
            if batch_processor:
                batch_processor.process_batch(write_file)
        write_file.flush()
        if batch_processor:
            batch_processor.process_batch(
                write_file, process_file_instantly=True
            )


def read_file_content(file_location: str):
    with open(file_location, 'r') as open_file:
        data = open_file.readlines()
    return data


def get_yaml_file_as_dict(file_location: str) -> Union[dict, list]:
    with open(file_location, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)


def get_data_config_from_file_path_in_env_var(
        env_var_name: str, default_value: str = None
):
    conf_file_path = os.getenv(
        env_var_name, default_value
    )
    data_config_dict = get_yaml_file_as_dict(
        conf_file_path
    )
    return data_config_dict
