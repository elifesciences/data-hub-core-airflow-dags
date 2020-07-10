import logging
from io import StringIO
from threading import Thread
from queue import Queue
import multiprocessing as mp
import re

import json
from google.cloud.bigquery import Client
from tweepy import Stream, StreamListener

from data_pipeline.twitter.process_tweet_response import extract_tweet_from_twitter_response
from data_pipeline.twitter.twitter_config import (
    TwitterPipelineModuleConstants, TwitterDataPipelineConfig, ETL_TIMESTAMP_FORMAT
)
from data_pipeline.utils.data_store.bq_data_service import get_bq_table
from data_pipeline.utils.data_pipeline_timestamp import get_current_timestamp_as_string
from data_pipeline.utils.record_processing import (
    add_provenance_to_record
)

LOGGER = logging.getLogger(__name__)


def get_queue_and_worker_func(multi_processing: bool = None):
    if not multi_processing:
        queue = Queue
        worker_func = Thread
    else:
        queue = mp.Queue
        worker_func = mp.Process
    return queue, worker_func


class TransformAndLoadListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """

    def __init__(
            self, streaming_config: TwitterDataPipelineConfig,
            multi_processing: bool = None, num_workers: int = None):
        super().__init__()
        self.client = Client()
        self.streaming_config = streaming_config
        self.bq_table = get_bq_table(
            streaming_config.gcp_project,
            streaming_config.dataset,
            streaming_config.tweet_table
        )
        queue, worker_func = get_queue_and_worker_func(multi_processing)
        self.queue = queue()
        num_workers = num_workers or 1
        self._spin_worker(num_workers, worker_func)
        self.filter_regex_pattern = re.compile(
            '|'.join(streaming_config.search_terms).lower())
        self.tweet_counter = 0

    def _spin_worker(self, num_workers, worker_func):
        for _ in range(num_workers):
            worker = worker_func(target=self.process_queue)
            worker.daemon = True
            worker.start()

    def on_data(self, data):
        self.tweet_counter += 1
        if self.tweet_counter % 50 == 0:
            LOGGER.info("Matched %d so far", self.tweet_counter)
        self.queue.put(data)

    def process_queue(self):
        while True:
            self.process_data(self.queue.get())
            self.queue.task_done()

    def process_data(self, data):
        tweets_in_reponse = []
        current_timestamp_as_provenance = get_current_timestamp_as_string(
            ETL_TIMESTAMP_FORMAT
        )

        record = json.load(StringIO(data))
        while True:
            extracted_tweet = extract_tweet_from_twitter_response(record)
            if self.filter_regex_pattern.search(extracted_tweet.get(
                    TwitterPipelineModuleConstants.TWEET_TEXT, ''
            ).lower()):
                tweets_in_reponse.append(
                    add_provenance_to_record(
                        extracted_tweet, current_timestamp_as_provenance,
                        self.streaming_config.timestamp_field_name,
                        self.streaming_config.provenance_field_name,
                        annotation_field_name='annotation',
                        record_annotation={'keywords': self.streaming_config.search_terms}
                    )
                )
            record = record.get('retweeted_status') or record.get('quoted_status')
            if not record:
                break
        if len(tweets_in_reponse) > 0:
            self.client.insert_rows_json(
                table=self.bq_table, json_rows=tweets_in_reponse
            )

    def on_error(self, status):
        LOGGER.debug(status)


def etl_track_keywords(
        authorized_oauth, streaming_config: TwitterDataPipelineConfig,
):
    tl_listener = TransformAndLoadListener(streaming_config, num_workers=10)
    tweepy_streaming_api = Stream(authorized_oauth, tl_listener, )
    tweepy_streaming_api.filter(
        track=streaming_config.search_terms,
        is_async=False
    )
