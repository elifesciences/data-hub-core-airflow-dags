from data_pipeline.twitter.scrape_old_tweets import etl_search_term_by_scraping
from data_pipeline.twitter.twitter_config import TwitterDataPipelineConfig

import time
import json
from tweepy import OAuthHandler, Stream, StreamListener, API
import tweepy
from data_pipeline.twitter.data_from_twitter_streaming_api import TransformAndLoadListener
from data_pipeline.utils.data_store.bq_data_service import get_table_schema

from data_pipeline.twitter.data_from_twitter_streaming_api import etl_track_keywords

from data_pipeline.twitter.twitter_config import TwitterDataPipelineConfig
from data_pipeline.twitter.twiter_auth import get_authorized_oauth
# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
#consumer_key="jF7JrEXzL7VdhlxnSbLkCQgon"
#consumer_secret="IhJRbuLSRbz25j5Lii5UfA6iMaLXT8w5U4zSzKUCDFpbqNcYVa"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
#access_token="1251350288-liFvPWSIs6e8ywEsJAaBVOmK2yvVlLIMbODY138"
#access_token_secret='kYzE2f48QLyqJZfhqxcuNKEn6zEKmLzU0M8ZoryKNGtsJ'


# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key = "jF7JrEXzL7VdhlxnSbLkCQgon"
consumer_secret = "IhJRbuLSRbz25j5Lii5UfA6iMaLXT8w5U4zSzKUCDFpbqNcYVa"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token = "1251350288-liFvPWSIs6e8ywEsJAaBVOmK2yvVlLIMbODY138"
access_token_secret = 'kYzE2f48QLyqJZfhqxcuNKEn6zEKmLzU0M8ZoryKNGtsJ'

#a = TwitterDataPipelineConfig()
#etl_search_term_by_scraping('eLife', a, '2020-01-01 10:00:00')

#l = TransformAndLoadListener()
#auth = OAuthHandler(consumer_key, consumer_secret)
#auth.set_access_token(access_token, access_token_secret)
#stream = Stream(auth, l, tweet_mode='extended')
#stream.filter(track=['covid19'], is_async=True)

#print(
#    get_table_schema('elife-data-pipeline', 'me_dev', 'twitter2')
#)

from data_pipeline.twitter.data_from_twitter_rest_api import TwitterRestApi


def extract_user_properties_to_dict(user: dict):
    return {
        resp_key: user.get(resp_key)
        for resp_key in [
            'id', 'name', 'screen_name', 'location', 'description', 'followers_count',
            'friends_count','listed_count', 'favourites_count','statuses_count', 'created_at'
        ]
    }


def twitter_data_etl():
    auth = get_authorized_oauth(
        consumer_key, consumer_secret,
        access_token, access_token_secret
    )
    t =TwitterRestApi(auth)

    for x in t.get_latest_api_restricted_tweets_from_user('michaelowonibi'):

        print("\n\n\n\n")
        #print(type(x), dir(x))
        print(x._json)
    #config = TwitterDataPipelineConfig()

    #etl_track_keywords(auth, config, ['#covid'])
    #get_followers(api)


    print('Do nothing')

#from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
#print(get_yaml_file_as_dict('/home/michael/PycharmProjects/data-hub-core-airflow-dags_twitt/app_aux_files/twitter/twitter_user.schema.json'))
#twitter_data_etl()

