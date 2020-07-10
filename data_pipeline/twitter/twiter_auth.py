from tweepy import OAuthHandler
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


def get_authorized_oauth(consumer_key: str, consumer_secret: str,
            access_token: str, access_token_secret: str):
    authorized_oauth = OAuthHandler(consumer_key, consumer_secret)
    authorized_oauth.set_access_token(access_token, access_token_secret)
    return authorized_oauth


def get_authorized_from_file(file_path):
    auth_dict = get_yaml_file_as_dict(file_path)
    return get_authorized_oauth(
        auth_dict.get('consumer_key'),
        auth_dict.get('consumer_secret'),
        auth_dict.get('access_token'),
        auth_dict.get('access_token_secret')
    )
