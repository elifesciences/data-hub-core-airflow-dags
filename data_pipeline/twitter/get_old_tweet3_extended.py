import sys
import time
import datetime
import re
import random
import logging
import http.cookiejar
import json
from typing import List
from urllib import request, parse
from pyquery import PyQuery
from GetOldTweets3.manager import TweetManager as tm
from GetOldTweets3.manager import TweetCriteria
from GetOldTweets3 import models


LOGGER = logging.getLogger(__name__)


class TCriteria(TweetCriteria):
    def set_to_username(self, to_username: List):
        # pylint: disable=attribute-defined-outside-init
        self.to_username = to_username
        return self


class TManager(tm):
    # pylint: disable=too-many-statements,too-many-branches,too-many-locals
    @staticmethod
    def get_iter_twitter_response(tweet_criteria: TCriteria, proxy=None):
        """
        Modify code copied  from
        https://github.com/Mottl/GetOldTweets3/blob/0.0.11/GetOldTweets3/manager/TweetManager.py#L25
        Get tweets that match the tweetCriteria parameter
        A static method.
        Parameters
        ----------
        tweet_criteria : tweetCriteria, specifies a match criteria
        proxy: str, a proxy server to use
        debug: bool, output debug information
        """
        cookie_jar = http.cookiejar.CookieJar()
        user_agent = random.choice(TManager.user_agents)

        all_usernames = []
        usernames_per_batch = 20

        if hasattr(tweet_criteria, 'username'):
            if (
                    isinstance(tweet_criteria.username, str)
                    or not hasattr(tweet_criteria.username, '__iter__')
            ):
                tweet_criteria.username = [tweet_criteria.username]

            usernames_ = [u.lstrip('@') for u in tweet_criteria.username if u]
            all_usernames = sorted({u.lower() for u in usernames_ if u})
            n_usernames = len(all_usernames)
            n_batches = (
                n_usernames // usernames_per_batch
                + (n_usernames % usernames_per_batch > 0)
            )
        else:
            n_batches = 1

        for batch in range(n_batches):  # process all_usernames by batches
            refresh_cursor = ''
            batch_cnt_results = 0
            if all_usernames:  # a username in the criteria?
                tweet_criteria.username = (
                    all_usernames[
                        batch * usernames_per_batch:
                        batch * usernames_per_batch + usernames_per_batch
                    ]
                )
            while True:
                json_response_data = (
                    TManager.get_json_response(
                        tweet_criteria, refresh_cursor,
                        cookie_jar, proxy, user_agent
                    )
                )
                if len(json_response_data['items_html'].strip()) == 0:
                    break
                refresh_cursor = json_response_data['min_position']
                scraped_tweets = PyQuery(json_response_data['items_html'])
                # Remove incomplete tweets withheld by Twitter Guidelines
                scraped_tweets.remove('div.withheld-tweet')
                tweets = scraped_tweets('div.js-stream-tweet')

                if len(tweets) == 0:
                    break

                for tweet_html in tweets:
                    tweet_pq = PyQuery(tweet_html)
                    tweet = models.Tweet()
                    usernames = tweet_pq(
                        "span.username.u-dir b"
                    ).text().split()
                    if len(usernames) == 0:  # fix for issue #13
                        continue

                    tweet.username = usernames[0]
                    # take the first recipient if many
                    tweet.to = usernames[1] if len(usernames) >= 2 else None
                    rawtext = TManager.textify(
                        tweet_pq("p.js-tweet-text").html(),
                        tweet_criteria.emoji
                    )
                    tweet.text = (
                        re.sub(
                            r"\s+", " ", rawtext
                        ).replace(
                            '# ', '#'
                        ).replace('@ ', '@').replace('$ ', '$')
                    )
                    tweet.retweets = int(
                        tweet_pq(
                            "span.ProfileTweet-action--retweet "
                            + "span.ProfileTweet-actionCount"
                        ).attr("data-tweet-stat-count").replace(",", ""))
                    tweet.favorites = int(
                        tweet_pq(
                            "span.ProfileTweet-action--favorite "
                            + "span.ProfileTweet-actionCount"
                        ).attr("data-tweet-stat-count").replace(",", ""))
                    tweet.replies = int(
                        tweet_pq(
                            "span.ProfileTweet-action--reply "
                            + "span.ProfileTweet-actionCount"
                        ).attr("data-tweet-stat-count").replace(",", ""))
                    tweet.id = tweet_pq.attr("data-tweet-id")
                    tweet.permalink = (
                        'https://twitter.com'
                        + tweet_pq.attr("data-permalink-path")
                    )
                    tweet.author_id = int(
                        tweet_pq("a.js-user-profile-link").attr("data-user-id")
                    )

                    date_sec = int(
                        tweet_pq(
                            "small.time span.js-short-timestamp"
                        ).attr("data-time")
                    )
                    tweet.date = datetime.datetime.fromtimestamp(
                        date_sec, tz=datetime.timezone.utc
                    )
                    tweet.formatted_date = datetime.datetime.fromtimestamp(
                        date_sec, tz=datetime.timezone.utc
                    ).strftime("%a %b %d %X +0000 %Y")
                    tweet.hashtags, tweet.mentions = (
                        TManager.getHashtagsAndMentions(tweet_pq)
                    )

                    geo_span = tweet_pq('span.Tweet-geo')
                    if len(geo_span) > 0:
                        tweet.geo = geo_span.attr('title')
                    else:
                        tweet.geo = ''

                    urls = []
                    for link in tweet_pq("a"):
                        try:
                            urls.append((link.attrib["data-expanded-url"]))
                        except KeyError:
                            pass

                    tweet.urls = ",".join(urls)
                    batch_cnt_results += 1

                    yield tweet

                    if 0 < tweet_criteria.maxTweets <= batch_cnt_results:
                        break

    # pylint: disable=too-many-statements,too-many-branches,
    # pylint: disable=too-many-locals,too-many-arguments
    @staticmethod
    def get_json_response(
            tweet_criteria: TCriteria, refresh_cursor: str, cookie_jar,
            proxy, useragent=None, debug=False
    ):
        """
        Code adapted from
        https://github.com/Mottl/GetOldTweets3/blob/0.0.11/GetOldTweets3/manager/TweetManager.py#L274
        Invoke an HTTP query to Twitter.
        Should not be used as an API function. A static method.
        """
        url = "https://twitter.com/i/search/timeline?"

        if not tweet_criteria.topTweets:
            url += "f=tweets&"

        url += (
            "vertical=news&q=%s&src=typd&%s"
            "&include_available_features=1&include_entities=1&max_position=%s"
            "&reset_error_state=false"
        )

        url_get_data = ''

        if hasattr(tweet_criteria, 'querySearch'):
            url_get_data += tweet_criteria.querySearch

        if hasattr(tweet_criteria, 'excludeWords'):
            url_get_data += ' -'.join([''] + tweet_criteria.excludeWords)

        if hasattr(tweet_criteria, 'username'):
            if not hasattr(tweet_criteria.username, '__iter__'):
                tweet_criteria.username = [tweet_criteria.username]

            usernames_ = [u.lstrip('@') for u in tweet_criteria.username if u]
            tweet_criteria.username = {u.lower() for u in usernames_ if u}

            usernames = [' from:' + u for u in sorted(tweet_criteria.username)]
            if usernames:
                url_get_data += ' OR'.join(usernames)

        if hasattr(tweet_criteria, 'to_username'):
            if not hasattr(tweet_criteria.to_username, '__iter__'):
                tweet_criteria.to_username = [tweet_criteria.to_user_name]

            to_usernames_ = [
                u.lstrip('@')
                for u in tweet_criteria.to_username
                if u
            ]
            tweet_criteria.to_username = {
                u.lower() for u in to_usernames_ if u
            }

            to_usernames = [
                ' to:' + u
                for u in sorted(tweet_criteria.to_username)
            ]
            if to_usernames:
                url_get_data += ' OR'.join(to_usernames)

        if hasattr(tweet_criteria, 'within'):
            if hasattr(tweet_criteria, 'near'):
                url_get_data += ' near:"%s" within:%s' % (
                    tweet_criteria.near, tweet_criteria.within
                )
            elif (
                    hasattr(tweet_criteria, 'lat')
                    and hasattr(tweet_criteria, 'lon')
            ):
                url_get_data += ' geocode:%f,%f,%s' % (
                    tweet_criteria.lat, tweet_criteria.lon,
                    tweet_criteria.within
                )

        if hasattr(tweet_criteria, 'since'):
            url_get_data += ' since:' + tweet_criteria.since

        if hasattr(tweet_criteria, 'until'):
            url_get_data += ' until:' + tweet_criteria.until

        if hasattr(tweet_criteria, 'minReplies'):
            url_get_data += ' min_replies:' + tweet_criteria.minReplies

        if hasattr(tweet_criteria, 'minFaves'):
            url_get_data += ' min_faves:' + tweet_criteria.minFaves

        if hasattr(tweet_criteria, 'minRetweets'):
            url_get_data += ' min_retweets:' + tweet_criteria.minRetweets

        if hasattr(tweet_criteria, 'lang'):
            url_lang = 'l=' + tweet_criteria.lang + '&'
        else:
            url_lang = ''
        url = url % (
            parse.quote(url_get_data.strip()),
            url_lang, parse.quote(refresh_cursor)
        )
        useragent = useragent or TManager.user_agents[0]

        headers = [
            ('Host', "twitter.com"),
            ('User-Agent', useragent),
            ('Accept', "application/json, text/javascript, */*; q=0.01"),
            ('Accept-Language', "en-US,en;q=0.5"),
            ('X-Requested-With', "XMLHttpRequest"),
            ('Referer', url),
            ('Connection', "keep-alive")
        ]

        if proxy:
            opener = request.build_opener(
                request.ProxyHandler({'http': proxy, 'https': proxy}),
                request.HTTPCookieProcessor(cookie_jar)
            )
        else:
            opener = request.build_opener(
                request.HTTPCookieProcessor(cookie_jar)
            )
        opener.addheaders = headers
        if debug:
            LOGGER.debug(url)
            message = '\n'.join(h[0]+': '+h[1] for h in headers)
            LOGGER.debug(message)
        # pylint: disable=broad-except
        try:
            time.sleep(2)
            response = opener.open(url)
            json_response = response.read()
        except Exception:
            try:
                time.sleep(20)
                response = opener.open(url)
                json_response = response.read()
            except Exception as exception:
                LOGGER.info(
                    "An error occured during an HTTP request: %s",
                    str(exception)
                )
                LOGGER.info(
                    "Try to open in browser: "
                    "https://twitter.com/search?q=%s&src=typd",
                    parse.quote(url_get_data))
                sys.exit()

        try:
            s_json = json_response.decode()
        except Exception:
            LOGGER.info("Invalid response from Twitter")
            sys.exit()

        try:
            data_json = json.loads(s_json)
        except Exception:
            LOGGER.info("Error parsing JSON: %s", s_json)
            sys.exit()

        return data_json
