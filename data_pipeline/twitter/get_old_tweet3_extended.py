import sys
import time
import datetime
import re
import random
import logging
import http.cookiejar
import json
from typing import List
from pyquery import PyQuery
from GetOldTweets3.manager import TweetManager as tm
from GetOldTweets3.manager import TweetCriteria
import urllib.request, urllib.parse, urllib.error
from GetOldTweets3 import models


LOGGER = logging.getLogger(__name__)


class TCriteria(TweetCriteria):
    def set_to_username(self, to_username: List):
        self.to_username = to_username
        return self


class TManager(tm):

    @staticmethod
    def get_iter_twitter_response(tweet_criteria: TCriteria, proxy=None):
        """
        Modify code copied  from
        https://github.com/Mottl/GetOldTweets3/blob/0.0.11/GetOldTweets3/manager/TweetManager.py#L25
        Get tweets that match the tweetCriteria parameter
        A static method.
        Parameters
        ----------
        tweet_criteria : tweetCriteria, an object that specifies a match criteria
        proxy: str, a proxy server to use
        debug: bool, output debug information
        """
        cookieJar = http.cookiejar.CookieJar()
        user_agent = random.choice(TManager.user_agents)

        all_usernames = []
        usernames_per_batch = 20

        if hasattr(tweet_criteria, 'username'):
            if type(tweet_criteria.username) == str or not hasattr(tweet_criteria.username, '__iter__'):
                tweet_criteria.username = [tweet_criteria.username]

            usernames_ = [u.lstrip('@') for u in tweet_criteria.username if u]
            all_usernames = sorted({u.lower() for u in usernames_ if u})
            n_usernames = len(all_usernames)
            n_batches = n_usernames // usernames_per_batch + (n_usernames % usernames_per_batch > 0)
        else:
            n_batches = 1

        for batch in range(n_batches):  # process all_usernames by batches
            refreshCursor = ''
            batch_cnt_results = 0
            if all_usernames:  # a username in the criteria?
                tweet_criteria.username = all_usernames[
                                         batch * usernames_per_batch:batch * usernames_per_batch + usernames_per_batch]
            while True:
                json = TManager.get_json_response(tweet_criteria, refreshCursor, cookieJar, proxy, user_agent)
                if len(json['items_html'].strip()) == 0:
                    break
                refreshCursor = json['min_position']
                scrapedTweets = PyQuery(json['items_html'])
                # Remove incomplete tweets withheld by Twitter Guidelines
                scrapedTweets.remove('div.withheld-tweet')
                tweets = scrapedTweets('div.js-stream-tweet')

                if len(tweets) == 0:
                    break

                for tweetHTML in tweets:
                    tweetPQ = PyQuery(tweetHTML)
                    tweet = models.Tweet()
                    usernames = tweetPQ("span.username.u-dir b").text().split()
                    if not len(usernames):  # fix for issue #13
                        continue

                    tweet.username = usernames[0]
                    tweet.to = usernames[1] if len(usernames) >= 2 else None  # take the first recipient if many
                    rawtext = TManager.textify(tweetPQ("p.js-tweet-text").html(), tweet_criteria.emoji)
                    tweet.text = re.sub(r"\s+", " ", rawtext) \
                        .replace('# ', '#').replace('@ ', '@').replace('$ ', '$')
                    tweet.retweets = int(
                        tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                            "data-tweet-stat-count").replace(",", ""))
                    tweet.favorites = int(
                        tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                            "data-tweet-stat-count").replace(",", ""))
                    tweet.replies = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                    tweet.id = tweetPQ.attr("data-tweet-id")
                    tweet.permalink = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")
                    tweet.author_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

                    dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
                    tweet.date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc)
                    tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec, tz=datetime.timezone.utc) \
                        .strftime("%a %b %d %X +0000 %Y")
                    tweet.hashtags, tweet.mentions = TManager.getHashtagsAndMentions(tweetPQ)

                    geoSpan = tweetPQ('span.Tweet-geo')
                    if len(geoSpan) > 0:
                        tweet.geo = geoSpan.attr('title')
                    else:
                        tweet.geo = ''

                    urls = []
                    for link in tweetPQ("a"):
                        try:
                            urls.append((link.attrib["data-expanded-url"]))
                        except KeyError:
                            pass

                    tweet.urls = ",".join(urls)
                    batch_cnt_results += 1

                    yield tweet

                    if 0 < tweet_criteria.maxTweets <= batch_cnt_results:
                        break

    @staticmethod
    def get_json_response(
            tweet_criteria: TCriteria, refresh_cursor: str, cookie_jar, proxy, useragent=None, debug=False
    ):
        """
        Code adapted from https://github.com/Mottl/GetOldTweets3/blob/0.0.11/GetOldTweets3/manager/TweetManager.py#L274
        Invoke an HTTP query to Twitter.
        Should not be used as an API function. A static method.
        """
        url = "https://twitter.com/i/search/timeline?"

        if not tweet_criteria.topTweets:
            url += "f=tweets&"

        url += ("vertical=news&q=%s&src=typd&%s"
                "&include_available_features=1&include_entities=1&max_position=%s"
                "&reset_error_state=false")

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

            to_usernames_ = [u.lstrip('@') for u in tweet_criteria.to_username if u]
            tweet_criteria.to_username = {u.lower() for u in to_usernames_ if u}

            to_usernames = [' to:' + u for u in sorted(tweet_criteria.to_username)]
            if to_usernames:
                url_get_data += ' OR'.join(to_usernames)

        if hasattr(tweet_criteria, 'within'):
            if hasattr(tweet_criteria, 'near'):
                url_get_data += ' near:"%s" within:%s' % (tweet_criteria.near, tweet_criteria.within)
            elif hasattr(tweet_criteria, 'lat') and hasattr(tweet_criteria, 'lon'):
                url_get_data += ' geocode:%f,%f,%s' % (tweet_criteria.lat, tweet_criteria.lon, tweet_criteria.within)

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
            urlLang = 'l=' + tweet_criteria.lang + '&'
        else:
            urlLang = ''
        url = url % (urllib.parse.quote(url_get_data.strip()), urlLang, urllib.parse.quote(refresh_cursor))
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
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({'http': proxy, 'https': proxy}), urllib.request.HTTPCookieProcessor(cookie_jar))
        else:
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        opener.addheaders = headers
        if debug:
            print(url)
            print('\n'.join(h[0]+': '+h[1] for h in headers))

        try:
            time.sleep(2)
            response = opener.open(url)
            jsonResponse = response.read()
        except Exception:
            try:
                time.sleep(20)
                response = opener.open(url)
                jsonResponse = response.read()
            except Exception as e:
                LOGGER.info("An error occured during an HTTP request: {}".format(str(e)))
                LOGGER.info("Try to open in browser: https://twitter.com/search?q=%s&src=typd" % urllib.parse.quote(
                    url_get_data))
                sys.exit()

        try:
            s_json = jsonResponse.decode()
        except:
            LOGGER.info("Invalid response from Twitter")
            sys.exit()

        try:
            dataJson = json.loads(s_json)
        except:
            LOGGER.info("Error parsing JSON: %s" % s_json)
            sys.exit()

        return dataJson
