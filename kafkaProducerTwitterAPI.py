#inportant!!!
####!!!!!need to downgrade tweepy version to 3.10.0 for support twitter API v1!!!!!!####

from tweepy import OAuthHandler
import configuration as c
import tweepy as tw
from kafka import KafkaProducer
import json
from time import sleep
import re
import logHandler
import gsheetsHandler as gs
from ast import literal_eval




#  Twitter API key and API secret
access_token = c.access_token
access_token_secret = c.access_secret
consumer_key = c.consumer_key
consumer_secret = c.consumer_secret

def twitterData(event_data):

    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)



    event_keys = ['created_at', 'id', 'text']

    twitter_events = {k: v for k, v in event_data.items()
                      if k in event_keys}

    twitter_events["text"] = emoji_pattern.sub(r'', twitter_events.pop('text'))  # no emoji

    twitter_events['tweet_created_at'] = twitter_events.pop('created_at')
    twitter_events['tweet_id'] = twitter_events.pop('id')

    hashtags_events = [item["text"] for item in event_data['entities']['hashtags']]
    url_events = [item["url"] for item in event_data['entities']['urls']]

    twitter_events["tweetUrls"] = ', '.join(url_events)
    twitter_events["hashtags"] = ', '.join(hashtags_events)

    user_keys = ['id', 'name', 'created_at', 'location', 'url',
                 'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                 'statuses_count']

    user_events = {k: v for k, v in event_data['user'].items() if k in user_keys}

    user_events['user_account_created_at'] = user_events.pop('created_at')
    user_events['user_id'] = user_events.pop('id')
    if(user_events['location']):
        user_events['location'] = emoji_pattern.sub(r'', user_events.pop('location'))  # no emoji

    twitter_events.update(user_events)
    events = twitter_events
    return events

#handle tweests that return from the API
class handleTweets(tw.StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=c.brokers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def on_data(self, data):
        # data is the full *tweet* json data
        api_events = json.loads(data)
        # print(api_events)

        #send twitter data to kafka topic

        # Gathring relevant values
        event = twitterData(api_events)
        print(event)

        self.producer.send(c.topic, event)
        self.producer.send(c.topicHdfs, event)
        self.producer.flush()

        sleep(10)

    def on_error(self, status_code):
        print(status_code)
        # if status_code == 420:
        logger = logHandler.myLogger("handleTweets", "send tweets to kafka")
        logger.logError('error:kafkaProducerTwitterAPI.handleTweets ' + status_code)

        return False

####connect To twitter API and filter tweets####
def connectToTwitter():
    # authenticate
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    gsObj = gs.connectByName(c.keywords_file_name, c.ws_en_hashtags)
    hashtags_en = gs.getFirstColumn(gsObj, False)

    stream = handleTweets()
    twitter_stream = tw.Stream(auth=api.auth, listener=stream)
    twitter_stream.filter(track=hashtags_en, languages=['en'])


connectToTwitter()

