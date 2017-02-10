import json
import logging
import sys
import tweepy
from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from kafka.client import KafkaClient

from twitter_creds import *

# http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, filter_terms=None):
        self.logger = logging.getLogger()

        self.filters = dict(async=True)
        if filter_terms:
            self.filters.update(track=filter_terms)
        else:
            self.filters.update(locations=[-180,-90,180,90])

        self.api = self.get_auth_api()
        self.producer = KafkaProducer(bootstrap_servers='kafka:9092',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.run_stream()

    # http://docs.tweepy.org/en/v3.5.0/auth_tutorial.html#auth-tutorial
    def get_auth_api(self):
        auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return tweepy.API(auth)

    def on_data(self, data):
        text = json.loads(data).get(u'text')
        if text:
            self.producer.send('tweets', text)
            self.producer.flush()
            print(text)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

    def run_stream(self):
        # myStreamListener = MyStreamListener()
        self.logger.info('Initializing stream')
        print('Initializing stream: {}'.format(self.filters))
        myStream = tweepy.Stream(auth = self.api.auth, listener=self)
        myStream.filter(**self.filters)

if __name__ == '__main__':
    MyStreamListener(sys.argv[1:])

