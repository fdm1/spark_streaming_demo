import json
import logging
import os
import sys
import tweepy
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable
from time import sleep

consumer_token = os.environ['consumer_token']
consumer_secret = os.environ['consumer_secret']
access_token = os.environ['access_token']
access_token_secret = os.environ['access_token_secret']


# http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, tweet_topics=None, kafka=True, run=True):
        self.logger = logging.getLogger(__file__)
        self.logger.setLevel(logging.WARN)

        self.filters = dict(async=True)
        if tweet_topics:
            self.filters.update(track=tweet_topics)
        else:
            # at a minimum, enforce only tweets with geolocation
            self.filters.update(locations=[-180,-90,180,90])

        self.api = self.get_auth_api()
        self.use_kafka=kafka
        if self.use_kafka:
            self.producer = None
            while not self.producer:
                 self.initialize_kafka_producer()
        if run:
            self.run_stream()

    def initialize_kafka_producer(self):
        self.logger.warn('Attempting to initialize Kafka Producer')
        try:
            self.producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_CONNECT'],
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            self.logger.warn('Kafka Producer initialized!')
        except (BrokerNotAvailableError, NoBrokersAvailable):
            self.logger.warn('Kafka server not ready yet')
            sleep(2)
            return False

    # http://docs.tweepy.org/en/v3.5.0/auth_tutorial.html#auth-tutorial
    def get_auth_api(self):
        auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return tweepy.API(auth)

    def on_data(self, data):
        payload = json.loads(data)
        if payload.get('text'):
            text = payload.get('text')
            if payload.get('retweeted_status') and payload['retweeted_status'].get('text'):
                text = payload['retweeted_status']['text']
            if self.use_kafka:
                self.producer.send('tweets', {'text': text})
                self.producer.flush()
            else:
                self.logger.warn(payload)

    def on_error(self, status_code):
        self.logger.warn("TwitterStreamError: {}".format(status_code))
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

    def on_connect(self):
        self.logger.warn('Stream Initialized!')

    def on_limit(self, track):
        self.logger.warn('Stream is being limited: {}'.format(track))

    def on_exception(self, exception):
        self.logger.warn('Stream raised exception: {}'.format(exception))

    def run_stream(self):
        self.logger.warn('Initializing stream')
        stream = tweepy.Stream(auth = self.api.auth, listener=self)
        stream.filter(**self.filters)

if __name__ == '__main__':
    topics = sys.argv[1:]
    if not topics:
        topics = [t.strip() for t in os.environ.get('tweet_topics').split(',')]
    print(topics)
    MyStreamListener(tweet_topics=topics)

